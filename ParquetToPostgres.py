import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text
import io
import os
from typing import Any  # 仅需保留 Any，其余使用内置类型

# ==================== 配置区 ====================
from Config import ToPostgresConfig

# ================================================


class ParquetToPostgres:
    def __init__(
        self, db_config: dict[str, Any]
    ) -> None:  # dict[str, Any] 保留 Any 因为值类型混合
        self.conn_str = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        self.engine = create_engine(self.conn_str, pool_size=5)

    def map_arrow_to_postgres(self, arrow_type: pa.DataType) -> str:
        """精确映射 PyArrow 类型到 PostgreSQL 类型"""
        type_mapping: dict[pa.DataType, str] = {  # 使用内置 dict 泛型
            pa.int8(): "SMALLINT",
            pa.int16(): "SMALLINT",
            pa.int32(): "INTEGER",
            pa.int64(): "BIGINT",
            pa.uint8(): "SMALLINT",
            pa.uint16(): "INTEGER",
            pa.uint32(): "BIGINT",
            pa.uint64(): "NUMERIC(20,0)",
            pa.float32(): "REAL",
            pa.float64(): "DOUBLE PRECISION",
            pa.bool_(): "BOOLEAN",
            pa.string(): "TEXT",
            pa.large_string(): "TEXT",
            pa.binary(): "BYTEA",
            pa.date32(): "DATE",
            pa.date64(): "DATE",
            pa.timestamp("s"): "TIMESTAMP",
            pa.timestamp("ms"): "TIMESTAMP",
            pa.timestamp("us"): "TIMESTAMP",
            pa.timestamp("ns"): "TIMESTAMP",
        }

        if isinstance(arrow_type, pa.TimestampType):
            return "TIMESTAMPTZ" if arrow_type.tz else "TIMESTAMP"

        if isinstance(arrow_type, pa.Decimal128Type):
            return f"NUMERIC({arrow_type.precision}, {arrow_type.scale})"

        if isinstance(arrow_type, (pa.ListType, pa.MapType, pa.StructType)):
            return "JSONB"

        return type_mapping.get(arrow_type, "TEXT")

    def sanitize_column_name(self, col: str) -> str:
        """清洗列名"""
        cleaned = col.strip().replace(" ", "_").replace("-", "_").replace(".", "_")
        if cleaned[0].isdigit():
            cleaned = f"col_{cleaned}"
        return cleaned[:60]  # PostgreSQL 标识符限制

    def generate_ddl_from_parquet(
        self, parquet_path: str, table_name: str, schema: str
    ) -> tuple[str, dict[str, str], list[str]]:  # 元组类型直接声明
        """
        返回: (建表SQL, 列名映射字典, 原始列名列表)
        """
        parquet_file = pq.ParquetFile(parquet_path)
        arrow_schema = parquet_file.schema_arrow

        columns: list[str] = []  # list[str] 而非 List[str]
        col_mapping: dict[str, str] = {}  # dict[str, str] 而非 Dict[str, str]

        for field in arrow_schema:
            original_name = field.name
            clean_name = self.sanitize_column_name(original_name)
            col_mapping[original_name] = clean_name

            pg_type = self.map_arrow_to_postgres(field.type)
            columns.append(f'"{clean_name}" {pg_type}')

        cols_def = ",\n    ".join(columns)
        ddl = (
            f'CREATE TABLE IF NOT EXISTS {schema}."{table_name}" (\n    {cols_def}\n);'
        )

        return ddl, col_mapping, arrow_schema.names

    def import_copy_fast(
        self,
        parquet_path: str,
        table_name: str,
        db_config: dict[str, str],
        schema: str = "public",
        batch_size: int = 50000,
    ) -> None:
        """极速 COPY 模式"""
        print(f"🚀 极速 COPY 模式: {parquet_path}")

        ddl, col_mapping, original_cols = self.generate_ddl_from_parquet(
            parquet_path, table_name, schema
        )

        with self.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name}"))
            conn.execute(text(ddl))
            conn.commit()
        print(f"✅ 已建表，列数: {len(col_mapping)}")

        parquet_file = pq.ParquetFile(parquet_path)
        total_rows = 0
        raw_conn = psycopg2.connect(**db_config)  # type: ignore
        cursor = raw_conn.cursor()

        try:
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                df = batch.to_pandas()
                df = df.rename(columns=col_mapping)

                # 处理嵌套类型转为 JSON
                for col in df.columns:
                    if df[col].dtype == "object":
                        sample = df[col].dropna().iloc[0] if not df[col].empty else None
                        if isinstance(sample, (list, dict)):
                            import json

                            df[col] = df[col].apply(
                                lambda x: (
                                    json.dumps(x, ensure_ascii=False)
                                    if x is not None
                                    else None
                                )
                            )

                df = df.where(pd.notnull(df), None)

                buffer = io.StringIO()
                df.to_csv(
                    buffer,
                    index=False,
                    header=False,
                    sep=",",
                    quoting=1,
                    quotechar='"',
                    escapechar="\\",
                    na_rep="\\N",
                )
                buffer.seek(0)

                cursor.copy_expert(
                    f"COPY {schema}.{table_name} FROM STDIN WITH (FORMAT csv, NULL '\\N')",
                    buffer,
                )

                total_rows += len(df)
                if total_rows % 100000 == 0:
                    print(f"  已导入 {total_rows} 行...")
                    raw_conn.commit()

            raw_conn.commit()
            print(f"✅ 导入完成，总计: {total_rows} 行")

        except Exception as e:
            raw_conn.rollback()
            raise e
        finally:
            cursor.close()
            raw_conn.close()

    def import_batch_with_progress(
        self,
        parquet_path: str,
        table_name: str,
        db_config: dict[str, str],
        schema: str = "public",
        batch_size: int = 50000,
    ) -> None:
        """带进度条的批量导入"""
        from tqdm import tqdm

        ddl, col_mapping, _ = self.generate_ddl_from_parquet(
            parquet_path, table_name, schema
        )

        with self.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name}"))
            conn.execute(text(ddl))
            conn.commit()

        parquet_file = pq.ParquetFile(parquet_path)
        total_rows_meta = parquet_file.metadata.num_rows

        with psycopg2.connect(**db_config) as conn:  # type: ignore
            with conn.cursor() as cur:
                with tqdm(total=total_rows_meta, desc="导入进度") as pbar:
                    for batch in parquet_file.iter_batches(batch_size=batch_size):
                        df = batch.to_pandas()
                        df = df.rename(columns=col_mapping)
                        df = df.where(pd.notnull(df), None)

                        cols = ", ".join([f'"{c}"' for c in df.columns])
                        placeholders = ", ".join(["%s"] * len(df.columns))
                        sql = f"INSERT INTO {schema}.{table_name} ({cols}) VALUES ({placeholders})"

                        data_tuples: list[tuple[Any, ...]] = [  # 复杂元组列表
                            tuple(row) for row in df.to_numpy()
                        ]
                        execute_batch(cur, sql, data_tuples, page_size=1000)

                        pbar.update(len(df))
                conn.commit()
        print("✅ 批量导入完成")

    def import_streaming_low_memory(
        self,
        parquet_path: str,
        table_name: str,
        db_config: dict[str, str],
        schema: str = "public",
        batch_size: int = 50000,
    ) -> None:
        """零拷贝流式模式"""
        print(f"🌊 零拷贝流式模式（极低内存）...")

        ddl, col_mapping, _ = self.generate_ddl_from_parquet(
            parquet_path, table_name, schema
        )

        raw_conn = psycopg2.connect(**db_config)  # type: ignore
        cursor = raw_conn.cursor()

        try:
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
            cursor.execute(ddl)

            parquet_file = pq.ParquetFile(parquet_path)
            total_rows = 0

            for batch in parquet_file.iter_batches(batch_size=batch_size):
                table = pa.Table.from_batches([batch])
                # 使用类型映射避免 int64 转 float
                type_map: dict[pa.DataType, Any] = {pa.int64(): pd.Int64Dtype()}
                df = table.to_pandas(types_mapper=type_map.get)
                df = df.rename(columns=col_mapping)

                buffer = io.StringIO()
                df.to_csv(buffer, index=False, header=False, sep=",", na_rep="\\N")
                buffer.seek(0)

                cursor.copy_expert(
                    f"COPY {schema}.{table_name} FROM STDIN WITH (FORMAT csv, NULL '\\N')",
                    buffer,
                )
                total_rows += len(df)

                del df, table, buffer

            raw_conn.commit()
            print(f"✅ 流式导入完成: {total_rows} 行")

        finally:
            cursor.close()
            raw_conn.close()


# ==================== 使用示例 ====================
def datatosql(config: ToPostgresConfig) -> None:
    """主运行方法

    Args:
        config (ToPostgresConfig): 类的配置参数
    """

    DB_CONFIG = config.DB_CONFIG
    PARQUET_FILE = config.PARQUET_FILE
    TABLE_NAME = config.TABLE_NAME
    SCHEMA_NAME = config.SCHEMA_NAME
    BATCH_SIZE = config.BATCH_SIZE

    importer = ParquetToPostgres(DB_CONFIG)

    file_size = os.path.getsize(PARQUET_FILE)
    print(f"Parquet 文件大小: {file_size/1024/1024:.2f} MB")

    # 使用 match-case (Python 3.10+) 替代 if-elif 更清晰
    match file_size:
        case size if size > 1024 * 1024 * 1024:  # > 1GB
            importer.import_streaming_low_memory(
                PARQUET_FILE, TABLE_NAME, DB_CONFIG, SCHEMA_NAME, BATCH_SIZE
            )
        case size if size > 100 * 1024 * 1024:  # > 100MB
            importer.import_copy_fast(
                PARQUET_FILE, TABLE_NAME, DB_CONFIG, SCHEMA_NAME, BATCH_SIZE
            )
        case _:  # 其他情况
            importer.import_batch_with_progress(
                PARQUET_FILE, TABLE_NAME, DB_CONFIG, SCHEMA_NAME, BATCH_SIZE
            )
