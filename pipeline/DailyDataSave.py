import pandas as pd
from pathlib import Path
from sqlalchemy import Engine, text
from datetime import datetime, timedelta

import ToolClass as tc


class DailyDataSave:

    def __init__(
        self, engine: Engine, days: int = 1, project_name: str = "DailyDataSave"
    ):
        """初始化 DailyDataSave 类

        Args:
            engine (Engine) SQLAlchemy 数据库引擎，由外部传入.
            days (int): 传输的日期差. Defualts to 1.
            project_name (str): 项目名称. Defaults to "DailyDataSave".
        """
        self.engine = engine
        self.days = days
        self.project_name = project_name

    def get_yesterday(self) -> str:
        """获取昨天的日期字符串

        Returns:
            str: 格式为 YYYY-MM-DD 的日期字符串
        """
        return (datetime.now() - timedelta(days=self.days)).strftime("%Y-%m-%d")

    def import_to_ods(self) -> None:
        """导入数据到 ODS 层"""
        dkit: tc.DataKit = tc.DataKit(project_name=self.project_name)
        path_list: list[Path] = dkit.cvs_operation()

        config = tc.ToPostgresConfig()
        config.SCHEMA_NAME = "outbound_ods"

        for p in path_list:
            config.PARQUET_FILE = str(p.resolve())

            if "下车" in p.name:
                config.TABLE_NAME = "route_total_data"
            elif "破损" in p.name:
                config.TABLE_NAME = "route_breakage_data"
            elif "ccr" in p.name:
                config.TABLE_NAME = "ccr_data"
            else:
                continue

            tc.datatosql(config, self.project_name)

    def clean_breakage_data(self, process_date: str) -> pd.DataFrame:
        """清洗破损数据

        Args:
            process_date (str): 要处理的日期，格式 YYYY-MM-DD

        Returns:
            pd.DataFrame: 清洗后的破损数据
        """
        sql = text(
            """SELECT "日期", "省区名称", "中心编码", "中心名称", "上报中心名称", "破损程度", "是否装包", "产品类型", "是否换包装"
               FROM outbound_ods.route_breakage_data
               WHERE "日期" = :date
            """
        )
        params: dict[str, str] = {"date": process_date}
        bk_df: pd.DataFrame = pd.read_sql(sql, con=self.engine, params=params)

        # 处理缺失值
        for col in ["破损程度", "是否装包", "产品类型", "是否换包装"]:
            bk_df.loc[bk_df[col] == "NaN", col] = "缺失信息"

        # 构建线路字段（处理缺失值）
        bk_df["上报中心名称"] = (
            bk_df["中心名称"].fillna("") + "-" + bk_df["上报中心名称"].fillna("")
        )
        bk_df.rename(columns={"上报中心名称": "线路"}, inplace=True)

        return bk_df

    def clean_total_data(self, process_date: str) -> pd.DataFrame:
        """清洗全量数据

        Args:
            process_date (str): 要处理的日期，格式 YYYY-MM-DD

        Returns:
            pd.DataFrame: 清洗后的全量数据
        """
        sql = text(
            """SELECT "日期", "省区名称", "中心编码", "中心名称", "目的中心名称", "下车票数"
               FROM outbound_ods.route_total_data
               WHERE "日期" = :date
            """
        )
        params: dict[str, str] = {"date": process_date}
        tt_df: pd.DataFrame = pd.read_sql(sql, con=self.engine, params=params)

        # 构建线路字段（处理缺失值）
        tt_df["目的中心名称"] = (
            tt_df["中心名称"].fillna("") + "-" + tt_df["目的中心名称"].fillna("")
        )
        tt_df.rename(columns={"目的中心名称": "线路"}, inplace=True)

        return tt_df

    def save_to_dwd(self, df: pd.DataFrame, table_name: str, process_date: str) -> None:
        """保存数据到 DWD 层（先删除该日期数据，避免重复）

        Args:
            df (pd.DataFrame): 要保存的数据
            table_name (str): 目标表名
            process_date (str): 处理日期，用于删除该日期的旧数据
        """
        # 先删除该日期的旧数据，避免重复
        delete_sql = text(
            f"""DELETE FROM outbound_dwd.{table_name} WHERE "日期" = :date"""
        )
        with self.engine.begin() as conn:
            conn.execute(delete_sql, {"date": process_date})

        df.to_sql(
            table_name,
            con=self.engine,
            schema="outbound_dwd",
            if_exists="append",
            index=False,
        )

    def clean_to_dwd(self) -> None:
        """清洗数据到 DWD 层"""
        process_date: str = self.get_yesterday()

        # 清洗并保存破损数据
        bk_df = self.clean_breakage_data(process_date)
        self.save_to_dwd(bk_df, "route_breakage_dwd", process_date)

        # 清洗并保存全量数据
        tt_df = self.clean_total_data(process_date)
        self.save_to_dwd(tt_df, "route_total_dwd", process_date)

    def operation(self) -> None:
        """数据保存任务的主运行方法"""
        # 导入数据到 ODS 层
        self.import_to_ods()

        # 清洗数据到 DWD 层
        self.clean_to_dwd()
