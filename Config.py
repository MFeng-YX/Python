from dataclasses import dataclass, field


@dataclass
class ToPostgresConfig:

    DB_CONFIG: dict[str, str] = field(
        default_factory=lambda: {
            "dbname": "breakage",
            "user": "postgres",
            "password": "20251224",
            "host": "localhost",
            "port": "1224",
        }
    )

    TABLE_NAME: str = field(default="route_breakage_data")

    SCHEMA_NAME: str = field(default="outbound")

    PARQUET_FILE: str = field(
        default=r"D:\Download\Microsoft Edge\异常件管理➣破损件上报-责任-明细_20260303163256.parquet"
    )

    BATCH_SIZE: int = field(default=50000)
