from dataclasses import dataclass, field


@dataclass
class ToPostgresConfig:
    PARQUET_FILE: str = field(default="")

    TABLE_NAME: str = field(default="")

    SCHEMA_NAME: str = field(default="")

    DB_CONFIG: dict[str, str] = field(
        default_factory=lambda: {
            "dbname": "breakage",
            "user": "postgres",
            "password": "20251224",
            "host": "localhost",
            "port": "1224",
        }
    )

    BATCH_SIZE: int = field(default=50000)
