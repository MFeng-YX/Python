import logging
from typing import Any  # 必须导入

from logging.handlers import RotatingFileHandler
from pathlib import Path


class LogConfig:
    """日志初始化配置日志器"""

    def __init__(self, project_name: str | None = None):
        """初始化 LogConfig 类实例

        Args:
            projiect_name (str, optional): 项目名称. Defaults to None.
        """

        self.project_name: str | None = project_name

        self.log_config: dict[str, Any] = {
            "log_file": f"./logs/{project_name}.log",
            "level": "INFO",
            "max_bytes": 512 * 1024,  # 0.5MB
            "backup_count": 10,
        }

    def setup_logger(self) -> logging.Logger:
        """为项目初始化配置日志处理器

        Returns:
            logging.Logger: 日志处理器
        """

        log_file = self.log_config["log_file"]

        try:
            log_file = Path(log_file)
        except (ValueError, TypeError) as e:
            raise type(e)(f"配置文件中的的文件路径不符合规范: {e}") from e

        log_file.parent.mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger(f"{self.project_name}")
        level = getattr(logging, self.log_config["level"].upper())
        logger.setLevel(level)

        # 如果已经配置过，直接返回
        if logger.handlers:
            return logger

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=self.log_config["max_bytes"],
            backupCount=self.log_config["backup_count"],
            encoding="utf-8",
        )

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 重要：不要关闭传播（默认就是 True）
        # logger.propagate = True  # 这是默认值

        return logger
