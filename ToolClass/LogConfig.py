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
        """为项目初始化配置日志处理器（文件详细记录 + 终端简洁显示）

        Returns:
            logging.Logger: 配置好的日志处理器
        """
        log_file = self.log_config["log_file"]

        try:
            log_file = Path(log_file)
        except (ValueError, TypeError) as e:
            raise type(e)(f"配置文件中的文件路径不符合规范: {e}") from e

        # 确保日志目录存在
        log_file.parent.mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger(self.project_name)
        level = getattr(logging, self.log_config["level"].upper())
        logger.setLevel(level)

        # 防重复配置：如果已有 handlers，直接返回已有实例
        if logger.handlers:
            return logger

        # ========== 1. 文件处理器（详细格式，用于事后分析） ==========
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=self.log_config["max_bytes"],
            backupCount=self.log_config["backup_count"],
            encoding="utf-8",
        )

        # 详细格式：保留完整路径、行号、模块名（适合排错）
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)  # 文件记录所有级别
        logger.addHandler(file_handler)

        # ========== 2. 控制台处理器（极简格式，用于实时查看） ==========
        console_handler = logging.StreamHandler()

        # 极简格式：只看时间和关键信息（适合批处理进度监控）
        console_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%H:%M:%S",  # 只显示时分秒，更清爽
        )
        console_handler.setFormatter(console_formatter)

        # 可选：控制台只显示 INFO 及以上（减少 DEBUG 噪音）
        # 如果需要看 DEBUG，注释掉下一行或改为 DEBUG
        console_handler.setLevel(level)

        logger.addHandler(console_handler)

        return logger
