import logging
import time
import pandas as pd

from alive_progress import alive_bar
from pathlib import Path
from tqdm import tqdm


class DataCvs:
    """数据转换类"""

    def __init__(
        self, project_name: str = "DataCvs", method: str = "dir", silent: bool = False
    ):
        """初始化 DataCvs类实例

        Args:
            project_name (str, optional): 项目名称. Defaults to "DataCvs".
            method (str, optional): {"dir", "file"}.类运行模式. Defaults to "dir".
            silent (bool, optional): 静默模式. Defaults to False.
        """

        self.logger: logging.Logger = logging.getLogger(f"{project_name}.{__name__}")
        self.method: str = self._verify_params(method)
        self.silent: bool = silent
        self.dtype: list[str] = ["csv", "xlsx", "parquet"]

    def _verify_params(self, method: str) -> str:
        """检验模式是否输入正确

        Args:
            method (str): 输入的模式名称

        Returns:
            str: 检验后模式名称
        """

        if method not in ("dir", "file"):
            self.logger.error(f"method参数只能是: 'dir', 'file'. 当前参数值: {method}")
            raise ValueError(f"无效的参数: {method}")
        return method

    def path_exists(self, file_path: str) -> Path:
        """检验路径是否存在且符合规范

        Args:
            file_path (str): 输入的路径

        Returns:
            Path: 验证后路径
        """

        try:
            path: Path = Path(file_path)
        except (ValueError, TypeError) as e:
            self.logger.error(f"路径格式错误: {file_path}")
            raise e

        if self.method == "dir" and path.is_file():
            self.logger.error(f"路径类型错误: {path}")
            raise ValueError(f"当前是文件夹模式(dir)，但提供了文件路径: {path}")
        elif self.method == "file" and path.is_dir():
            self.logger.error(f"路径类型错误: {path}")
            raise ValueError(f"当前是文件模式(file)，但提供了文件夹路径: {path}")

        return path
