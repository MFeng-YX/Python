import logging

from pathlib import Path


class PathReading:
    """读取需要的文件路径"""

    def __init__(self, project_name: str = "PathReading", method: str = "csv"):
        """初始化 PathReading 类实例

        Args:
            project_name (str, optional): 项目名称. Defaults to "PathReading".
            method (str, optional): 读取的文件类型, 参数值有: "excel", "csv". Defaults to "csv".
        """

        self.logger: logging.Logger = logging.getLogger(f"{project_name}.{__name__}")
        self.method: str | None = self.__verify_params(method)

    def __verify_params(self, method: str) -> str | None:
        """检验类的初始化参数是否正确"""

        method_list: list[str] = ["csv", "excel"]
        if method not in method_list:
            self.logger.error(
                f"PathReading类的method参数没有{method}值, method参数值有: 'csv', 'excel'."
            )
            raise ValueError()
        else:
            return method

    def path_exists(self, file_path: str) -> Path:
        """判断输入的文件路径是否符合规范或存在

        Args:
            path (str): 文件夹路径

        Returns:
            Path: 路径的Path对象
        """

        try:
            path: Path = Path(file_path)
        except (TypeError, ValueError) as e:
            self.logger.error(e, f"输入的路径: {file_path} 不符合规范, 请重新输入")
            raise e

        if path.is_file():
            self.logger.error(f"输入的路径: {path} 不是文件夹路径, 请重新输入")
            raise ValueError()

        return path

    def path_reading(self, dir_path: Path) -> list[Path]:
        """

        Args:
            dir_path (Path): 待读取的文件夹路径

        Returns:
            list[Path]: 文件路径
        """

        path_list: list[Path] = list()

        if self.method == "csv":
            for p in dir_path.rglob("*.csv"):
                path_list.append(p)

        if self.method == "excel":
            suffix: list[str] = ["xlsx", "xls"]
            for s in suffix:
                for p in dir_path.rglob(f"*.{s}"):
                    path_list.append(p)

        self.logger.info(f"读取到{len(path_list)}个文件.")

        return path_list

    def operation(self) -> list[Path]:
        """该类的主运行方法

        Returns:
            list[Path]: 文件路径
        """

        self.logger.info("\n----PathReading模块-开始----")

        dir_path: Path = self.path_exists(
            input("请输入需读取的文件夹路径:\t").strip().strip("'").strip('"')
        )

        path_list: list[Path] = self.path_reading(dir_path)

        self.logger.info("\n----PathReading模块-结束----")

        return path_list
