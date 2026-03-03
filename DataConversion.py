import logging
import time
import pandas as pd

from alive_progress import alive_bar
from pathlib import Path
from tqdm import tqdm


class DataCvs:
    """数据转换类"""

    def __init__(self, project_name: str = "DataCvs", method: str = "dir"):
        """初始化 DataCvs 类实例

        Args:
            project_name (str, optional): 项目名称. Defaults to "DataCvs".
            method (str, optional): {"dir", "file"}.该类的转换模式. Defaults to "dir".
        """

        self.logger: logging.Logger = logging.getLogger(f"{project_name}.{__name__}")
        self.method: str = self.__verify_params(method)
        self.dtype: list[str] = ["xlsx", "csv", "parquet"]

    def __verify_params(self, method: str) -> str:
        """检验类的初始化参数是否正确"""

        method_list: list[str] = ["dir", "file"]
        if method not in method_list:
            self.logger.error(
                f"DataCvs类的method参数没有{method}值, method参数值有: 'dir', 'file'."
            )
            raise ValueError()
        else:
            return method

    def path_exists(self, file_path: str) -> Path:
        """判断输入的文件路径是否符合规范或存在

        Args:
            path (str): 文件/文件夹路径

        Returns:
            Path: 路径的Path对象
        """

        try:
            path: Path = Path(file_path)
        except TypeError as T:
            self.logger.error(f"{T}.输入的路径: {file_path} 不是文字路径, 请重新输入")
            raise T
        except ValueError as V:
            self.logger.error(f"{V}.输入的路径: {file_path} 不是规范的路径, 请重新输入")
            raise V

        if self.method == "dir":
            if path.is_file():
                self.logger.error(f"输入的路径: {path} 不是文件夹路径, 请重新输入")
                raise ValueError()

        elif self.method == "file":
            if path.is_dir():
                self.logger.error(f"输入的路径: {path} 不是文件路径, 请重新输入")
                raise ValueError()

        return path

    def __data_read(self, path: Path, dtype: str) -> pd.DataFrame:
        """读取数据文件

        Args:
            path (Path): 文件路径
            dtype (str): 文件类型

        Returns:
            pd.DataFrame: 读取的数据表
        """
        if dtype not in self.dtype:
            self.logger.error(
                "输入的文件类型不符合要求, 请输入: xlsx, csv, parquet 中的一种"
            )
            raise ValueError()

        self.logger.info(f"\n--读取 '{path.name}' --")
        with alive_bar(title=f"读取 '{path.name}' ", spinner="waves") as bar:
            start_time: float = time.time()

            # 读取 excel 文件
            if dtype == "xlsx":
                df: pd.DataFrame = pd.read_excel(path)
            # 读取 csv 文件
            elif dtype == "csv":
                df: pd.DataFrame = pd.read_csv(path)
            # 读取 parquet 文件
            elif dtype == "parquet":
                df: pd.DataFrame = pd.read_parquet(path)

            bar()  # 更新进度条

            end_time: float = time.time()
        # 读取过程耗时
        elapsed: float = end_time - start_time
        self.logger.info(
            f"读取完成! 耗时: {elapsed:.2f}秒 |"
            f"行数: {df.shape[0]: ,} , 列数: {df.shape[1]: ,}"
        )

        return df

    def __conversion(
        self, df: pd.DataFrame, cvsdtype: str, path: Path, encoding: str = "utf-8"
    ) -> Path | None:
        """转换数据

        Args:
            df (pd.DataFrame): 读取到的数据
            cvsdtype (str): 需要转换成为的文件类型
            path (Path): 读取的文件路径
            encoding (str): 文件编码格式. Defaults to "utf-8"

        Returns:
            Path | None: 转换后的文件路径 | None
        """

        if cvsdtype not in self.dtype:
            self.logger.error(
                "输入的文件类型不符合要求, 请输入: xlsx, csv, parquet 中的一种"
            )
            raise ValueError()

        conver_path: Path = path.with_suffix(f".{cvsdtype}")

        # 判断是否已经转换
        if conver_path.exists():
            self.logger.info(f"'{path.name}' 已经转换为 '{conver_path.name}', 跳过")
            return None

        # 设置参数
        chunk_size: int = 5_000  # 数据块大小
        max_row: int = df.shape[0]  # 最大行数

        self.logger.info(f"开始写入 '{conver_path.name}.'")
        # 转换并输出:
        if cvsdtype == "xlsx":
            with pd.ExcelWriter(conver_path, engine="openpyxl") as writer:
                start_row = 0
                for idx, i in enumerate(
                    tqdm(range(0, max_row, chunk_size), desc="写入进度", unit="块")
                ):
                    chunk: pd.DataFrame = df.iloc[i : i + chunk_size]
                    # header 只在第一个 chunk 写入，startrow 控制写入位置避免覆盖
                    chunk.to_excel(
                        writer, index=False, header=(idx == 0), startrow=start_row
                    )
                    start_row += len(chunk)

        elif cvsdtype == "csv":
            with open(conver_path, mode="w", encoding=encoding, newline="") as f:
                for idx, i in enumerate(
                    tqdm(range(0, max_row, chunk_size), desc="写入进度", unit="块")
                ):
                    chunk: pd.DataFrame = df.iloc[i : i + chunk_size]
                    # 只有第一个 chunk 写入 header，避免表头重复
                    chunk.to_csv(f, index=False, header=(idx == 0))

        elif cvsdtype == "parquet":
            with open(conver_path, mode="wb") as f:
                start_time: float = time.time()  # 开始时间

                df.to_parquet(conver_path, engine="pyarrow")

                end_time: float = time.time()  # 结束时间
                # 打印写入耗时
                print(f"{conver_path.name}写入耗时: {end_time-start_time}.")

        self.logger.info(f"'{conver_path.name}' 写入完成.")

        return conver_path

    def __process(
        self, path: Path, dtype: str = "xlsx", cvsdtype: str = "parquet"
    ) -> list[Path]:
        """转换流程

        Args:
            path (Path): 读取的文件路径
            dtype (str , optional): {"xlsx", "csv", "parquet"}.待转换文件格式. Defaults to "xlsx".
            cvsdtype (str , optional): {"xlsx", "csv", "parquet"}.转换后文件格式. Defaults to "parquet".
        Returns:
            list[Path]: 成功转换的数据路径列表
        """

        if dtype not in self.dtype:
            self.logger.error(
                "输入的文件类型不符合要求, 请输入: xlsx, csv, parquet 中的一种"
            )
            raise ValueError()
        # 返回的数据字典
        res_list: list[Path] = list()

        # dir-文件夹模式
        if self.method == "dir":
            # 遍历指定类型的文件
            path_list = [p for p in path.rglob(f"*.{dtype}")]
            self.logger.info(f"一共读取到: {len(path_list)} 个文件.")
            # 读取文件数据
            for p in path_list:
                df: pd.DataFrame = self.__data_read(p, dtype)
                conver_path: Path | None = self.__conversion(df, cvsdtype, p)
                if conver_path:
                    res_list.append(conver_path)

            self.logger.info(f"成功转换: {len(res_list)} 个文件.")

        # file-文件模式
        elif self.method == "file":
            df: pd.DataFrame = self.__data_read(path, dtype)
            conver_path: Path | None = self.__conversion(df, cvsdtype, path)
            if conver_path:
                res_list.append(conver_path)

        return res_list

    def operation(self) -> list[Path]:
        """该类的主运行方法

        Returns:
            list[Path]: 转换后数据路径列表
        """

        self.logger.info("--数据转换流程--开始")
        # 检验输入路径
        path: Path = self.path_exists(
            input("请输入需要转换的文件/文件夹路径:\t").strip().strip("'").strip('"')
        )

        # 是否需要自定义转换格式
        judge: str = input(
            "是否需要自定义转换格式？\n"
            "(当前默认 '待转换格式: xlsx', '转换后格式': parquet):\t"
        )
        count: int = 1
        dtype: str = "xlsx"
        cvsdtype: str = "parquet"
        while judge == "是" and count <= 3:
            dtype: str = input("请输入'待转换格式'(xlsx | csv | parquet):\t")
            cvsdtype: str = input("请输入'转换后格式'(xlsx | csv | parquet):\t")

            if dtype not in self.dtype or cvsdtype not in self.dtype:
                print("输入的转换格式不符合要求, 请重新填写")
                count += 1
                continue

            break

        if judge == "是" and count < 4:
            res_list: list[Path] = self.__process(path, dtype=dtype, cvsdtype=cvsdtype)
        else:
            res_list: list[Path] = self.__process(path)

        return res_list
