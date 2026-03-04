import logging
import time
import pandas as pd
import sys

from alive_progress import alive_bar
from pathlib import Path
from tqdm import tqdm


class DataKit:
    """数据转换类"""

    def __init__(
        self, project_name: str = "DataKit", method: str = "dir", silent: bool = False
    ):
        """初始化 DataKit类实例

        Args:
            project_name (str, optional): 项目名称. Defaults to "DataKit".
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

    def _read_data(self, path: Path, dtype: str) -> pd.DataFrame:
        """读取数据（原 __data_read，改为单下划线便于测试）
        测试时可传入 silent=True 避免 alive_bar 输出

        Args:
            path (Path): 待读取文件路径
            dtype (str): 待读取文件格式

        Raises:
            ValueError: 文件类型错误

        Returns:
            pd.DataFrame: 文件数据
        """

        if dtype not in self.dtype:
            raise ValueError(f"不支持的文件类型: {dtype}，支持: {self.dtype}")

        self.logger.info(f"\n--读取 '{path.name}' --")

        # 测试模式：直接读取，不显示进度条
        if self.silent:
            if dtype == "xlsx":
                df = pd.read_excel(path)
            elif dtype == "csv":
                df = pd.read_csv(path)
            elif dtype == "parquet":
                df = pd.read_parquet(path)
            self.logger.info(f"读取完成 | 行数: {df.shape[0]:,}, 列数: {df.shape[1]:,}")
            return df

        # 正常模式：带 alive_bar 动画
        with alive_bar(title=f"读取 '{path.name}'", spinner="waves") as bar:
            start_time: float = time.time()

            if dtype == "xlsx":
                df = pd.read_excel(path)
            elif dtype == "csv":
                df = pd.read_csv(path)
            elif dtype == "parquet":
                df = pd.read_parquet(path)

            bar()  # 更新进度条
            end_time: float = time.time()

        elapsed: float = end_time - start_time
        self.logger.info(
            f"读取完成! 耗时: {elapsed:.2f}秒 | "
            f"行数: {df.shape[0]: ,}, 列数: {df.shape[1]: ,}"
        )
        return df

    def _convert(
        self, df: pd.DataFrame, cvsdtype: str, path: Path, encoding: str = "utf-8"
    ) -> Path | None:
        """转换数据（原 __conversion，改为单下划线）
        返回转换后的路径，如果文件已存在则返回 None

        Args:
            df (pd.DataFrame): 文件数据
            cvsdtype (str): 转换数据格式
            path (Path): 转换后数据输出路径
            encoding (str, optional): 文件编码格式. Defaults to "utf-8".

        Raises:
            ValueError: 转换格式错误

        Returns:
            Path | None: 转换后文件路径
        """

        if cvsdtype not in self.dtype:
            raise ValueError(f"不支持的输出格式: {cvsdtype}")

        conver_path: Path = path.with_suffix(f".{cvsdtype}")

        # 幂等性检查：已存在则跳过
        if conver_path.exists():
            self.logger.info(f"'{path.name}' 已转换为 '{conver_path.name}'，跳过")
            return None

        chunk_size: int = 5_000
        max_row: int = df.shape[0]

        self.logger.info(f"开始写入 '{conver_path.name}'")

        if cvsdtype == "xlsx":
            range_iter = range(0, max_row, chunk_size)
            # 延迟创建 tqdm：只在非 silent 模式下创建，且设置 ncols 防止换行
            iter_obj = (
                range_iter
                if self.silent
                else tqdm(range_iter, desc="写入进度", unit="块", leave=True, ncols=80)
            )

            with pd.ExcelWriter(conver_path, engine="openpyxl") as writer:
                start_row = 0
                for idx, start_idx in enumerate(iter_obj):
                    chunk: pd.DataFrame = df.iloc[start_idx : start_idx + chunk_size]
                    chunk.to_excel(
                        writer, index=False, header=(idx == 0), startrow=start_row
                    )
                    start_row += len(chunk)

        elif cvsdtype == "csv":
            range_iter = range(0, max_row, chunk_size)
            iter_obj = (
                range_iter
                if self.silent
                else tqdm(range_iter, desc="写入进度", unit="块", leave=True, ncols=80)
            )

            with open(conver_path, mode="w", encoding=encoding, newline="") as f:
                for idx, start_idx in enumerate(iter_obj):
                    chunk: pd.DataFrame = df.iloc[start_idx : start_idx + chunk_size]
                    chunk.to_csv(f, index=False, header=(idx == 0))

        elif cvsdtype == "parquet":
            # parquet 写入是原子操作，无法分块显示进度，也不创建 tqdm
            start_time: float = time.time()
            df.to_parquet(conver_path, engine="pyarrow")
            elapsed = time.time() - start_time

            if not self.silent:
                # 用 logger 替代 print，避免与 tqdm 的终端控制冲突（虽然这里没 tqdm，但统一风格）
                self.logger.info(f"{conver_path.name} 写入耗时: {elapsed:.2f}秒")

        self.logger.info(f"'{conver_path.name}' 写入完成")
        return conver_path

    def cvs_process(
        self, path: Path, dtype: str = "xlsx", cvsdtype: str = "parquet"
    ) -> list[Path]:
        """核心处理逻辑

        Args:
            path (Path): 文件路径
            dtype (str, optional): {"xlsx", "csv", "parquet"}.待转换文件格式. Defaults to "xlsx".
            cvsdtype (str, optional): {"xlsx", "csv", "parquet"}.转换后文件格式. Defaults to "parquet".

        Raises:
            ValueError: 原始/转换格式错误

        Returns:
            list[Path]: 文件路径列表
        """

        if dtype not in self.dtype or cvsdtype not in self.dtype:
            raise ValueError(f"格式必须是以下之一: {self.dtype}")

        res_list: list[Path] = []

        if self.method == "dir":
            # 文件夹模式：批量处理
            path_list = [p for p in path.glob(f"*.{dtype}")]
            self.logger.info(f"共发现 {len(path_list)} 个 {dtype} 文件")

            for p in path_list:
                df = self._read_data(p, dtype)
                conver_path = self._convert(df, cvsdtype, p)
                if conver_path:
                    res_list.append(conver_path)

            self.logger.info(f"成功转换: {len(res_list)} 个文件")

        elif self.method == "file":
            # 单文件模式
            df = self._read_data(path, dtype)
            conver_path = self._convert(df, cvsdtype, path)
            if conver_path:
                res_list.append(conver_path)

        return res_list

    def cvs_operation(self) -> list[Path]:
        """交互式入口（保持你的使用习惯）
        内部调用 process，便于测试时绕过 input 直接测 process

        Returns:
            list[Path]: 文件路径列表
        """

        self.logger.info("--数据转换流程--开始")

        # 获取路径
        user_input = (
            input("请输入需要转换的文件/文件夹路径:\t").strip().strip("'").strip('"')
        )
        path = self.path_exists(user_input)

        # 格式选择逻辑（保持你的原逻辑）
        judge = input(
            "是否需要自定义转换格式？\n(当前默认 '待转换格式: xlsx', '转换后格式': parquet):\t"
        )
        count = 1
        dtype = "xlsx"
        cvsdtype = "parquet"

        while judge == "是" and count <= 3:
            dtype = input("请输入'待转换格式'(xlsx | csv | parquet):\t")
            cvsdtype = input("请输入'转换后格式'(xlsx | csv | parquet):\t")

            if dtype not in self.dtype or cvsdtype not in self.dtype:
                print("输入的转换格式不符合要求, 请重新填写")
                count += 1
                continue
            break

        if judge == "是" and count > 3:
            print("输入错误次数过多，使用默认格式 xlsx -> parquet")

        # 调用核心逻辑
        return self.cvs_process(path, dtype=dtype, cvsdtype=cvsdtype)

    def path_reading(self, dir_path: Path, dtype: str = "xlsx") -> list[Path]:
        """读取文件路径

        Args:
            dir_path (Path): 待读取文件夹路径
            method (str, optional): {"csv", "xlsx", "parquet"}读取文件格式. Defaults to "xlsx".

        Returns:
            list[Path]: _description_
        """

        path_list: list[Path] = list()

        if dtype == "csv":
            for p in dir_path.glob("*.csv"):
                path_list.append(p)

        if dtype == "xlsx":
            for p in dir_path.glob("*.xlsx"):
                path_list.append(p)

        if dtype == "parquet":
            for p in dir_path.glob("*.parquet"):
                path_list.append(p)

        self.logger.info(f"读取到{len(path_list)}个文件.")

        return path_list

    def rp_operation(self) -> list[Path]:
        """该类的主运行方法

        Returns:
            list[Path]: 文件路径
        """

        self.logger.info("\n----PathReading流程-开始----")

        dir_path: Path = self.path_exists(
            input("请输入需读取的文件夹路径:\t").strip().strip("'").strip('"')
        )
        dtype: str = input('请输入需读取的文件格式({"csv", "xlsx", "parquet"}):\t')

        if dtype not in ["csv", "xlsx", "parquet"]:
            self.logger.error("文件类型输入错误")
            sys.exit()

        path_list: list[Path] = self.path_reading(dir_path, dtype=dtype)

        self.logger.info("\n----PathReading流程-结束----")

        return path_list
