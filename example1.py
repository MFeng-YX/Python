import logging
import time
import pandas as pd

from alive_progress import alive_bar
from pathlib import Path
from tqdm import tqdm
from typing import List, Optional


class DataCvs:
    """数据转换类 - 可测试版本"""

    def __init__(
        self,
        project_name: str = "DataCvs",
        method: str = "dir",
        silent: bool = False,  # 新增：测试时设为 True 关闭进度条动画
    ):
        """
        Args:
            project_name: 项目名称，用于日志标识
            method: "dir"(文件夹模式) 或 "file"(单文件模式)
            silent: 静默模式，测试时开启以避免进度条输出
        """
        self.logger: logging.Logger = logging.getLogger(f"{project_name}.{__name__}")
        self.method: str = self._verify_params(method)
        self.dtype: list[str] = ["xlsx", "csv", "parquet"]
        self.silent: bool = silent  # 控制是否显示进度条

    def _verify_params(self, method: str) -> str:
        """验证初始化参数（单下划线：子类和测试可访问，但不建议外部调用）"""
        if method not in ("dir", "file"):
            self.logger.error(f"method 参数只能是 'dir' 或 'file'，当前值: {method}")
            raise ValueError(f"无效的 method 参数: {method}")
        return method

    def path_exists(self, file_path: str) -> Path:
        """
        验证路径（保持公有：用户交互时需要，测试时也需要）
        注意：这里保持你原有的验证逻辑，但确保返回 Path 对象
        """
        try:
            path: Path = Path(file_path)
        except (TypeError, ValueError) as e:
            self.logger.error(f"路径格式错误: {file_path}")
            raise e

        if self.method == "dir" and path.is_file():
            raise ValueError(f"当前是文件夹模式(dir)，但提供了文件路径: {path}")
        if self.method == "file" and path.is_dir():
            raise ValueError(f"当前是文件模式(file)，但提供了文件夹路径: {path}")

        return path

    def _read_data(self, path: Path, dtype: str) -> pd.DataFrame:
        """
        读取数据（原 __data_read，改为单下划线便于测试）
        测试时可传入 silent=True 避免 alive_bar 输出
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
    ) -> Optional[Path]:
        """
        转换数据（原 __conversion，改为单下划线）
        返回转换后的路径，如果文件已存在则返回 None
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

        # 根据 silent 决定是否使用 tqdm
        range_iter = range(0, max_row, chunk_size)
        progress_iter = (
            range_iter if self.silent else tqdm(range_iter, desc="写入进度", unit="块")
        )

        if cvsdtype == "xlsx":
            with pd.ExcelWriter(conver_path, engine="openpyxl") as writer:
                start_row = 0
                for idx, i in enumerate(progress_iter):
                    chunk: pd.DataFrame = df.iloc[i : i + chunk_size]
                    chunk.to_excel(
                        writer, index=False, header=(idx == 0), startrow=start_row
                    )
                    start_row += len(chunk)

        elif cvsdtype == "csv":
            with open(conver_path, mode="w", encoding=encoding, newline="") as f:
                for idx, i in enumerate(progress_iter):
                    chunk: pd.DataFrame = df.iloc[i : i + chunk_size]
                    chunk.to_csv(f, index=False, header=(idx == 0))

        elif cvsdtype == "parquet":
            # parquet 通常很快，不需要分块进度条
            start_time: float = time.time()
            df.to_parquet(conver_path, engine="pyarrow")
            if not self.silent:
                print(f"{conver_path.name} 写入耗时: {time.time() - start_time:.2f}秒")

        self.logger.info(f"'{conver_path.name}' 写入完成")
        return conver_path

    def process(
        self, path: Path, dtype: str = "xlsx", cvsdtype: str = "parquet"
    ) -> List[Path]:
        """
        核心处理流程（原 __process，改为公有方法）

        Args:
            path: 文件或文件夹路径（Path 对象）
            dtype: 源文件格式
            cvsdtype: 目标文件格式

        Returns:
            成功转换的文件路径列表
        """
        if dtype not in self.dtype or cvsdtype not in self.dtype:
            raise ValueError(f"格式必须是以下之一: {self.dtype}")

        res_list: List[Path] = []

        if self.method == "dir":
            # 文件夹模式：批量处理
            path_list = [p for p in path.rglob(f"*.{dtype}")]
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

    def operation(self) -> List[Path]:
        """
        交互式入口（保持你的使用习惯）
        内部调用 process，便于测试时绕过 input 直接测 process
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
        return self.process(path, dtype=dtype, cvsdtype=cvsdtype)


# 保留你的使用方式：命令行直接运行
if __name__ == "__main__":
    # 快速自测示例
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    converter = DataCvs(method="file", silent=True)
    # 这里可以写简单的自测逻辑，正式测试在 test_converter.py 中
    print(
        "工具已加载，请使用 converter.operation() 开始交互，或导入后使用 converter.process()"
    )
