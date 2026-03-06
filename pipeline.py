import logging
import sys
from dataclasses import dataclass
from pathlib import Path


from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError

# 导入你已封装好的业务逻辑
from pipeline.DailyDataSave import DailyDataSave
from pipeline.DailyADS import DailyADS
from ToolClass.LogConfig import LogConfig


# ==================== 配置类 ====================


@dataclass
class PipelineStage:
    """阶段开关配置（默认为全部执行）"""

    ods_to_dwd: bool = True  # Stage 1: 导入原始数据并清洗
    dwd_to_dws: bool = True  # Stage 2: 轻度聚合，构建事实表
    dws_to_ads: bool = True  # Stage 3: 主题宽表，计算破损率
    ads_to_report: bool = True  # Stage 4: 透视表生成与导出


# ==================== 日志配置 ====================


def setup_logging() -> logging.Logger:
    """配置管道日志"""
    logger = logging.getLogger("DataPipeline")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


# ==================== 流程编排类 ====================


class DataPipeline:
    """数据仓库 ETL 流程编排器

    整合 ODS->DWD->DWS->ADS->报表 全流程，支持分阶段执行。
    """

    def __init__(
        self,
        db_url: str,
        sql_dir: str | None = None,
        stage_config: PipelineStage | None = None,
        logger: logging.Logger | None = None,
        project_name: str = "pipeline",
    ):
        """
        Args:
            db_url: 数据库连接 URL
            sql_dir: SQL 脚本存放目录
            stage_config: 阶段开关配置（默认全部开启）
            logger: 日志记录器实例
            project_name: 项目名称
        """
        self.engine: Engine = create_engine(
            db_url, pool_size=5, max_overflow=10, echo=False
        )
        self.logger = logger or setup_logging()
        self.sql_dir = Path(sql_dir) if sql_dir else Path(__file__).parent
        self.stage = stage_config or PipelineStage()  # 默认全量执行
        self.project_name = project_name

        self._validate_stage_config()

    def _validate_stage_config(self) -> None:
        """验证阶段配置的合理性"""
        # 如果跳过了 DWD→DWS，却又要执行 DWS→ADS，给出警告
        if not self.stage.dwd_to_dws and self.stage.dws_to_ads:
            self.logger.warning(
                "⚠️ 配置警告：跳过了 DWD→DWS 阶段，"
                "但启用了 DWS→ADS 阶段（可能导致 ADS 数据过期）"
            )
        # 如果跳过了 DWS→ADS，却又要执行 ADS→报表，给出警告
        if not self.stage.dws_to_ads and self.stage.ads_to_report:
            self.logger.warning(
                "⚠️ 配置警告：跳过了 DWS→ADS 阶段，"
                "但启用了 ADS→报表阶段（报表基于旧 ADS 数据）"
            )

    def set_stage(self, **kwargs) -> "DataPipeline":
        """链式设置阶段开关（方便调试时快速调整）

        Examples:
            >>> pipeline.set_stage(ods_to_dwd=False, ads_to_report=True)
        """
        for key, value in kwargs.items():
            if hasattr(self.stage, key):
                setattr(self.stage, key, value)
            else:
                raise ValueError(f"无效的阶段名称: {key}")

        # 重新验证
        self._validate_stage_config()
        return self  # 支持链式调用

    def execute_sql_file(self, filename: str) -> None:
        """执行 SQL 文件（保持原有实现）"""
        sql_path = self.sql_dir / filename
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL 文件不存在: {sql_path}")

        self.logger.info(f"开始执行 SQL 阶段: {filename}")
        sql_content: str = sql_path.read_text(encoding="utf-8")

        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql_content))
                self.logger.info(f"✅ SQL 阶段完成: {filename}")
        except SQLAlchemyError as e:
            self.logger.error(f"❌ SQL 阶段失败 [{filename}]: {e}")
            raise

    def run_python_stage(self, stage_func, stage_name: str) -> None:
        """执行 Python 处理阶段（保持原有实现）"""
        self.logger.info(f"开始 Python 阶段: {stage_name}")
        try:
            stage_func()
            self.logger.info(f"✅ Python 阶段完成: {stage_name}")
        except Exception as e:
            self.logger.error(f"❌ Python 阶段失败 [{stage_name}]: {e}")
            raise

    def run(self) -> None:
        """根据阶段配置执行 ETL 流程"""
        self.logger.info("🚀 启动数据管道")
        self.logger.info(
            f"阶段配置: ODS→DWD[{self.stage.ods_to_dwd}] "
            f"DWD→DWS[{self.stage.dwd_to_dws}] "
            f"DWS→ADS[{self.stage.dws_to_ads}] "
            f"ADS→报表[{self.stage.ads_to_report}]"
        )

        executed_stages = 0

        try:
            # Stage 1: ODS -> DWD (Python)
            if self.stage.ods_to_dwd:
                datasave = DailyDataSave(
                    self.engine, days=1, project_name=self.project_name
                )
                self.run_python_stage(datasave.operation, "ODS→DWD 数据导入与清洗")
                executed_stages += 1
            else:
                self.logger.info("⏭️ 跳过阶段: ODS→DWD")

            # Stage 2: DWD -> DWS (SQL)
            if self.stage.dwd_to_dws:
                self.execute_sql_file("DWS_Daily.sql")
                executed_stages += 1
            else:
                self.logger.info("⏭️ 跳过阶段: DWD→DWS")

            # Stage 3: DWS -> ADS (SQL)
            if self.stage.dws_to_ads:
                self.execute_sql_file("ADS_Daily.sql")
                executed_stages += 1
            else:
                self.logger.info("⏭️ 跳过阶段: DWS→ADS")

            # Stage 4: ADS -> 报表 (Python)
            if self.stage.ads_to_report:
                ads = DailyADS(self.engine)
                self.run_python_stage(ads.operation, "ADS→报表 透视分析")
                executed_stages += 1
            else:
                self.logger.info("⏭️ 跳过阶段: ADS→报表")

            self.logger.info(f"🎉 流程结束，共执行 {executed_stages} 个阶段")

        except Exception as e:
            self.logger.critical(f"💥 流程终止，已执行 {executed_stages} 个阶段: {e}")
            raise


# ==================== 入口函数（示例） ====================


def main() -> None:
    """Pipeline 入口示例"""
    DB_URL = "postgresql+psycopg2://postgres:20251224@localhost:1224/breakage"
    project_name = "pipeline"
    log = LogConfig(project_name=project_name)
    logger: logging.Logger = log.setup_logger()

    # 方式1: 默认执行全部阶段
    pipeline = DataPipeline(db_url=DB_URL, sql_dir="./pipeline", logger=logger)

    # 方式2: 初始化时指定阶段（示例：只跑报表，跳过前置处理）
    # stage_config = PipelineStage(
    #     ods_to_dwd=False,
    #     dwd_to_dws=False,
    #     dws_to_ads=False,
    #     ads_to_report=True
    # )
    # pipeline = DataPipeline(db_url=DB_URL, sql_dir="./pipeline", stage_config=stage_config)

    # 方式3: 链式设置（适合调试时快速调整）
    # pipeline.set_stage(ods_to_dwd=True, dwd_to_dws=True) \
    #         .set_stage(dws_to_ads=False)  # 可以连续调用

    # 执行
    pipeline.run()


if __name__ == "__main__":
    main()
