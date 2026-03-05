import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

import ToolClass as tc

# ==================== 配置 ====================
project_name: str = "DailyDataSave"


def import_to_ods():
    """导入数据到ODS层"""
    logger.info("【步骤1】开始导入数据到ODS层...")

    dkit: tc.DataKit = tc.DataKit(project_name=project_name)
    path_list: list[Path] = dkit.cvs_operation()
    logger.info(f"扫描到 {len(path_list)} 个数据文件")

    config = tc.ToPostgresConfig()
    config.SCHEMA_NAME = "outbound_ods"

    for p in path_list:
        config.PARQUET_FILE = str(p.resolve())

        if "下车" in p.name:
            config.TABLE_NAME = "route_total_data"
        elif "破损" in p.name:
            config.TABLE_NAME = "route_breakage_data"
        elif "ccr" in p.name:
            config.TABLE_NAME = "ccr_data"
        else:
            logger.warning(f"跳过未知类型文件: {p.name}")
            continue

        logger.info(f"导入 [{p.name}] -> {config.SCHEMA_NAME}.{config.TABLE_NAME}")
        tc.datatosql(config, project_name)

    logger.info("【步骤1完成】ODS层数据导入完成")


def clean_to_dwd():
    """清洗数据到DWD层"""
    logger.info("【步骤2】开始清洗数据到DWD层...")

    url = "postgresql+psycopg2://postgres:20251224@localhost:1224/breakage"
    engine = create_engine(url, pool_size=5, max_overflow=10, echo=False)

    now = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"处理日期: {now}")

    # 2.1 清洗破损数据
    logger.info("【步骤2.1】清洗破损数据...")

    sql1 = text(
        """SELECT "日期", "省区名称", "中心编码", "中心名称", "上报中心名称", "破损程度", "是否装包", "产品类型", "是否换包装"
           FROM outbound_ods.route_breakage_data
           WHERE "日期" = :date
        """
    )
    params = {"date": now}
    bk_df = pd.read_sql(sql1, con=engine, params=params)
    logger.info(f"读取到 {len(bk_df)} 条破损数据")

    for col in ["破损程度", "是否装包", "产品类型", "是否换包装"]:
        bk_df.loc[bk_df[col] == "NaN", col] = "缺失信息"

    bk_df["上报中心名称"] = bk_df["中心名称"] + "-" + bk_df["上报中心名称"]
    bk_df.rename(columns={"上报中心名称": "线路"}, inplace=True)

    bk_df.to_sql(
        "route_breakage_dwd",
        con=engine,
        schema="outbound_dwd",
        if_exists="append",
        index=False,
    )
    logger.info(f"破损数据已导入DWD层: {len(bk_df)} 条")

    # 2.2 清洗全量数据
    logger.info("【步骤2.2】清洗全量数据...")

    sql2 = text(
        """SELECT "日期", "省区名称", "中心编码", "中心名称", "目的中心名称", "下车票数"
           FROM outbound_ods.route_total_data
           WHERE "日期" = :date
        """
    )
    tt_df = pd.read_sql(sql2, con=engine, params=params)
    logger.info(f"读取到 {len(tt_df)} 条全量数据")

    tt_df["目的中心名称"] = tt_df["中心名称"] + "-" + tt_df["目的中心名称"]
    tt_df.rename(columns={"目的中心名称": "线路"}, inplace=True)

    tt_df.to_sql(
        "route_total_dwd",
        con=engine,
        schema="outbound_dwd",
        if_exists="append",
        index=False,
    )
    logger.info(f"全量数据已导入DWD层: {len(tt_df)} 条")


if __name__ == "__main__":
    # 初始化日志
    logconfig = tc.LogConfig(project_name)
    logger: logging.Logger = logconfig.setup_logger()

    logger.info("=" * 50)
    logger.info("【DailyDataSave】数据保存任务开始")
    logger.info("=" * 50)

    # 执行数据导入
    import_to_ods()

    # 执行数据清洗
    clean_to_dwd()

    logger.info("=" * 50)
    logger.info("【DailyDataSave】所有任务执行成功！")
    logger.info("=" * 50)
