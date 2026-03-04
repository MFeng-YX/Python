import logging
from pathlib import Path

import ToolClass as tc

# 初始化日志器配置
project_name: str = "DailyDataSave"
logconfig = tc.LogConfig(project_name)
logger = logconfig.setup_logger()

# 转换数据
dkit: tc.DataKit = tc.DataKit(project_name=project_name)
cvs_list: list[Path] = dkit.cvs_operation()

# 读取数据
# rp_list = dkit.rp_operation()

# 将数据导入至数据库
config = tc.ToPostgresConfig()
config.SCHEMA_NAME = "outbound"
for p in cvs_list:
    config.PARQUET_FILE = str(p.resolve())

    if "下车" in p.name:
        config.TABLE_NAME = "route_total_data"
    if "破损" in p.name:
        config.TABLE_NAME = "route_breakage_data"
    if "ccr" in p.name:
        config.TABLE_NAME = "ccr_data"

    tc.datatosql(config, project_name)
