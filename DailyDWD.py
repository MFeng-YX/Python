import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

url = "postgresql+psycopg2://postgres:20251224@localhost:1224/breakage"
engine = create_engine(url, pool_size=5, max_overflow=10, echo=False)

# 时间约束
now = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# # -------------------1.清洗出破损数据DWD层-------------------
# # 从数据库读取数据
# sql1 = text(
#     """SELECT "日期", "省区名称", "中心编码", "中心名称", "上报中心名称", "破损程度", "是否装包", "产品类型", "是否换包装"
#        FROM outbound_ods.route_breakage_data
#        WHERE "日期" = :date
#     """
# )
# params = {"date": now}
# bk_df = pd.read_sql(sql1, con=engine, params=params)
# # 替换NaN值
# for col in ["破损程度", "是否装包", "产品类型", "是否换包装"]:
#     bk_df.loc[bk_df[col] == "NaN", col] = "缺失信息"
# # 修改为'线路'列
# bk_df["上报中心名称"] = bk_df["中心名称"] + "-" + bk_df["上报中心名称"]
# bk_df.rename(columns={"上报中心名称": "线路"}, inplace=True)
# # 导入至数据库
# bk_df.to_sql(
#     "route_breakage_dwd",
#     con=engine,
#     schema="outbound_dwd",
#     if_exists="append",
#     index=False,
# )

# -------------------2.清洗出全量数据DWD层-------------------
# 从数据库读取数据
sql1 = text(
    """SELECT "日期", "省区名称", "中心编码", "中心名称", "目的中心名称", "下车票数"
       FROM outbound_ods.route_total_data
       WHERE "日期" = :date
    """
)
params = {"date": now}
tt_df = pd.read_sql(sql1, con=engine, params=params)
# 修改为'线路'列
tt_df["目的中心名称"] = tt_df["中心名称"] + "-" + tt_df["目的中心名称"]
tt_df.rename(columns={"目的中心名称": "线路"}, inplace=True)
# 导入至数据库
tt_df.to_sql(
    "route_total_dwd",
    con=engine,
    schema="outbound_dwd",
    if_exists="append",
    index=False,
)
