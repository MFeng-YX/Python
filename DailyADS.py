import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

url = "postgresql+psycopg2://postgres:20251224@localhost:1224/breakage"
engine = create_engine(url, pool_size=5, max_overflow=10, echo=False)


def load_data(table_name: str) -> pd.DataFrame:
    """从数据库中拉取数据

    Args:
        table_name (str): 表名称.

    Reurns:
        pd.DataFrame: 读取到的数据.
    """

    # 拉取数据的SQL
    sql1 = text(
        """ SELECT *
        FROM :table
    """
    )
    # SQL中注入的参数
    params: dict[str, str] = {"table": table_name}
    # 拉取数据
    load_df: pd.DataFrame = pd.read_sql(sql1, params=params, con=engine)

    return load_df


def route(load_df: pd.DataFrame):

    # 选取需要的列数据
    col_need: list[str] = [
        "日期",
        "省区名称",
        "中心名称",
        "线路",
        "下车量",
        "破损量",
        "破损率",
    ]
    primary_df = load_df.loc[:, col_need]
    # 对破损量进行透视
    breakage_pivot = (
        primary_df.pivot(
            index=["省区名称", "中心名称", "线路"], columns="日期", values="破损量"
        )
        .assign(月破损量=lambda x: x.sum(axis=1))
        .reset_index()
    )
    # 对下车量进行透视
    total_pivot = primary_df.pivot(
        index=["省区名称", "中心名称", "线路"], columns="日期", values="下车量"
    ).assign(月下车量=lambda x: x.sum(axis=1))
    # 连接两个透视表
