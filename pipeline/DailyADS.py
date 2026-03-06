import pandas as pd
from sqlalchemy import Engine, text


class DailyADS:
    def __init__(self, engine: Engine):
        """初始化 DailyADS 类

        Args:
            engine: SQLAlchemy 数据库引擎，由外部传入
        """
        self.engine = engine

    def load_data(self, table_name: str) -> pd.DataFrame:
        """从数据库中拉取数据

        Args:
            table_name (str): 表名称.

        Reurns:
            pd.DataFrame: 读取到的数据.
        """

        # 拉取数据的SQL（表名不能参数绑定，使用字符串格式化）
        sql1 = text(f""" SELECT * FROM {table_name} """)
        # 拉取数据
        load_df: pd.DataFrame = pd.read_sql(sql1, con=self.engine)

        return load_df

    def route(self, load_df: pd.DataFrame):
        """线路破损率透视报表

        Args:
            load_df (pd.DataFrame): 拉取的数据
        """

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
        # 聚合月度数据（处理除零）
        month_df = (
            primary_df.groupby("线路")
            .agg(月破损量=("破损量", "sum"), 月下车量=("下车量", "sum"))
            .assign(
                月破损率=lambda x: round(
                    x["月破损量"] / x["月下车量"].replace(0, pd.NA) * 100000, 2
                )
            )
            .reset_index()
        )
        # 对破损率进行透视（使用pivot_table处理重复索引）
        rate_df = primary_df.pivot_table(
            index=["省区名称", "中心名称", "线路"],
            columns="日期",
            values="破损率",
            aggfunc="mean",
        ).reset_index()
        # 连接月度聚合表和破损率表格
        route_df = pd.merge(rate_df, month_df, how="left", on="线路")
        col_order = [
            "省区名称",
            "中心名称",
            "线路",
            "月下车量",
            "月破损量",
            "月破损率",
        ]
        col_order = col_order + sorted(primary_df["日期"].unique())
        route_df = route_df.loc[:, col_order]
        # 导入至数据库
        route_df.to_sql(
            "route_rate_report",
            con=self.engine,
            schema="outbound_ads",
            if_exists="replace",
            index=False,
        )

    def center(self, load_df: pd.DataFrame):
        """中心破损率透视表格

        Args:
            load_df (pd.DataFrame): 拉取的数据
        """

        # 选取需要的列数据
        col_need = ["日期", "省区名称", "中心名称", "下车量", "破损量", "破损率"]
        primary_df = load_df.loc[:, col_need]
        # 聚合月度数据（处理除零）
        month_df = (
            primary_df.groupby("中心名称")
            .agg(月破损量=("破损量", "sum"), 月下车量=("下车量", "sum"))
            .assign(
                月破损率=lambda x: round(
                    x["月破损量"] / x["月下车量"].replace(0, pd.NA) * 100000, 2
                )
            )
            .reset_index()
        )
        # 对破损率进行透视（使用pivot_table处理重复索引）
        center_df = primary_df.pivot_table(
            index=["省区名称", "中心名称"],
            columns="日期",
            values="破损率",
            aggfunc="mean",
        ).reset_index()
        # 连接月度聚合表和破损率表格
        center_df = pd.merge(center_df, month_df, how="left", on="中心名称")
        col_order = ["省区名称", "中心名称", "月下车量", "月破损量", "月破损率"]
        col_order = col_order + sorted(primary_df["日期"].unique())
        center_df = center_df.loc[:, col_order]
        # 导入至数据库
        center_df.to_sql(
            "center_rate_report",
            con=self.engine,
            schema="outbound_ads",
            if_exists="replace",
            index=False,
        )

    def operation(self):
        """制作报表的主运行方法"""

        # 线路报表

        route_data = self.load_data("outbound_ads.route_breakage_rate")
        self.route(route_data)

        # 中心报表
        center_data = self.load_data("outbound_ads.center_breakage_rate")
        self.center(center_data)
