-- 事务开始
BEGIN;

-- 全量刷新
TRUNCATE TABLE outbound_ads.center_breakage_rate RESTART IDENTITY ;
TRUNCATE TABLE outbound_ads.route_breakage_rate RESTART IDENTITY;

-- 创建临时表
CREATE TEMP TABLE bk_tt AS (
    SELECT rb.日期, rb.省区名称, rb.中心名称, rb.线路, rt.下车票数, rb.破损量, rb.单件票数, rb.单件占比
    FROM outbound_dws.route_breakage_dws AS rb
    LEFT OUTER JOIN outbound_dwd.route_total_dwd AS rt
    ON rb.日期 = TO_DATE(rt.日期, 'YYYY-MM-DD') AND rb.省区名称 = rt.省区名称 AND rb.中心名称 = rb.中心名称 AND rb.线路 = rt.线路
);

-- 写入线路破损率
INSERT INTO
    outbound_ads.route_breakage_rate("日期", "省区名称", "中心名称", "线路", "下车量", "破损量", "单件票数", "单件占比", "破损率", "环比")

SELECT
     "日期", "省区名称", "中心名称", "线路", "下车量", "破损量", "单件票数", "单件占比", "破损率",
     CASE
         WHEN  LAG("破损率") OVER (PARTITION BY "中心名称", "线路" ORDER BY "日期") = 0 OR
              LAG("破损率") OVER (PARTITION BY "中心名称", "线路" ORDER BY "日期") IS NULL
         THEN NULL
         ELSE ROUND(
                ("破损率" - LAG("破损率") OVER (PARTITION BY "中心名称", "线路" ORDER BY "日期"))::numeric /
                LAG("破损率") OVER (PARTITION BY "中心名称", "线路" ORDER BY "日期") * 100, 2)
         END
     AS "环比"
FROM (
    SELECT
        "日期", "省区名称", "中心名称", "线路", "下车票数" AS "下车量", "破损量", "单件票数", "单件占比",
        ROUND("破损量"::numeric / "下车票数" * 100000, 2) AS "破损率"
    FROM
        bk_tt
    ORDER BY
        "中心名称", "线路", "日期"
     ) table1
ORDER BY "日期" DESC, "破损率" DESC;


-- 写入中心破损率
INSERT INTO
    outbound_ads.center_breakage_rate("日期", "省区名称", "中心名称", "下车量", "破损量", "单件票数", "单件占比", "破损率", "环比")

SELECT
    "日期", "省区名称", "中心名称", "下车量", "破损量", "单件票数", "单件占比", "破损率",
    CASE
         WHEN  LAG("破损率") OVER (PARTITION BY "中心名称" ORDER BY "日期") = 0 OR
              LAG("破损率") OVER (PARTITION BY "中心名称" ORDER BY "日期") IS NULL
         THEN NULL
         ELSE ROUND(
                ("破损率" - LAG("破损率") OVER (PARTITION BY "中心名称" ORDER BY "日期"))::numeric /
                LAG("破损率") OVER (PARTITION BY "中心名称" ORDER BY "日期") * 100, 2)
         END
     AS "环比"
FROM (
    SELECT
        "日期", "省区名称", "中心名称", SUM("下车票数") AS "下车量", SUM("破损量") AS "破损量", SUM("单件票数") AS "单件票数",
        ROUND(SUM("单件票数") :: numeric / SUM("破损量") * 100, 2)   AS "单件占比",
        ROUND(SUM("破损量") ::numeric / SUM("下车票数") * 100000, 2) AS "破损率"
      FROM bk_tt
      GROUP BY "日期", "省区名称", "中心名称"
) table1
ORDER BY "日期" DESC, "破损率" DESC;


-- 提交事务
COMMIT;
-- ROLLBACK ;


-- -- 3.删除top线路视图
--
-- -- 开始事务
-- BEGIN;
--
-- -- 删除视图
-- DROP VIEW outbound_ads.top_route;
--
-- -- 提交事务
-- COMMIT;