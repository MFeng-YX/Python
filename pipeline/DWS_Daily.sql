-- 1-线路破损

-- 事务开始
BEGIN;

-- 全量刷新
TRUNCATE TABLE outbound_dws.route_breakage_dws RESTART IDENTITY ;

-- 插入数据
INSERT INTO outbound_dws.route_breakage_dws("日期", "省区名称", "中心名称", "线路", "破损量", "单件票数", "单件占比")

SELECT
    TO_DATE("日期", 'YYYY-mm-dd') AS "日期",
    "省区名称", "中心名称", "线路", COUNT(*) AS "破损量",
    SUM(CASE WHEN "是否装包" = '否' THEN 1 ELSE 0 END) AS "单件票数",
    ROUND(SUM(CASE WHEN "是否装包" = '否' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2)::numeric AS "单件占比"
FROM
    outbound_dwd.route_breakage_dwd
WHERE "省区名称" != '其他'
GROUP BY "日期", "省区名称", "中心名称", "线路"
ORDER BY  "日期", "破损量" DESC;

-- 查询行数
SELECT COUNT(*) FROM outbound_dws.route_breakage_dws;

-- 提交事务
COMMIT;


-- 2-中心破损

-- 事务开始
BEGIN;

-- 全量刷新
TRUNCATE TABLE outbound_dws.center_breakage_dws RESTART IDENTITY ;

-- 插入数据
INSERT INTO outbound_dws.center_breakage_dws("日期", "省区名称", "中心名称", "破损量", "单件票数", "单件占比")

SELECT
    TO_DATE("日期", 'YYYY-mm-dd') AS "日期",
    "省区名称", "中心名称", COUNT(*) AS "破损量",
    SUM(CASE WHEN "是否装包" = '否' THEN 1 ELSE 0 END) AS "单件票数",
    ROUND(SUM(CASE WHEN "是否装包" = '否' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2)::numeric AS "单件占比"
FROM
    outbound_dwd.route_breakage_dwd
WHERE "省区名称" != '其他'
GROUP BY "日期", "省区名称", "中心名称"
ORDER BY "日期", "破损量" DESC;

-- 查询行数
SELECT COUNT(*) FROM outbound_dws.center_breakage_dws;

-- 提交事务
COMMIT;


