/*
-- SQL file for dwh data monitoring dashboard in metabase
-- Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
-- Updated: 20250624
*/

-- 1. Data Quality Daily Trend
SELECT 
    DATE_TRUNC('day', execution_dt) AS exec_day,
    table_name,
    ROUND(AVG(failed_percentage), 2) AS avg_failed_pct
FROM (
    SELECT * FROM data_monitoring_dwh.fact_checkin_weather_monitoring
    UNION ALL
    SELECT * FROM data_monitoring_dwh.fact_review_weather_monitoring
) t
GROUP BY exec_day, table_name
ORDER BY exec_day;

-- 2. Top Columns with Highest Average Failure Rate
SELECT 
    table_name,
    column_name,
    validation_rule,
    ROUND(AVG(failed_percentage), 2) AS avg_fail_pct,
    MAX(failed_percentage) AS max_fail_pct,
    COUNT(*) AS check_count
FROM (
    SELECT * FROM data_monitoring_dwh.fact_checkin_weather_monitoring
    UNION ALL
    SELECT * FROM data_monitoring_dwh.fact_review_weather_monitoring
) t
GROUP BY table_name, column_name, validation_rule
ORDER BY avg_fail_pct DESC
LIMIT 15;


-- 3. Compare Data Quality Against Volume
WITH volume AS (
    SELECT 'fact_checkin_weather' AS table_name, COUNT(*) AS row_count FROM dwh.fact_checkin_weather
    UNION ALL
    SELECT 'fact_review_weather' AS table_name, COUNT(*) AS row_count FROM dwh.fact_review_weather
),
fail_rate AS (
    SELECT 
        table_name,
        ROUND(AVG(failed_percentage::numeric), 2) AS avg_fail_pct
    FROM (
        SELECT * FROM data_monitoring_dwh.fact_checkin_weather_monitoring
        UNION ALL
        SELECT * FROM data_monitoring_dwh.fact_review_weather_monitoring
    ) t
    GROUP BY table_name
)
SELECT 
    v.table_name,
    v.row_count,
    f.avg_fail_pct,
    ROUND(row_count * f.avg_fail_pct / 100.0, 0) AS est_bad_rows
FROM volume v
JOIN fail_rate f ON v.table_name = f.table_name
ORDER BY est_bad_rows DESC;

-- 4. Column Quality Score Index
SELECT 
    table_name,
    column_name,
    ROUND(100.0 - AVG(failed_percentage), 2) AS quality_score
FROM (
    SELECT * FROM data_monitoring_dwh.fact_checkin_weather_monitoring
    UNION ALL
    SELECT * FROM data_monitoring_dwh.fact_review_weather_monitoring
) t
GROUP BY table_name, column_name
ORDER BY quality_score ASC;

-- 5. Validation Rule Performance Comparison
SELECT 
    validation_rule,
    COUNT(*) AS affected_columns,
    ROUND(AVG(failed_percentage), 2) AS avg_fail_pct
FROM (
    SELECT * FROM data_monitoring_dwh.fact_checkin_weather_monitoring
    UNION ALL
    SELECT * FROM data_monitoring_dwh.fact_review_weather_monitoring
) t
GROUP BY validation_rule
ORDER BY avg_fail_pct DESC;

-- 6. DQ Issue vs Business Dimensions
SELECT 
    b.city,
    COUNT(f.business_id) AS total_records,
    ROUND(AVG(m.failed_percentage), 2) AS avg_fail_pct
FROM dwh.fact_checkin_weather f
JOIN yelp.business b ON f.business_id = b.business_id
LEFT JOIN data_monitoring_dwh.fact_checkin_weather_monitoring m 
    ON m.table_name = 'fact_checkin_weather' AND m.column_name = 'city'
GROUP BY b.city
HAVING COUNT(f.business_id) > 5
ORDER BY avg_fail_pct DESC
LIMIT 15;