/*
-- SQL file for weather data monitoring dashboard in metabase
-- Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
-- Updated: 20250624
*/

-- 1. Failed Per Column Failure
SELECT 
    table_name,
    column_name,
    validation_rule,
    AVG(failed_percentage) AS avg_failed_pct
FROM data_monitoring.temperature_monitoring
GROUP BY table_name, column_name, validation_rule
ORDER BY avg_failed_pct DESC

-- 2. Trend Failed Validation Over Time
SELECT 
    execution_dt::DATE AS date,
    AVG(failed_percentage) AS avg_failed_pct
FROM data_monitoring.temperature_monitoring
GROUP BY date
ORDER BY date

-- 3. Top 5 Worst Columns (Validity & Completeness)
SELECT 
    column_name,
    validation_rule,
    MAX(failed_percentage) AS worst_case_pct
FROM data_monitoring.temperature_monitoring
WHERE validation_rule IN ('not_null', 'checking_date_format', 'fahrenheit_range')
GROUP BY column_name, validation_rule
ORDER BY worst_case_pct DESC
LIMIT 5

-- 4. Daily Anomaly in Temperature
SELECT 
    date,
    min,
    max,
    CASE 
        WHEN min < -30 OR max > 120 THEN 'Outlier'
        ELSE 'Normal'
    END AS status
FROM weather.temperature
ORDER BY date

-- 5. Data Drift via Temp Stats
WITH recent AS (
  SELECT 
    AVG(max) AS recent_max_avg,
    AVG(min) AS recent_min_avg
  FROM weather.temperature
  WHERE date >= CURRENT_DATE - INTERVAL '7 days'
),
previous AS (
  SELECT 
    AVG(max) AS prev_max_avg,
    AVG(min) AS prev_min_avg
  FROM weather.temperature
  WHERE date BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE - INTERVAL '8 days'
)
SELECT 
  recent.recent_max_avg, 
  previous.prev_max_avg,
  recent.recent_max_avg - previous.prev_max_avg AS max_drift,
  recent.recent_min_avg - previous.prev_min_avg AS min_drift
FROM recent, previous

-- 6. Volatility of Failed %
SELECT 
    column_name,
    validation_rule,
    STDDEV(failed_percentage) AS stddev_failed_pct,
    AVG(failed_percentage) AS avg_failed_pct
FROM data_monitoring.temperature_monitoring
GROUP BY column_name, validation_rule
ORDER BY stddev_failed_pct DESC
LIMIT 10;

-- 7. Temperature vs Precipitation Missing Rate
WITH temp_nulls AS (
  SELECT
    date,
    1.0 * COUNT(*) FILTER (WHERE min IS NULL OR max IS NULL) / COUNT(*) AS temp_null_rate
  FROM weather.temperature
  GROUP BY date
),
precip_nulls AS (
  SELECT
    date,
    1.0 * COUNT(*) FILTER (WHERE precipitation IS NULL) / COUNT(*) AS precip_null_rate
  FROM weather.precipitation
  GROUP BY date
)
SELECT 
    t.date,
    t.temp_null_rate,
    p.precip_null_rate
FROM temp_nulls t
JOIN precip_nulls p ON t.date = p.date
ORDER BY t.date;

-- 8. Completeness vs Validity
SELECT 
    CASE 
        WHEN validation_rule IN ('not_null', 'checking_date_format') THEN 'Completeness'
        ELSE 'Validity'
    END AS category,
    AVG(failed_percentage) AS avg_failed_pct
FROM data_monitoring.temperature_monitoring
GROUP BY category;

-- 9. Trend Daily Precipitation + Missing Value Overlay
SELECT 
    p.date,
    AVG(p.precipitation) AS avg_precipitation,
    COUNT(*) FILTER (WHERE p.precipitation IS NULL) AS missing_count,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE p.precipitation IS NULL) / COUNT(*), 2) AS missing_pct
FROM weather.precipitation p
GROUP BY p.date
ORDER BY p.date;

-- 10. Freshness Delay Monitoring: Latest Data Ingestion Check
SELECT 
    'precipitation' AS table_name,
    MAX(load_dt) AS latest_loaded,
    CURRENT_TIMESTAMP - MAX(load_dt) AS delay
FROM weather.precipitation
UNION ALL
SELECT 
    'temperature',
    MAX(load_dt),
    CURRENT_TIMESTAMP - MAX(load_dt)
FROM weather.temperature;

-- 11. Weather Outlier Detector: High Rain, Low Temp
SELECT 
    wp.date,
    wp.precipitation,
    wt.min AS temperature_min,
    wt.max AS temperature_max
FROM weather.precipitation wp
JOIN weather.temperature wt ON wp.date = wt.date
WHERE wp.precipitation > 100 AND wt.max < 40
ORDER BY wp.date;

-- 12. Rule Failure Comparison: `precipitation` vs `precipitation_normal`
SELECT 
    column_name,
    validation_rule,
    AVG(failed_percentage) AS avg_failed_pct
FROM data_monitoring.weather_precipitation_monitoring
WHERE column_name IN ('precipitation', 'precipitation_normal')
GROUP BY column_name, validation_rule
ORDER BY avg_failed_pct DESC;

-- 13. Rainfall Spike Detector vs Historical Normal
SELECT 
    date,
    precipitation,
    precipitation_normal,
    (precipitation - precipitation_normal) AS delta,
    ROUND(
        (100.0 * (precipitation - precipitation_normal) / NULLIF(precipitation_normal, 0))::numeric,
        2
    ) AS delta_pct
FROM weather.precipitation
WHERE precipitation_normal IS NOT NULL
ORDER BY delta_pct DESC
LIMIT 20;
