/*
-- SQL file for yelp data monitoring dashboard in metabase
-- Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
-- Updated: 20250624
*/

-- 1. Top Cities by Business Count
SELECT city, COUNT(*) AS business_count
FROM yelp.business
GROUP BY city
ORDER BY business_count DESC
LIMIT 15;

-- 2. Review Count vs Stars Distribution
SELECT review_count, stars FROM yelp.business
WHERE review_count < 1000;

-- 3. Average Stars by Business Category
SELECT 
    TRIM(category) AS category,
    ROUND(AVG(stars::numeric), 2) AS avg_rating,
    COUNT(*) AS business_count
FROM (
    SELECT 
        b.stars,
        unnest(string_to_array(b.categories, ',')) AS category
    FROM yelp.business b
    WHERE b.categories IS NOT NULL
) AS exploded
GROUP BY category
ORDER BY avg_rating DESC
LIMIT 20;

-- 4. Top Businesses by Review Volume & Avg Rating
SELECT 
    b.name,
    COUNT(r.review_id) AS total_reviews,
    ROUND(AVG(r.stars), 2) AS avg_rating
FROM yelp.review r
JOIN yelp.business b ON r.business_id = b.business_id
GROUP BY b.name
HAVING COUNT(r.review_id) > 10
ORDER BY total_reviews DESC
LIMIT 20;

-- 5. Review Rating Trends Over Time
SELECT 
    DATE_TRUNC('month', date) AS month,
    ROUND(AVG(stars), 2) AS avg_stars
FROM yelp.review
GROUP BY month
ORDER BY month;

-- 6. Check-in Volume by Day of Week
SELECT 
    TO_CHAR(date, 'Day') AS day_of_week,
    COUNT(*) AS checkin_count
FROM yelp.checkin
GROUP BY day_of_week
ORDER BY checkin_count DESC;

-- 7. User Engagement Scoring
SELECT 
    user_id,
    review_count,
    average_stars,
    fans
FROM yelp.user
WHERE review_count > 5;

-- 8. Top Columns with Highest Failure Rate per Validation Rule
SELECT 
    table_name,
    column_name,
    validation_rule,
    MAX(failed_percentage) AS max_failed_pct,
    ROUND(AVG(failed_percentage::numeric), 2) AS avg_failed_pct,
    COUNT(*) AS observation_count
FROM (
    SELECT * FROM data_monitoring.business_monitoring
    UNION ALL
    SELECT * FROM data_monitoring.checkin_monitoring
    UNION ALL
    SELECT * FROM data_monitoring.tip_monitoring
    UNION ALL
    SELECT * FROM data_monitoring.user_monitoring
    UNION ALL
    SELECT * FROM data_monitoring.review_monitoring
) dm
GROUP BY table_name, column_name, validation_rule
ORDER BY avg_failed_pct DESC
LIMIT 20;

-- 9. Trend of Data Quality Issues Over Time
SELECT 
    DATE_TRUNC('day', execution_dt) AS day,
    table_name,
    ROUND(AVG(failed_percentage::numeric), 2) AS avg_fail_pct
FROM (
    SELECT * FROM data_monitoring.business_monitoring
    UNION ALL
    SELECT * FROM data_monitoring.checkin_monitoring
    UNION ALL
    SELECT * FROM data_monitoring.tip_monitoring
    UNION ALL
    SELECT * FROM data_monitoring.user_monitoring
    UNION ALL
    SELECT * FROM data_monitoring.review_monitoring
) dm
GROUP BY day, table_name
ORDER BY day ASC;

-- 10. Tip and Business Null Rate Comparison
SELECT 
    b.business_id,
    b.name,
    COUNT(t.text) AS total_tips,
    bm.failed_percentage AS business_name_fail_pct,
    tm.failed_percentage AS tip_text_fail_pct
FROM yelp.business b
LEFT JOIN yelp.tip t ON b.business_id = t.business_id
LEFT JOIN data_monitoring.business_monitoring bm 
    ON bm.table_name = 'business' AND bm.column_name = 'name'
LEFT JOIN data_monitoring.tip_monitoring tm 
    ON tm.table_name = 'tip' AND tm.column_name = 'text'
GROUP BY b.business_id, b.name, bm.failed_percentage, tm.failed_percentage
ORDER BY tip_text_fail_pct DESC NULLS LAST
LIMIT 20;

-- 11. Anomaly Review Distribution vs Business Rating
SELECT 
    b.name,
    b.stars AS business_rating,
    COUNT(r.review_id) AS total_reviews,
    AVG(r.stars) AS avg_review_rating,
    rm.failed_percentage AS review_star_quality_issue
FROM yelp.business b
JOIN yelp.review r ON b.business_id = r.business_id
LEFT JOIN data_monitoring.review_monitoring rm 
    ON rm.table_name = 'review' AND rm.column_name = 'stars'
GROUP BY b.name, b.stars, rm.failed_percentage
HAVING COUNT(r.review_id) > 20 AND rm.failed_percentage IS NOT NULL
ORDER BY review_star_quality_issue DESC;