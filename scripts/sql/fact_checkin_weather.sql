/*
-- SQL file for fact checkin weather
-- Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
-- Updated: 20250624
*/

WITH checkin_agg AS (
    SELECT 
        business_id,
        date::DATE AS checkin_date,
        COUNT(*) AS checkin_count
    FROM yelp.checkin
    GROUP BY business_id, date::DATE
)
SELECT 
    c.business_id,
    c.checkin_date,
    c.checkin_count,
    b.city,
    b.state,
    b.is_open,
    wt.min AS temperature_min,
    wt.max AS temperature_max,
    wp.precipitation,
    NOW() AS load_dt
FROM checkin_agg c
JOIN yelp.business b ON c.business_id = b.business_id
LEFT JOIN weather.temperature wt ON c.checkin_date = wt.date
LEFT JOIN weather.precipitation wp ON c.checkin_date = wp.date
;