SELECT
    r.review_id,
    r.user_id,
    r.business_id,
    r.date::DATE AS review_date,
    r.stars,
    b.city,
    b.state,
    b.is_open,
    wt.min AS temperature_min,
    wt.max AS temperature_max,
    wp.precipitation,
    NOW() AS load_dt
FROM yelp.review r
JOIN yelp.business b ON r.business_id = b.business_id
LEFT JOIN weather.temperature wt ON r.date::DATE = wt.date
LEFT JOIN weather.precipitation wp ON r.date::DATE = wp.date
;