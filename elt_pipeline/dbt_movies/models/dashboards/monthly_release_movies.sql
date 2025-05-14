-- models/analytics/monthly_release_summary.sql

SELECT 
    TO_CHAR(mi.release_date, 'YYYY-MM') AS release_month,
    COUNT(*) AS movie_count
from {{source('movies','movies_infor')}} mi 
WHERE mi.release_date IS NOT NULL
GROUP BY release_month
ORDER BY release_month
