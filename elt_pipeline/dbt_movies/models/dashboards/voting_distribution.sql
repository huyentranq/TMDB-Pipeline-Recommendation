
WITH binned AS (
    SELECT 
        *,
        FLOOR(vote_average)::int AS bin_floor
    FROM {{ ref('infor_rating') }}
    WHERE vote_average IS NOT NULL
)

SELECT 
    release_year,
    CASE 
        WHEN bin_floor BETWEEN 0 AND 9 THEN CONCAT(bin_floor, '-', bin_floor + 1)
        ELSE 'unknown'
    END AS rating_bin_label,
    COUNT(*) AS movie_count
FROM binned
GROUP BY rating_bin_label, release_year
ORDER BY rating_bin_label
