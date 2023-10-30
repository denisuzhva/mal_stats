WITH top_popular AS (
    SELECT 
        count(*) AS popularity, 
        anime_id
    FROM mal_rating
    GROUP BY anime_id
    ORDER BY popularity DESC
    LIMIT 100
)
SELECT
    anime_id,
    user_id,
    score 
INTO mal_rating_filtered
FROM (
    SELECT 
        r.anime_id,
        r.user_id,
        r.score,
        count(*) OVER (PARTITION BY user_id) AS num_items
    FROM mal_rating r TABLESAMPLE SYSTEM (1)
    INNER JOIN top_popular t ON r.anime_id = t.anime_id
) top_filtered
WHERE num_items >= 5 AND num_items <= 100