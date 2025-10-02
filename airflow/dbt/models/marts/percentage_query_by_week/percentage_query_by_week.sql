       {{ config(materialized='table') }}     
                     
SELECT *,
    ROUND(CAST(sum_visitors AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_visitors,
    ROUND(CAST(sum_pages_viewed AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_pages_viewed,
    ROUND(CAST(sum_food_articles AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_food_articles,
    ROUND(CAST(sum_wear_articles AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_wear_articles,
    ROUND(CAST(sum_electronics_articles AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_electronics_articles,
    ROUND(CAST(sum_sports_articles AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_sports_articles,
    ROUND(CAST(sum_toys_articles AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_toys_articles,
    ROUND(CAST(sum_home_articles AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_home_articles,
    ROUND(CAST(sum_garden_articles AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_garden_articles,
    ROUND(CAST(sum_beauty_articles AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_beauty_articles,
    ROUND(CAST(sum_automotive_articles AS NUMERIC) / NULLIF(CAST(total_sum AS NUMERIC), 0), 2) * 100 AS prct_automotive_articles,
    ROUND(CAST(total_sum AS NUMERIC), 2) AS Total
FROM {{ ref('base_data_week') }}

