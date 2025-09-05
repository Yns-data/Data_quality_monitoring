       {{ config(materialized='table') }}     
                     
SELECT *,
    ROUND(sum_visitors::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_visitors,
    ROUND(sum_pages_viewed::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_pages_viewed,
    ROUND(sum_food_articles::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_food_articles,
    ROUND(sum_wear_articles::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_wear_articles,
    ROUND(sum_electronics_articles::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_electronics_articles,
    ROUND(sum_sports_articles::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_sports_articles,
    ROUND(sum_toys_articles::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_toys_articles,
    ROUND(sum_home_articles::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_home_articles,
    ROUND(sum_garden_articles::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_garden_articles,
    ROUND(sum_beauty_articles::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_beauty_articles,
    ROUND(sum_automotive_articles::numeric / NULLIF(total_sum, 0)::numeric, 2) * 100 AS prct_automotive_articles,
    ROUND(total_sum::numeric, 2) AS Total
FROM {{ref('base_data_month')}}
