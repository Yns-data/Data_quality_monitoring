{{ config(materialized='view') }}     

SELECT *,
    (sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + 
        sum_electronics_articles + sum_sports_articles + sum_toys_articles + 
        sum_home_articles + sum_garden_articles + sum_beauty_articles + 
        sum_automotive_articles) AS total_sum
FROM {{ ref('sum_aggregate_transformed_table_shops_by_week') }}
