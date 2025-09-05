{{ config(materialized='view') }}     


SELECT 
    week,
    SUM(visitors) AS sum_visitors,
    SUM(pages_viewed) AS sum_pages_viewed,
    SUM(food_articles) AS sum_food_articles,
    SUM(wear_articles) AS sum_wear_articles,
    SUM(electronics_articles) AS sum_electronics_articles,
    SUM(sports_articles) AS sum_sports_articles,
    SUM(toys_articles) AS sum_toys_articles,
    SUM(home_articles) AS sum_home_articles,
    SUM(garden_articles) AS sum_garden_articles,
    SUM(beauty_articles) AS sum_beauty_articles,
    SUM(automotive_articles) AS sum_automotive_articles
FROM
    {{ ref('transformed_table_shops') }}
GROUP BY 
    week
ORDER BY 
    week