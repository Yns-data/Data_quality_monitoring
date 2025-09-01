       {{ config(materialized='view') }}     
            
            
            
            SELECT *,
            ROUND(sum_visitors::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_visitors
           ,ROUND(sum_pages_viewed::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_pages_viewed
           ,ROUND(sum_food_articles::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_food_articles
           ,ROUND(sum_wear_articles::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_wear_articles
           ,ROUND(sum_electronics_articles::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_electronics_articles
           ,ROUND(sum_sports_articles::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_sports_articles
           ,ROUND(sum_toys_articles::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_toys_articles
           ,ROUND(sum_home_articles::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_home_articles
           ,ROUND(sum_garden_articles::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_garden_articles
           ,ROUND(sum_beauty_articles::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_beauty_articles
           ,ROUND(sum_automotive_articles::numeric/NULLIF(sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles,0)::numeric,2)*100 AS prct_automotive_articles,
            ROUND((sum_visitors + sum_pages_viewed + sum_food_articles + sum_wear_articles + sum_electronics_articles + sum_sports_articles + sum_toys_articles + sum_home_articles + sum_garden_articles + sum_beauty_articles + sum_automotive_articles)::numeric,2) AS Total
            FROM(
            
        SELECT 
            day,
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
            (
        SELECT 
            *,
            RIGHT(dates, 2) AS heure,
            LEFT(dates, 10) AS day,
            TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD') AS date_val,
            TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'IYYY-IW') AS week,
            TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'YYYY-MM') AS month, 
            TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'YYYY') AS year
        FROM 
            {{ source('public', 'table_shops') }}
        WHERE 
            GREATEST(visitors, pages_viewed, food_articles, wear_articles, electronics_articles, sports_articles, toys_articles, home_articles, garden_articles, beauty_articles, automotive_articles) IS NOT NULL)
        GROUP BY 
            day
        ORDER BY 
            day
            )