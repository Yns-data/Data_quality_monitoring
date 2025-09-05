{{ config(materialized='view') }}     

SELECT 
    *,
    RIGHT(dates, 2) AS heure,
    LEFT(dates, 10) AS day,
    TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'Day') AS day_name,
    TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD') AS date_val,
    TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'IYYY-IW') AS week,
    TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'YYYY-MM') AS month,
    TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'Month') AS month_name,
    TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'YYYY') AS year
FROM 
    {{ source('public', 'table_shops') }}