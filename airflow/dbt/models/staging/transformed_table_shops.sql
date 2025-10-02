{{ config(materialized='view') }}     

SELECT
    *,
    SUBSTR(dates, -2) AS heure,
    SUBSTR(dates, 1, 10) AS day,
    FORMAT_DATE('%A', PARSE_DATE('%Y-%m-%d', SUBSTR(dates, 1, 10))) AS day_name,
    PARSE_DATE('%Y-%m-%d', SUBSTR(dates, 1, 10)) AS date_val,
    FORMAT_DATE('%G-%V', PARSE_DATE('%Y-%m-%d', SUBSTR(dates, 1, 10))) AS week,
    FORMAT_DATE('%Y-%m', PARSE_DATE('%Y-%m-%d', SUBSTR(dates, 1, 10))) AS month,
    FORMAT_DATE('%B', PARSE_DATE('%Y-%m-%d', SUBSTR(dates, 1, 10))) AS month_name,
    FORMAT_DATE('%Y', PARSE_DATE('%Y-%m-%d', SUBSTR(dates, 1, 10))) AS year
FROM
    {{ source('public', 'table_shops') }}
