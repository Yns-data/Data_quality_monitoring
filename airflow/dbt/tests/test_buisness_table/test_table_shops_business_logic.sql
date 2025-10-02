SELECT 
    dates,
    pages_viewed,
    cities
FROM {{ source('public', env_var('table_id')) }}
WHERE 
    (LENGTH(TRIM(cities)) = 0)
    OR REGEXP_CONTAINS(cities, r'[<>{}\[\]\\|`~!@#$%^&*()+=]')

    

