SELECT 
    id,
    dates,
    pages_viewed,
    cities
FROM {{ source('public', 'table_shops') }}
WHERE 
    (LENGTH(TRIM(cities)) = 0)
    OR (cities ~ '[<>{}\\[\\]\\\\|`~!@#$%^&*()+=]')
    

