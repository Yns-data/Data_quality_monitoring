SELECT 
    dates
FROM {{ source('public', env_var('table_id')) }}
WHERE 
    dates IS NULL
    OR LENGTH(TRIM(dates)) = 0
    OR LENGTH(dates) < 13
    OR NOT REGEXP_CONTAINS(SUBSTR(dates, 1, 4), r'^[0-9]{4}$')
    OR SUBSTR(dates, 5, 1) != '-'
    OR NOT REGEXP_CONTAINS(SUBSTR(dates, 6, 2), r'^[0-9]{2}$')
    OR SUBSTR(dates, 8, 1) != '-'
    OR NOT REGEXP_CONTAINS(SUBSTR(dates, 9, 2), r'^[0-9]{2}$')
    OR SUBSTR(dates, 11, 1) != '-'
    OR NOT REGEXP_CONTAINS(SUBSTR(dates, 12, 2), r'^[0-9]{2}$')
    OR CAST(SUBSTR(dates, 6, 2) AS INT64) < 1
    OR CAST(SUBSTR(dates, 6, 2) AS INT64) > 12
    OR CAST(SUBSTR(dates, 9, 2) AS INT64) < 1
    OR CAST(SUBSTR(dates, 9, 2) AS INT64) > 31
    OR CAST(SUBSTR(dates, 12, 2) AS INT64) < 0
    OR CAST(SUBSTR(dates, 12, 2) AS INT64) > 23

