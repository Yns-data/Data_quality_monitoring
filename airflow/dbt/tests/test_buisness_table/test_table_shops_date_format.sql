SELECT 
    id,
    dates
FROM {{ source('public', 'table_shops') }}
WHERE 
    LENGTH(TRIM(dates)) = 0
    OR LENGTH(dates) < 13
    OR NOT (SUBSTRING(dates, 1, 4) ~ '^[0-9]{4}$')
    OR SUBSTRING(dates, 5, 1) != '-'
    OR NOT (SUBSTRING(dates, 6, 2) ~ '^[0-9]{2}$')
    OR SUBSTRING(dates, 8, 1) != '-'
    OR NOT (SUBSTRING(dates, 9, 2) ~ '^[0-9]{2}$')
    OR SUBSTRING(dates, 11, 1) != '-'
    OR NOT (SUBSTRING(dates, 12, 2) ~ '^[0-9]{2}$')
    OR CAST(SUBSTRING(dates, 6, 2) AS INTEGER) < 1 
    OR CAST(SUBSTRING(dates, 6, 2) AS INTEGER) > 12
    OR CAST(SUBSTRING(dates, 9, 2) AS INTEGER) < 1 
    OR CAST(SUBSTRING(dates, 9, 2) AS INTEGER) > 31
    OR CAST(SUBSTRING(dates, 12, 2) AS INTEGER) < 0
    OR CAST(SUBSTRING(dates, 12, 2) AS INTEGER) > 23
