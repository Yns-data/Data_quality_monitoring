-- Test de logique métier : cohérence entre les données
-- Ce test échoue s'il trouve des incohérences métier

SELECT 
    id,
    dates,
    visitors,
    pages_viewed,
    cities
FROM {{ source('public', 'table_shops') }}
WHERE 
    visitors > 0
    OR (cities IS NOT NULL AND LENGTH(TRIM(cities)) = 0)
    OR (cities ~ '[<>{}[\]\\|`~!@#$%^&*()+=]')
    

