-- Test que les dates sont dans un format valide
-- Ce test échoue s'il trouve des dates mal formatées

SELECT 
    id,
    dates
FROM {{ source('public', 'table_shops') }}
WHERE 
    -- Vérifier que la date n'est pas vide
    dates IS NULL 
    OR LENGTH(TRIM(dates)) = 0
    -- Vérifier le format de base YYYY-MM-DD-HH (au moins 13 caractères)
    OR LENGTH(dates) < 13
    -- Vérifier que les 4 premiers caractères sont numériques (année)
    OR NOT (SUBSTRING(dates, 1, 4) ~ '^[0-9]{4}$')
    -- Vérifier que le 5ème caractère est un tiret
    OR SUBSTRING(dates, 5, 1) != '-'
    -- Vérifier que les caractères 6-7 sont numériques (mois)
    OR NOT (SUBSTRING(dates, 6, 2) ~ '^[0-9]{2}$')
    -- Vérifier que le 8ème caractère est un tiret
    OR SUBSTRING(dates, 8, 1) != '-'
    -- Vérifier que les caractères 9-10 sont numériques (jour)
    OR NOT (SUBSTRING(dates, 9, 2) ~ '^[0-9]{2}$')
    -- Vérifier que le 11ème caractère est un tiret
    OR SUBSTRING(dates, 11, 1) != '-'
    -- Vérifier que les caractères 12-13 sont numériques (heure)
    OR NOT (SUBSTRING(dates, 12, 2) ~ '^[0-9]{2}$')
    -- Vérifier que le mois est valide (01-12)
    OR CAST(SUBSTRING(dates, 6, 2) AS INTEGER) < 1 
    OR CAST(SUBSTRING(dates, 6, 2) AS INTEGER) > 12
    -- Vérifier que le jour est valide (01-31)
    OR CAST(SUBSTRING(dates, 9, 2) AS INTEGER) < 1 
    OR CAST(SUBSTRING(dates, 9, 2) AS INTEGER) > 31
    -- Vérifier que l'heure est valide (00-23)
    OR CAST(SUBSTRING(dates, 12, 2) AS INTEGER) < 0
    OR CAST(SUBSTRING(dates, 12, 2) AS INTEGER) > 23
