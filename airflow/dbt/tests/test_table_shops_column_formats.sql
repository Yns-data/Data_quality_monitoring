-- Test des formats et types des colonnes (sauf dates)
-- Ce test échoue s'il trouve des valeurs avec des formats incorrects

SELECT 
    id,
    visitors,
    pages_viewed,
    cities,
    food_articles,
    wear_articles,
    electronics_articles,
    sports_articles,
    toys_articles,
    home_articles,
    garden_articles,
    beauty_articles,
    automotive_articles
FROM {{ source('public', 'table_shops') }}
WHERE 
    -- Test format ID (doit être un entier positif)
    id IS NULL
    OR id <= 0
    OR id::text ~ '[^0-9]'  -- Ne doit contenir que des chiffres
    
    -- Test format visitors (entier, peut être 0 ou positif)
    OR (visitors IS NOT NULL AND visitors::text ~ '[^0-9]')
    OR (visitors IS NOT NULL AND visitors < 0)
    
    -- Test format pages_viewed (entier, peut être 0 ou positif)
    OR (pages_viewed IS NOT NULL AND pages_viewed::text ~ '[^0-9]')
    OR (pages_viewed IS NOT NULL AND pages_viewed < 0)
    
    -- Test format cities (texte valide, pas de caractères de contrôle)
    OR (cities IS NOT NULL AND cities ~ '[\x00-\x1F\x7F]')  -- Caractères de contrôle
    OR (cities IS NOT NULL AND LENGTH(cities) > 255)  -- Longueur raisonnable
    OR (cities IS NOT NULL AND cities ~ '^[[:space:]]*$')  -- Que des espaces
    
    -- Test format food_articles (nombre réel, peut être 0 ou positif)
    OR (food_articles IS NOT NULL AND food_articles < 0)
    OR (food_articles IS NOT NULL AND food_articles::text ~ '[^0-9.]')
    OR (food_articles IS NOT NULL AND food_articles::text ~ '\..*\.')  -- Plusieurs points décimaux
    
    -- Test format wear_articles (nombre réel, peut être 0 ou positif)
    OR (wear_articles IS NOT NULL AND wear_articles < 0)
    OR (wear_articles IS NOT NULL AND wear_articles::text ~ '[^0-9.]')
    OR (wear_articles IS NOT NULL AND wear_articles::text ~ '\..*\.')
    
    -- Test format electronics_articles (nombre réel, peut être 0 ou positif)
    OR (electronics_articles IS NOT NULL AND electronics_articles < 0)
    OR (electronics_articles IS NOT NULL AND electronics_articles::text ~ '[^0-9.]')
    OR (electronics_articles IS NOT NULL AND electronics_articles::text ~ '\..*\.')
    
    -- Test format sports_articles (nombre réel, peut être 0 ou positif)
    OR (sports_articles IS NOT NULL AND sports_articles < 0)
    OR (sports_articles IS NOT NULL AND sports_articles::text ~ '[^0-9.]')
    OR (sports_articles IS NOT NULL AND sports_articles::text ~ '\..*\.')
    
    -- Test format toys_articles (nombre réel, peut être 0 ou positif)
    OR (toys_articles IS NOT NULL AND toys_articles < 0)
    OR (toys_articles IS NOT NULL AND toys_articles::text ~ '[^0-9.]')
    OR (toys_articles IS NOT NULL AND toys_articles::text ~ '\..*\.')
    
    -- Test format home_articles (nombre réel, peut être 0 ou positif)
    OR (home_articles IS NOT NULL AND home_articles < 0)
    OR (home_articles IS NOT NULL AND home_articles::text ~ '[^0-9.]')
    OR (home_articles IS NOT NULL AND home_articles::text ~ '\..*\.')
    
    -- Test format garden_articles (nombre réel, peut être 0 ou positif)
    OR (garden_articles IS NOT NULL AND garden_articles < 0)
    OR (garden_articles IS NOT NULL AND garden_articles::text ~ '[^0-9.]')
    OR (garden_articles IS NOT NULL AND garden_articles::text ~ '\..*\.')
    
    -- Test format beauty_articles (nombre réel, peut être 0 ou positif)
    OR (beauty_articles IS NOT NULL AND beauty_articles < 0)
    OR (beauty_articles IS NOT NULL AND beauty_articles::text ~ '[^0-9.]')
    OR (beauty_articles IS NOT NULL AND beauty_articles::text ~ '\..*\.')
    
    -- Test format automotive_articles (nombre réel, peut être 0 ou positif)
    OR (automotive_articles IS NOT NULL AND automotive_articles < 0)
    OR (automotive_articles IS NOT NULL AND automotive_articles::text ~ '[^0-9.]')
    OR (automotive_articles IS NOT NULL AND automotive_articles::text ~ '\..*\.')
