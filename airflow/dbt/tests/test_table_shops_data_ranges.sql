-- Test que toutes les valeurs numériques sont dans des plages logiques
-- Ce test échoue s'il trouve des valeurs en dehors des plages acceptables

SELECT 
    id,
    visitors,
    pages_viewed,
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
    visitors < 0 
    OR pages_viewed < 0
    OR food_articles < 0 
    OR wear_articles < 0
    OR electronics_articles < 0
    OR sports_articles < 0
    OR toys_articles < 0
    OR home_articles < 0
    OR garden_articles < 0
    OR beauty_articles < 0
    OR automotive_articles < 0
    OR visitors > 1000000  -- Limite supérieure raisonnable
    OR pages_viewed > 10000000  -- Limite supérieure raisonnable
