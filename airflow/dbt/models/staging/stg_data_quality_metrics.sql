{{ config(materialized='view') }}

-- Modèle de staging pour les métriques de qualité des données
-- Génère des données d'exemple pour les tests

select
    1 as id,
    'example_table' as table_name,
    'id' as column_name,
    'completeness' as metric_type,
    95.5 as metric_value,
    current_timestamp as check_timestamp,
    'example_table' as clean_table_name,
    'id' as clean_column_name,
    'completeness' as clean_metric_type,
    current_timestamp as normalized_timestamp

union all

select
    2 as id,
    'users_table' as table_name,
    'email' as column_name,
    'uniqueness' as metric_type,
    98.2 as metric_value,
    current_timestamp as check_timestamp,
    'users_table' as clean_table_name,
    'email' as clean_column_name,
    'uniqueness' as clean_metric_type,
    current_timestamp as normalized_timestamp
