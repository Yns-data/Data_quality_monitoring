{{ config(materialized='view') }}

-- Exemple de modèle de staging pour les métriques de qualité des données
-- Ce modèle nettoie et standardise les données de la table source

with source_data as (
    select
        id,
        table_name,
        column_name,
        metric_type,
        metric_value,
        check_timestamp,
        -- Standardisation des données
        lower(trim(table_name)) as clean_table_name,
        lower(trim(column_name)) as clean_column_name,
        lower(trim(metric_type)) as clean_metric_type,
        -- Conversion du timestamp
        check_timestamp::timestamp as normalized_timestamp
    from {{ var('source_schema', 'public') }}.{{ var('source_table', 'data_quality_metrics') }}
    where check_timestamp is not null
)

select * from source_data
