-- Modèle exemple pour tester le pipeline
{{ config(materialized='view') }}

select 
    1 as id,
    'test' as name,
    current_timestamp as created_at

union all

select 
    2 as id,
    'example' as name,
    current_timestamp as created_at
