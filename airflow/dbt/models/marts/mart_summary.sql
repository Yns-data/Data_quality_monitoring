-- Modèle mart exemple
{{ config(materialized='table') }}

select 
    count(*) as total_records,
    max(created_at) as last_updated
from {{ ref('stg_example') }}
