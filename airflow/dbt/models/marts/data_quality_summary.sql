{{ config(materialized='table') }}

-- Modèle de mart pour résumer les métriques de qualité des données par table et jour
-- Ce modèle crée une table d'analyse prête pour le reporting

with daily_metrics as (
    select
        clean_table_name,
        clean_metric_type,
        date(normalized_timestamp) as check_date,
        count(*) as total_checks,
        avg(metric_value) as avg_metric_value,
        min(metric_value) as min_metric_value,
        max(metric_value) as max_metric_value,
        stddev(metric_value) as stddev_metric_value
    from {{ ref('stg_data_quality_metrics') }}
    group by 
        clean_table_name,
        clean_metric_type,
        date(normalized_timestamp)
),

quality_trends as (
    select
        *,
        -- Calcul d'un score de qualité simple (exemple)
        case 
            when clean_metric_type = 'completeness' then avg_metric_value * 100
            when clean_metric_type = 'accuracy' then avg_metric_value * 100
            else avg_metric_value
        end as quality_score,
        -- Trend analysis
        lag(avg_metric_value) over (
            partition by clean_table_name, clean_metric_type 
            order by check_date
        ) as previous_day_value
    from daily_metrics
)

select
    clean_table_name as table_name,
    clean_metric_type as metric_type,
    check_date,
    total_checks,
    round(avg_metric_value, 4) as avg_metric_value,
    round(min_metric_value, 4) as min_metric_value,
    round(max_metric_value, 4) as max_metric_value,
    round(stddev_metric_value, 4) as stddev_metric_value,
    round(quality_score, 2) as quality_score,
    case 
        when previous_day_value is null then 'N/A'
        when avg_metric_value > previous_day_value then 'Improved'
        when avg_metric_value < previous_day_value then 'Declined'
        else 'Stable'
    end as trend
from quality_trends
order by table_name, metric_type, check_date desc
