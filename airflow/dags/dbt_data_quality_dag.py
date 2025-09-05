# type : ignore
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_quality_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'dbt_data_quality_pipeline',
    default_args=default_args,
    description='Data quality pipeline with dbt',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['dbt', 'data_quality'],
)

# ========== INITIAL TESTS ==========
# Test 1: Source data business logic tests
dbt_test_business_table = BashOperator(
    task_id='dbt_test_business_table',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select path:tests/test_buisness_table --target-path target
    ''',
    dag=dag,
)

# Test 2: Source data schema tests (from schema.yml)
dbt_test_source_schema = BashOperator(
    task_id='dbt_test_source_schema',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select source:public.table_shops --target-path target
    ''',
    dag=dag,
)

# ========== TRANSFORMATIONS ==========
# Initial transformation: transformed shops table
dbt_run_transformed_table_shops = BashOperator(
    task_id='dbt_run_transformed_table_shops',
    bash_command='cd /opt/airflow/dbt && dbt run --models transformed_table_shops --target-path target',
    dag=dag,
)

# Test 3: Transformed table validation
dbt_test_transformed_table = BashOperator(
    task_id='dbt_test_transformed_table',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select transformed_table_shops --target-path target
    ''',
    dag=dag,
)

# ========== STAGING AGGREGATIONS ==========
# Staging aggregation by day
dbt_run_staging_aggregate_day = BashOperator(
    task_id='dbt_run_staging_aggregate_day',
    bash_command='cd /opt/airflow/dbt && dbt run --select path:models/staging/staging_percentage_query_by_day --target-path target',
    dag=dag,
)

# Test 4: Staging day aggregation validation
dbt_test_staging_day = BashOperator(
    task_id='dbt_test_staging_day',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select path:models/staging/staging_percentage_query_by_day --target-path target
    ''',
    dag=dag,
)

# Staging aggregation by week
dbt_run_staging_aggregate_week = BashOperator(
    task_id='dbt_run_staging_aggregate_week',
    bash_command='cd /opt/airflow/dbt && dbt run --select path:models/staging/staging_percentage_query_by_week --target-path target',
    dag=dag,
)

# Test 5: Staging week aggregation validation
dbt_test_staging_week = BashOperator(
    task_id='dbt_test_staging_week',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select path:models/staging/staging_percentage_query_by_week --target-path target
    ''',
    dag=dag,
)

# Staging aggregation by month
dbt_run_staging_aggregate_month = BashOperator(
    task_id='dbt_run_staging_aggregate_month',
    bash_command='cd /opt/airflow/dbt && dbt run --select path:models/staging/staging_percentage_query_by_month --target-path target',
    dag=dag,
)

# Test 6: Staging month aggregation validation
dbt_test_staging_month = BashOperator(
    task_id='dbt_test_staging_month',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select path:models/staging/staging_percentage_query_by_month --target-path target
    ''',
    dag=dag,
)

# ========== MARTS ==========
# Mart: percentage of queries by day
dbt_run_marts_percentage_query_by_day = BashOperator(
    task_id='dbt_run_marts_percentage_query_by_day',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt run --select path:models/marts/percentage_query_by_day --target-path target
    ''',
    dag=dag,
)

# Test 7: Day mart validation
dbt_test_marts_day = BashOperator(
    task_id='dbt_test_marts_day',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select path:models/marts/percentage_query_by_day --target-path target
    ''',
    dag=dag,
)

# Mart: percentage of queries by week
dbt_run_marts_percentage_query_by_week = BashOperator(
    task_id='dbt_run_marts_percentage_query_by_week',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt run --select path:models/marts/percentage_query_by_week --target-path target
    ''',
    dag=dag,
)

# Test 8: Week mart validation
dbt_test_marts_week = BashOperator(
    task_id='dbt_test_marts_week',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select path:models/marts/percentage_query_by_week --target-path target
    ''',
    dag=dag,
)

# Mart: percentage by month
dbt_run_marts_percentage_query_by_month = BashOperator(
    task_id='dbt_run_marts_percentage_query_by_month',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt run --select path:models/marts/percentage_query_by_month --target-path target
    ''',
    dag=dag,
)

# Test 9: Month mart validation
dbt_test_marts_month = BashOperator(
    task_id='dbt_test_marts_month',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select path:models/marts/percentage_query_by_month --target-path target
    ''',
    dag=dag,
)

# ========== FINAL VALIDATION ==========
# Test 10: Complete pipeline validation
dbt_test_all_marts = BashOperator(
    task_id='dbt_test_all_marts',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt test --select path:models/marts --target-path target
    ''',
    dag=dag,
)

# Documentation generation
dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='''
    cd /opt/airflow/dbt && 
    dbt docs generate --target-path target
    ''',
    dag=dag,
)

def send_success_notification():
    """Function to send success notification"""
    print("dbt pipeline executed successfully!")

success_notification = PythonOperator(
    task_id='success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# ========== DEPENDENCIES DEFINITION ==========
# Initial tests in parallel
[dbt_test_business_table, dbt_test_source_schema] >> dbt_run_transformed_table_shops

# Test transformed data before proceeding
dbt_run_transformed_table_shops >> dbt_test_transformed_table

# Branch for daily aggregation with tests
dbt_test_transformed_table >> dbt_run_staging_aggregate_day >> dbt_test_staging_day >> dbt_run_marts_percentage_query_by_day >> dbt_test_marts_day

# Branch for weekly aggregation with tests
dbt_test_transformed_table >> dbt_run_staging_aggregate_week >> dbt_test_staging_week >> dbt_run_marts_percentage_query_by_week >> dbt_test_marts_week

# Branch for monthly aggregation with tests
dbt_test_transformed_table >> dbt_run_staging_aggregate_month >> dbt_test_staging_month >> dbt_run_marts_percentage_query_by_month >> dbt_test_marts_month

# Final validation and documentation
[dbt_test_marts_day, dbt_test_marts_week, dbt_test_marts_month] >> dbt_test_all_marts >> dbt_docs_generate >> success_notification