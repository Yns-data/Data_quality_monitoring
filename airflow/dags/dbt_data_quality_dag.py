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
    description='Pipeline de qualité des données avec dbt',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['dbt', 'data_quality'],
)

dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='cd /opt/airflow/dbt && dbt deps',
    dag=dag,
)

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='cd /opt/airflow/dbt && dbt run --models staging',
    dag=dag,
)

dbt_run_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command='cd /opt/airflow/dbt && dbt run --models marts',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test',
    dag=dag,
)

dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /opt/airflow/dbt && dbt docs generate',
    dag=dag,
)

def send_success_notification():
    """Fonction pour envoyer une notification de succès"""
    print("Pipeline dbt exécuté avec succès!")

success_notification = PythonOperator(
    task_id='success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_test >> dbt_docs_generate >> success_notification
