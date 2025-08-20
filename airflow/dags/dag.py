from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Fonction générique pour exécuter un script Python et afficher stdout/stderr
def run_script(script_path):
    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True
    )
    print(f"=== STDOUT ({script_path}) ===\n{result.stdout}")
    print(f"=== STDERR ({script_path}) ===\n{result.stderr}")
    result.check_returncode()  # lève une erreur si exit code != 0

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="multi_scripts_every_5min",
    default_args=default_args,
    start_date=datetime(2025, 8, 12),
    schedule_interval="0 * * * *",  # toutes les heures
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="run_script1",
        python_callable=run_script,
        op_args=["/opt/airflow/scripts/test_db.py"]
    )

    task2 = PythonOperator(
        task_id="run_script2",
        python_callable=run_script,
        op_args=["/opt/airflow/scripts/test_sql_query_gen.py"]
    )

    # Ordonnancement : task1 s'exécute avant task2
    task1 >> task2