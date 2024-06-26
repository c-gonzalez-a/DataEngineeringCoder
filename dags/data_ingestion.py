from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.main import connect_to_api, etl_process

def _connect_to_api(ti):
    base_url, params = connect_to_api()
    ti.xcom_push(key='base_url', value=base_url)
    ti.xcom_push(key='params', value=params)

def _etl_process(ti):
    base_url = ti.xcom_pull(key='base_url', task_ids='API_connection')
    params = ti.xcom_pull(key='params', task_ids='API_connection')
    etl_process(base_url, params)

default_args = {
    'owner': 'Camila Gonzalez',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='my_daily_dag',
    default_args=default_args,
    description='DAG para ejecutar tareas diarias',
    #schedule_interval="@daily",
    schedule_interval = timedelta(minutes=60),
    start_date=datetime(2024, 3, 28),
    catchup=False,
):
    task_1 = PythonOperator(
        task_id = 'API_connection',
        python_callable = _connect_to_api,
        )
    task_2 = PythonOperator(
        task_id = 'ETL_process',
        python_callable = _etl_process,
        )
    
    # Definir las dependencias entre tareas
    task_1 >> task_2