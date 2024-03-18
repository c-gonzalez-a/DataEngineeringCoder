from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.main import data_ingestion

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='my_daily_dag',
    default_args=default_args,
    description='DAG para ejecutar tareas diarias',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 3, 16),
    catchup=False,
)

task_1 = PythonOperator(
    task_id='transformar_data',
    python_callable=data_ingestion,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=dag,
)