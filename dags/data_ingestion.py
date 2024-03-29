from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.main import data_ingestion

default_args = {
    'owner': 'Camila Gonzalez',
    'email': ['shark.of.diamond@gmail.com'],
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='my_daily_dag',
    default_args=default_args,
    description='DAG para ejecutar tareas diarias',
    schedule_interval="@daily",
    start_date=datetime(2024, 3, 28),
    catchup=False,
)

# Definir la hora específica a la que se ejecutará la tarea (en este caso, 1:00 AM)
# execution_time = datetime.now() + timedelta(days=1)
# execution_time = execution_time.replace(hour=1, minute=0, second=0, microsecond=0)

task_1 = PythonOperator(
    task_id='transformar_data',
    python_callable=data_ingestion,
    dag=dag,
)