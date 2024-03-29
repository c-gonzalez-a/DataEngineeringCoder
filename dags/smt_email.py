from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from scripts.email_utils import enviar

with DAG( 
    dag_id='dag_smtp_email_automatico',
    schedule_interval="* * * * *",
    on_success_callback=None,
    catchup=False,
    start_date=datetime(2024,3,27)
):
    task_1 = PythonOperator(
        task_id='send_email',
        python_callable=enviar,
        )