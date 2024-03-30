from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from scripts.email_utils import enviar
from scripts.alert_utils import get_subte_alerts

def _get_subte_alerts(ti):
    alert = get_subte_alerts()
    ti.xcom_push(key='alert', value=alert)

def _enviar_mail(ti):
    msg = ti.xcom_pull(key='alert', task_ids='Get_subte_alert')
    enviar(msg)

with DAG( 
    dag_id='dag_email_alert',
    schedule_interval="* * * * *",
    on_success_callback=None,
    catchup=False,
    start_date=datetime(2024,3,27)
):
    task_1 = PythonOperator(
        task_id = 'Get_subte_alert',
        python_callable = _get_subte_alerts,
        )
    task_2 = PythonOperator(
        task_id = 'send_email',
        python_callable = _enviar_mail,
        )
    
    # Definir las dependencias entre tareas
    task_1 >> task_2