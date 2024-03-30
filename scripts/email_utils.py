from airflow.models import Variable
import smtplib
from scripts.alert_utils import get_subte_alerts

def enviar_alerta(**context):

    string_alert = get_subte_alerts()

    try:
        x = smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        
        print(f"Mi clave es: {Variable.get('GMAIL_SECRET')}")
        x.login(
            'blue.photographer01@gmail.com',
            Variable.get('GMAIL_SECRET')
        )

        subject = f'Airflow reporte {context["dag"]} {context["ds"]}'
        body_text = string_alert
        message='Subject: {}\n\n{}'.format(subject,body_text)
        
        x.sendmail('blue.photographer01@gmail.com', 'blue.photographer01@gmail.com', message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')