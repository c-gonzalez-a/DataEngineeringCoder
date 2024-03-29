from airflow.models import Variable
import smtplib

def enviar(**context):
    try:
        x = smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        
        print(f"Mi clave es: {Variable.get('GMAIL_SECRET')}")
        x.login(
            'blue.photographer01@gmail.com',
            Variable.get('GMAIL_SECRET')
        )

        subject = f'Airflow report {context["dag"]} {context["ds"]}'
        body_text = f'Task {context["task_instance_key_str"]} execute'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        
        x.sendmail('blue.photographer01@gmail.com', 'blue.photographer01@gmail.com', message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')