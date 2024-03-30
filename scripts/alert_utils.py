import requests
import pandas as pd
from scripts.main import connect_to_api

def get_subte_alerts_(base_url, params):
    endpoint_busAlerts = "subtes/serviceAlerts"
    full_url_busPositions = f"{base_url}/{endpoint_busAlerts}"

    formato_json = {'json': 1}
    params_PJalert = params.copy()
    params_PJalert.update(formato_json)

    try:
        r_PJalert = requests.get(full_url_busPositions, params=params_PJalert)
        json_PJalert = r_PJalert.json()

        # Filtrar y normalizar solo la parte necesaria del JSON
        entities = [entity for entity in json_PJalert.get('entity', []) if 'alert' in entity]
        if not entities:
            raise ValueError("No alerts found.")
        
        df_alerts = pd.json_normalize(entities)
        df_alerts['text'] = df_alerts['alert.header_text.translation'].apply(lambda x: x[0]['text'] if x else '')

        # Concatenar el texto de la alerta con su ID
        df_alerts['alert_string'] = df_alerts['id'] + ": " + df_alerts['text']

        # Unir todas las alertas en una sola cadena
        result_string = '\n'.join(df_alerts['alert_string'])

        return result_string

    except ValueError as e:
        print("Warning:", e)

def get_subte_alerts():
    base_url, params = connect_to_api()
    alert = get_subte_alerts_(base_url, params)
    return alert

