import requests
import pandas as pd
from main import connect_to_api

def get_df_subte_alerts(base_url, params):

    endpoint_busAlerts = "subtes/serviceAlerts"
    full_url_busPositions = f"{base_url}/{endpoint_busAlerts}"

    formato_json = {'json': 1}

    params_PJalert = params.copy()
    params_PJalert.update(formato_json)

    try:
        r_PJalert = requests.get(full_url_busPositions, params=params_PJalert)
        json_PJalert = r_PJalert.json()
        df_alerts = pd.json_normalize(json_PJalert)

        df_exploded = df_alerts.explode('entity')

        # Normalizar la columna 'entity' para expandir los diccionarios en columnas separadas
        df_normalized = pd.json_normalize(df_exploded['entity'])

        if df_normalized.empty:
            raise ValueError("There is no alerts.")
        else:
            return df_normalized

    except ValueError as e:
        print("Warning:", e)

def string_alert(df_alert):

    df_alert_ = df_alert.explode('alert.header_text.translation')
    df_alert_text = pd.json_normalize(df_alert_['alert.header_text.translation'])

    #Crear un nuevo DataFrame con los valores concatenados
    df_concatenated = df_alert['id'] + ": " + df_alert_text['text']

    # Convertir el nuevo DataFrame a una cadena
    result_string = df_concatenated.to_string(index=False)

    return result_string

def get_subte_alerts():
    base_url, params = connect_to_api()
    df = get_df_subte_alerts(base_url, params)
    alert = string_alert(df)
    return alert

# MAIN
    
# base_url, params = connect_to_api()
# alerts = get_subte_alerts(base_url, params)
# final_alert = string_alert(alerts)
# print(final_alert)