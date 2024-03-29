#### SETUP

import requests
import pandas as pd
from scripts.utils import read_api_credentials, load_to_sql
from datetime import datetime
import sqlalchemy as sa

#### FUNCIONES

def load_data_to_df(df_origen, df_destino, columnas_a_considerar):
    """
    Carga en el df_destino el contenido del df_origen solo de la "columnas a considerar".  Tienen que llamarse igual en ambos df.

    Parametros:
    df_origen: Dataframe de origen
    df_destino: Dataframe de destino
    columnas_a_considerar: Lista de strings, con el nombre de las columnas que se tendran en cuenta del df_origen para pasar al df_destino
    """
    
    df_fusionado = pd.merge(df_destino, df_origen, on=columnas_a_considerar, how='outer')
    
    df_destino_actualizado = df_fusionado[columnas_a_considerar]
    
    return df_destino_actualizado

def load_df_ecobici_stations_status(df_origen, df_destino): 
    """
    Carga en el df_destino el contenido del df_origen. Considerando solo las columnas que estan en la tabla de la db de ecobici_stations_status

    Parametros:
    df_origen: Dataframe de origen
    df_destino: Dataframe de destino
    """

    columnas_a_considerar = ['station_id', 'num_bikes_available_mechanical', 'num_bikes_available_ebike', 'num_bikes_available', 'num_bikes_disabled', 'status']

    return load_data_to_df(df_origen, df_destino, columnas_a_considerar)

def load_df_ecobici_stations(df_origen, df_destino): 
    """
    Carga en el df_destino el contenido del df_origen. Considerando solo las columnas que estan en la tabla de la db de ecobici_stations

    Parametros:
    df_origen: Dataframe de origen
    df_destino: Dataframe de destino
    """

    columnas_a_considerar = ['station_id', 'name', 'address', 'capacity', 'lat', 'lon', 'neighborhood']

    return load_data_to_df(df_origen, df_destino, columnas_a_considerar)

def load_df_bus_positions(df_origen, df_destino): 
    """
    Carga en el df_destino el contenido del df_origen. Considerando solo las columnas que estan en la tabla de la db de bus_positions

    Parametros:
    df_origen: Dataframe de origen
    df_destino: Dataframe de destino
    """

    columnas_a_considerar = ['id', 'agency_id', 'route_id', 'latitude', 'longitude', 'speed', 'timestamp', 'route_short_name', 'trip_headsign']
    
    return load_data_to_df(df_origen, df_destino, columnas_a_considerar)

def load_df_agencies(fila_origen, df_destino):
    """
    Recibe una fila y carga la columna agency_name y agency_id en el df_destino

    Parametros:
    fila_origen: Fila de un df
    df_destino: Dataframe de destino
    """

    # Obtener los datos de agency_name y agency_id de la fila de origen
    agency_name = fila_origen['agency_name']
    agency_id = fila_origen['agency_id']

    # Agregar los datos al DataFrame de destino
    nueva_fila = pd.DataFrame({'agency_name': [agency_name], 'agency_id': [agency_id]})

    # Concatenar df_destino y nueva_fila a lo largo de las filas
    df_destino_actualizado = pd.concat([df_destino, nueva_fila], ignore_index=True)

    return df_destino_actualizado

#### CONEXION CON API

def connect_to_api():

    base_url = "https://apitransporte.buenosaires.gob.ar"

    api_keys = read_api_credentials("config/pipeline.conf", "api_transporte")

    params = { 
        "client_id" : api_keys["client_id"],
        "client_secret" : api_keys["client_secret"]
    }

    formato_json = {'json': 1}

    return base_url,params

#### EXTRACCION Y TRANSFORMACION DE DATOS DE LOS BUS

def get_bus_data(base_url, params):

    endpoint_bus = "colectivos"

    la_nueva_metropol = {'agency_id': 9}
    primera_junta = {'agency_id': 145}
    talp = {'agency_id': 155}

    # CRACION DE DF PARA GUARDAR DATOS

    # Creo un DataFrame para agencies vacio con las columnas que tendra en la base de datos
    df_agencies = pd.DataFrame(columns=['agency_id', 'agency_name'])
    df_agencies = df_agencies.astype({'agency_id': 'int', 'agency_name': 'str'})

    # Igual para la tabla de posiciones del bus
    column_specifications = {
        'id': str,
        'agency_id': int,
        'route_id': str,
        'latitude': float,
        'longitude': float,
        'speed': float,
        'timestamp': int,
        'route_short_name': str,
        'trip_headsign': str
    }
    df_bus_positions = pd.DataFrame(columns=column_specifications.keys())
    for column, dtype in column_specifications.items():
        df_bus_positions[column] = df_bus_positions[column].astype(dtype)

    # Obtencion de la posición de los vehículos monitoreados actualizada cada 30 segundos. 
    # Si no se pasan parámetros de entrada, retorna la posición actual de todos los vehículos monitoreados.

    endpoint_busPositions = f"{endpoint_bus}/vehiclePositionsSimple"
    full_url_busPositions = f"{base_url}/{endpoint_busPositions}"

    # ACCEDER DATOS DE PJ

    params_PJPositions = params.copy()
    params_PJPositions.update(primera_junta)

    try:
        r_PJPositions = requests.get(full_url_busPositions, params=params_PJPositions)
        json_PJData = r_PJPositions.json()
        df_PJPositions = pd.json_normalize(json_PJData)
    
        if df_PJPositions.empty:
            raise ValueError("There is no information about the bus positions.")

        # TRANSFORMACION DE LOS DATOS

        # Suponiendo que df_PJPositions es tu DataFrame y 'timestamp' es la columna que deseas convertir
        df_PJPositions['timestamp'] = pd.to_datetime(df_PJPositions['timestamp'], unit='s')
        
        # CARGADO DE PJ AL DF

        df_bus_positions = load_df_bus_positions(df_PJPositions, df_bus_positions)
        df_agencies = load_df_agencies(df_PJPositions.iloc[0], df_agencies)

    except ValueError as e:
        print("Warning Primera Junta:", e)

    # ACCEDER DATOS DE LA NUEVA METROPOL

    # params_NMPositions = params.copy()
    # params_NMPositions.update(la_nueva_metropol)
    # r_NMPositions = requests.get(full_url_busPositions, params=params_NMPositions)

    # json_NMData = r_NMPositions.json()
    # df_NMPositions = pd.json_normalize(json_NMData)

    # CARGADO DE LA NUEVA METROPOL AL DF

    # df_bus_positions = load_df_bus_positions(df_NMPositions, df_bus_positions)
    # df_agencies = load_df_agencies(df_NMPositions.iloc[0], df_agencies)

    # ACCEDER DATOS DE TALP

    # params_TALPPositions = params.copy()
    # params_TALPPositions.update(talp)
    # r_TALPPositions = requests.get(full_url_busPositions, params=params_TALPPositions)

    # json_TALPData = r_TALPPositions.json()
    # df_TALPPositions = pd.json_normalize(json_TALPData)

    # CARGADO DEL TALP

    # df_bus_positions = load_df_bus_positions(df_TALPPositions, df_bus_positions)
    # # Convierte el timestamp a un objeto datetime
    # df_bus_positions['timestamp'] = pd.to_datetime(df_bus_positions['timestamp'], unit='s')
    # df_agencies = load_df_agencies(df_TALPPositions.iloc[0], df_agencies)

    return df_bus_positions, df_agencies

# EXTRACCION Y TRANSFORMACION DE DATOS DE LAS ESTACIONES DE ECOBICI
    
def get_ecobici_data(base_url, params):

    endpoint_ecobici = "ecobici/gbfs"

    column_specifications = {
        'station_id': int,
        'name': str,
        'address': str,
        'capacity': int,
        'lat': float,
        'lon': float,
        'neighborhood': str
    }

    df_ecobici_stations = pd.DataFrame(columns=column_specifications.keys())

    for column, dtype in column_specifications.items():
        df_ecobici_stations[column] = df_ecobici_stations[column].astype(dtype)

    # Creacion del df para la tabla que da datos del estado de las estaciones de ecobicis (informacion dinamica)

    column_specifications = {
        'station_id': int,
        'num_bikes_available_mechanical': int,
        'num_bikes_available_ebike': int,
        'num_bikes_available': int,
        'num_bikes_disabled': int,
        'status': str
    }

    df_ecobici_stations_status = pd.DataFrame(columns=column_specifications.keys())

    for column, dtype in column_specifications.items():
        df_ecobici_stations_status[column] = df_ecobici_stations_status[column].astype(dtype)

    # Listado estático de todas las estaciones, sus capacidades y ubicaciones

    endpoint_ecobiciSI = f"{endpoint_ecobici}/stationInformation"

    full_url_ecobiciSI = f"{base_url}/{endpoint_ecobiciSI}"

    r_ecobiciSI = requests.get(full_url_ecobiciSI, params=params)

    json_ecobiciSI = r_ecobiciSI.json()

    data_ecobiciSI= json_ecobiciSI['data']['stations']
    df_ecobiciSI = pd.DataFrame(data_ecobiciSI)

    # Aplicar una función para extraer el primer elemento de cada lista en la columna "groups"
    df_ecobiciSI['neighborhood'] = df_ecobiciSI['groups'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)

    # Eliminar la columna "groups"
    df_ecobiciSI.drop('groups', axis=1, inplace=True)

    # Filtro para solo guardar los barrios que frecuento
    neighborhoods = ["RETIRO", "PUERTO MADERO", "SAN CRISTOBAL", "BOCA", "BOEDO", "SAN TELMO", "PARQUE PATRICIOS"]
    df_ecobiciSI = df_ecobiciSI[df_ecobiciSI['neighborhood'].isin(neighborhoods)]

    df_ecobici_stations = load_df_ecobici_stations(df_ecobiciSI, df_ecobici_stations)

    # Obtencion del número de bicicletas y anclajes disponibles en cada estación y disponibilidad de estación.

    endpoint_ecobiciSS = f"{endpoint_ecobici}/stationStatus"

    full_url_ecobiciSS = f"{base_url}/{endpoint_ecobiciSS}"

    r_ecobiciSS = requests.get(full_url_ecobiciSS, params=params)

    json_ecobiciSS = r_ecobiciSS.json()
    data_ecobiciSS= json_ecobiciSS['data']
    df_ecobiciSS = pd.DataFrame(data_ecobiciSS)

    # Para pasar el json a una dataframe

    data_ecobiciSS= json_ecobiciSS['data']['stations']
    df_ecobiciSS = pd.DataFrame(data_ecobiciSS)

    # Filtrado de estaciones de ecobici que me interesan
    df_ecobiciSS = df_ecobiciSS[df_ecobiciSS['station_id'].isin(df_ecobiciSI['station_id'])]

    # Extraer los valores de las claves 'mechanical' y 'ebike' en nuevas columnas, porque asi lo arme en la tabla de la DB
    df_ecobiciSS['num_bikes_available_mechanical'] = df_ecobiciSS['num_bikes_available_types'].apply(lambda x: x['mechanical'] if isinstance(x, dict) and 'mechanical' in x else None)
    df_ecobiciSS['num_bikes_available_ebike'] = df_ecobiciSS['num_bikes_available_types'].apply(lambda x: x['ebike'] if isinstance(x, dict) and 'ebike' in x else None)

    # Eliminar la columna original 'num_bikes_available_types'
    df_ecobiciSS.drop('num_bikes_available_types', axis=1, inplace=True)

    df_ecobici_stations_status = load_df_ecobici_stations_status(df_ecobiciSS, df_ecobici_stations_status)

    # Falta agregar en cada elemento el tiempo
    last_updated_timestamp = json_ecobiciSI['last_updated']

    # Convierte el timestamp a un objeto datetime
    last_updated_datetime = datetime.fromtimestamp(last_updated_timestamp)

    # Crea una nueva columna en el DataFrame con el valor de "last_updated"
    df_ecobici_stations_status['last_reported'] = last_updated_datetime

    return df_ecobici_stations, df_ecobici_stations_status

#### CONEXION CON BASE DE DATOS

def crear_tablas_en_db(conn):
    try:
        conn.execute("""
            DROP TABLE camilagonzalezalejo02_coderhouse.agencies;
            create table if not exists  camilagonzalezalejo02_coderhouse.agencies
            (       	
            agency_id INTEGER,
            agency_name VARCHAR(100)
            )
        DISTSTYLE ALL
        sortkey(agency_id)
        """)
        #conn.commit()
        print("Agencies table created successfully in DB")
        
    except Exception as e:
        print("Unable to create agencies table in DB: ")
        print(e)

    try:
        conn.execute("""
            DROP TABLE camilagonzalezalejo02_coderhouse.bus_positions;
            create table if not exists  camilagonzalezalejo02_coderhouse.bus_positions
            (	
            id INTEGER,
            agency_id INTEGER,
            route_id INTEGER,
            latitude NUMERIC,
            longitude NUMERIC,
            speed NUMERIC,
            timestamp TIMESTAMP,
            route_short_name VARCHAR,
            trip_headsign VARCHAR
            )
        DISTKEY (agency_id)
        sortkey(agency_id)   
        """)
        print("bus_positions table created successfully in DB")
        
    except Exception as e:
        print("Unable to create tabla bus_positions in DB: ")
        print(e)

    try:
        conn.execute("""
            DROP TABLE camilagonzalezalejo02_coderhouse.ecobici_stations;
            create table if not exists  camilagonzalezalejo02_coderhouse.ecobici_stations
            (       	
            station_id INTEGER,
            name VARCHAR,
            address VARCHAR,
            capacity INTEGER,
            lat NUMERIC,   
            lon NUMERIC, 
            neighborhood VARCHAR      
            )
        DISTKEY (station_id)
        sortkey(station_id)
        """)
        print("ecobici_stations table created successfully in DB")
        
    except Exception as e:
        print("Unable to create tabla ecobici_stations in DB: ")
        print(e)

    try:
        conn.execute("""
            DROP TABLE camilagonzalezalejo02_coderhouse.ecobici_stations_status;
            create table if not exists  camilagonzalezalejo02_coderhouse.ecobici_stations_status
            (  
            station_id INTEGER,     	
            num_bikes_available_mechanical INTEGER, 
            num_bikes_available_ebike INTEGER,
            num_bikes_available INTEGER,
            num_bikes_disabled INTEGER,
            status VARCHAR,
            last_reported TIMESTAMP         
            )
        DISTKEY (station_id)
        sortkey(station_id)
        """)
        print("ecobici_stations_status table created successfully in DB")
        
    except Exception as e:
        print("Unable to create ecobici_stations_status in DB: ")
        print(e)

def connect_to_redshift():
    
    db_keys = read_api_credentials("config/pipeline.conf", "RedShift")

    try:  
        string_conn =  f"postgresql://{db_keys['user']}:{db_keys['pwd']}@{db_keys['host']}:{db_keys['port']}/{db_keys['dbname']}"
        engine = sa.create_engine(string_conn)
        conn = engine.connect()

        print("Conectado a Redshift con éxito!")

        return conn
        
    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)

#### SUBIDA DE DATOS REDSHIFT
        
def upload_data_to_redshift(df_agencies, df_bus_positions, df_ecobici_stations, df_ecobici_stations_status, conn):
    
    #string_conn =  f"postgresql://{db_keys['user']}:{db_keys['pwd']}@{db_keys['host']}:{db_keys['port']}/{db_keys['dbname']}"

    #engine = sa.create_engine(string_conn)

    #conn = engine.connect()

    load_to_sql(df_agencies, "agencies", conn)

    load_to_sql(df_bus_positions, "bus_positions", conn)

    load_to_sql(df_ecobici_stations, "ecobici_stations", conn)

    load_to_sql(df_ecobici_stations_status, "ecobici_stations_status", conn)

#### ETL PROCESS SUMMARY
    
def etl_process(base_url, params, conn):

    df_bus_positions, df_agencies = get_bus_data(base_url, params)

    df_ecobici_stations, df_ecobici_stations_status = get_ecobici_data(base_url, params)

    crear_tablas_en_db(conn)

    upload_data_to_redshift(df_agencies, df_bus_positions, df_ecobici_stations, df_ecobici_stations_status, conn)

    conn.close()


# DATA INGESTION
    
def data_ingestion():
    
    base_url, params = connect_to_api()

    try:
        conn = connect_to_redshift()

        if conn is not None:

            df_bus_positions, df_agencies = get_bus_data(base_url, params)

            df_ecobici_stations, df_ecobici_stations_status = get_ecobici_data(base_url, params)

            crear_tablas_en_db(conn)

            upload_data_to_redshift(df_agencies, df_bus_positions, df_ecobici_stations, df_ecobici_stations_status, conn)

            conn.close()
        
    except Exception as e:
        
        print(e)

# MAIN
    
data_ingestion()

