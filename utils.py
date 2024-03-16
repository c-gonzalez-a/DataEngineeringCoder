from configparser import ConfigParser
from pathlib import Path
import logging
import sqlalchemy as sa


def read_api_credentials(config_file: Path, section: str) -> dict:
    """
    Lee las credenciales de la API desde un archivo de configuracion

    Parametros:
    config_file: Ruta del archivo de configuracion
    section: seccion del archivo de configuracion que contiene las credenciales
    """
    config = ConfigParser()
    config.read(config_file)
    api_credentials = dict(config[section])
    return api_credentials

# def load_to_sql(df, table_name, conn, if_exists="replace"):
#     """
#     Carga un DataFrame en la base de datos especificada.

#     Parameters:
#     df (pandas.DataFrame): El DataFrame a cargar en la base de datos.
#     table_name (str): El nombre de la tabla en la base de datos.
#     engine (sqlalchemy.engine.base.Engine): Un objeto de conexión a la base de datos.
#     if_exists (str): "append OR replace"
#     """
#     try:
#         logging.info("Cargando datos en la base de datos...")

#         df.to_sql(
#             table_name,
#             conn,
#             if_exists='replace',
#             index=False,
#             method="multi"
#             )

#         print("Datos cargados exitosamente en la base de datos")
#     except Exception as e:
#         print(f"Error al cargar los datos en la base de datos: {e}")

def load_to_sql(df, table_name, conn, if_exists="replace"):
    """
    Carga un DataFrame en la base de datos especificada.

    Parameters:
    df (pandas.DataFrame): El DataFrame a cargar en la base de datos.
    table_name (str): El nombre de la tabla en la base de datos.
    engine (sqlalchemy.engine.base.Engine): Un objeto de conexión a la base de datos.
    if_exists (str): "append OR replace"
    """
    try:
        logging.info("Cargando datos en la base de datos...")

        # Iterar sobre los registros del DataFrame e insertar uno por uno
        for index, row in df.iterrows():
            try:
                # Crear una consulta SQL para insertar el registro actual
                insert_query = sa.text(f"INSERT INTO {table_name} VALUES ({', '.join([':'+str(col) for col in df.columns])})")

                # Ejecutar la consulta SQL con los valores del registro actual
                conn.execute(insert_query, **row)
            except Exception as e:
                print(f"Error al insertar el registro {index}: {e}")
        
        print("Datos cargados exitosamente en la base de datos")
    except Exception as e:
        print(f"Error al cargar los datos en la base de datos: {e}")