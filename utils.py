from configparser import ConfigParser
from pathlib import Path
import logging

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

def load_to_sql(df, table_name, engine, if_exists="replace"):
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
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method="multi"
            )
        logging.info("Datos cargados exitosamente en la base de datos")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la base de datos: {e}")