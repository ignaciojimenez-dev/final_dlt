import json
import logging
import sys
from typing import Optional
from pyspark.dbutils import DBUtils # type: ignore
from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
except ImportError:
    print("DBUtils no encontrado.")
    dbutils = None
    
# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)



def load_config(path: str) -> Optional[dict]:
    """
    Carga un fichero de configuración JSON desde una ruta absoluta.
    """
    config_path = path
    
    log.info(f"Cargando configuración desde: {config_path}")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        log.info("Configuración cargada exitosamente.")
        return config
    except Exception as e:
        log.error(f"Error inesperado al cargar {config_path}: {e}")
        return None

def main():
    if not dbutils:
        log.error("DBUtils no está disponible. Este script debe ejecutarse como un Job de Databricks.")
        return

    # leemos el parametro de inputs , ruta del volumen en UNIT catalog donde esta el metadatos
    config_path = sys.argv[1]
    log.info(f"Tarea 1: Iniciando. Leyendo metadatos desde {config_path}")

    # Cargar la configuracion del json de metadatos
    config = load_config(config_path)

    # comprobamos que el fichero no esta vacio y hay dataflow
    ##### añadir mas validaciones al metadatos, existe dataflows, estos contienen sinks,
    ##### sources  tranformations , y a su vez estos tienen input params etc

    #CAMBIAR CONFIG POR FUNCT BOOLEANA QUE ESTA VALIDADO O NO
    
    if config and 'dataflows' in config:
        dataflows_list = config['dataflows']
        log.info(f"Metadatos cargados. {len(dataflows_list)} dataflows encontrados.")
        
        # 2. Pasar la lista de de dict's de dataflows a la Tarea 2
        # se convierte  la lista de diccionarios a un string JSON
        dbutils.jobs.taskValues.set(key="dataflows_config_list", value=dataflows_list)
        
        log.info(f"Lista de {len(dataflows_list)} dataflows pasada a la siguiente tarea.")
        
    else:
        log.error("No se pudieron cargar los metadatos o la clave 'dataflows' no existe.")
        raise ValueError("Error al cargar la configuración de dataflows.")

if __name__ == "__main__":
    main()