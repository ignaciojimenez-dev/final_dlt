import json
import logging
import sys
from typing import Optional, Dict, List, Any
from pyspark.sql import SparkSession


try:
    from pyspark.dbutils import DBUtils # type: ignore
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
except ImportError:
    print("DBUtils no encontrado (Ejecución local?).")
    dbutils = None


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("MetadataValidator")

def load_config(path: str) -> Optional[dict]:
    """Carga un fichero de configuración JSON desde una ruta absoluta."""
    log.info(f"Cargando configuración desde: {path}")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        log.error(f"Error fatal al leer el fichero JSON: {e}")
        return None

def validate_metadata_schema(config: Dict[str, Any]) -> bool:
    """
    Función booleana de validación profunda.
    Retorna True si la estructura es válida para el Pipeline Maestro.
    """
    if not config or "dataflows" not in config:
        log.error("ERROR Falta la clave raíz 'dataflows'")
        return False
    
    if not isinstance(config["dataflows"], list) or len(config["dataflows"]) == 0:
        log.error("ERROR 'dataflows' debe ser una lista no vacía")
        return False

    for idx, flow in enumerate(config["dataflows"]):
        flow_name = flow.get("name", f"Index Name Dataflow :  {idx}")
        
        # 1. Validar Sources
        if "sources" not in flow or not flow["sources"]:
            log.error(f"ERROR Dataflow '{flow_name}' no tiene 'sources'")
            return False
            
        for source in flow["sources"]:
            # Validar que tenga path o paths
            if not source.get("path") and not source.get("paths"):
                log.error(f"ERROR Source '{source.get('name')}' en '{flow_name}' no tiene 'path' ni 'paths'")
                return False
            if "format" not in source:
                log.error(f"ERROR Source '{source.get('name')}' falta 'format'")
                return False

        # 2. Validar Bronze Sinks
        if "bronze_sinks" not in flow:
            log.error(f"ERROR Dataflow '{flow_name}' falta 'bronze_sinks'")
            return False

        # 3. Validar Silver Logic (si existe)
        if "silver_logic" in flow:
            sl = flow["silver_logic"]
            required_silver = ["target_silver_table", "source_bronze", "scd_config"]
            for req in required_silver:
                if req not in sl:
                    log.error(f"ERROR Silver Logic en '{flow_name}' falta campo requerido: {req}")
                    return False
            
            # Validar estructura SCD
            if "keys" not in sl["scd_config"] or "sequence_by" not in sl["scd_config"]:
                log.error(f"ERROR Configuración SCD incompleta en '{flow_name}'")
                return False

    log.info("EXITO! Validación de esquema exitosa.")
    return True

def main():
    if not dbutils:
        log.error("Este script requiere entorno Databricks.")
        return

    # SE LEE EL PARAMETER GLOBAL 
    try:
        config_path_arg = sys.argv[1]
    except IndexError:
        log.error("Falta el argumento de la ruta del fichero JSON")
        sys.exit(1)

    # 1. Cargar
    config = load_config(config_path_arg)
    
    # 2. Validar 
    is_valid = validate_metadata_schema(config)

    if is_valid:
        log.info("Pipeline de validación superado.")
        
        # Pasamos la RUTA validada. La tarea 2 leerá de esa ruta confiable.
        dbutils.jobs.taskValues.set(key="validated_config_path", value=config_path_arg)
        
        log.info(f"Ruta validada '{config_path_arg}' disponible para la siguiente tarea.")
    else:
        log.error("La validación de metadatos falló. Deteniendo el Job.")
        sys.exit(1)

if __name__ == "__main__":
    main()