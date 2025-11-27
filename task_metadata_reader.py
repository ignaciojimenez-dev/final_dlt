import json
import logging
import sys
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

try:
    from pyspark.dbutils import DBUtils # type: ignore
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
except ImportError:
    print("DBUtils no encontrado.")
    dbutils = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("MetadataValidator")

def load_config(path: str) -> Optional[dict]:
    log.info(f"Cargando configuración desde: {path}")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        log.error(f"Error fatal al leer JSON: {e}")
        return None

def validate_metadata_schema(config: Dict[str, Any]) -> bool:
    """Valida que la estructura del JSON sea correcta para el Monolito."""
    if not config or "dataflows" not in config:
        log.error("❌ Falta la clave raíz 'dataflows'")
        return False
    
    if not isinstance(config["dataflows"], list) or len(config["dataflows"]) == 0:
        log.error("❌ 'dataflows' debe ser una lista no vacía")
        return False

    # Validaciones rápidas de estructura interna
    for idx, flow in enumerate(config["dataflows"]):
        flow_name = flow.get("name", f"Index {idx}")
        
        if "sources" not in flow or not flow["sources"]:
            log.error(f"❌ '{flow_name}' no tiene 'sources'")
            return False
            
        if "silver_logic" in flow:
            sl = flow["silver_logic"]
            if "target_silver_table" not in sl:
                log.error(f"❌ Silver Logic en '{flow_name}' falta target")
                return False

    log.info(f"✅ Validación exitosa. Se encontraron {len(config['dataflows'])} flujos listos para procesar.")
    return True

def main():
    if not dbutils:
        log.error("Requiere entorno Databricks.")
        return

    try:
        config_path_arg = sys.argv[1]
    except IndexError:
        log.error("Falta argumento: ruta del JSON")
        sys.exit(1)

    config = load_config(config_path_arg)
    is_valid = validate_metadata_schema(config)

    if is_valid:
        # En el Monolito, Task 1 solo actúa de "Semáforo Verde".
        # No necesitamos pasar listas complejas, solo confirmar que Task 2 puede leer el fichero.
        log.info("Semáforo en VERDE. Iniciando orquestación monolítica.")
        dbutils.jobs.taskValues.set(key="validation_status", value="OK")
    else:
        log.error("Semáforo en ROJO. Deteniendo Job.")
        sys.exit(1)

if __name__ == "__main__":
    main()