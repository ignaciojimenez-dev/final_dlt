import json
import logging
import sys
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession # type: ignore

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
    """Valida que la estructura del JSON sea correcta para el Monolito (Bronze, Silver y Gold)."""
    
    # 1. Validaciones Raíz
    if not config or "dataflows" not in config:
        log.error("XXXX Falta la clave raíz 'dataflows'")
        return False
    
    if not isinstance(config["dataflows"], list) or len(config["dataflows"]) == 0:
        log.error("XXXX 'dataflows' debe ser una lista no vacía")
        return False

    # 2. Validaciones por Flujo
    for idx, flow in enumerate(config["dataflows"]):
        flow_name = flow.get("name", f"Index {idx}")
        
        # --- A. VALIDAR SOURCES ---
        if "sources" not in flow or not flow["sources"]:
            log.error(f"XXXX '{flow_name}' no tiene 'sources'")
            return False
        
        # --- B. VALIDAR SILVER Lista de transformaciones ---

        if "transformations_silver" in flow:
            for i, sl in enumerate(flow["transformations_silver"]):
                if "target_silver_table" not in sl:
                    log.error(f"XXXX Silver en '{flow_name}' (item {i}): falta 'target_silver_table'")
                    return False
                if "source_bronze" not in sl:
                    log.error(f"XXXX Silver en '{flow_name}' (item {i}): falta 'source_bronze'")
                    return False

        # --- C. VALIDAR GOLD  ---
        if "transformations_gold" in flow:
            for i, gl in enumerate(flow["transformations_gold"]):
                if "target_gold_table" not in gl:
                    log.error(f"XXXX Gold en '{flow_name}' (item {i}): falta 'target_gold_table'")
                    return False
                if "source_silver" not in gl:
                    log.error(f"XXXX Gold en '{flow_name}' (item {i}): falta 'source_silver'")
                    return False

    log.info(f"OKKK Validación exitosa. Se encontraron {len(config['dataflows'])} flujos revisados (Bronze, Silver, Gold).")
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
        log.info("PERFECT. Iniciando orquestación monolítica.")
        dbutils.jobs.taskValues.set(key="validation_status", value="OK")
    else:
        log.error("MAL. El JSON tiene errores estructurales. Deteniendo Job.")
        sys.exit(1)

if __name__ == "__main__":
    main()