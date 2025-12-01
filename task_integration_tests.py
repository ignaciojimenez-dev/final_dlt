# task_integration_tests.py
import sys
import json
import logging
from pyspark.sql import SparkSession

# Setup simple
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [PRE-FLIGHT] - %(message)s')
log = logging.getLogger("IntegrationTests")
spark = SparkSession.builder.getOrCreate()

try:
    from pyspark.dbutils import DBUtils # type: ignore
    dbutils = DBUtils(spark)
except ImportError:
    log.error("XXXX Este test requiere entorno Databricks (DBUtils).")
    sys.exit(1)

def run_preflight_checks(metadata_path, target_schema_full):
    """
    Valida que el entorno y los inputs existan antes de arrancar la maquinaria pesada.
    """
    errors = []
    
    # 1. CHECK METADATOS (Accesibilidad básica)
    log.info(f"🔍 1. Verificando lectura de metadatos: {metadata_path}")
    try:
        with open(metadata_path, 'r') as f:
            config = json.load(f)
    except Exception as e:
        log.error(f"XXXX No se puede leer el fichero de metadatos. Abortando.")
        sys.exit(1)

    # 2. CHECK INFRAESTRUCTURA (Target Schema)
    # Asumimos formato "catalogo.esquema"
    log.info(f" 2. Verificando infraestructura destino: {target_schema_full}")
    try:
        if "." in target_schema_full:
            cat, sch = target_schema_full.split(".")
            # Verificamos si spark puede ver el esquema
            check = spark.sql(f"SHOW SCHEMAS IN {cat} LIKE '{sch}'").collect()
            if not check:
                 errors.append(f"El esquema '{target_schema_full}' no existe o no hay permisos.")
        else:
            errors.append(f"Formato de esquema incorrecto: {target_schema_full}. Se espera 'catalogo.esquema'")
    except Exception as e:
        errors.append(f"Error conectando al catálogo: {e}")

    # 3. CHECK FUENTES (Solo accesibilidad de rutas)
    log.info(" 3. Verificando accesibilidad a rutas fuente (Sources)...")
    dataflows = config.get("dataflows", [])
    
    for flow in dataflows:
        sources = flow.get("sources", [])
        for src in sources:
            # Obtiene path o paths
            p_raw = src.get("path") or src.get("paths")
            paths = [p_raw] if isinstance(p_raw, str) else p_raw
            
            for p in paths:
                # Quitamos wildcards para validar la carpeta contenedora
                clean_path = p.split("*")[0]
                try:
                    dbutils.fs.ls(clean_path)
                    # Si no da error, es que accedemos. No imprimimos nada para no ensuciar, salvo error.
                except Exception as e:
                    # Si falla, es un riesgo alto de fallo en Pipeline
                    errors.append(f"Fuente inaccesible en flujo '{flow.get('name')}': {clean_path}")

    if errors:
        log.error("ERROR ! PRE-FLIGHT CHECKS FALLIDOS:")
        for e in errors:
            log.error(f"   - {e}")
        sys.exit(1) # Rompe el Job aquí
    else:
        log.info("OKKK Todo listo. Iniciando orquestación.")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        run_preflight_checks(sys.argv[1], sys.argv[2])
    else:
        log.error("Faltan argumentos.")
        sys.exit(1)