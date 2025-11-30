import json
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("SinkExecutor")

# Inicializar Spark (necesario para jobs normales, no DLT)
spark = SparkSession.builder.getOrCreate()

# Intentar importar dbutils
try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except ImportError:
    log.warning("DBUtils no disponible (¿ejecución local?).")
    dbutils = None

def load_config(path: str):
    """Carga el JSON de metadatos."""
    log.info(f"📂 Cargando configuración desde: {path}")
    try:
        # Si la ruta empieza por /dbfs o /Volumes, python open() suele funcionar en Databricks
        # si es dbfs:/ puro, mejor usar dbutils o spark.read.text
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log.error(f"❌ Error fatal leyendo el JSON: {e}")
        raise e

def run_sinks(metadata_path, source_catalog_schema):
    """
    Recorre los metadatos, busca 'sinks' y escribe los DataFrames.
    
    :param metadata_path: Ruta al archivo JSON.
    :param source_catalog_schema: Nombre del esquema (o catalogo.esquema) donde DLT escribió las tablas.
                                  Ej: "mi_catalogo.mi_esquema" o simplemente "mi_esquema".
    """
    config = load_config(metadata_path)
    dataflows = config.get("dataflows", [])
    
    log.info(f"🚀 Iniciando procesamiento de Sinks para {len(dataflows)} flujos de datos.")
    log.info(f"🔎 Buscando tablas origen en: {source_catalog_schema}")

    success_count = 0
    fail_count = 0

    for flow in dataflows:
        flow_name = flow.get("name", "Unnamed Flow")
        sinks = flow.get("sinks", [])
        
        if not sinks:
            log.info(f"ℹ️ Flujo '{flow_name}' no tiene sinks definidos. Saltando.")
            continue

        log.info(f"▶ Procesando sinks para flujo: {flow_name}")

        for sink in sinks:
            try:
                # 1. Extraer parámetros del JSON
                input_table_name = sink["input"]   # Nombre de la tabla DLT (ej: silver_person_clean)
                output_format = sink["format"]     # ej: DELTA, JSON, PARQUET
                save_mode = sink.get("saveMode", "append") # default append si no viene
                
                # Manejo de inconsistencia 'path' vs 'paths' en tu JSON
                output_path = sink.get("path") or sink.get("paths")
                
                if not output_path:
                    log.error(f"⚠️ Sink para '{input_table_name}' no tiene 'path' definido. Omitiendo.")
                    fail_count += 1
                    continue

                # 2. Construir referencia a la tabla origen
                # Asumimos que DLT publicó la tabla en source_catalog_schema
                full_source_table = f"{source_catalog_schema}.{input_table_name}"
                
                log.info(f"   🔄 Leyendo tabla: {full_source_table}")
                
                # 3. Leer DataFrame
                df = spark.read.table(full_source_table)
                
                # 4. Escribir (Sink)
                log.info(f"   💾 Escribiendo en: {output_path} | Formato: {output_format} | Modo: {save_mode}")
                
                (df.write
                   .format(output_format)
                   .mode(save_mode)
                   .save(output_path))
                
                log.info(f"   ✅ Sink completado exitosamente para {input_table_name}")
                success_count += 1

            except AnalysisException as ae:
                log.error(f"   ❌ Error: No se encontró la tabla origen '{full_source_table}'. ¿Ejecutó el DLT correctamente? Detalle: {ae}")
                fail_count += 1
            except Exception as e:
                log.error(f"   ❌ Error inesperado escribiendo sink para '{sink.get('input', 'unknown')}': {e}")
                fail_count += 1

    log.info("="*50)
    log.info(f"🏁 Proceso finalizado. Exitosos: {success_count} | Fallidos: {fail_count}")
    
    if fail_count > 0:
        raise RuntimeError("Hubo errores durante la ejecución de los Sinks. Revisa los logs.")

if __name__ == "__main__":
    # Configuración de Widgets para recibir parámetros del Job de Databricks
    if dbutils:
        dbutils.widgets.text("metadata_file_path", "", "Ruta al JSON de Metadatos")
        dbutils.widgets.text("target_schema", "", "Esquema Destino DLT (ej: hive_metastore.mi_schema)")

        path_arg = dbutils.widgets.get("metadata_file_path")
        schema_arg = dbutils.widgets.get("target_schema")
    else:
        # Fallback para pruebas locales si se pasan por sys.argv
        if len(sys.argv) > 2:
            path_arg = sys.argv[1]
            schema_arg = sys.argv[2]
        else:
            log.error("Faltan argumentos. Uso: task_sinks.py <ruta_json> <esquema_dlt>")
            sys.exit(1)

    if not path_arg or not schema_arg:
        log.error("❌ Debes especificar 'metadata_file_path' y 'target_schema'")
        sys.exit(1)

    run_sinks(path_arg, schema_arg)