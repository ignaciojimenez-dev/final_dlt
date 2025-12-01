import json
import sys
import logging
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.utils import AnalysisException # type: ignore

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("SinkExecutor")

# Inicializar Spark
spark = SparkSession.builder.getOrCreate()

# Intentar obtener dbutils para entornos interactivos
try:
    from pyspark.dbutils import DBUtils # type: ignore
    dbutils = DBUtils(spark)
except ImportError:
    dbutils = None

def load_config(path: str):
    """Carga el JSON de metadatos."""
    log.info(f" Cargando configuración desde: {path}")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log.error(f"XXXX Error fatal leyendo el JSON en {path}: {e}")
        raise e

def run_sinks(metadata_path, source_catalog_schema):
    """
    Lógica principal de Sinks.
    """
    config = load_config(metadata_path)
    dataflows = config.get("dataflows", [])
    
    log.info(f" Iniciando procesamiento de Sinks para {len(dataflows)} flujos.")
    log.info(f" Esquema origen (DLT): {source_catalog_schema}")

    success_count = 0
    fail_count = 0

    for flow in dataflows:
        flow_name = flow.get("name", "Unnamed Flow")
        sinks = flow.get("sinks", [])
        
        if not sinks:
            continue

        log.info(f" Procesando sinks para flujo: {flow_name}")

        for sink in sinks:
            try:
                # 1. Parámetros
                input_table_name = sink["input"]
                output_format = sink["format"]
                save_mode = sink.get("saveMode", "append")
                output_path = sink.get("path") or sink.get("paths")
                
                if not output_path:
                    log.warning(f"!!!! Sink para '{input_table_name}' sin path. Omitiendo.")
                    fail_count += 1
                    continue

                # 2. Referencia a Tabla
                full_source_table = f"{source_catalog_schema}.{input_table_name}"
                
                # 3. Lectura y Escritura
                df = spark.read.table(full_source_table)
                
                log.info(f"  Sink: {input_table_name} -> {output_path} ({output_format})")
                
                (df.write
                   .format(output_format)
                   .mode(save_mode)
                   .save(output_path))
                
                success_count += 1

            except AnalysisException:
                log.error(f"   XXXX Tabla no encontrada: {full_source_table}")
                fail_count += 1
            except Exception as e:
                log.error(f"   XXXX Error en sink '{sink.get('input')}': {e}")
                fail_count += 1

    log.info(f" Finalizado. OK: {success_count} | Error: {fail_count}")
    if fail_count > 0:
        raise RuntimeError("Fallos detectados en Sinks.")

if __name__ == "__main__":
    # ==============================================================================
    # LECTURA DE PARÁMETROS 
    # ==============================================================================
    path_arg = None
    schema_arg = None

    # 1. MODO JOB sys.argv
    # s ["param1", "param2"] en la Task, llegan como sys.argv[1] y sys.argv[2]
    log.info(f"Argumentos recibidos (sys.argv): {sys.argv}")
    
    if len(sys.argv) > 2:
        path_arg = sys.argv[1]
        schema_arg = sys.argv[2]
        log.info(" OKKK Parámetros detectados vía sys.argv (Modo Job)")

    # 2. dbutils widgets
    # Solo si no se encontraron en sys.argv y dbutils  disponible
    if (not path_arg or not schema_arg) and dbutils:
        try:
            log.info("WARNING!!! No hay argumentos de Job. Intentando leer Widgets...")
            dbutils.widgets.text("metadata_file_path", "", "Ruta JSON Metadatos")
            dbutils.widgets.text("target_schema", "", "Esquema Target DLT")
            
            p_widget = dbutils.widgets.get("metadata_file_path")
            s_widget = dbutils.widgets.get("target_schema")
            
            # Solo usamos los widgets si tienen contenido
            if p_widget: path_arg = p_widget
            if s_widget: schema_arg = s_widget
            
            if path_arg and schema_arg:
                log.info("OKKK Parámetros detectados vía Widgets")
        except Exception as e:
            log.warning(f"Error accediendo a widgets: {e}")

    # 3. VALIDACIÓN FINAL
    if not path_arg or not schema_arg:
        log.error("XXXX ERROR FATAL: Faltan parámetros.")
        log.error(" No se pasaron argumentos en la Task o los Widgets están vacíos.")
        log.error(f"Valor actual - Path: '{path_arg}' | Schema: '{schema_arg}'")
        sys.exit(1)

    # Ejecutar
    run_sinks(path_arg, schema_arg)