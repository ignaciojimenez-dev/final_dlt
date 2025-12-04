import json
import sys
import os
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.utils import AnalysisException # type: ignore

# ==============================================================================
#  IMPORTAR MÓDULOS SRC
# ==============================================================================
try:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))
except NameError:
    sys.path.append(os.getcwd())

from src.security_factory import build_security_setup_statements

# Inicializamos Spark
spark = SparkSession.builder.getOrCreate()

# Intentar obtener dbutils
try:
    from pyspark.dbutils import DBUtils # type: ignore
    dbutils = DBUtils(spark)
except ImportError:
    dbutils = None

# ==============================================================================
# 1. LECTURA DE PARÁMETROS
# ==============================================================================
metadata_path_arg = None
schema_arg = None

if len(sys.argv) > 2:
    metadata_path_arg = sys.argv[1]
    schema_arg = sys.argv[2]
    print(f" OKKK !! Parámetros detectados .")

if (not metadata_path_arg or not schema_arg) and dbutils:
    try:
        dbutils.widgets.text("metadata_file_path", "/Volumes/dev_dlt/esquema_ingesta/volumen_metadatos/metadata_dev.json", "Ruta JSON Metadatos")
        dbutils.widgets.text("security_schema", "dev_dlt.esquema_ingesta", "Esquema Funciones ")
        
        p_widget = dbutils.widgets.get("metadata_file_path")
        s_widget = dbutils.widgets.get("security_schema")
        
        if p_widget: metadata_path_arg = p_widget
        if s_widget: schema_arg = s_widget
    except Exception as e:
        print(f"Advertencia leyendo widgets: {e}")

if not metadata_path_arg or not schema_arg:
    print("XXX ERROR FATAL: Faltan parámetros.")
    sys.exit(1)

METADATA_PATH = metadata_path_arg
SECURITY_CATALOG_SCHEMA = schema_arg

print(f"🔧 Configuración: {METADATA_PATH} | Schema: {SECURITY_CATALOG_SCHEMA}")

# ==============================================================================
# 2. LOGICA PRINCIPAL
# ==============================================================================

def run_sql(sql_command):
    """
    Ejecuta SQL con reintentos  para objetos que no son tablas estándar (Views/MVs).
    Itera sobre una lista de estrategias de reemplazo.
    """
    
    # 1. Definimos las estrategias de fallback en orden de prioridad
    fallback_strategies = [
        ("ALTER MATERIALIZED VIEW", "Materialized View"),
        ("ALTER VIEW", "Standard View"),
        ("ALTER STREAMING TABLE","ALTER STREAMING TABLE")
    ]

    # 2. Intento inicial Tabla estándar.
    try:
        spark.sql(sql_command)
        return 
    except AnalysisException as e:
        msg = str(e)
        if not ("expects a table" in msg and "is a view" in msg):
            raise e
        
        print(f" El objeto es una VISTA. Iniciando secuencia de reintentos...")

    # 3. Bucle de intentos
    last_error = None
    
    for replacement, strategy_name in fallback_strategies:
        try:
            # Reemplazamos "ALTER TABLE" por lo nuevo

            new_sql = sql_command.replace("ALTER TABLE", replacement, 1)
            
            spark.sql(new_sql)
            print(f"  Éxito aplicando estrategia: {strategy_name}")
            return # Éxito, terminamos
            
        except Exception as e:
            print(f"  - Falló estrategia {strategy_name}")
            last_error = e

    # sino lee ninguna tabla
    print("  Fallaron todas las estrategias de aplicación.")
    if last_error:
        raise last_error

def main():
    print(f" Iniciando Governance Enforcer...")

    try:
        with open(METADATA_PATH, 'r') as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error leyendo metadatos: {e}")
        raise e

    # Recolectar Políticas
    all_security_policies = []
    dataflows = config.get("dataflows", [])
    
    for flow in dataflows:
        flow_policies = flow.get("security_policies", [])
        if flow_policies:
            all_security_policies.extend(flow_policies)
    
    if not all_security_policies:
        print("  Sin políticas. Fin.")
        return

    print(f" Procesando {len(all_security_policies)} tablas...")

    # Generar SQL
    setup_sqls, apply_sqls = build_security_setup_statements(
        all_security_policies, 
        catalog_schema=SECURITY_CATALOG_SCHEMA
    )

    print("\n--- [PASO 1] Funciones (UDFs) ---")
    for sql in setup_sqls:
        print(f"Setup: {sql.strip().split('RETURN')[0]} ...") 
        spark.sql(sql)

    print("\n--- [PASO 2] Row Filters & Masks (Smart Apply) ---")
    for sql in apply_sqls:
        print(f"Aplicando: {sql}")
        try:
            run_sql(sql)
        except Exception as e:
            print(f"XXX ERROR CRÍTICO aplicando seguridad: {e}")
            raise e
            
    print("\n OKKK !! Governance Enforcement completado.")

if __name__ == "__main__":
    main()