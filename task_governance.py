import json
import sys
import os
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.utils import AnalysisException # type: ignore

# ==============================================================================
# FIX: GESTIÓN DE PATH PARA IMPORTAR MÓDULOS (SRC)
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
    print(f"✅ Parámetros detectados vía Job Arguments.")

if (not metadata_path_arg or not schema_arg) and dbutils:
    try:
        print("⚠️ No hay argumentos de Job. Buscando Widgets...")
        dbutils.widgets.text("metadata_file_path", "", "Ruta JSON Metadatos")
        dbutils.widgets.text("security_schema", "main.default", "Esquema Funciones Seguridad")
        
        p_widget = dbutils.widgets.get("metadata_file_path")
        s_widget = dbutils.widgets.get("security_schema")
        
        if p_widget: metadata_path_arg = p_widget
        if s_widget: schema_arg = s_widget
    except Exception as e:
        print(f"Advertencia leyendo widgets: {e}")

if not metadata_path_arg or not schema_arg:
    print("❌ ERROR FATAL: Faltan parámetros.")
    sys.exit(1)

METADATA_PATH = metadata_path_arg
SECURITY_CATALOG_SCHEMA = schema_arg

print(f"🔧 Configuración: {METADATA_PATH} | Schema: {SECURITY_CATALOG_SCHEMA}")

# ==============================================================================
# 2. LOGICA PRINCIPAL
# ==============================================================================

def run_sql_smartly(sql_command):
    """
    Intenta ejecutar SQL. Si falla porque el objeto es una VIEW/MV en lugar de TABLE,
    reescribe el comando y reintenta.
    """
    try:
        spark.sql(sql_command)
    except AnalysisException as e:
        msg = str(e)
        # Detectamos el error específico: "expects a table but ... is a view"
        if "expects a table" in msg and "is a view" in msg:
            print(f"   ⚠️ Objeto detectado como VISTA. Reintentando como MATERIALIZED VIEW...")
            
            # Intento 1: DLT suele crear Materialized Views
            new_sql = sql_command.replace("ALTER TABLE", "ALTER MATERIALIZED VIEW")
            try:
                spark.sql(new_sql)
                print("   ✅ Éxito usando ALTER MATERIALIZED VIEW")
                return
            except Exception as e2:
                print(f"   ⚠️ Falló como MV. Intentando como VIEW genérica... ({e2})")
                
                # Intento 2: Vista normal
                new_sql_v = sql_command.replace("ALTER TABLE", "ALTER VIEW")
                try:
                    spark.sql(new_sql_v)
                    print("   ✅ Éxito usando ALTER VIEW")
                    return
                except Exception as e3:
                    print(f"   ❌ Fallaron todos los reintentos.")
                    raise e3
        else:
            # Si es otro error (sintaxis, permisos), fallamos normal
            raise e

def main():
    print(f"🔒 Iniciando Governance Enforcer...")

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
        print("ℹ️ Sin políticas. Fin.")
        return

    print(f"📋 Procesando {len(all_security_policies)} tablas...")

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
            # Usamos la función inteligente aquí
            run_sql_smartly(sql)
        except Exception as e:
            print(f"❌ ERROR CRÍTICO aplicando seguridad: {e}")
            raise e
            
    print("\n✅ Governance Enforcement completado.")

if __name__ == "__main__":
    main()