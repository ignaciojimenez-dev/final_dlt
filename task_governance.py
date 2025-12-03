import json
import sys
import os
from pyspark.sql import SparkSession # type: ignore

try:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))
except NameError:
    sys.path.append(os.getcwd())

from src.security_factory import build_security_setup_statements


spark = SparkSession.builder.getOrCreate()

# Intentar obtener dbutils para entornos interactivos (Widgets)
try:
    from pyspark.dbutils import DBUtils # type: ignore
    dbutils = DBUtils(spark)
except ImportError:
    dbutils = None

# ==============================================================================
# 1. LECTURA DE PARÁMETROS (Job Arguments vs Widgets)
# ==============================================================================
metadata_path_arg = None
schema_arg = None

# A. Intento leer argumentos de Job (sys.argv)
# sys.argv[0] es el nombre del script, [1] primer param, [2] segundo param
if len(sys.argv) > 2:
    metadata_path_arg = sys.argv[1]
    schema_arg = sys.argv[2]
    print(f"✅ Parámetros detectados vía Job Arguments.")

# B. Intento leer Widgets (Si no hay args de Job y dbutils existe)
if (not metadata_path_arg or not schema_arg) and dbutils:
    try:
        print("⚠️ No hay argumentos de Job. Buscando Widgets...")
        dbutils.widgets.text("metadata_file_path", "", "Ruta JSON Metadatos")
        dbutils.widgets.text("security_schema", "main.default", "Esquema Funciones Seguridad")
        
        p_widget = dbutils.widgets.get("metadata_file_path")
        s_widget = dbutils.widgets.get("security_schema")
        
        # Solo asignamos si no están vacíos
        if p_widget: metadata_path_arg = p_widget
        if s_widget: schema_arg = s_widget
    except Exception as e:
        print(f"Advertencia leyendo widgets: {e}")

# C. Validación Final
if not metadata_path_arg or not schema_arg:
    print("❌ ERROR FATAL: Faltan parámetros.")
    print("   Uso: task_governance.py <ruta_json> <catalogo.esquema_seguridad>")
    sys.exit(1)

METADATA_PATH = metadata_path_arg
SECURITY_CATALOG_SCHEMA = schema_arg

print(f"🔧 Configuración establecida:\n   - JSON: {METADATA_PATH}\n   - Schema Seguridad: {SECURITY_CATALOG_SCHEMA}")

# ==============================================================================
# 2. LOGICA PRINCIPAL
# ==============================================================================

def main():
    print(f"🔒 Iniciando Governance Enforcer...")

    # A. Leer Configuración
    try:
        with open(METADATA_PATH, 'r') as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error leyendo metadatos en {METADATA_PATH}: {e}")
        raise e

    # B. Recolectar Políticas de Seguridad (CORREGIDO)
    # Iteramos por todos los dataflows para buscar "security_policies" dentro de cada uno
    all_security_policies = []
    dataflows = config.get("dataflows", [])
    
    for flow in dataflows:
        # Extraemos las políticas de este flujo específico
        flow_policies = flow.get("security_policies", [])
        if flow_policies:
            print(f"   -> Encontradas {len(flow_policies)} políticas en flujo '{flow.get('name', 'Unnamed')}'")
            all_security_policies.extend(flow_policies)
    
    if not all_security_policies:
        print("ℹ️ No hay políticas de seguridad definidas en ningún flujo del JSON. Finalizando.")
        return

    print(f"📋 Procesando un total de {len(all_security_policies)} tablas con políticas...")

    # C. Generar Sentencias SQL usando el Factory
    setup_sqls, apply_sqls = build_security_setup_statements(
        all_security_policies, 
        catalog_schema=SECURITY_CATALOG_SCHEMA
    )

    # D. Ejecutar Creación de Funciones (UDFs)
    # Esto es idempotente (CREATE OR REPLACE)
    print("\n--- [PASO 1] Desplegando Funciones de Lógica de Seguridad ---")
    for sql in setup_sqls:
        # Imprimimos para log (truncado visualmente)
        print(f"Ejecutando setup: {sql.strip().split('RETURN')[0]} ...") 
        spark.sql(sql)

    # E. Aplicar Políticas a las Tablas (ALTER TABLE)
    print("\n--- [PASO 2] Aplicando Row Filters y Masks en Tablas ---")
    for sql in apply_sqls:
        print(f"Ejecutando apply: {sql}")
        try:
            spark.sql(sql)
        except Exception as e:
            # Capturamos el error para que el Job falle explícitamente si la seguridad no se aplica
            print(f"❌ ERROR CRÍTICO aplicando seguridad: {e}")
            raise e
            
    print("\n✅ Governance Enforcement completado exitosamente.")

if __name__ == "__main__":
    main()