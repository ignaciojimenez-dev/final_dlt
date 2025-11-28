import pyspark.pipelines as dp # type: ignore
from pyspark.sql.functions import *
import json
# codigo propio
from src.rules_engine import build_validation_rules
from src.transformations_factory import build_transformation_map

# ==============================================================================
# 0. CARGA DE CONFIGURACIÓN 
# ==============================================================================
try:
    # Leemos la configuración global del Pipeline
    config_path = spark.conf.get("metadata_file_path", None) # type: ignore
    
    if not config_path:
        raise ValueError("CRITICAL: No se recibió 'metadata_file_path' en la configuración del Pipeline.")

    print(f"📄 Cargando Monolito desde: {config_path}")
    
    with open(config_path, 'r') as f:
        full_config = json.load(f)

    all_flows = full_config.get("dataflows", [])
    
    print(f"🚀 Orquestando {len(all_flows)} flujos en un solo DAG.")

except Exception as e:
    raise RuntimeError(f"FATAL SETUP: {e}")

# ==============================================================================
# 1. GENERADOR BRONZE
# ==============================================================================

def create_bronze_container(table_name, location_path=None):
    """
    Crea la tabla contenedor vacía. 
    DLT esperará a que el primer 'append_flow' escriba para decidir el esquema.
    """
    table_args = {
        "name": table_name,
        "comment": f"Repositorio central (Raw) para {table_name}",
        "table_properties": {
            "quality": "bronze",
            "delta.enableChangeDataFeed": "true"
        }
    }
    

    # Registra la tabla vacía en el grafo DLT
    dp.create_streaming_table(**table_args)

def create_bronze_append_flow(target_table, source_config, index_id):
    """
    Crea flujo de ingesta. Soporta esquema explícito (DDL) para evitar conflictos de tipos.
    """
    flow_name = f"ingest_{target_table}_{index_id}"
    
    # Extraer configuración
    raw_path = source_config.get("path") or source_config.get("paths")
    input_path = raw_path[0] if isinstance(raw_path, list) else raw_path
    
    file_format = source_config.get("format", "JSON")
    schema_loc = source_config.get("schema_location")
    
    # Leemos el esquema si viene en el JSON 
    explicit_schema = source_config.get("schema") 

    @dp.append_flow(target=target_table, name=flow_name)
    def ingest_files():
        reader = spark.readStream.format("cloudFiles") # type: ignore
        
        # 1. Configuración del Formato
        reader = reader.option("cloudFiles.format", file_format)
        
        # Esquema Fijo o Inferencia
        if explicit_schema:
            reader = reader.schema(explicit_schema)
        else:
            reader = reader.option("cloudFiles.inferColumnTypes", "true")

    
        reader = reader.option("multiLine", "true") 
        
        if schema_loc:
             reader = reader.option("cloudFiles.schemaLocation", schema_loc)
        
        # 4. opciones extra
        if "options" in source_config:
            for key, value in source_config["options"].items():
                reader = reader.option(key, value)
        
        return reader.load(input_path)

# ==============================================================================
# 2. GENERADOR SILVER (Router -> Quarantine -> Prep -> SCD)
# ==============================================================================

def create_silver_logic_pipeline(config):
    
    bronze_source = config["source_bronze"]
    silver_target = config["target_silver_table"]
    quarantine_name = f"{silver_target}_quarantine"
    router_name = f"{silver_target}_router_temp"
    prep_view_name = f"{silver_target}_prep_view"
    
    #  A. Validaciones 
    rules_dict, quarantine_sql = build_validation_rules(config["validations"])
    # ======================================
    # 1. Tabla enrutadora , temporal y particionada
    # ======================================
    @dp.table(
        name=router_name, temporary=True, partition_cols=["is_quarantined"],
        comment="Router físico de calidad"
    )
    @dp.expect_all(rules_dict)
    def quality_router_temp():
        return dp.read_stream(bronze_source).withColumn("is_quarantined", expr(quarantine_sql))

    # ======================================
    # 2. CUARENTENA 
    # ======================================
    quarantine_args = {
        "name": quarantine_name,
        "comment": f"Registros rechazados de {silver_target}"
    }
    @dp.table(**quarantine_args)
    def quarantine_table():
        return dp.read_stream(router_name).filter("is_quarantined = true").withColumn("quarantined_at", current_timestamp())

    
    # ======================================    
    # 3. Vista Preparación 
    # Transformaciones Maps + withColumns
    # ======================================
    @dp.view(name=prep_view_name)
    def silver_prep_view():
        # filtrar validos
        df = (dp.read_stream(router_name)
              .filter("is_quarantined = false")
              .drop("is_quarantined"))
        
        # mapa de transformaciones del scrit factory
        #  devuelve { "col1": expr(...),... }
        transforms_list = config.get("transformations", [])
        transform_map = build_transformation_map(transforms_list)
        
        # aplicar transformaciones optimizadas
        if transform_map:
            df = df.withColumns(transform_map)
            
        return df
    # ======================================
    # 4. TABLA FINAL 
    # ======================================
    
    scd_conf = config["scd_config"]
    #  argumentos de la tabla
    table_args = {
        "name": silver_target,
        "comment": "Tabla Silver limpia y deduplicada"
    }

    # Llamada , cargar refencia a la tabla
    dp.create_streaming_table(**table_args)

    dp.apply_changes(
        target=silver_target,
        source=prep_view_name,
        keys=scd_conf["keys"],
        sequence_by=col(scd_conf["sequence_by"]),
        stored_as_scd_type=scd_conf["stored_as_scd_type"]
    )

# ==============================================================================
# 3. ORQUESTADOR QUE ITERA POR SOURCES Y PATHS
# ==============================================================================

for flow in all_flows:
    
    # --- A. Lógica Bronze Dinámica basada en 'name'
    if "sources" in flow:    
        sources_by_name = {}
        for src in flow["sources"]:
            t_name = src["name"]
            if t_name not in sources_by_name:
                sources_by_name[t_name] = []
            sources_by_name[t_name].append(src)
            
        # Generamos los objetos DLT
        for table_name, src_list in sources_by_name.items():
            # 1. Crear tabla contenedora (1 vez por nombre)
            create_bronze_container(table_name)
            
            # 2. Crear flujos de ingesta (N veces)
            for idx, source_conf in enumerate(src_list):
                create_bronze_append_flow(table_name, source_conf, idx)

    # --- B. Lógica Silver (Iterar lista de transformaciones) ---
    if "transformations_silver" in flow:
        for silver_conf in flow["transformations_silver"]:
            create_silver_logic_pipeline(silver_conf)