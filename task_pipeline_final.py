import pyspark.pipelines as dp # type: ignore
from pyspark.sql.functions import *
import json


from src.rules_engine import build_validation_rules
from src.transformations_factory import build_transformation_map

# ==============================================================================
# 0. CARGA DE CONFIGURACIÓN (Integración con Task 1)
# ==============================================================================
# En DLT, pasamos la ruta del JSON como un parámetro de configuración del Pipeline
# Key: "metadata_file_path"
# Value: "/Volumes/empresa/esquema_ingesta/configs/metadata.json"

try:
    # Opción A: Leer de configuración Spark (Recomendado para DLT puro)
    config_path = spark.conf.get("metadata_file_path", None) # type: ignore
    
    # Opción B: Fallback a sys.argv si se invoca distinto
    if not config_path and len(sys.argv) > 1:
        config_path = sys.argv[1]
        
    if not config_path:
        raise ValueError("No se especificó la ruta de metadatos (metadata_file_path)")

    print(f"Iniciando Pipeline DLT con configuración: {config_path}")
    
    with open(config_path, 'r') as f:
        config_data = json.load(f)

except Exception as e:
    # Si falla la carga, el DLT no debe arrancar nada
    raise RuntimeError(f"FATAL: No se pudo leer el JSON de metadatos: {e}")

# ==============================================================================
# 1. GENERADOR BRONZE
# ==============================================================================

def create_bronze_container(table_name, location_path=None):
    """
    Crea la tabla contenedor usando llamada directa.
    """
    #  1 argumentos dinamicos
    table_args = {
        "name": table_name,
        "comment": f"Repositorio central (Raw) para {table_name}",
        "table_properties": {
            "quality": "bronze",
            "delta.enableChangeDataFeed": "true"
        }
    }
    
    # Si hay storage para la tabla
    if location_path:
        table_args["path"] = location_path

    # 2 Llamada directa a DLT 
    # registra la tabla en el grafo de ejecución sin cuerpo de funcion, crea o declara
    dp.create_streaming_table(**table_args)

def create_bronze_append_flow(target_table, source_config, index_id):
    """
    Crea flujo de ingesta soportando multiples PATHS.
    """
    flow_name = f"ingest_{source_config['name']}_{index_id}"
    
    @dp.append_flow(target=target_table, name=flow_name)
    def ingest_files():
        reader = (
            spark.readStream.format("cloud_files") # type: ignore
            .option("cloud_files.format", source_config["format"])
            .option("cloud_files.schemaLocation", source_config["schema_location"])
            .option("cloudFiles.inferColumnTypes", "true")
        )
        
        # 2. Inyeccion dinamica de opciones
        # itera sobre el diccionario "options" del JSON y las aplica al reader
        if "options" in source_config:
            for key, value in source_config["options"].items():
                reader = reader.option(key, value)
        
        # 3. Carga del path, soporta string único o lista
        path_input = source_config.get("path") or source_config.get("paths")
        
        return reader.load(path_input)

# ==============================================================================
# 2. GENERADOR SILVER (Router -> Quarantine -> Prep -> SCD)
# ==============================================================================

def create_silver_logic_pipeline(config):
    
    bronze_source = config["source_bronze"]
    silver_target = config["target_silver_table"]
    silver_location = config.get("target_location") 
    
    quarantine_location = config.get("quarantine_location")
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
    # 2. CUARENTENA con sink
    # ======================================
    quarantine_args = {
        "name": quarantine_name,
        "comment": f"Registros rechazados de {silver_target}"
    }
    if quarantine_location:
        quarantine_args["path"] = quarantine_location

    @dp.table(**quarantine_args)
    def sales_quarantine():
        return dp.read_stream(router_name).filter("is_quarantined = true").withColumn("quarantined_at", current_timestamp())
        # return dp.read_stream(router_name).filter("is_quarantined = true").select(col("*")) 
    
    # ======================================    
    # 3. VISTA DE PREPARACIoN Maps + withColumns
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
    scd_conf = config["scd_config"]
    # ======================================
    
    #  argumentos de la tabla
    table_args = {
        "name": silver_target,
        "comment": "Tabla Silver limpia y deduplicada"
    }
    if silver_location:
        table_args["path"] = silver_location

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

for flow in config_data.get("dataflows", []):
    
    #  BRONZE 
    input_to_sink_config = { sink["input"]: sink for sink in flow["bronze_sinks"] }
    created_bronze_tables = set()
    
    for i, source in enumerate(flow["sources"]):
        input_name = source["name"]
        
        if input_name in input_to_sink_config:
            sink_config = input_to_sink_config[input_name]
            target_table_name = sink_config["name"]
            target_location = sink_config.get("location") 
            
            if target_table_name not in created_bronze_tables:
                create_bronze_container(target_table_name, target_location)
                created_bronze_tables.add(target_table_name)
            
            create_bronze_append_flow(target_table_name, source, i)

    #  SILVER 
    if "silver_logic" in flow:
        create_silver_logic_pipeline(flow["silver_logic"])