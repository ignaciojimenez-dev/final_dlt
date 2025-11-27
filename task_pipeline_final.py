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

    # En el monolito, PROCESAMOS TODO. No hay if/else de filtrado.
    # Spark optimizará la ejecución paralela de estos flujos.
    all_flows = full_config.get("dataflows", [])
    
    print(f"🚀 Orquestando {len(all_flows)} flujos en un solo DAG.")

except Exception as e:
    raise RuntimeError(f"FATAL SETUP: {e}")

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
    #if location_path:
    #    table_args["path"] = location_path

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
            #.option("cloud_files.schemaLocation", source_config["schema_location"])
            #.option("cloudFiles.inferColumnTypes", "true")
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
    #if quarantine_location:
      #  quarantine_args["path"] = quarantine_location

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
    #if silver_location:
        #table_args["path"] = silver_location

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

# Iteramos sobre TODOS los flujos.
# DLT registra todas estas definiciones en el grafo antes de empezar a procesar datos.
for flow in all_flows:
    # --- Bronze ---
    input_to_sink = {s["input"]: s for s in flow["bronze_sinks"]}
    created_tables = set()
    
    for i, src in enumerate(flow["sources"]):
        if src["name"] in input_to_sink:
            sink_cfg = input_to_sink[src["name"]]
            t_name = sink_cfg["name"]
            
            if t_name not in created_tables:
                create_bronze_container(t_name, sink_cfg.get("location"))
                created_tables.add(t_name)
            
            create_bronze_append_flow(t_name, src, i)

    # --- Silver ---
    if "silver_logic" in flow:
        create_silver_logic_pipeline(flow["silver_logic"])