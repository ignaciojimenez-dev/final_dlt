import dlt
from pyspark.sql.functions import *


config = [
    {
  "dataflows": [
    {
      "name": "df1_persons_employees",
      "sources": [
        {
          "name": "person_inputs",
          "path": "/Volumes/workspace/elt_modular/data/inputs/events/person/*",
          "format": "JSON",
          "schema_location": "/Volumes/workspace/elt_modular/schemas/bronze_person"
        },
        {
          "name": "employees_inputs",
          "path": "/Volumes/workspace/elt_modular/data/inputs/events/employees/*",
          "format": "JSON",
          "schema_location": "/Volumes/workspace/elt_modular/schemas/bronze_employees"
        }
      ],
      "bronze_sinks": [
        {
          "input": "person_inputs",
          "name": "bronze_person"
        },
        {
          "input": "employees_inputs",
          "name": "bronze_employees"
        }
      ],
      "transformations_silver": [
        {
          "name": "validation_person",
          "type": "validate_fields",
          "params": {
            "input": "bronze_person",
            "validations": [
              {"field": "office", "validations": ["notEmpty"]},
              {"field": "age", "validations": ["notNull"]}
            ]
          }
        },
        {
          "name": "person_ok_with_date",
          "type": "add_fields",
          "params": {
            "input": "validation_person_ok",
            "addFields": [{"name": "dt", "function": "current_timestamp"}]
          }
        }
      ],
      "silver_sinks": [
        {
          "input": "person_ok_with_date",
          "name": "silver_person_ok"
        }
      ],
      "transformations_gold": [
        {
          "name": "mask_person_pii",
          "type": "apply_masking",
          "params": {
            "input": "silver_person_ok",
            "masking_rules": [
              {"field": "email", "function": "sha2"},
              {"field": "name", "function": "md5"}
            ]
          }
        }
      ],
      "gold_sinks": [
        {
          "input": "mask_person_pii",
          "name": "gold_persons_anonymized"
        }
      ]
    }
  ]
}
]

dataflow_json_string = sys.argv[1]
        
dataflow_config = json.loads(dataflow_json_string)
    
def generate_dlt_pipeline(table_config):
    
    # Nombres
    table_base = table_config["name"]
    bronze_table = f"{table_base}_bronze"
    valid_raw_view = f"{table_base}_valid_raw_view" # Vista intermedia: Raw pero validada
    quarantine_table = f"{table_base}_quarantine"
    prep_view = f"{table_base}_prep_view" # Vista final: Transformada
    silver_table = f"{table_base}_silver"

    # 1. BRONZE: Ingesta
    @dlt.table(name=bronze_table, comment=f"Raw ingestion for {table_base}")
    def bronze_ingestion():
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", table_config["format"])
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(table_config["source_path"])
        )

    # 2. VALIDATION VIEW: Filtra ANTES de transformar
    # Solo pasan los registros RAW que cumplen las reglas y no tienen JSON corrupto
    @dlt.view(name=valid_raw_view)
    @dlt.expect_all(table_config["validations"])
    def validation_logic():
        return dlt.readStream(bronze_table).filter("_rescued_data IS NULL")

    # 3. QUARANTINE: Atrapa lo que Validation View va a soltar
    # Lee de BRONZE (Raw) y aplica la lógica inversa
    @dlt.table(name=quarantine_table)
    def quarantine_logic():
        validations = table_config.get("validations", {})
        
        # Si no hay reglas, solo validamos que el JSON no esté roto
        if not validations:
            return dlt.readStream(bronze_table).filter("_rescued_data IS NOT NULL")
        
        # Lógica Inversa
        rules_sql = [f"({rule})" for rule in validations.values()]
        combined_rules = " AND ".join(rules_sql)
        failure_condition = f"(_rescued_data IS NOT NULL) OR NOT ({combined_rules})"
        
        return (
            dlt.readStream(bronze_table)
            .filter(expr(failure_condition))
            .withColumn("quarantined_at", current_timestamp())
        )

    # 4. PREP VIEW: Transformaciones
    # Ahora lee de 'valid_raw_view'. 
    # Solo gastamos cómputo transformando registros que sabemos que son válidos.
    @dlt.view(name=prep_view)
    def prep_logic():
        df = dlt.readStream(valid_raw_view) # <--- OJO: Leemos de la vista validada
        
        transformations = table_config.get("transformations", {})
        for col_name, expr_sql in transformations.items():
            df = df.withColumn(col_name, expr(expr_sql))
            
        return df

    # 5. SILVER: Upsert
    dlt.create_streaming_table(name=silver_table)
    
    dlt.apply_changes(
        target=silver_table,
        source=prep_view,  
        keys=table_config["primary_keys"],
        sequence_by=col(table_config["sequence_col"]),
        stored_as_scd_type=1
    )
