from pyspark.sql.functions import * # type: ignore


# MAPA DE TRANSFORMACIONES
# diccionario que mapea el "type" del JSON a una lambda.
# params: es el valor que viene del JSON ,puede ser string, dict o null
SECRET_KEY_AES = "SDG_BOOTCAMP_2025_KEY_32_BYTES"
TRANSFORMATION_DISPATCHER = {
    #  Expresiones SQL , posible logica de negocio 
    # JOSN: { "type": "sql_expr", "params": "CASE WHEN ..." }
    "sql_expr": lambda params: expr(params), # type: ignore

    # Funciones de Tiempo
    #  { "type": "current_timestamp", "params": null }
    "current_timestamp": lambda params: current_timestamp(), # type: ignore
    
    #  { "type": "to_date", "params": "columna_fecha_string" }
    "to_date": lambda params: to_date(col(params)), # type: ignore

    #  Casteos y Literales
    #  { "type": "cast", "params": {"col": "amount", "type": "double"} }
    "cast": lambda params: col(params["col"]).cast(params["type"]), # type: ignore
    
    #  { "type": "literal", "params": "VALOR_FIJO" }
    "literal": lambda params: lit(params), # type: ignore

    #  Masking Irreversible
    "hash_sha256": lambda params: sha2(col(params), 256), # type: ignore
    "hash_md5": lambda params: md5(col(params)), # type: ignore

    # ---  Masking Reversible (AES) ---
    # params: nombre de la columna a encriptar/desencriptar
    "aes_encrypt": lambda params: aes_encrypt(col(params), lit(SECRET_KEY_AES)), # type: ignore
    "aes_decrypt": lambda params: aes_decrypt(col(params), lit(SECRET_KEY_AES)).cast("string") # type: ignore
}

def get_transformation_expression(t_type, params):
    """
    Busca la funcion en el diccionario y la ejecuta.
    Devuelve un objeto Column o Expression de Spark.
    """
    if t_type not in TRANSFORMATION_DISPATCHER:

        valid_keys = list(TRANSFORMATION_DISPATCHER.keys())
        raise ValueError(f"Transformación '{t_type}' no soportada. Las válidas son: {valid_keys}")
    

    transformation_func = TRANSFORMATION_DISPATCHER[t_type]
    return transformation_func(params)

def build_transformation_map(transformations_list):
    """
    Construye el diccionario para df.withColumns(map).
    """
    transform_map = {}
    
    if not transformations_list:
        return transform_map
        
    for item in transformations_list:
        target_col = item["target_col"]
        t_type = item["type"]
        params = item.get("params")
        
        transform_map[target_col] = get_transformation_expression(t_type, params)
        
    return transform_map

# ==============================================================================
# 2. DISPATCHER DE AGREGACIONES (Group By) + MAP DISPACHER
# ==============================================================================

# Mapeamos el string del JSON a una lambda que recibe el nombre de la columna
AGGREGATION_DISPATCHER = {
    "sum":   lambda c: sum(col(c)), # type: ignore
    "avg":   lambda c: avg(col(c)), # type: ignore
    "min":   lambda c: min(col(c)), # type: ignore
    "max":   lambda c: max(col(c)), # type: ignore
    "first": lambda c: first(col(c)), # type: ignore
    "last":  lambda c: last(col(c)), # type: ignore
    "count": lambda c: count(lit(1)) if c == "*" else count(col(c)) # type: ignore
}

def apply_aggregation(df, agg_config):
    """
    Aplica group_by y métricas usando el diccionario AGGREGATION_DISPATCHER.
    """
    if not agg_config:
        return df

    group_cols = agg_config.get("group_by", [])
    metrics_list = agg_config.get("metrics", [])

    # Si no hay métricas, devolvemos distinct
    if not metrics_list:
        return df.select(group_cols).distinct()

    # Construimos la lista de expresiones dinámicamente
    agg_exprs = []
    
    for m in metrics_list:
        col_name = m.get("col")
        agg_type = m.get("type") # ej: "sum", "count"
        
        # Calculamos el alias por defecto si no viene
        alias = m.get("alias", f"{agg_type}_{col_name}")

        # Buscamos la función en el mapa
        if agg_type in AGGREGATION_DISPATCHER:
            agg_func = AGGREGATION_DISPATCHER[agg_type]
            # Ejecutamos la lambda y añadimos el alias
            agg_exprs.append(agg_func(col_name).alias(alias))
        else:
            print(f"!!!! Warning: Tipo de agregación '{agg_type}' no soportado. Se ignora.")

    # Ejecutamos la agregación final
    return df.groupBy(*group_cols).agg(*agg_exprs)