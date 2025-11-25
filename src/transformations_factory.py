from pyspark.sql.functions import *


# MAPA DE TRANSFORMACIONES
# diccionario que mapea el "type" del JSON a una lambda.
# params: es el valor que viene del JSON ,puede ser string, dict o null

TRANSFORMATION_DISPATCHER = {
    #  Expresiones SQL , posible logica de negocio 
    # JOSN: { "type": "sql_expr", "params": "CASE WHEN ..." }
    "sql_expr": lambda params: expr(params),

    # Funciones de Tiempo
    #  { "type": "current_timestamp", "params": null }
    "current_timestamp": lambda params: current_timestamp(),
    
    #  { "type": "to_date", "params": "columna_fecha_string" }
    "to_date": lambda params: to_date(col(params)),

    #  Casteos y Literales
    #  { "type": "cast", "params": {"col": "amount", "type": "double"} }
    "cast": lambda params: col(params["col"]).cast(params["type"]),
    
    #  { "type": "literal", "params": "VALOR_FIJO" }
    "literal": lambda params: lit(params),

    #  Masking
    "hash_sha256": lambda params: sha2(col(params), 256),
    "hash_md5": lambda params: md5(col(params))
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