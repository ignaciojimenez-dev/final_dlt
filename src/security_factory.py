# src/security_factory.py

# ==============================================================================
# DISPATCHER DINÁMICO
# Las claves ahora ejecutan una lambda que construye el SQL string usando los params del JSON
# ==============================================================================
SECURITY_LOGIC_DISPATCHER = {
    
    # --- 1. MASKING DINÁMICO ---
    # params requeridos: 'allowed_group'
    "mask_email_dynamic": lambda p: f"""
        CASE 
            WHEN is_account_group_member('{p['allowed_group']}') THEN val 
            ELSE '***@****' 
        END
    """,

    # --- 2. ROW FILTER DINÁMICO ---
    # params requeridos: 'allowed_group', 'restrict_to_dept'
    # Nota: En Row Filter, la columna de la tabla se pasa en el "ON (...)" del ALTER,
    # aquí definimos la lógica booleana.
    # El parametro de entrada de la SQL UDF será 'dept_val'
    "filter_dept_dynamic": lambda p: f"""
        CASE 
            WHEN is_account_group_member('{p['allowed_group']}') THEN true
            ELSE dept_val = '{p['restrict_to_dept']}'
        END
    """
}

def get_udf_name(policy_type, table_name):
    # Hacemos el nombre único por tabla para evitar conflictos si usas params distintos
    return f"sec_{table_name}_{policy_type}"

def build_security_setup_statements(security_config_list, catalog_schema="main.security_schema"):
    setup_sqls = []
    apply_sqls = []
    
    for policy in security_config_list:
        table = policy["table_name"]
        
        # --- A. ROW FILTERS ---
        for rf in policy.get("row_filters", []):
            p_type = rf["type"]
            params = rf["params"]
            
            # Nombre columna física (ej: department)
            col_name_phys = params.pop("col_dept") 
            
            # Generamos nombre único y obtenemos el SQL Body inyectando params
            udf_name = f"{catalog_schema}.{get_udf_name(p_type, table)}"
            
            if p_type not in SECURITY_LOGIC_DISPATCHER:
                 raise ValueError(f"Security Type {p_type} no existe.")
                 
            sql_body = SECURITY_LOGIC_DISPATCHER[p_type](params)

            # 1. Crear UDF (recibe el valor de la columna como argumento)
            setup_sqls.append(f"""
                CREATE OR REPLACE FUNCTION {udf_name}(dept_val STRING) 
                RETURN {sql_body}
            """)
            
            # 2. Aplicar ALTER TABLE
            apply_sqls.append(f"ALTER TABLE {table} SET ROW FILTER {udf_name} ON ({col_name_phys})")

        # --- B. COLUMN MASKS ---
        for mask in policy.get("masking_functions", []):
            p_type = mask["type"]
            target_col = mask["target_col"]
            params = mask["params"]
            
            udf_name = f"{catalog_schema}.{get_udf_name(p_type, table)}"
            sql_body = SECURITY_LOGIC_DISPATCHER[p_type](params)

            # 1. Crear UDF (siempre recibe 'val')
            setup_sqls.append(f"""
                CREATE OR REPLACE FUNCTION {udf_name}(val STRING) 
                RETURN {sql_body}
            """)
            
            # 2. Aplicar ALTER TABLE
            apply_sqls.append(f"ALTER TABLE {table} ALTER COLUMN {target_col} SET MASK {udf_name}")

    return setup_sqls, apply_sqls