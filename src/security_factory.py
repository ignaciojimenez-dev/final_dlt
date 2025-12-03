# src/security_factory.py

# ==============================================================================
# DISPATCHER DINÁMICO
# ==============================================================================
SECURITY_LOGIC_DISPATCHER = {
    "mask_email_dynamic": lambda p: f"""
        CASE 
            WHEN is_account_group_member('{p['allowed_group']}') THEN val 
            ELSE '***@****' 
        END
    """,
    "filter_dept_dynamic": lambda p: f"""
        CASE 
            WHEN is_account_group_member('{p['allowed_group']}') THEN true
            ELSE dept_val = '{p['restrict_to_dept']}'
        END
    """
}

def get_udf_name(policy_type, table_name):
    return f"sec_{table_name}_{policy_type}"

def build_security_setup_statements(security_config_list, catalog_schema="main.security_schema"):
    setup_sqls = []
    apply_sqls = []
    
    for policy in security_config_list:
        # Nombre corto para generar nombres de funciones limpias
        raw_table_name = policy["table_name"]
        
        # Nombre  para ejecutar el ALTER TABLE en el lugar correcto
        # Si catalog_schema viene vacío o nulo, usamos solo el nombre de tabla
        if catalog_schema:
            full_table_name = f"{catalog_schema}.{raw_table_name}"
        else:
            full_table_name = raw_table_name
            
        # ---  ROW FILTERS ---
        for rf in policy.get("row_filters", []):
            p_type = rf["type"]
            params = rf["params"]
            col_name_phys = params.pop("col_dept") 
            
            # Usamos raw_table_name para el nombre de la función 
            udf_name = f"{catalog_schema}.{get_udf_name(p_type, raw_table_name)}"
            
            if p_type not in SECURITY_LOGIC_DISPATCHER:
                 raise ValueError(f"Security Type {p_type} no existe.")
                 
            sql_body = SECURITY_LOGIC_DISPATCHER[p_type](params)

            setup_sqls.append(f"""
                CREATE OR REPLACE FUNCTION {udf_name}(dept_val STRING) 
                RETURN {sql_body}
            """)
            
            #  full_table_name para aplicar la regla
            apply_sqls.append(f"ALTER TABLE {full_table_name} SET ROW FILTER {udf_name} ON ({col_name_phys})")

        # ---  COLUMN MASKS ---
        for mask in policy.get("masking_functions", []):
            p_type = mask["type"]
            target_col = mask["target_col"]
            params = mask["params"]
            
            udf_name = f"{catalog_schema}.{get_udf_name(p_type, raw_table_name)}"
            sql_body = SECURITY_LOGIC_DISPATCHER[p_type](params)

            setup_sqls.append(f"""
                CREATE OR REPLACE FUNCTION {udf_name}(val STRING) 
                RETURN {sql_body}
            """)
            
            apply_sqls.append(f"ALTER TABLE {full_table_name} ALTER COLUMN {target_col} SET MASK {udf_name}")

    return setup_sqls, apply_sqls