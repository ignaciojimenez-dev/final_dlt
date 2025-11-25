def get_sql_constraint(field, rule_name):
    """
    Traduce una regla abstracta json a una expresión SQL concreta.
    ejemplo : 'notNull' -> 'col IS NOT NULL'
    """
    rules_map = {
        "notNull": f"{field} IS NOT NULL",
        "notEmpty": f"length({field}) > 0",
        "positive": f"{field} > 0",
        # mas reglas  aquí
        "valid_email": f"{field} RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{{2,}}$'"
    }
    
    if rule_name not in rules_map:
        raise ValueError(f"Regla desconocida: {rule_name} para el campo {field}")
        
    return rules_map[rule_name]

def build_validation_rules(validations_config):
    """
    Construye el diccionario de reglas para DLT expect_all y la lógica SQL para el Router.
    Retorna: (dict_reglas, string_sql_cuarentena)
    """
    rules_dict = {}
    
    for item in validations_config:
        field = item["field"]
        for validation_type in item["validations"]:
            rule_key = f"valid_{field}_{validation_type}"
            sql_expr = get_sql_constraint(field, validation_type)
            rules_dict[rule_key] = f"({sql_expr})"
            
    # logica AND para todas las reglas a la vez, negada para cuarentena
    # NOT((regla1) AND (regla2)...)
    quarantine_sql = "NOT({0})".format(" AND ".join(rules_dict.values()))
    
    return rules_dict, quarantine_sql