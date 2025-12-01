# test_unit_logic.py
import pytest
from pyspark.sql import SparkSession
import sys
import os
sys.path.append(os.getcwd())
# Asumiendo que tus archivos están en la misma carpeta o ajusta los imports
from src.rules_engine import get_sql_constraint, build_validation_rules
from src.transformations_factory import build_transformation_map
from task_metadata_reader import validate_metadata_schema

# --- Configuración previa ---
@pytest.fixture(scope="session")
def spark_local():
    """Sesión de Spark local mínima para validar objetos Column si fuera necesario."""
    return SparkSession.builder.master("local[1]").appName("UnitTest").getOrCreate()

# --- TEST RULES ENGINE  ---
def test_get_sql_constraint_ok():
    assert get_sql_constraint("age", "positive") == "age > 0"
    assert "IS NOT NULL" in get_sql_constraint("id", "notNull")

def test_get_sql_constraint_error():
    with pytest.raises(ValueError):
        get_sql_constraint("id", "regla_inventada_que_no_existe")

def test_build_validation_rules():
    config = [{"field": "email", "validations": ["notNull", "notEmpty"]}]
    rules, quarantine_sql = build_validation_rules(config)
    
    assert "valid_email_notNull" in rules
    assert "NOT(" in quarantine_sql and "AND" in quarantine_sql

# --- TEST METADATA READER Estructura JSON ---
def test_validate_metadata_structure():
    good_config = {
        "dataflows": [{"name": "flow1", "sources": ["s1"], "silver_logic": {"target_silver_table": "t1"}}]
    }
    bad_config = {"dataflows": []} 
    
    assert validate_metadata_schema(good_config) is True
    assert validate_metadata_schema(bad_config) is False

# --- TEST TRANSFORMATIONS FACTORY ---
def test_transformation_dispatcher(spark_local):
    cfg = [
        {"target_col": "ts", "type": "current_timestamp", "params": None},
        {"target_col": "clean_name", "type": "sql_expr", "params": "upper(name)"}
    ]
    result_map = build_transformation_map(cfg)
    
    assert "ts" in result_map
    assert "clean_name" in result_map
    assert str(result_map["clean_name"]).strip() != ""

def test_transformation_invalid():
    with pytest.raises(ValueError):
        build_transformation_map([{"target_col": "x", "type": "fake_type", "params": None}])