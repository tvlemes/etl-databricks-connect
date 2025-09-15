'''
Teste unitário para create_bronze_table utilizando mocks.
Autor: Thiago Vilarinho Lemes
Projeto: ETL no Databricks utilizando Catalog
Data: 15/09/2025
'''

import sys
import os
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

# Adiciona a pasta prod ao sys.path para importar create_bronze_table
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../prod")))
import create_bronze_table as cbt

# -----------------------------
# Fixture para mockar Spark
# -----------------------------
@pytest.fixture
def mock_spark():
    mock_spark = MagicMock()
    mock_spark.createDataFrame.side_effect = lambda df: f"spark_df_{df.shape}"
    mock_spark.sql = MagicMock()
    return mock_spark

# -----------------------------
# Testa get_env_variables
# -----------------------------
@patch("create_bronze_table.os.getenv")
@patch("create_bronze_table.load_dotenv")
def test_get_env_variables(mock_load_dotenv, mock_getenv):
    mock_getenv.side_effect = lambda key: f"{key}_value"
    env = cbt.get_env_variables()
    assert env["catalog_name"] == "NAME_CATALOG_value"
    assert env["schema_bronze"] == "NAME_SCHEMA_BRONZE_value"
    assert env["table_bronze"] == "NAME_TABLE_BRONZE_value"

# -----------------------------
# Testa set_catalog_and_schema
# -----------------------------
def test_set_catalog_and_schema(mock_spark):
    cbt.set_catalog_and_schema(mock_spark, "cat", "schema")
    mock_spark.sql.assert_any_call("USE CATALOG cat")
    mock_spark.sql.assert_any_call("USE SCHEMA schema")

# -----------------------------
# Testa load_parquet_files
# -----------------------------
@patch("create_bronze_table.pd.read_parquet")
def test_load_parquet_files(mock_read_parquet):
    df1 = pd.DataFrame({"a": [1,2]})
    df2 = pd.DataFrame({"a": [3,4]})
    mock_read_parquet.side_effect = [df1, df2]

    df_1, df_2 = cbt.load_parquet_files("file1.parquet", "file2.parquet")
    
    pd.testing.assert_frame_equal(df_1, df1)
    pd.testing.assert_frame_equal(df_2, df2)

# -----------------------------
# Testa convert_to_spark_df
# -----------------------------
def test_convert_to_spark_df(mock_spark):
    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"a": [2]})
    
    sdf1, sdf2 = cbt.convert_to_spark_df(mock_spark, df1, df2)
    assert sdf1 == f"spark_df_{df1.shape}"
    assert sdf2 == f"spark_df_{df2.shape}"

# -----------------------------
# Testa combine_dataframes
# -----------------------------
def test_combine_dataframes():
    df1 = MagicMock()
    df2 = MagicMock()
    df1.columns = ["a", "b"]
    df2.columns = ["b", "a"]
    
    df1.unionByName.return_value = "combined_df"
    result = cbt.combine_dataframes(df1, df2)
    assert result == "combined_df"

def test_combine_dataframes_columns_diferentes():
    df1 = MagicMock()
    df2 = MagicMock()
    df1.columns = ["a", "b"]
    df2.columns = ["a", "c"]
    
    with pytest.raises(ValueError):
        cbt.combine_dataframes(df1, df2)

# -----------------------------
# Testa write_delta_table
# -----------------------------
def test_write_delta_table(mock_spark):
    df = MagicMock()
    cbt.write_delta_table(df, "cat", "schema", "table")
    df.write.format.assert_called_with("delta")
    df.write.format().mode().saveAsTable.assert_called_with("cat.schema.table")

# -----------------------------
# Testa run_pipeline
# -----------------------------
@patch("create_bronze_table.get_env_variables")
@patch("create_bronze_table.get_spark_session")
@patch("create_bronze_table.load_parquet_files")
@patch("create_bronze_table.convert_to_spark_df")
@patch("create_bronze_table.combine_dataframes")
@patch("create_bronze_table.write_delta_table")
def test_run_pipeline(mock_write, mock_combine, mock_convert, mock_load, mock_spark_fn, mock_env):
    mock_env.return_value = {
        "catalog_name": "cat",
        "schema_bronze": "schema_bronze",
        "table_bronze": "table_bronze",
        "schema_raw": "schema_raw",
        "bucket_raw": "bucket_raw"
    }
    mock_spark = MagicMock()
    mock_spark_fn.return_value = mock_spark
    mock_df1 = MagicMock()
    mock_df2 = MagicMock()
    mock_load.return_value = (mock_df1, mock_df2)
    mock_sdf1 = MagicMock()
    mock_sdf2 = MagicMock()
    mock_convert.return_value = (mock_sdf1, mock_sdf2)
    mock_combine.return_value = "final_df"

    result = cbt.run_pipeline("f1", "f2")
    
    assert result == "final_df"
    mock_write.assert_called_once_with("final_df", "cat", "schema_raw", "bucket_raw")
