"""
Create Bronze Table
Autor: Thiago Vilarinho Lemes
Projeto: ETL no Databricks utilizando Catalog
Data: 14/09/2025
"""

# COMMAND ----------
from databricks.connect import DatabricksSession
import pandas as pd
import os
from dotenv import load_dotenv

# COMMAND ----------
def get_env_variables():
    '''
    Carrega as variáveis de ambiente do .env.
    Returns: 
        Dicionário com as variáveis de ambiente necessárias.
    '''
    load_dotenv()
    return {
        "catalog_name": os.getenv("NAME_CATALOG"),
        "schema_bronze": os.getenv("NAME_SCHEMA_BRONZE"),
        "table_bronze": os.getenv("NAME_TABLE_BRONZE"),
        "schema_silver": os.getenv("NAME_SCHEMA_SILVER"),
        "table_silver": os.getenv("NAME_TABLE_SILVER"),
        "schema_gold": os.getenv("NAME_SCHEMA_GOLD"),
        "table_gold": os.getenv("NAME_TABLE_GOLD"),
        "schema_raw": os.getenv("NAME_SCHEMA_RAW"),
        "bucket_raw": os.getenv("NAME_STORAGE"),
    }

# COMMAND ----------
def get_spark_session():
    '''
    Cria a sessão Spark usando Databricks Connect.
    Returns:
        sessão Spark
    '''
    return DatabricksSession.builder.getOrCreate()

# COMMAND ----------
def set_catalog_and_schema(spark, catalog_name, schema_name):
    '''
    Configura o catálogo e schema no Spark.
    Args:
        spark: sessão Spark
        catalog_name: nome do catálogo
        schema_name: nome do schema
    '''
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------
def load_parquet_files(file1, file2):
    '''
    Carrega dois arquivos parquet em DataFrames Pandas.
    Args:
        file1: caminho do primeiro arquivo parquet
        file2: caminho do segundo arquivo parquet
    Returns:
        dois DataFrames Pandas
    '''
    df_1 = pd.read_parquet(file1)
    df_2 = pd.read_parquet(file2)
    return df_1, df_2

# COMMAND ----------
def convert_to_spark_df(spark, df_1, df_2):
    '''
    Converte DataFrames Pandas para Spark.
    Args:
        spark: sessão Spark
        df_1: DataFrame Pandas 1
        df_2: DataFrame Pandas 2
    Returns:
        dois DataFrames Spark    
    '''
    return spark.createDataFrame(df_1), spark.createDataFrame(df_2)

# COMMAND ----------
def combine_dataframes(df_1, df_2):
    '''
    Combina dois DataFrames Spark usando unionByName.
    Args:
        df_1: DataFrame Spark 1
        df_2: DataFrame Spark 2
    Returns:
        DataFrame Spark combinado
    '''
    if set(df_1.columns) != set(df_2.columns):
        raise ValueError("Os DataFrames possuem colunas diferentes.")
    return df_1.unionByName(df_2)

# COMMAND ----------
def write_delta_table(df, catalog_name, schema_name, table_name):
    '''
    Escreve o DataFrame Spark em uma tabela Delta.
    Args:
        df: DataFrame Spark
        catalog_name: nome do catálogo
        schema_name: nome do schema
        table_name: nome da tabela
    '''
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog_name}.{schema_name}.{table_name}"
    )

# COMMAND ----------
def run_pipeline(file1="../../dataset/netflix_v01.parquet", file2="../../dataset/netflix_v02.parquet"):
    '''
    Executa toda a pipeline do Bronze Schema.
    Args:
        file1: caminho do primeiro arquivo parquet
        file2: caminho do segundo arquivo parquet
    Returns:
        DataFrame Spark final
    '''
    env = get_env_variables()
    spark = get_spark_session()

    set_catalog_and_schema(spark, env["catalog_name"], env["schema_bronze"])

    df_1, df_2 = load_parquet_files(file1, file2)
    sdf_1, sdf_2 = convert_to_spark_df(spark, df_1, df_2)
    df_combined = combine_dataframes(sdf_1, sdf_2)

    write_delta_table(df_combined, env["catalog_name"], env["schema_raw"], env["bucket_raw"])
    return df_combined
