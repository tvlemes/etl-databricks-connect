"""
Criação do Schema Silver
Autor: Thiago Vilarinho Lemes
Projeto: ETL no Databricks utilizando Catalog
Data: 14/09/2025
"""

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, when, lit
import os
from dotenv import load_dotenv
import pandas as pd

# -----------------------------
# Carrega variáveis de ambiente
# -----------------------------
load_dotenv()
catalog_name = os.getenv("NAME_CATALOG")
schema_bronze = os.getenv("NAME_SCHEMA_BRONZE")
table_bronze = os.getenv("NAME_TABLE_BRONZE")
schema_silver = os.getenv("NAME_SCHEMA_SILVER")
table_silver = os.getenv("NAME_TABLE_SILVER")

# -----------------------------
# Cria sessão Spark
# -----------------------------
def get_spark_session():
    '''
    Cria uma sessão Spark usando Databricks Connect.
    Returns:
        sessão Spark
    '''
    return DatabricksSession.builder.getOrCreate()


# -----------------------------
# Função principal da pipeline
# -----------------------------
def run_silver_pipeline(spark=None):
    """
    Executa a pipeline Silver: lê tabela Bronze, normaliza dados e salva em Silver.

    Args:
        spark (DatabricksSession, opcional): Sessão Spark. Se não fornecida, cria automaticamente.

    Returns:
        df_clean (DataFrame Spark): DataFrame Silver pronto.
    """
    if spark is None:
        spark = get_spark_session()

    # Seleciona catálogo e schema
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE SCHEMA {schema_silver}")

    # Leitura da tabela Bronze
    df = spark.read.table(f"{catalog_name}.{schema_bronze}.{table_bronze}")

    # Conversão para pandas para tratar datas misturadas
    df_pandas = df.toPandas()

    # Normalização da coluna date_added
    df_pandas['date_added'] = df_pandas['date_added'].astype(str).str.strip()
    df_pandas['date_added'] = df_pandas['date_added'].replace(['', 'None', 'nan', 'NaT'], 'noIns')
    df_pandas['date_added'] = pd.to_datetime(df_pandas['date_added'], format='mixed', errors='coerce')
    df_pandas['date_added'] = df_pandas['date_added'].dt.strftime('%Y-%m-%d').fillna('noIns')

    # Recria Spark DataFrame
    df = spark.createDataFrame(df_pandas)

    # Normalização de todas as colunas (nulo ou vazio -> "notIns")
    df_clean = df
    for c in df.columns:
        df_clean = df_clean.withColumn(
            c,
            F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), lit("notIns")).otherwise(F.col(c))
        )

    # Salva tabela Silver
    df_clean.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.{table_silver}")

    return df_clean


# -----------------------------
# Função auxiliar para testes
# -----------------------------
def get_env_variables():
    """
    Retorna variáveis de ambiente utilizadas no pipeline.
    """
    return {
        "catalog_name": catalog_name,
        "schema_bronze": schema_bronze,
        "table_bronze": table_bronze,
        "schema_silver": schema_silver,
        "table_silver": table_silver
    }
