'''
Schema Bronze
Autor: Thiago Vilarinho Lemes\
Projeto: ETL no Databricks utilizando Catalog \
Data: 14/09/2025
'''

# COMMAND ----------

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv
import pandas as pd

# COMMAND ----------

# Carrega as variáveis de ambiente do arquivo .env

load_dotenv()

catalog_name = os.getenv("NAME_CATALOG")

schema_bronze = os.getenv("NAME_SCHEMA_BRONZE")
table_bronze = os.getenv("NAME_TABLE_BRONZE")

schema_silver = os.getenv("NAME_SCHEMA_SILVER")
table_silver = os.getenv("NAME_TABLE_SILVER")

schema_gold = os.getenv("NAME_SCHEMA_GOLD")
table_gold = os.getenv("NAME_TABLE_GOLD")

schema_raw = os.getenv("NAME_SCHEMA_RAW")
bucket_raw = os.getenv("NAME_STORAGE")

# COMMAND ----------

# Cria a sessão Spark usando Databricks Connect
spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

# Seleciona o catalog e schema a serem utilizados
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_bronze}")

# COMMAND ----------

try:
    # Carrega os datasets dos arquivos Parquet
    df_1 = pd.read_parquet("../../dataset/netflix_v01.parquet")
    df_2 = pd.read_parquet("../../dataset/netflix_v02.parquet")

    # Transforma os DataFrames do Pandas em DataFrames do Spark
    df_1 = spark.createDataFrame(df_1)
    df_2 = spark.createDataFrame(df_2)
    print("Arquivos Parquet carregados com sucesso.")
except Exception as e:
    print("Erro ao carregar os arquivos Parquet:", e)
    raise

# COMMAND ----------

# Verifica o schema e o tamanho dos DataFrames
print("DataFrame 1:")
print("Tamanho:", df_1.count(), "linhas")
print("*" * 20)
print(df_1.show(5))
print("\n")
print("DataFrame 2:")
print("Tamanho:", df_2.count(), "linhas")
print("*" * 20)
print(df_2.show(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Analise Exploratória

# COMMAND ----------

# Quantidade de linhas e colunas dos DataFrames
print("Dataframe 1")
print(f"Quantidade de linha: {df_1.count()} linhas")
print(f"Quantidade de colunas: {len(df_1.columns)} colunas")
print("*" * 50)
print("Dataframe 2")
print(f"Quantidade de linha: {df_2.count()} linhas")
print(f"Quantidade de colunas: {len(df_2.columns)} colunas")

# COMMAND ----------

# Nome das colunas
print("Colunas dos DataFrames 1")
print(df_1.columns)

print("*" * 139)

print("Colunas dos DataFrames 2")
print(df_2.columns)

print("*" * 139)

# Verifica se os DataFrames possuem as mesmas colunas para realizar o merge
if set(df_1.columns) != set(df_2.columns):
    raise ValueError("Os DataFrames possuem colunas diferentes.")
else:
    print("Os DataFrames possuem as mesmas colunas.")

# COMMAND ----------

# 1° Forma - Realiza o merge dos datasets
# df_combined = df_1.join(df_2, on="id", how="outer")  # outer join mantém todas as linhas

# COMMAND ----------

# 2° Forma - Realiza o merge dos datasets
df = df_1.unionByName(df_2)

# COMMAND ----------

print("DataFrame:")
print("Tamanho:", df.count(), "linhas")
print("*" * 21)
df.show(5, truncate=False)

# COMMAND ----------

try:
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_raw}.{bucket_raw}")
    print("DataFrames carregados com sucesso!")
except Exception as e:
    print(f"Erro ao carregar DataFrames: {e}")