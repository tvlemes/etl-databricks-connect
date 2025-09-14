'''
Schema Silver
Autor: Thiago Vilarinho Lemes\
Projeto: ETL no Databricks utilizando Catalog \
Data: 14/09/2025
'''

# COMMAND ----------

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, when
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
try:
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE SCHEMA {schema_silver}")
    print(f"Catalog e Schema selecionados: {catalog_name}.{schema_silver}")
except Exception as e:
    print(f"Erro ao selecionar Catalog ou Schema: {e}")

# COMMAND ----------

catalog_name = catalog_name

# COMMAND ----------

try:
    df = spark.read.table(f"{catalog_name}.{schema_bronze}.{table_bronze}")
    print(f"Leitura da tabela {catalog_name}.{schema_bronze}.{table_bronze} realizada com sucesso.")
    df.show(5)
except Exception as e:
    print(f"Erro ao ler a tabela: {e}")

# COMMAND ----------

# Analise Exploratória
# COMMAND ----------

# Nome das colunas
print("Nomes das colunas no DataFrame:")
print(df.columns)

# COMMAND ----------

# Quantidade de linhas e colunas do DataFrame
print(f"Quantidade de linhas do Dataframe: {df.count()} linhas")
print(f"Quantidade de colunas do Dataframe: {len(df.columns)} colunas")

# COMMAND ----------

# Verificando os valores nulos de cada coluna
try:
    nulos_por_coluna = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ])
    print("Verificação de valores nulos realizada com sucesso.")
    nulos_por_coluna.show()
except Exception as e:
    print(f"Erro ao verificar valores nulos: {e}")

# COMMAND ----------

# Verificando os valores únicos da coluna date_added
df_unicos = df.select("date_added").distinct()

# Mostra os valores
print("Valores únicos na coluna date_added:")
df_unicos.show()

# COMMAND ----------

# Verificando os valores únicos da coluna release_year
df_unicos = df.select("release_year").distinct()

# Mostra os valores
print("Valores únicos na coluna release_year:")
df_unicos.show()

# COMMAND ----------

# Verificando os valores únicos da coluna type
df_unicos = df.select("type").distinct()

# Mostra os valores
print("Valores únicos na coluna type:")
df_unicos.show()

# COMMAND ----------

# Verificando os valores únicos da coluna rating
df_unicos = df.select("rating").distinct()

# Mostra os valores
print("Valores únicos na coluna rating:")
df_unicos.show()

# COMMAND ----------

# Normalizando dados
# COMMAND ----------

df.show(15)

# COMMAND ----------

# Converte para pandas
# Melhora a manipulação de datas com formatos mistos
try:

    # Converte DataFrame Spark para pandas
    df_pandas = df.toPandas()
    print("- Conversão para pandas realizada com sucesso.")

    # Remove espaços e normaliza valores nulos/vazios
    df_pandas['date_added'] = df_pandas['date_added'].astype(str).str.strip()
    df_pandas['date_added'] = df_pandas['date_added'].replace(['', 'None', 'nan', 'NaT'], 'noIns')
    print("- Limpeza inicial da coluna date_added realizada com sucesso.")

    # Converte para datetime de forma inteligente (mixed format)
    df_pandas['date_added'] = pd.to_datetime(df_pandas['date_added'], format='mixed', errors='coerce')
    print("- Conversão para datetime realizada com sucesso.")

    # Converte para string no formato yyyy-MM-dd, substituindo nulos por "noIns"
    df_pandas['date_added'] = df_pandas['date_added'].dt.strftime('%Y-%m-%d').fillna('noIns')
    print("- Formatação da coluna date_added realizada com sucesso.")

    # Recria o DataFrame do Spark
    df = spark.createDataFrame(df_pandas)
    print("- Conversão da coluna date_added para datetime realizada com sucesso.")
    df.show(5)
except Exception as e:
    print(f"Erro ao converter a coluna date_added: {e}")

# COMMAND ----------

# Verificando os valores únicos da coluna date_added após a limpeza
df_unicos = df.select("date_added").distinct()
print("Valores únicos na coluna date_added após a limpeza:")
df_unicos.show(5)

# COMMAND ----------

# Verificando os valores nulos de cada coluna novamente
print("Verificando os valores nulos da coluna 'date_added':")
df.filter(F.col("date_added") == "noIns").show(5)

# COMMAND ----------

# Normalizando dados
# Substituindo valores nulos ou vazios por 'notIns'
try:
    df_clean = df
    for c in df.columns:
        df_clean = df_clean.withColumn(
            c,
            F.when(
                F.col(c).isNull() | (F.trim(F.col(c)) == ""),  # nulo ou vazio
                F.lit("notIns")
            ).otherwise(F.col(c))
        )
    print("- Normalização de dados realizada com sucesso.")
    df_clean.show(5)
except Exception as e:
    print(f"Erro ao normalizar dados: {e}")

# COMMAND ----------

# Verificando os valores nulos de cada coluna
try:
    nulos_por_coluna = df_clean.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df_clean.columns
    ])
    print("Verificação de valores nulos após a normalização.")
    nulos_por_coluna.show()
except Exception as e:
    print(f"Erro ao verificar valores nulos: {e}")

# COMMAND ----------

try:
    df_clean.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.{table_silver}")
    print("DataFrames carregados com sucesso!")
except Exception as e:
    print(f"Erro ao carregar DataFrames: {e}")

# COMMAND ----------

# Carregando os dados da tabela silver para verificação
df_silver = spark.read.table(f"{catalog_name}.{schema_silver}.{table_silver}")
df_silver.show(5)