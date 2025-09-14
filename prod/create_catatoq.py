'''
Catalog Netflix
Autor: Thiago Vilarinho Lemes\
Projeto: ETL no Databricks utilizando Catalog \
Data: 14/09/2025
'''

# COMMAND ----------

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from dotenv import load_dotenv
import os

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

# Substitua pelo nome do catálogo que você quer criar
catalog_name = catalog_name

# COMMAND ----------

# Comando SQL para criar o catálogo
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog_name}
COMMENT 'Catálogo criado via Databricks Connect'
""")
print(f"Catálogo {catalog_name} criado com sucesso!")

# COMMAND ----------

# Criando um Schema dentro do Catálogo

# Nome do schema 
schema_name = "bronze"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
COMMENT 'Schema Bronze para dados raw'
""")

print(f"Schema '{schema_bronze}' criado no catálogo {catalog_name}.")


# COMMAND ----------

# Criando um Schema dentro do Catálogo

# Nome do schema 
schema_name = "silver"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
COMMENT 'Schema Bronze para dados raw'
""")

print(f"Schema '{schema_silver}' criado no catálogo {catalog_name}.")

# COMMAND ----------

# Criando um Schema dentro do Catálogo

# Nome do schema 
schema_name = "gold"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
COMMENT 'Schema Bronze para dados raw'
""")

print(f"Schema '{schema_gold}' criado no catálogo {catalog_name}.")

# COMMAND ----------


# Verifica se o catálogo foi criado
catalogs = spark.sql("SHOW CATALOGS").collect()
if (catalog_name,) in catalogs:
    print(f"Catálogo '{catalog_name}' criado com sucesso!")
else:
    print(f"Catálogo '{catalog_name}' não foi encontrado.")

# COMMAND ----------

# Verifica os schemas dentro do catálogo 
schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
print(f"Schemas no catálogo {catalog_name}:")
print("-" * 45)
for schema in schemas:
    print(f" - {schema.databaseName}")

# COMMAND ----------

