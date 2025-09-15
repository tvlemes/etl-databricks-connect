"""
Catalog Netflix
Autor: Thiago Vilarinho Lemes
Projeto: ETL no Databricks utilizando Catalog
Data: 14/09/2025
"""
# COMMAND ----------
# Importa as bibliotecas
from databricks.connect import DatabricksSession
from dotenv import load_dotenv
import os

# COMMAND ----------
# Carrega variáveis de ambiente
load_dotenv()

# COMMAND ----------
def get_spark_session():
    '''
    Cria e retorna a sessão Spark via Databricks Connect.
    Returns:
        sessão Spark
    '''
    return DatabricksSession.builder.getOrCreate()

# COMMAND ----------
def create_catalog(spark=None, catalog_name=None):
    '''
    Cria o catálogo se não existir.
    Args:
        spark: sessão Spark (opcional)
        catalog_name: nome do catálogo (opcional)
    '''

    if spark is None:
        spark = get_spark_session()
    if catalog_name is None:
        catalog_name = os.getenv("NAME_CATALOG")

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name} COMMENT 'Catálogo criado via Databricks Connect'")
    print(f"Catálogo {catalog_name} criado com sucesso!")

# COMMAND ----------
def create_schema(spark, catalog_name, schema_name):
    '''
    Cria schema dentro do catálogo.
    Args:
        spark: sessão Spark
        catalog_name: nome do catálogo
        schema_name: nome do schema
    '''

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name} COMMENT 'Schema {schema_name}'")
    print(f"Schema '{schema_name}' criado no catálogo {catalog_name}.")

# COMMAND ----------
def list_catalogs(spark=None):
    '''
    Retorna a lista de catálogos existentes.
    Args:
        spark: sessão Spark (opcional)
    Returns:
        lista de catálogos
    '''
    if spark is None:
        spark = get_spark_session()
    catalogs = spark.sql("SHOW CATALOGS").collect()
    return [catalog[0] for catalog in catalogs]

# COMMAND ----------
# Função para listar schemas
def list_schemas(spark=None, catalog_name=None):
    '''
    Retorna a lista de schemas dentro de um catálogo.
    Args:
        spark: sessão Spark (opcional)
        catalog_name: nome do catálogo (opcional)
    Returns:
        lista de schemas
    '''
    if spark is None:
        spark = get_spark_session()
    if catalog_name is None:
        catalog_name = os.getenv("NAME_CATALOG")

    schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
    return [schema.databaseName for schema in schemas]
