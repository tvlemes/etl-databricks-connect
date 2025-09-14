'''
Limpar Cache do Spark
Autor: Thiago Vilarinho Lemes\
Projeto: ETL no Databricks utilizando Catalog \
Data: 14/09/2025
'''

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# Para evitar conflitos, certifique-se de que não há sessões Spark ativas 
spark.stop()

# COMMAND ----------

# Iniciando uma nova sessão Spark
spark = SparkSession.builder \
    .appName("MeuAppLimpo") \
    .getOrCreate()