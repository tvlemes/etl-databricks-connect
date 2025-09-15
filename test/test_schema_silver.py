'''
Teste unitário para pipeline Silver utilizando mocks.
Autor: Thiago Vilarinho Lemes
Projeto: ETL no Databricks utilizando Catalog
Data: 15/09/2025
'''

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import importlib

# -----------------------------
# Fixture Spark mockado
# -----------------------------
@pytest.fixture
def mock_spark():
    mock_spark = MagicMock()

    class FakeSparkDF:
        def __init__(self, df):
            self.df = df
            self.columns = list(df.columns)

        def toPandas(self):
            return self.df

        def select(self, *args, **kwargs):
            return self

        def distinct(self):
            return self

        def show(self, n=5):
            return None

        def withColumn(self, col_name, value):
            return self

        def filter(self, condition):
            return self

        def count(self):
            return len(self.df)

        @property
        def write(self):
            class Writer:
                def format(inner_self, fmt):
                    return inner_self
                def mode(inner_self, mode):
                    return inner_self
                def saveAsTable(inner_self, table_name):
                    return None
            return Writer()

    # Mock do método createDataFrame
    mock_spark.createDataFrame.side_effect = lambda df: FakeSparkDF(df)

    # Mock do método read.table para retornar tabela Bronze
    mock_spark.read.table.side_effect = lambda table_name: FakeSparkDF(
        pd.DataFrame({
            "id": [1, 2, 3],
            "title": ["A", "B", "C"],
            "date_added": ["January 1, 2020", "2020-05-10", None],
            "release_year": [2020, 2021, 2022],
            "type": ["Movie", "TV Show", "Movie"],
            "rating": ["PG", "R", "PG-13"]
        })
    )

    return mock_spark

# -----------------------------
# Teste unitário da pipeline Silver
# -----------------------------
@patch("prod.create_silver_table.get_spark_session")
def test_silver_pipeline(mock_get_spark, mock_spark):
    # Configura o Spark mockado
    mock_get_spark.return_value = mock_spark

    # Importa o módulo refatorado
    create_silver_table = importlib.import_module("prod.create_silver_table")

    # Executa a pipeline
    df_clean = create_silver_table.run_silver_pipeline()

    env = create_silver_table.get_env_variables()

    # -----------------------------
    # Verificações
    # -----------------------------
    # 1️⃣ Verifica se USE CATALOG e USE SCHEMA foram chamados
    mock_spark.sql.assert_any_call(f"USE CATALOG {env['catalog_name']}")
    mock_spark.sql.assert_any_call(f"USE SCHEMA {env['schema_silver']}")

    # 2️⃣ Verifica se DataFrame final contém coluna date_added
    assert "date_added" in df_clean.columns

    # 3️⃣ Verifica se valores nulos foram tratados (nenhum None ou NaN)
    pandas_df = df_clean.toPandas()
    assert pandas_df['date_added'].isnull().sum() == 0
    assert "notIns" in pandas_df['date_added'].values or "noIns" in pandas_df['date_added'].values

    # 4️⃣ Verifica se todas as colunas originais estão presentes
    expected_cols = ["id", "title", "date_added", "release_year", "type", "rating"]
    for col_name in expected_cols:
        assert col_name in df_clean.columns
