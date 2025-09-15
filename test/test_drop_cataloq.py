# test_clean_spark_cache.py

import pytest
from unittest.mock import patch, MagicMock

# -----------------------------
# Testa limpar cache e iniciar Spark
# -----------------------------
@patch("pyspark.sql.SparkSession")
def test_spark_restart(mock_spark_class):
    # Mock da sessão Spark
    mock_spark_instance = MagicMock()
    mock_builder = MagicMock()
    mock_builder.getOrCreate.return_value = mock_spark_instance
    mock_spark_class.builder = mock_builder

    # Mock do stop
    mock_spark_instance.stop = MagicMock()

    # Simula código do seu script
    # Primeiro, tenta parar a sessão se existir
    try:
        mock_spark_instance.stop()
    except Exception:
        pass

    # Inicia nova sessão
    spark = mock_spark_class.builder.getOrCreate()

    # -----------------------------
    # Asserts
    # -----------------------------
    mock_spark_instance.stop.assert_called_once()
    mock_builder.getOrCreate.assert_called_once()
    assert spark == mock_spark_instance
