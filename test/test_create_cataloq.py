'''
Teste unitário para criação de catálogo e schemas no Databricks utilizando mocks.
Autor: Thiago Vilarinho Lemes
Projeto: ETL no Databricks utilizando Catalog
Data: 15/09/2025
'''

import unittest
from unittest.mock import MagicMock, patch
from prod import create_cataloq

class TestDatabricksCatalog(unittest.TestCase):

    @patch("prod.create_cataloq.DatabricksSession")
    def test_create_catalog_and_schemas(self, mock_session):
        mock_spark = MagicMock()
        mock_session.builder.getOrCreate.return_value = mock_spark

        # Mock único para SHOW CATALOGS e SHOW SCHEMAS
        mock_spark.sql.return_value.collect.side_effect = [
            [("netflix_catalog",)],  # SHOW CATALOGS
            [MagicMock(databaseName="bronze"),
             MagicMock(databaseName="silver"),
             MagicMock(databaseName="gold")]  # SHOW SCHEMAS
        ]

        # Criação de catálogo e schemas
        create_cataloq.create_catalog(mock_spark, "netflix_catalog")
        create_cataloq.create_schema(mock_spark, "netflix_catalog", "bronze")
        create_cataloq.create_schema(mock_spark, "netflix_catalog", "silver")
        create_cataloq.create_schema(mock_spark, "netflix_catalog", "gold")

        calls = [call.args[0].replace("\n", " ").strip() for call in mock_spark.sql.call_args_list]
        self.assertTrue(any("CREATE CATALOG" in c for c in calls))
        self.assertTrue(any("CREATE SCHEMA IF NOT EXISTS netflix_catalog.bronze" in c for c in calls))
        self.assertTrue(any("CREATE SCHEMA IF NOT EXISTS netflix_catalog.silver" in c for c in calls))
        self.assertTrue(any("CREATE SCHEMA IF NOT EXISTS netflix_catalog.gold" in c for c in calls))

        # Teste de listagem
        catalogs = create_cataloq.list_catalogs(mock_spark)
        schemas = create_cataloq.list_schemas(mock_spark, "netflix_catalog")
        self.assertIn("netflix_catalog", catalogs)
        self.assertIn("bronze", schemas)
        self.assertIn("silver", schemas)
        self.assertIn("gold", schemas)


if __name__ == "__main__":
    unittest.main()
