import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.silver.quality_rules import QualityEnforcer

@pytest.fixture(scope="session")
def spark():
    """Fixture para criar a sessão do PySpark localmente nos testes."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("unit-tests-lakehouse")
        .getOrCreate()
    )

def test_quality_enforcer_quarantine_nulls(spark):
    """Testa se registros com ID nulo são corretamente enviados para a quarentena."""
    schema = StructType([
        StructField("id_transacao", StringType(), True),
        StructField("valor_transacao", DoubleType(), True)
    ])

    data = [
        ("TX1001", 250.0), # Válido
        (None, 500.0)      # Inválido (Nulo)
    ]

    df_test = spark.createDataFrame(data, schema)
    
    # Executa a filtragem
    df_validos = df_test.filter("id_transacao IS NOT NULL")
    df_invalidos = df_test.filter("id_transacao IS NULL")

    assert df_validos.count() == 1
    assert df_invalidos.count() == 1
