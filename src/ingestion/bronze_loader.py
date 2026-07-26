from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.config.settings import PipelineConfig

class BronzeLoader:
    """Módulo responsável pela ingestão incremental de arquivos brutos para a Camada Bronze (Delta Lake)."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def ingest_raw_file(
        self, 
        source_path: str, 
        file_format: str, 
        table_name: str, 
        options: dict = None
    ) -> DataFrame:
        """
        Lê arquivos brutos da landing zone, aplica metadados de governança e grava na Bronze em Delta.
        """
        options = options or {}
        
        # 1. Leitura dos dados brutos
        df_raw = (
            self.spark.read
            .format(file_format)
            .options(**options)
            .load(source_path)
        )

        # 2. Adição de Metadados Obrigatórios
        df_bronze = (
            df_raw
            .withColumn("_arquivo_origem", F.input_file_name())
            .withColumn("_data_ingestao", F.current_date())
            .withColumn("_timestamp_ingestao", F.lit(self.config.execution_timestamp))
            .withColumn("_batch_id", F.lit(self.config.batch_id))
            .withColumn("_schema_version", F.lit("v1.0"))
            # Hash SHA-256 combinando todas as colunas originais para auditoria
            .withColumn(
                "_hash_linha", 
                F.sha2(
                    F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_raw.columns]), 
                    256
                )
            )
        )

        # 3. Salvamento no formato Delta Lake com suporte a Schema Evolution
        target_path = self.config.get_table_path("bronze", table_name)
        
        (
            df_bronze.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(target_path)
        )

        print(f"✅ Ingestão Bronze concluída para a tabela '{table_name}' no caminho: {target_path}")
        return df_bronze
