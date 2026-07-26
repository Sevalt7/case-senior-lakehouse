from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.config.settings import PipelineConfig

class ScdType2Processor:
    """Módulo responsável por aplicar SCD Tipo 2 para manter o histórico de dimensões mutáveis."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def process_scd2(
        self,
        df_updates: DataFrame,
        target_table_name: str,
        primary_key: str,
        tracked_columns: list[str]
    ):
        """
        Aplica MERGE com SCD Tipo 2 no Delta Lake.
        """
        target_path = self.config.get_table_path("silver", target_table_name)
        
        # 1. Se a tabela destino ainda não existir no Delta, cria a versão inicial
        if not DeltaTable.isDeltaTable(self.spark, target_path):
            df_initial = (
                df_updates
                .withColumn("_data_inicio", F.lit(self.config.execution_timestamp))
                .withColumn("_data_fim", F.lit(None).cast("timestamp"))
                .withColumn("_registro_atual", F.lit(True))
            )
            df_initial.write.format("delta").mode("overwrite").save(target_path)
            print(f"✅ Tabela Prata inicial criada com SCD2: {target_path}")
            return

        # 2. Se a tabela já existir, realiza o MERGE SCD Tipo 2
        delta_target = DeltaTable.forPath(self.spark, target_path)

        # Prepara registros de atualização adicionando coluna de junção
        staged_updates = (
            df_updates
            .withColumn("merge_key", F.col(primary_key))
            .withColumn("_data_inicio", F.lit(self.config.execution_timestamp))
            .withColumn("_data_fim", F.lit(None).cast("timestamp"))
            .withColumn("_registro_atual", F.lit(True))
        )

        # Prepara os registros que vão fechar o histórico antigo (data_fim)
        condition_changes = " OR ".join([f"target.{col} <> source.{col}" for col in tracked_columns])
        
        staged_historical = (
            df_updates.alias("source")
            .join(
                delta_target.toDF().filter(F.col("_registro_atual") == True).alias("target"),
                primary_key
            )
            .filter(F.expr(condition_changes))
            .select("source.*")
            .withColumn("merge_key", F.lit(None))  # Null para forçar a inserção como nova linha
            .withColumn("_data_inicio", F.lit(self.config.execution_timestamp))
            .withColumn("_data_fim", F.lit(None).cast("timestamp"))
            .withColumn("_registro_atual", F.lit(True))
        )

        # Unifica os novos registros com as atualizações
        df_union = staged_updates.unionByName(staged_historical)

        # Executa a operação MERGE no Delta Lake
        (
            delta_target.alias("target")
            .merge(
                df_union.alias("source"),
                f"target.{primary_key} = source.merge_key AND target._registro_atual = true"
            )
            .whenMatchedUpdate(
                condition=F.expr(condition_changes),
                set={
                    "_data_fim": "source._data_inicio",
                    "_registro_atual": "false"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"✅ SCD Tipo 2 aplicado com sucesso na tabela Prata: {target_table_name}")
