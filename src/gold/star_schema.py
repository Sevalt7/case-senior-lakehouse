from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.config.settings import PipelineConfig

class GoldStarSchemaBuilder:
    """Módulo responsável por construir o modelo Star Schema e Data Products na Camada Ouro."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def build_fact_transacoes(self) -> DataFrame:
        """
        Cria a Tabela Fato de Transações unindo os eventos com as dimensões ativas (SCD Tipo 2).
        """
        # Leitura das tabelas Prata
        df_transacoes = self.spark.read.format("delta").load(self.config.get_table_path("silver", "transacoes"))
        df_clientes = self.spark.read.format("delta").load(self.config.get_table_path("silver", "clientes"))
        df_contas = self.spark.read.format("delta").load(self.config.get_table_path("silver", "contas"))

        # Filtra apenas a versão cadastral vigente no momento da transação ou a mais recente
        df_clientes_ativos = df_clientes.filter(F.col("_registro_atual") == True)
        df_contas_ativas = df_contas.filter(F.col("_registro_atual") == True)

        # Construção da Tabela Fato com enriched keys
        df_fato = (
            df_transacoes.alias("t")
            .join(df_clientes_ativos.alias("c"), "id_cliente", "left")
            .join(df_contas_ativas.alias("ct"), "id_conta", "left")
            .select(
                F.col("t.id_transacao"),
                F.col("t.id_cliente"),
                F.col("t.id_conta"),
                F.col("t.data_transacao"),
                F.col("t.valor_transacao"),
                F.col("t.tipo_transacao"),
                F.col("t.canal_transacao"),
                F.col("c.nome_cliente"),
                F.col("c.segmento_cliente"),
                F.col("ct.tipo_conta"),
                F.col("ct.saldo_atual")
            )
        )

        target_path = self.config.get_table_path("gold", "fact_transacoes")
        df_fato.write.format("delta").mode("overwrite").save(target_path)
        print(f"✅ Tabela Fato de Transações gerada com sucesso em: {target_path}")
        return df_fato

    def build_features_cliente(self) -> DataFrame:
        """
        Cria o Data Product com Features comportamentais de clientes para uso em Machine Learning.
        """
        df_fato = self.spark.read.format("delta").load(self.config.get_table_path("gold", "fact_transacoes"))

        df_features = (
            df_fato
            .groupBy("id_cliente")
            .agg(
                F.count("id_transacao").alias("qtde_transacoes_total"),
                F.sum("valor_transacao").alias("volume_total_transacionado"),
                F.avg("valor_transacao").alias("ticket_medio"),
                F.max("valor_transacao").alias("maior_transacao"),
                F.max("data_transacao").alias("data_ultima_transacao"),
                F.count(F.when(F.col("valor_transacao") > 10000, 1)).alias("qtde_transacoes_alto_valor")
            )
            .withColumn("_data_processamento", F.lit(self.config.execution_timestamp))
        )

        target_path = self.config.get_table_path("gold", "features_cliente")
        df_features.write.format("delta").mode("overwrite").save(target_path)
        print(f"✅ Data Product (Features de Cliente) gerado com sucesso em: {target_path}")
        return df_features
