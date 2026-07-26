from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.config.settings import PipelineConfig

class QualityEnforcer:
    """Módulo responsável por validar a qualidade dos dados da Bronze para a Prata."""

    def __init__(self, config: PipelineConfig):
        self.config = config

    def apply_quality_checks(
        self, 
        df_bronze: DataFrame, 
        rules_conditions: dict, 
        table_name: str
    ) -> tuple[DataFrame, DataFrame]:
        """
        Separa os dados em válidos (para Prata) e inválidos (para Quarentena).
        
        :param rules_conditions: Dicionário onde a chave é o nome da regra e o valor é a expressão SQL do erro.
                                Ex: {"cpf_nulo": "cpf IS NULL", "valor_invalido": "valor <= 0"}
        """
        # 1. Constrói coluna de flag indicando se alguma regra falhou
        condicoes_erro = [F.expr(condition) for condition in rules_conditions.values()]
        
        df_flagged = df_bronze.withColumn("_has_error", F.greatest(*[c.cast("int") for c in condicoes_erro]) > 0)

        # 2. Constrói o motivo da rejeição concatenando as regras violadas
        motivos = []
        for rule_name, condition in rules_conditions.items():
            motivos.append(F.when(F.expr(condition), F.lit(rule_name)))
            
        df_flagged = df_flagged.withColumn("_motivo_rejeicao", F.array_compact(F.array(*motivos)))

        # 3. Separa registros Válidos e Quarentena
        df_validos = df_flagged.filter(F.col("_has_error") == False).drop("_has_error", "_motivo_rejeicao")
        df_quarentena = df_flagged.filter(F.col("_has_error") == True).drop("_has_error")

        # 4. Salva os dados rejeitados na Tabela de Quarentena em Delta Lake
        if df_quarentena.count() > 0:
            quarantine_path = self.config.get_table_path("silver", f"{table_name}_quarantine")
            (
                df_quarentena.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(quarantine_path)
            )
            print(f"⚠️ {df_quarentena.count()} registros enviados para a Quarentena em: {quarantine_path}")

        print(f"✅ {df_validos.count()} registros aprovados nas regras de qualidade para '{table_name}'.")
        return df_validos, df_quarentena
