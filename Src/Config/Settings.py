import os
from dataclasses import dataclass
from datetime import datetime

@dataclass
class PipelineConfig:
    """Centraliza todos os parâmetros de ambiente do Lakehouse."""
    
    # 1. Nomes de Catalog e Schemas no Unity Catalog (Databricks)
    catalog_name: str = os.getenv("CATALOG_NAME", "lakehouse_dev")
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    
    # 2. Caminhos de Armazenamento (Delta Lake / DBFS / S3 / ADLS)
    base_landing_path: str = os.getenv("LANDING_PATH", "/mnt/landing")
    base_delta_path: str = os.getenv("DELTA_PATH", "/mnt/delta")
    
    # 3. Metadados do Lote de Execução
    execution_timestamp: datetime = datetime.now()
    batch_id: str = execution_timestamp.strftime("%Y%m%d_%H%M%S")

    def get_table_path(self, layer: str, table_name: str) -> str:
        """Gera o caminho absoluto no Delta Lake para cada camada e tabela."""
        return f"{self.base_delta_path}/{layer}/{table_name}"
