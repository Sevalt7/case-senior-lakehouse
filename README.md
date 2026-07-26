# 🏛️ Data Product Lakehouse: Plataforma de Transações Financeiras (Sicredi Case)

[![PySpark](https://img.shields.io/badge/PySpark-3.4+-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0+-blue.svg)](https://delta.io/)
[![Databricks](https://img.shields.io/badge/Databricks-Unity_Catalog-red.svg)](https://databricks.com/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Plataforma de Engenharia de Dados em arquitetura **Lakehouse Medallion** projetada para ingestão, governança, enriquecimento e geração de *Data Products* analíticos sobre transações financeiras e dados cadastrais.

---

## 📐 Arquitetura da Solução (Medallion Architecture)

[Fontes / APIs / OLTP]
│
▼
┌───────────────┐
│   BRONZE      │ ➔ Ingestão Raw Incremental (Append Only)
│  (Raw Zone)   │    - Metadados de Auditoria (_ingestion_timestamp, _batch_id, _hash_linha)
└───────┬───────┘
│
▼
┌───────────────┐
│    SILVER     │ ➔ Validação de Qualidade & Governança (Data Quality Rules)
│ (Trusted Zone)│    - Separação em Tabela Quarentena
└───────┬───────┘    - SCD Tipo 2 via MERGE Delta (Histórico Cadastral)
│
▼
┌───────────────┐
│     GOLD      │ ➔ Data Products & Modelagem Star Schema (Fact/Dimensions)
│(Analytics Zone)    - Features de Machine Learning para Detecção de Fraude
└───────────────┘    - Consumo BI / Consultas SQL Avançadas (Window Functions)

---

## 🛡️ Decisões de Arquitetura (ADR - Architecture Decision Records)

1. **Delta Lake + Unity Catalog:** Adotado para suporte a transações ACID, time travel, unificação de governança e controle de acesso granular (*RBAC*) por tabela/coluna.
2. **Estratégia de SCD Tipo 2 na Prata:** Garante rastro histórico completo das mudanças cadastrais de clientes/contas sem sobrescrever dados do passado.
3. **Mecanismo de Quarentena (QualityEnforcer):** Evita envenenamento dos dados analíticos (*Data Poisoning*), isolando inconsistências sem interromper a pipeline de produção.
4. **Data Products Desvinculados:** A camada Ouro entrega visões modeladas em *Star Schema* otimizadas para BI e *Feature Stores* preparadas para modelos de IA/ML.

---

## 📁 Estrutura do Repositório

```text
case-senior-lakehouse/
├── src/
│   ├── config/          # Definições centralizadas e variáveis de ambiente
│   │   └── settings.py
│   ├── ingestion/       # Pipeline da Camada Bronze (Injeção de Metadados)
│   │   └── bronze_ingestion.py
│   ├── silver/          # Qualidade de Dados & Gestão de SCD Tipo 2
│   │   ├── quality_rules.py
│   │   └── scd_type2.py
│   ├── gold/            # Modelagem Star Schema & Features de ML
│   │   └── star_schema.py
│   └── sql/             # Consultas SQL Avançadas (CTEs, Window Functions, SCD2)
│       └── queries.sql
├── tests/               # Suíte de Testes Unitários Automatizados
│   └── test_quality.py
├── .gitignore
├── pyproject.toml       # Gerenciamento de Dependências Python
└── README.md
```

---

## 🛠️ Tecnologias e Padrões
* **Linguagens:** Python 3.10+, PySpark 3.4+, SQL Avançado (Databricks SQL)
* **Storage/Format:** Delta Lake 3.0 (Parquet com transações ACID)
* **Qualidade & Testes:** `pytest`, `chispa`, Pydantic
* **Engenharia de Software:** Modularização OOP, Clean Code, Type Hinting

---

## 👤 Autor
**Steffan Sevalt** — *Data Engineer Senior Case Solution*
