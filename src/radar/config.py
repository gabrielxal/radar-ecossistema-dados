"""Configuracao central do projeto.

Somente constantes e funcoes puras. Este modulo e importado por todos os
outros, entao nao pode ter dependencia externa nem efeito colateral.
"""

# --------------------------------------------------------------------------
# API do GitHub
# --------------------------------------------------------------------------

API_BASE = "https://api.github.com"

# Versao fixada: o GitHub evolui a API e uma mudanca de contrato quebraria
# o pipeline sem que nada tenha mudado no nosso codigo.
API_VERSION = "2022-11-28"

# O GitHub exige User-Agent e o usa para contato em caso de uso abusivo.
USER_AGENT = "radar-ecossistema-dados"

# 100 e o maximo. Menos paginas = menos requisicoes = menos quota.
PER_PAGE = 100

# requests NAO tem timeout por padrao: sem isto, uma chamada trava o job.
TIMEOUT = 30

# --------------------------------------------------------------------------
# Lakehouse
# --------------------------------------------------------------------------

CATALOG = "workspace"
BRONZE = "radar_bronze"
SILVER = "radar_silver"
GOLD = "radar_gold"
VOLUME = "raw"

# --------------------------------------------------------------------------
# Escopo da analise
# --------------------------------------------------------------------------

# Tupla, nao lista: imutavel. Ninguem faz REPOS.append() por engano.
REPOS = (
    "apache/airflow",
    "apache/spark",
    "dbt-labs/dbt-core",
    "duckdb/duckdb",
    "pola-rs/polars",
    "delta-io/delta",
    "dagster-io/dagster",
    "PrefectHQ/prefect",
    "trinodb/trino",
    "great-expectations/great_expectations",
    "apache/iceberg",
    "apache/hudi",
    "sqlfluff/sqlfluff",
    "datahub-project/datahub",
)


def fqn(schema: str, tabela: str, catalog: str = CATALOG) -> str:
    """Nome totalmente qualificado: catalog.schema.tabela.

    Existe para que nenhum notebook escreva o nome da tabela a mao.
    """
    return f"{catalog}.{schema}.{tabela}"