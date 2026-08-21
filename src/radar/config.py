"""Constantes do projeto. Sem dependencia externa e sem efeito colateral."""

# --------------------------------------------------------------------------
# API do GitHub
# --------------------------------------------------------------------------

API_BASE = "https://api.github.com"

# Versao fixada para o pipeline nao quebrar quando a API evoluir.
API_VERSION = "2022-11-28"

# Exigido pelo GitHub.
USER_AGENT = "radar-ecossistema-dados"

# 100 e o maximo aceito por pagina.
PER_PAGE = 100

# requests nao tem timeout por padrao.
TIMEOUT = 30

# --------------------------------------------------------------------------
# Politica de retry
# --------------------------------------------------------------------------

MAX_TENTATIVAS = 5

# Segundos da primeira espera; dobra a cada tentativa.
BACKOFF_BASE = 2

# Teto da espera, em segundos.
ESPERA_MAXIMA = 120

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

# Tupla para nao ser alterada em tempo de execucao.
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
    """Nome totalmente qualificado: catalog.schema.tabela."""
    return f"{catalog}.{schema}.{tabela}"
