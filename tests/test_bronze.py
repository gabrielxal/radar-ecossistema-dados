"""Testes das funcoes puras da bronze. Sem Spark, sem Databricks."""

from radar import bronze
from radar.config import REPOS
from radar.ingestao import ENDPOINTS, sanitizar_repo

COMMITS = ENDPOINTS["commits"]


# --------------------------------------------------------------------------
# Nomes e caminhos
# --------------------------------------------------------------------------

def test_nome_tabela_e_totalmente_qualificado():
    assert bronze.nome_tabela(COMMITS) == "workspace.radar_bronze.commits"


def test_caminho_endpoint_para_na_raiz_do_endpoint():
    caminho = bronze.caminho_endpoint("/Volumes/workspace/radar_bronze/raw", COMMITS)
    assert caminho == "/Volumes/workspace/radar_bronze/raw/commits"


# --------------------------------------------------------------------------
# Decodificacao do repositorio
# --------------------------------------------------------------------------

def test_dessanitizar_repo_restaura_a_barra():
    assert bronze.dessanitizar_repo("duckdb__duckdb") == "duckdb/duckdb"


def test_dessanitizar_repo_preserva_underscore_do_nome():
    # O separador e o PRIMEIRO `__`; o do nome do repositorio fica intacto.
    assert (
        bronze.dessanitizar_repo("great-expectations__great_expectations")
        == "great-expectations/great_expectations"
    )


def test_dessanitizar_repo_e_inverso_de_sanitizar_para_todos_os_alvos():
    for repo in REPOS:
        assert bronze.dessanitizar_repo(sanitizar_repo(repo)) == repo


def test_dessanitizar_repo_nao_altera_nome_sem_separador():
    assert bronze.dessanitizar_repo("semseparador") == "semseparador"


# --------------------------------------------------------------------------
# DDL
# --------------------------------------------------------------------------

def test_ddl_usa_a_chave_natural_do_endpoint():
    assert "sha" in bronze.ddl(COMMITS)


def test_ddl_e_idempotente_e_delta():
    texto = bronze.ddl(COMMITS)
    assert "CREATE TABLE IF NOT EXISTS workspace.radar_bronze.commits" in texto
    assert "USING DELTA" in texto


def test_ddl_exige_chave_e_payload():
    texto = bronze.ddl(COMMITS)
    assert "STRING NOT NULL COMMENT 'chave natural" in texto
    assert "payload         STRING NOT NULL" in texto


def test_ddl_carrega_os_tres_metadados_de_proveniencia():
    texto = bronze.ddl(COMMITS)
    for coluna in ("_ingerido_em", "_arquivo_origem", "_endpoint"):
        assert coluna in texto


def test_ddl_nao_tipa_nada_alem_de_string_e_timestamp():
    # Regra da bronze: todo campo de dado e STRING. So o metadado de tempo
    # foge, e por ser controle nosso, nao dado da origem.
    texto = bronze.ddl(COMMITS)
    for tipo in ("BIGINT", "INT", "DOUBLE", "DECIMAL", "BOOLEAN"):
        assert tipo not in texto
