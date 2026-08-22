"""Schema da silver verificado contra o motor Spark.

O que estes testes cobrem e o que os testes de string nao alcancam: se o DDL
declarado e sintaticamente aceito, e como o `from_json` responde a payload
incompleto, invalido ou com campo a mais.
"""

import pytest

from radar import silver

pytestmark = pytest.mark.spark


COMMIT = {
    "sha": "abc123",
    "commit": {
        "author": {"name": "Ana", "email": "ana@exemplo.com", "date": "2026-08-01T10:00:00Z"},
        "committer": {"name": "Ana", "email": "ana@exemplo.com", "date": "2026-08-02T10:00:00Z"},
        "message": "fix: corrige leitura",
        "comment_count": 2,
        "verification": {"verified": True, "reason": "valid"},
    },
    "author": {"login": "ana", "id": 42, "type": "User"},
    "committer": {"login": "ana", "id": 42, "type": "User"},
    "parents": [{"sha": "p1"}, {"sha": "p2"}],
}


def dados(df_payload, dado):
    """Aplica o schema e devolve a linha ja estruturada."""
    return silver.parsear(df_payload(dado)).select("dados.*").collect()[0]


# --------------------------------------------------------------------------
# O DDL e valido
# --------------------------------------------------------------------------

def test_schema_declarado_e_aceito_pelo_spark(spark):
    # Um STRUCT mal fechado so apareceria aqui, ou na carga em producao.
    spark.sql(f"SELECT from_json('{{}}', '{silver.SCHEMA_COMMIT}') AS d")


def test_payload_completo_preenche_os_campos(df_payload):
    linha = dados(df_payload, COMMIT)
    assert linha["sha"] == "abc123"
    assert linha["commit"]["message"] == "fix: corrige leitura"
    assert linha["commit"]["comment_count"] == 2
    assert linha["commit"]["verification"]["verified"] is True
    assert len(linha["parents"]) == 2


# --------------------------------------------------------------------------
# A decisao central, verificada no motor
# --------------------------------------------------------------------------

def test_data_chega_como_string_e_nao_convertida(df_payload):
    linha = dados(df_payload, COMMIT)
    assert linha["commit"]["author"]["date"] == "2026-08-01T10:00:00Z"
    assert isinstance(linha["commit"]["author"]["date"], str)


def test_tipo_declarado_da_data_e_string(spark, df_payload):
    df = silver.parsear(df_payload(COMMIT))
    tipo = df.selectExpr("typeof(dados.commit.author.date) AS t").collect()[0]["t"]
    assert tipo == "string"


# --------------------------------------------------------------------------
# Os dois `author` sao independentes
# --------------------------------------------------------------------------

def test_identidade_do_git_e_usuario_do_github_convivem(df_payload):
    linha = dados(df_payload, COMMIT)
    assert linha["commit"]["author"]["name"] == "Ana"
    assert linha["author"]["login"] == "ana"
    assert linha["author"]["id"] == 42


def test_commit_sem_usuario_do_github_nao_quebra(df_payload):
    # Acontece quando o e-mail do commit nao esta associado a conta nenhuma.
    sem_conta = {**COMMIT, "author": None, "committer": None}
    linha = dados(df_payload, sem_conta)
    assert linha["author"] is None
    assert linha["commit"]["author"]["name"] == "Ana"


# --------------------------------------------------------------------------
# Bordas do contrato
# --------------------------------------------------------------------------

def test_campo_fora_do_schema_e_ignorado(df_payload):
    # A silver declara o que promete; o resto segue guardado na bronze.
    com_extra = {**COMMIT, "html_url": "https://github.com/x/y/commit/abc123"}
    linha = dados(df_payload, com_extra)
    assert linha["sha"] == "abc123"
    assert "html_url" not in linha.asDict()


def test_campo_do_schema_ausente_no_json_vira_nulo(df_payload):
    incompleto = {"sha": "abc123"}
    linha = dados(df_payload, incompleto)
    assert linha["sha"] == "abc123"
    assert linha["commit"] is None
    assert linha["parents"] is None


def test_json_invalido_nao_levanta_e_zera_todos_os_campos(df_payload):
    # A carga nao aborta. Mas o `from_json` tambem nao devolve NULL na coluna:
    # devolve um struct com todos os campos NULL. Logo `dados IS NULL` nao
    # detecta nada, e a quarentena tera de olhar a chave natural.
    linha = silver.parsear(df_payload("{isso nao e json")).collect()[0]
    assert linha["dados"] is not None
    assert all(valor is None for valor in linha["dados"].asDict().values())


def test_tipo_divergente_no_json_vira_nulo(df_payload):
    # comment_count declarado BIGINT chegando como texto nao numerico.
    torto = {**COMMIT, "commit": {**COMMIT["commit"], "comment_count": "muitos"}}
    linha = dados(df_payload, torto)
    assert linha["commit"]["comment_count"] is None
    assert linha["sha"] == "abc123"
