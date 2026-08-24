"""Silver de issues verificada contra o motor Spark.

Cobre o que teste de string nao alcanca: como o `from_json` responde ao
payload de issue, o campo que separa issue de pull request, e o SQL de
deduplicacao que decide qual versao sobrevive.
"""

from datetime import datetime, timezone

import pytest

from radar import silver_issues as si

pytestmark = pytest.mark.spark

MOMENTO = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)


ISSUE = {
    "id": 900100,
    "number": 42,
    "title": "  Leitura falha em arquivo vazio  ",
    "state": "Closed",
    "state_reason": "Completed",
    "comments": 3,
    "created_at": "2026-06-01T10:00:00Z",
    "updated_at": "2026-08-01T10:00:00Z",
    "closed_at": "2026-07-01T10:00:00Z",
    "author_association": "CONTRIBUTOR",
    "user": {"login": "ana", "id": 42, "type": "User"},
    "labels": [{"name": "bug"}, {"name": "good first issue"}],
    "assignees": [{"login": "bia"}],
}

PULL_REQUEST = dict(ISSUE, id=900200, number=43, pull_request={"url": "https://x/pulls/43"})


@pytest.fixture
def tipado(spark, df_payload):
    """Payload cru -> parsear -> tipar, com as colunas da bronze em volta."""
    from pyspark.sql import functions as F

    def constroi(dado, repo="duckdb/duckdb"):
        df = (
            df_payload(dado)
            .withColumn("repo", F.lit(repo))
            .withColumn("_ingerido_em", F.lit(MOMENTO).cast("timestamp"))
            .withColumn("_arquivo_origem", F.lit("a.jsonl"))
        )
        return si.tipar(si.parsear(df), MOMENTO)

    return constroi


# --------------------------------------------------------------------------
# O schema
# --------------------------------------------------------------------------

def test_schema_declarado_e_aceito_pelo_spark(spark):
    spark.sql(f"SELECT from_json('{{}}', '{si.SCHEMA_ISSUE}') AS d")


def test_campos_do_payload_viram_colunas(tipado):
    linha = tipado(ISSUE).collect()[0]
    assert linha["id"] == 900100
    assert linha["numero"] == 42
    assert linha["comentarios"] == 3
    assert linha["autor_id"] == 42


def test_texto_e_aparado(tipado):
    assert tipado(ISSUE).collect()[0]["titulo"] == "Leitura falha em arquivo vazio"


def test_dominio_fechado_e_normalizado(tipado):
    """`Closed` e `closed` seriam duas categorias da mesma coisa."""
    linha = tipado(ISSUE).collect()[0]
    assert linha["estado"] == "closed"
    assert linha["motivo_estado"] == "completed"
    assert linha["associacao_autor"] == "contributor"
    assert linha["autor_tipo"] == "user"


def test_rotulos_viram_lista_de_nomes(tipado):
    linha = tipado(ISSUE).collect()[0]
    assert linha["rotulos"] == ["bug", "good first issue"]
    assert linha["qtd_rotulos"] == 2
    assert linha["qtd_responsaveis"] == 1


def test_ausencia_de_rotulo_nao_vira_menos_um(tipado):
    """`size(NULL)` devolve -1 em modo legado. Diario de bordo 10."""
    sem = {k: v for k, v in ISSUE.items() if k not in ("labels", "assignees")}
    linha = tipado(sem).collect()[0]
    assert linha["qtd_rotulos"] == 0
    assert linha["qtd_responsaveis"] == 0


def test_datas_viram_timestamp(spark, tipado):
    from pyspark.sql import functions as F

    linha = (
        tipado(ISSUE)
        .select(
            F.date_format("aberta_em", "yyyy-MM-dd").alias("a"),
            F.date_format("fechada_em", "yyyy-MM-dd").alias("f"),
        )
        .collect()[0]
    )
    assert linha["a"] == "2026-06-01"
    assert linha["f"] == "2026-07-01"


def test_issue_aberta_nao_tem_marco_final(tipado):
    aberta = dict(ISSUE, state="open", closed_at=None, state_reason=None)
    assert tipado(aberta).collect()[0]["fechada_em"] is None


# --------------------------------------------------------------------------
# O campo que separa as duas entidades
# --------------------------------------------------------------------------

def test_issue_nao_e_marcada_como_pull_request(tipado):
    assert tipado(ISSUE).collect()[0][si.COLUNA_PR] is False


def test_pull_request_e_reconhecido(tipado):
    """Um PR e uma issue com o campo `pull_request` preenchido."""
    assert tipado(PULL_REQUEST).collect()[0][si.COLUNA_PR] is True


# --------------------------------------------------------------------------
# Quarentena
# --------------------------------------------------------------------------

@pytest.fixture
def classificado(spark, df_payload):
    from pyspark.sql import functions as F

    def constroi(dado, repo="duckdb/duckdb"):
        df = (
            df_payload(dado)
            .withColumn("repo", F.lit(repo))
            .withColumn("_ingerido_em", F.lit(MOMENTO).cast("timestamp"))
            .withColumn("_arquivo_origem", F.lit("a.jsonl"))
        )
        return si.classificar(si.parsear(df), MOMENTO)

    return constroi


def test_issue_completa_e_aprovada(classificado):
    assert classificado(ISSUE).collect()[0][si.COLUNA_MOTIVO] is None


def test_json_invalido_cai_na_quarentena(classificado):
    """`from_json` permissivo devolve struct de nulos, nao NULL."""
    assert classificado("{isso nao e json").collect()[0][si.COLUNA_MOTIVO] == "payload_ilegivel"


def test_issue_sem_numero_nao_tem_grao(classificado):
    sem = {k: v for k, v in ISSUE.items() if k != "number"}
    assert classificado(sem).collect()[0][si.COLUNA_MOTIVO] == "numero_ausente"


def test_issue_sem_data_de_abertura_perde_o_primeiro_marco(classificado):
    sem = {k: v for k, v in ISSUE.items() if k != "created_at"}
    assert classificado(sem).collect()[0][si.COLUNA_MOTIVO] == "data_de_abertura_ausente"


def test_motivos_produzidos_estao_declarados(classificado):
    for dado in ("{quebrado", ISSUE, {k: v for k, v in ISSUE.items() if k != "number"}):
        motivo = classificado(dado).collect()[0][si.COLUNA_MOTIVO]
        assert motivo is None or motivo in si.MOTIVOS_DE_REJEICAO


def test_colunas_das_saidas_batem_com_os_ddl(classificado):
    colunas = set(classificado(ISSUE).columns)
    for coluna in si.COLUNAS_ISSUES:
        assert coluna in colunas, coluna


# --------------------------------------------------------------------------
# A deduplicacao do lote, que decide qual versao sobrevive
# --------------------------------------------------------------------------

def lote(spark, linhas):
    """Tabela temporaria no formato do lote classificado."""
    from pyspark.sql import functions as F

    esquema = (
        "id BIGINT, repo STRING, numero INT, titulo STRING, estado STRING, "
        "motivo_estado STRING, comentarios INT, rotulos ARRAY<STRING>, "
        "qtd_rotulos INT, qtd_responsaveis INT, autor_login STRING, "
        "autor_id BIGINT, autor_tipo STRING, associacao_autor STRING, "
        "aberta_em STRING, atualizada_em STRING, fechada_em STRING, "
        "payload STRING, "
        "_ingerido_em STRING, _arquivo_origem STRING, _processado_em STRING, "
        f"{si.COLUNA_MOTIVO} STRING, {si.COLUNA_PR} BOOLEAN"
    )
    df = spark.createDataFrame(linhas, esquema)
    for coluna in ("aberta_em", "atualizada_em", "fechada_em", "_ingerido_em", "_processado_em"):
        df = df.withColumn(coluna, F.to_timestamp(coluna))
    df.createOrReplaceTempView("_lote_issues")
    return "_lote_issues"


def versao(numero, titulo, atualizada, motivo=None, pr=False, ingerido="2026-08-01T00:00:00"):
    return (
        900000 + numero, "duckdb/duckdb", numero, titulo, "open", None, 0, [], 0, 0,
        "ana", 42, "user", "contributor",
        "2026-06-01T10:00:00", atualizada, None, "{}",
        ingerido, "a.jsonl", "2026-08-24T12:00:00", motivo, pr,
    )


def test_versao_mais_recente_vence(spark):
    origem = lote(spark, [
        versao(42, "titulo antigo", "2026-07-01T10:00:00"),
        versao(42, "titulo novo", "2026-08-01T10:00:00"),
    ])
    linhas = spark.sql(si.sql_fonte_entidade(origem, pull_request=False)).collect()

    assert len(linhas) == 1
    assert linhas[0]["titulo"] == "titulo novo"


def test_lote_com_duas_versoes_nao_chega_repetido_ao_merge(spark):
    """O MERGE recusa fonte com chave repetida: 'multiple source rows matched'."""
    origem = lote(spark, [
        versao(42, "a", "2026-07-01T10:00:00"),
        versao(42, "b", "2026-08-01T10:00:00"),
        versao(43, "c", "2026-08-01T10:00:00"),
    ])
    df = spark.sql(si.sql_fonte_entidade(origem, pull_request=False))
    assert df.count() == df.select("repo", "numero").distinct().count()


def test_empate_de_instante_e_desempatado_pela_ingestao(spark):
    origem = lote(spark, [
        versao(42, "primeira", "2026-08-01T10:00:00", ingerido="2026-08-01T00:00:00"),
        versao(42, "segunda", "2026-08-01T10:00:00", ingerido="2026-08-02T00:00:00"),
    ])
    assert spark.sql(
        si.sql_fonte_entidade(origem, pull_request=False)
    ).collect()[0]["titulo"] == "segunda"


def test_pull_request_nao_entra_na_fonte_de_issues(spark):
    origem = lote(spark, [
        versao(42, "issue", "2026-08-01T10:00:00"),
        versao(43, "pull request", "2026-08-01T10:00:00", pr=True),
    ])
    issues = spark.sql(si.sql_fonte_entidade(origem, pull_request=False)).collect()
    prs = spark.sql(si.sql_fonte_entidade(origem, pull_request=True)).collect()

    assert [linha["numero"] for linha in issues] == [42]
    assert [linha["numero"] for linha in prs] == [43]


def test_rejeitada_nao_entra_em_nenhuma_das_duas(spark):
    origem = lote(spark, [
        versao(42, "boa", "2026-08-01T10:00:00"),
        versao(43, "ruim", "2026-08-01T10:00:00", motivo="numero_ausente"),
    ])
    aprovadas = spark.sql(si.sql_fonte_entidade(origem, pull_request=False)).count()
    prs = spark.sql(si.sql_fonte_entidade(origem, pull_request=True)).count()
    rejeitadas = spark.sql(si.sql_fonte_rejeitados(origem)).count()

    assert (aprovadas, prs, rejeitadas) == (1, 0, 1)
