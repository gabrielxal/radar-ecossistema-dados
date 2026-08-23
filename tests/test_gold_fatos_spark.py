"""Os fatos verificados contra o motor Spark.

**Fora do alcance:** a gravacao em Delta, validada no Databricks.
"""

from datetime import datetime, timezone

import pytest

from radar import gold

pytestmark = pytest.mark.spark

MOMENTO = datetime(2026, 8, 23, 12, 0, 0, tzinfo=timezone.utc)

ESQUEMA_FOTOS = (
    "repo_id BIGINT, repo STRING, dt STRING, nome_completo STRING, dono STRING, "
    "dono_tipo STRING, linguagem STRING, licenca STRING, branch_padrao STRING, "
    "arquivado BOOLEAN, e_fork BOOLEAN"
)

ESQUEMA_SILVER = (
    "sha STRING, repo STRING, github_id BIGINT, github_login STRING, "
    "github_tipo STRING, autor_nome STRING, autor_email STRING, "
    "commitado_em STRING, autorado_em STRING, comentarios INT, qtd_pais INT, "
    "assinatura_verificada BOOLEAN"
)

ESQUEMA_MEDIDAS = (
    "repo_id BIGINT, dt STRING, stars INT, forks INT, issues_abertas INT, "
    "observadores INT, tamanho_kb INT"
)


def foto(repo_id, dt, licenca="mit", nome="duckdb/duckdb"):
    return (repo_id, nome, dt, nome, "duckdb", "organization", "c++", licenca,
            "main", False, False)


def commit(sha, repo="duckdb/duckdb", github_id=42, email="ana@exemplo.com",
           commitado="2026-08-01T10:00:00Z", autorado="2026-08-01T10:00:00Z",
           pais=1):
    return (sha, repo, github_id, "ana", "user", "Ana", email,
            commitado, autorado, 0, pais, True)


@pytest.fixture
def cenario(spark):
    """Silver de commits mais as duas dimensoes que o fato referencia."""
    from pyspark.sql import functions as F

    def constroi(linhas, fotos_repo=None):
        commits = spark.createDataFrame(linhas, ESQUEMA_SILVER).select(
            "sha", "repo", "github_id", "github_login", "github_tipo",
            "autor_nome", "autor_email",
            F.to_timestamp("commitado_em").alias("commitado_em"),
            F.to_timestamp("autorado_em").alias("autorado_em"),
            "comentarios", "qtd_pais", "assinatura_verificada",
        )
        dim_repo = gold.montar_dim_repositorio(
            spark.createDataFrame(fotos_repo or [foto(1, "2026-08-23")], ESQUEMA_FOTOS),
            MOMENTO,
        )
        dim_autor = gold.linha_desconhecida(spark, MOMENTO).union(
            gold.montar_dim_autor(commits, MOMENTO)
        )
        return commits, dim_repo, dim_autor

    return constroi


@pytest.fixture
def medidas(spark):
    def constroi(linhas):
        return spark.createDataFrame(linhas, ESQUEMA_MEDIDAS)

    return constroi


@pytest.fixture
def dimensao_repo(spark):
    def constroi(linhas):
        return gold.montar_dim_repositorio(
            spark.createDataFrame(linhas, ESQUEMA_FOTOS), MOMENTO
        )

    return constroi


# --------------------------------------------------------------------------
# fct_commit
# --------------------------------------------------------------------------

def test_fato_nao_perde_nem_duplica_commit(cenario):
    commits, dim_repo, dim_autor = cenario([commit("a"), commit("b")])
    fato = gold.montar_fct_commit(commits, dim_repo, dim_autor, MOMENTO)

    assert fato.count() == commits.count() == 2
    assert fato.select("sha").distinct().count() == 2


def test_commit_anterior_a_primeira_foto_encontra_versao(cenario):
    # O caso que motivou abrir a vigencia para tras: commit de maio, foto de
    # agosto. Sem isso o fato perderia tres meses de historico.
    commits, dim_repo, dim_autor = cenario(
        [commit("a", commitado="2026-05-25T10:00:00Z", autorado="2026-05-25T10:00:00Z")]
    )
    linha = gold.montar_fct_commit(commits, dim_repo, dim_autor, MOMENTO).collect()[0]
    assert linha["sk_repositorio"] is not None


def test_fato_aponta_para_a_versao_vigente_no_dia(cenario):
    # A razao de existir a SCD2: o commit pertence ao estado que o
    # repositorio tinha naquele dia, nao ao estado de hoje.
    fotos = [foto(1, "2026-08-01", licenca="mit"),
             foto(1, "2026-08-20", licenca="apache-2.0")]
    commits, dim_repo, dim_autor = cenario(
        [commit("antigo", commitado="2026-08-10T10:00:00Z"),
         commit("novo", commitado="2026-08-22T10:00:00Z")],
        fotos,
    )
    fato = gold.montar_fct_commit(commits, dim_repo, dim_autor, MOMENTO)
    ligado = fato.join(dim_repo, "sk_repositorio").select("sha", "licenca").collect()

    assert {l["sha"]: l["licenca"] for l in ligado} == {
        "antigo": "mit", "novo": "apache-2.0"
    }


def test_as_duas_chaves_de_tempo_sao_papeis_distintos(cenario):
    commits, dim_repo, dim_autor = cenario([
        commit("a", autorado="2026-06-01T10:00:00Z", commitado="2026-08-01T10:00:00Z")
    ])
    linha = gold.montar_fct_commit(commits, dim_repo, dim_autor, MOMENTO).collect()[0]

    assert linha["sk_data_autoria"] == 20260601
    assert linha["sk_data_commit"] == 20260801
    assert linha["dias_ate_o_commit"] == 61


def test_commit_sem_autor_resolve_no_membro_desconhecido(cenario):
    # Cair fora do fato quebraria a contagem de controle com a silver.
    commits, dim_repo, dim_autor = cenario([commit("a", github_id=None, email=None)])
    fato = gold.montar_fct_commit(commits, dim_repo, dim_autor, MOMENTO)
    desconhecida = dim_autor.where(
        f"origem_da_chave = '{gold.ORIGEM_DESCONHECIDA}'"
    ).collect()[0]["sk_autor"]

    assert fato.count() == 1
    assert fato.collect()[0]["sk_autor"] == desconhecida


def test_merge_e_derivado_da_contagem_de_pais(cenario):
    commits, dim_repo, dim_autor = cenario(
        [commit("simples", pais=1), commit("merge", pais=2)]
    )
    fato = gold.montar_fct_commit(commits, dim_repo, dim_autor, MOMENTO)

    assert {l["sha"]: l["e_merge"] for l in fato.collect()} == {
        "simples": False, "merge": True
    }


def test_colunas_do_fato_batem_com_o_ddl(cenario):
    commits, dim_repo, dim_autor = cenario([commit("a")])
    fato = gold.montar_fct_commit(commits, dim_repo, dim_autor, MOMENTO)

    assert tuple(fato.columns) == gold.COLUNAS_FCT_COMMIT
    for coluna in fato.columns:
        assert coluna in gold.ddl_fct_commit()


def test_ddl_declara_o_que_o_sql_nao_impede():
    # Nada impede somar `dias_ate_o_commit`. A defesa e o comentario.
    ddl = gold.ddl_fct_commit()
    assert "NAO ADITIVA" in ddl
    assert "DIMENSAO DEGENERADA" in ddl


# --------------------------------------------------------------------------
# fct_repo_snapshot
# --------------------------------------------------------------------------

def test_grao_e_um_repositorio_por_dia(dimensao_repo, medidas):
    dim = dimensao_repo([foto(1, "2026-08-22")])
    fato = gold.montar_fct_repo_snapshot(
        medidas([
            (1, "2026-08-22", 40000, 3500, 800, 260, 100),
            (1, "2026-08-23", 40100, 3510, 810, 261, 101),
        ]),
        dim,
        MOMENTO,
    )

    assert fato.count() == 2
    assert fato.select("repo_id", "sk_data").distinct().count() == 2


def test_medidas_sao_as_que_a_dimensao_recusou(dimensao_repo, medidas):
    dim = dimensao_repo([foto(1, "2026-08-23")])
    fato = gold.montar_fct_repo_snapshot(
        medidas([(1, "2026-08-23", 40000, 3500, 800, 260, 100)]), dim, MOMENTO
    )
    linha = fato.collect()[0]

    for medida in gold.MEDIDAS_SNAPSHOT:
        assert medida in fato.columns
        assert medida not in gold.ATRIBUTOS_VERSIONADOS
    assert linha["stars"] == 40000
    assert linha["sk_data"] == 20260823


def test_snapshot_liga_na_versao_vigente_do_dia(dimensao_repo, medidas):
    dim = dimensao_repo([foto(1, "2026-08-22", licenca="mit"),
                         foto(1, "2026-08-23", licenca="apache-2.0")])
    fato = gold.montar_fct_repo_snapshot(
        medidas([
            (1, "2026-08-22", 40000, 3500, 800, 260, 100),
            (1, "2026-08-23", 40100, 3510, 810, 261, 101),
        ]),
        dim,
        MOMENTO,
    )
    ligado = fato.join(dim, "sk_repositorio").select("sk_data", "licenca").collect()

    assert {l["sk_data"]: l["licenca"] for l in ligado} == {
        20260822: "mit", 20260823: "apache-2.0"
    }


def test_colunas_do_snapshot_batem_com_o_ddl(dimensao_repo, medidas):
    dim = dimensao_repo([foto(1, "2026-08-23")])
    fato = gold.montar_fct_repo_snapshot(
        medidas([(1, "2026-08-23", 40000, 3500, 800, 260, 100)]), dim, MOMENTO
    )

    assert tuple(fato.columns) == gold.COLUNAS_FCT_SNAPSHOT
    for coluna in fato.columns:
        assert coluna in gold.ddl_fct_repo_snapshot()


def test_ddl_do_snapshot_declara_a_semi_aditividade():
    ddl = gold.ddl_fct_repo_snapshot()
    assert ddl.count("SEMI-ADITIVA") == len(gold.MEDIDAS_SNAPSHOT)
    assert "nunca entre dias" in ddl
