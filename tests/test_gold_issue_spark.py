"""`fct_issue` e a dimensao de autor conformada, contra o motor Spark.

**Fora do alcance:** a gravacao em Delta, validada no Databricks.
"""

from datetime import datetime, timezone

import pytest

from radar import gold

pytestmark = pytest.mark.spark

MOMENTO = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)

ESQUEMA_FOTOS = (
    "repo_id BIGINT, repo STRING, dt STRING, nome_completo STRING, dono STRING, "
    "dono_tipo STRING, linguagem STRING, licenca STRING, branch_padrao STRING, "
    "arquivado BOOLEAN, e_fork BOOLEAN"
)

ESQUEMA_ISSUES = (
    "id BIGINT, repo STRING, numero INT, titulo STRING, estado STRING, "
    "motivo_estado STRING, comentarios INT, qtd_rotulos INT, "
    "qtd_responsaveis INT, autor_login STRING, autor_id BIGINT, "
    "autor_tipo STRING, aberta_em STRING, atualizada_em STRING, "
    "fechada_em STRING"
)

ESQUEMA_COMMITS = (
    "sha STRING, repo STRING, github_id BIGINT, github_login STRING, "
    "github_tipo STRING, autor_nome STRING, autor_email STRING, "
    "commitado_em STRING"
)


def foto(repo_id=1, dt="2026-08-24", nome="duckdb/duckdb"):
    return (repo_id, nome, dt, nome, "duckdb", "organization", "c++", "mit",
            "main", False, False)


def issue(numero, autor_id=42, aberta="2026-06-01T10:00:00Z", fechada=None,
          estado="open", motivo=None, comentarios=0, repo="duckdb/duckdb"):
    return (900000 + numero, repo, numero, f"issue {numero}", estado, motivo,
            comentarios, 0, 0, "ana", autor_id, "user",
            aberta, "2026-08-01T10:00:00Z", fechada)


def commit(sha="c1", github_id=42, login="ana", nome="Ana",
           email="ana@exemplo.com", commitado="2026-08-01T10:00:00Z"):
    return (sha, "duckdb/duckdb", github_id, login, "user", nome, email, commitado)


@pytest.fixture
def issues(spark):
    from pyspark.sql import functions as F

    def constroi(linhas):
        df = spark.createDataFrame(linhas, ESQUEMA_ISSUES)
        for coluna in ("aberta_em", "atualizada_em", "fechada_em"):
            df = df.withColumn(coluna, F.to_timestamp(coluna))
        return df

    return constroi


@pytest.fixture
def commits(spark):
    from pyspark.sql import functions as F

    def constroi(linhas):
        return spark.createDataFrame(linhas, ESQUEMA_COMMITS).withColumn(
            "commitado_em", F.to_timestamp("commitado_em")
        )

    return constroi


@pytest.fixture
def cenario(spark, issues, commits):
    """Silver de issues mais as dimensoes que o fato referencia."""

    def constroi(linhas_issue, linhas_commit=None):
        df_issues = issues(linhas_issue)
        df_commits = commits(linhas_commit or [commit()])
        dim_repo = gold.montar_dim_repositorio(
            spark.createDataFrame([foto()], ESQUEMA_FOTOS), MOMENTO
        )
        dim_autor = gold.linha_desconhecida(spark, MOMENTO).union(
            gold.montar_dim_autor(df_commits, MOMENTO, issues=df_issues)
        )
        return df_issues, dim_repo, dim_autor

    return constroi


# --------------------------------------------------------------------------
# A dimensao de autor conformada
# --------------------------------------------------------------------------

def test_quem_abre_issue_sem_commitar_entra_na_dimensao(spark, issues, commits):
    """Sem isso, todo relator externo cairia no membro desconhecido."""
    df = gold.montar_dim_autor(
        commits([commit(github_id=42)]),
        MOMENTO,
        issues=issues([issue(1, autor_id=99)]),
    )
    assert sorted(linha["github_id"] for linha in df.collect()) == [42, 99]


def test_quem_commita_e_abre_issue_e_uma_linha_so(spark, issues, commits):
    """A chave por conta e a mesma nos dois lados."""
    df = gold.montar_dim_autor(
        commits([commit(github_id=42)]),
        MOMENTO,
        issues=issues([issue(1, autor_id=42)]),
    )
    assert df.count() == 1


def test_issue_recente_nao_apaga_o_nome_que_o_commit_trouxe(spark, issues, commits):
    """O payload de issue nao tem nome nem e-mail do git.

    Se a escolha do atributo fosse pela linha mais recente sem ignorar nulo, o
    autor perderia identidade por ter aberto uma issue depois do ultimo commit.
    """
    df = gold.montar_dim_autor(
        commits([commit(github_id=42, nome="Ana", email="ana@exemplo.com",
                        commitado="2026-01-01T10:00:00Z")]),
        MOMENTO,
        issues=issues([issue(1, autor_id=42)]),  # atualizada em agosto
    )
    linha = df.collect()[0]
    assert linha["autor_nome"] == "Ana"
    assert linha["autor_email"] == "ana@exemplo.com"


def test_dimensao_sem_issues_continua_igual(spark, commits):
    """O parametro e opcional: o comportamento antigo nao mudou."""
    df = gold.montar_dim_autor(commits([commit(github_id=42)]), MOMENTO)
    assert df.count() == 1


def test_issue_sem_autor_identificavel_fica_de_fora(spark, issues, commits):
    df = gold.montar_dim_autor(
        commits([commit(github_id=42)]),
        MOMENTO,
        issues=issues([issue(1, autor_id=None)]),
    )
    assert df.count() == 1


# --------------------------------------------------------------------------
# fct_issue: os marcos
# --------------------------------------------------------------------------

def test_grao_e_uma_linha_por_issue(cenario):
    dados, repo, autor = cenario([issue(1), issue(2), issue(3)])
    assert gold.montar_fct_issue(dados, repo, autor, MOMENTO).count() == 3


def test_issue_aberta_nao_tem_marco_final(cenario):
    dados, repo, autor = cenario([issue(1)])
    linha = gold.montar_fct_issue(dados, repo, autor, MOMENTO).collect()[0]

    assert linha["esta_aberta"] is True
    assert linha["sk_data_fechamento"] is None
    assert linha["dias_ate_fechar"] is None


def test_issue_fechada_tem_os_dois_marcos(cenario):
    dados, repo, autor = cenario([
        issue(1, aberta="2026-06-01T10:00:00Z", fechada="2026-06-11T10:00:00Z",
              estado="closed", motivo="completed"),
    ])
    linha = gold.montar_fct_issue(dados, repo, autor, MOMENTO).collect()[0]

    assert linha["esta_aberta"] is False
    assert linha["sk_data_abertura"] == 20260601
    assert linha["sk_data_fechamento"] == 20260611
    assert linha["dias_ate_fechar"] == 10


def test_idade_do_processo_para_quando_fecha(cenario):
    """A medida que caracteriza o snapshot acumulado.

    Enquanto aberta ela cresce a cada execucao; fechada, congela no valor
    final. As duas issues abrem no mesmo dia e so uma fechou.
    """
    dados, repo, autor = cenario([
        issue(1, aberta="2026-06-01T10:00:00Z"),
        issue(2, aberta="2026-06-01T10:00:00Z", fechada="2026-06-11T10:00:00Z",
              estado="closed"),
    ])
    fato = gold.montar_fct_issue(dados, repo, autor, MOMENTO).collect()
    por_numero = {linha["numero"]: linha for linha in fato}

    assert por_numero[2]["dias_em_aberto"] == 10
    # 2026-06-01 ate o MOMENTO, 2026-08-24.
    assert por_numero[1]["dias_em_aberto"] == 84


def test_idade_cresce_entre_duas_execucoes(cenario):
    """Reconstruir o fato mais tarde produz numero maior, de proposito."""
    dados, repo, autor = cenario([issue(1, aberta="2026-06-01T10:00:00Z")])

    antes = gold.montar_fct_issue(dados, repo, autor, MOMENTO).collect()[0]
    depois = gold.montar_fct_issue(
        dados, repo, autor, datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc)
    ).collect()[0]

    assert depois["dias_em_aberto"] > antes["dias_em_aberto"]


def test_marco_final_e_coerente_com_a_flag(cenario):
    """E a invariante que a bateria dos fatos verifica de fora."""
    dados, repo, autor = cenario([
        issue(1),
        issue(2, fechada="2026-06-11T10:00:00Z", estado="closed"),
    ])
    for linha in gold.montar_fct_issue(dados, repo, autor, MOMENTO).collect():
        assert linha["esta_aberta"] == (linha["sk_data_fechamento"] is None)


# --------------------------------------------------------------------------
# fct_issue: as juncoes
# --------------------------------------------------------------------------

def test_numero_e_dimensao_degenerada(cenario):
    """O identificador fica no fato; uma dim_issue nao teria atributo a somar."""
    dados, repo, autor = cenario([issue(42)])
    assert gold.montar_fct_issue(dados, repo, autor, MOMENTO).collect()[0]["numero"] == 42


def test_autor_desconhecido_nao_derruba_a_linha(cenario):
    """A contagem do fato tem de fechar com a silver."""
    dados, repo, autor = cenario([issue(1, autor_id=None)])
    fato = gold.montar_fct_issue(dados, repo, autor, MOMENTO).collect()

    assert len(fato) == 1
    assert fato[0]["sk_autor"] is not None


def test_toda_issue_encontra_uma_versao_do_repositorio(cenario):
    """A SCD2 abre a vigencia para tras, entao issue antiga nao fica orfa."""
    dados, repo, autor = cenario([issue(1, aberta="2019-01-01T10:00:00Z")])
    linha = gold.montar_fct_issue(dados, repo, autor, MOMENTO).collect()[0]
    assert linha["sk_repositorio"] is not None


def test_colunas_batem_com_o_ddl(cenario):
    dados, repo, autor = cenario([issue(1)])
    fato = gold.montar_fct_issue(dados, repo, autor, MOMENTO)

    assert tuple(fato.columns) == gold.COLUNAS_FCT_ISSUE
    for coluna in gold.COLUNAS_FCT_ISSUE:
        assert coluna in gold.ddl_fct_issue(), coluna
