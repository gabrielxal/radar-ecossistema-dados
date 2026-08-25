"""As consultas de analise, exercitadas contra o motor.

Uma consulta de analise erra de dois jeitos. Ela nao roda, e isso a sintaxe
acusa. Ou ela roda e responde outra coisa, e so dado sintetico com resposta
conhecida acusa.

Os cenarios aqui sao pequenos e desenhados para ter resposta obvia a olho nu:
um repositorio com um autor dominante deve dar bus factor 1, e um com
distribuicao plana deve dar mais que 1.

**Fora do alcance:** o dado real. Estas consultas rodam no Databricks contra
as tabelas Delta; aqui elas rodam contra views temporarias.
"""

from datetime import date, timedelta

import pytest

from radar import analises

pytestmark = pytest.mark.spark

HOJE = date.today()


def dias_atras(n: int) -> date:
    return HOJE - timedelta(days=n)


def sk(dia: date) -> int:
    return int(dia.strftime("%Y%m%d"))


# --------------------------------------------------------------------------
# O cenario
# --------------------------------------------------------------------------

@pytest.fixture
def cenario(spark):
    """Views temporarias no formato da camada gold.

    Os nomes voltam num dicionario para as consultas apontarem para ca em vez
    do catalogo real.
    """

    def constroi(commits, autores, repositorios=None, issues=(), dias=None):
        repositorios = repositorios or [("skr1", 1, "org/alfa", dias_atras(400), True)]

        spark.createDataFrame(
            commits,
            "sha STRING, sk_repositorio STRING, sk_autor STRING, "
            "sk_data_autoria INT, dias_ate_o_commit INT",
        ).createOrReplaceTempView("_fct_commit")

        spark.createDataFrame(
            autores, "sk_autor STRING, github_tipo STRING"
        ).createOrReplaceTempView("_dim_autor")

        spark.createDataFrame(
            [
                (skr, rid, repo, vd, None if atual else dias_atras(1), atual, vd)
                for skr, rid, repo, vd, atual in repositorios
            ],
            "sk_repositorio STRING, repo_id BIGINT, repo STRING, valido_de DATE, "
            "valido_ate DATE, flag_atual BOOLEAN, observado_de DATE",
        ).createOrReplaceTempView("_dim_repositorio")

        calendario = dias or [d for d in {c[3] for c in commits}]
        spark.createDataFrame(
            [(s, date(s // 10000, s // 100 % 100, s % 100)) for s in calendario],
            "sk_tempo INT, data DATE",
        ).createOrReplaceTempView("_dim_tempo")

        spark.createDataFrame(
            list(issues),
            "numero INT, sk_repositorio STRING, sk_autor STRING, "
            "esta_aberta BOOLEAN, dias_ate_fechar INT, dias_em_aberto INT",
        ).createOrReplaceTempView("_fct_issue")

        return {
            "fato": "_fct_commit",
            "repositorio": "_dim_repositorio",
            "autor": "_dim_autor",
            "tempo": "_dim_tempo",
            "issue": "_fct_issue",
        }

    return constroi


def commit(sha, sk_autor, dia, atraso=0, skr="skr1"):
    return (sha, skr, sk_autor, sk(dias_atras(dia)), atraso)


# --------------------------------------------------------------------------
# Pergunta 1: ritmo
# --------------------------------------------------------------------------

def test_volume_sobe_e_por_autor_cai(spark, cenario):
    """O caso que a pergunta da secao 2.3 existe para separar.

    Dois autores fazendo dois commits cada viram quatro autores fazendo um
    cada: o volume dobra e a produtividade por pessoa cai pela metade.
    """
    t = cenario(
        commits=[
            commit("a1", "au1", 60), commit("a2", "au1", 61),
            commit("a3", "au2", 62), commit("a4", "au2", 63),
            commit("b1", "au1", 10), commit("b2", "au2", 11),
            commit("b3", "au3", 12), commit("b4", "au4", 13),
            commit("b5", "au5", 14), commit("b6", "au6", 15),
            commit("b7", "au7", 16), commit("b8", "au8", 17),
        ],
        autores=[(f"au{i}", "user") for i in range(1, 9)],
    )
    linha = spark.sql(analises.ritmo_por_autor(**t)).collect()[0]

    assert linha["commits_antes"] == 4
    assert linha["commits_depois"] == 8
    assert linha["variacao_volume_pct"] == 100
    assert linha["por_autor_antes"] == 2.0
    assert linha["por_autor_depois"] == 1.0
    assert linha["variacao_por_autor_pct"] == -50


def test_bot_nao_entra_no_ritmo(spark, cenario):
    t = cenario(
        commits=[commit("h", "humano", 10), commit("b", "robo", 10)],
        autores=[("humano", "user"), ("robo", "bot")],
    )
    assert spark.sql(analises.ritmo_por_autor(**t)).collect()[0]["commits_depois"] == 1


def test_autor_sem_tipo_conta_como_humano(spark, cenario):
    """Sao os 1,4% de commits sem conta do GitHub associada."""
    t = cenario(
        commits=[commit("h", "sem_conta", 10)],
        autores=[("sem_conta", None)],
    )
    assert spark.sql(analises.ritmo_por_autor(**t)).collect()[0]["commits_depois"] == 1


def test_historia_absorvida_de_uma_vez_nao_vira_produtividade(spark, cenario):
    """A correcao que a secao 10.6 mostrou valer 11 pontos percentuais."""
    t = cenario(
        commits=[commit("normal", "au1", 10, atraso=1),
                 commit("importado", "au1", 10, atraso=300)],
        autores=[("au1", "user")],
    )
    assert spark.sql(analises.ritmo_por_autor(**t)).collect()[0]["commits_depois"] == 1


# --------------------------------------------------------------------------
# Pergunta 2: bus factor
# --------------------------------------------------------------------------

def test_um_autor_dominante_da_bus_factor_um(spark, cenario):
    t = cenario(
        commits=[commit(f"d{i}", "dono", 10) for i in range(6)]
        + [commit("o1", "outro1", 10), commit("o2", "outro2", 10)],
        autores=[("dono", "user"), ("outro1", "user"), ("outro2", "user")],
    )
    linha = spark.sql(analises.bus_factor(**t)).collect()[0]

    assert linha["bus_factor"] == 1
    assert linha["autores"] == 3
    assert linha["commits"] == 8


def test_distribuicao_plana_exige_metade_das_pessoas(spark, cenario):
    """Quatro autores com um commit cada: metade do total sao dois deles."""
    t = cenario(
        commits=[commit(f"c{i}", f"au{i}", 10) for i in range(4)],
        autores=[(f"au{i}", "user") for i in range(4)],
    )
    linha = spark.sql(analises.bus_factor(**t)).collect()[0]

    assert linha["bus_factor"] == 2
    assert linha["concentracao_pct"] == 50


def test_bus_factor_e_por_repositorio(spark, cenario):
    t = cenario(
        commits=[commit("a", "au1", 10, skr="skr1"),
                 commit("b", "au2", 10, skr="skr2"),
                 commit("c", "au2", 10, skr="skr2")],
        autores=[("au1", "user"), ("au2", "user")],
        repositorios=[("skr1", 1, "org/alfa", dias_atras(400), True),
                      ("skr2", 2, "org/beta", dias_atras(400), True)],
    )
    por_repo = {l["repo"]: l for l in spark.sql(analises.bus_factor(**t)).collect()}

    assert por_repo["org/alfa"]["commits"] == 1
    assert por_repo["org/beta"]["commits"] == 2


def test_empate_nao_torna_a_resposta_dependente_da_leitura(spark, cenario):
    t = cenario(
        commits=[commit(f"c{i}", f"au{i}", 10) for i in range(4)],
        autores=[(f"au{i}", "user") for i in range(4)],
    )
    sql = analises.bus_factor(**t)
    assert spark.sql(sql).collect() == spark.sql(sql).collect()


# --------------------------------------------------------------------------
# Pergunta 3: ciclo de issues
# --------------------------------------------------------------------------

def test_estoque_e_vazao_sao_medidas_diferentes(spark, cenario):
    """Fecha rapido o que e facil e deixa o resto envelhecendo."""
    t = cenario(
        commits=[commit("a", "au1", 10)],
        autores=[("au1", "user")],
        issues=[
            (1, "skr1", "au1", False, 2, 2),
            (2, "skr1", "au1", False, 4, 4),
            (3, "skr1", "au1", True, None, 800),
            (4, "skr1", "au1", True, None, 900),
        ],
    )
    linha = spark.sql(analises.ciclo_de_issues(**t)).collect()[0]

    assert linha["issues"] == 4
    assert linha["em_aberto"] == 2
    assert linha["pct_em_aberto"] == 50
    assert linha["mediana_dias_ate_fechar"] == 3.0
    assert linha["mediana_idade_em_aberto"] == 850.0
    assert linha["idade_da_mais_velha"] == 900


def test_ciclo_sem_issue_nenhuma_nao_quebra(spark, cenario):
    t = cenario(commits=[commit("a", "au1", 10)], autores=[("au1", "user")])
    assert spark.sql(analises.ciclo_de_issues(**t)).count() == 0


# --------------------------------------------------------------------------
# Pergunta 4: o historico antigo muda?
# --------------------------------------------------------------------------

def test_uma_versao_por_repositorio_enquanto_nada_muda(spark, cenario):
    t = cenario(commits=[commit("a", "au1", 10)], autores=[("au1", "user")])
    linha = spark.sql(analises.versoes_do_repositorio(**t)).collect()[0]

    assert linha["versoes"] == 1


def test_duas_versoes_aparecem_quando_um_atributo_muda(spark, cenario):
    t = cenario(
        commits=[commit("a", "au1", 10, skr="v2")],
        autores=[("au1", "user")],
        repositorios=[("v1", 1, "org/antigo", dias_atras(400), False),
                      ("v2", 1, "org/alfa", dias_atras(2), True)],
    )
    linha = spark.sql(analises.versoes_do_repositorio(**t)).collect()

    assert sorted(l["versoes"] for l in linha) == [1, 1]
    assert spark.sql(
        f"SELECT count(DISTINCT repo_id) AS n FROM {t['repositorio']}"
    ).collect()[0]["n"] == 1


def test_commit_ligado_a_versao_antiga_conta_como_divergente(spark, cenario):
    """O dia em que `divergentes` deixa de ser zero e o dia em que a SCD2 paga."""
    t = cenario(
        commits=[commit("antigo", "au1", 300, skr="v1"),
                 commit("novo", "au1", 5, skr="v2")],
        autores=[("au1", "user")],
        repositorios=[("v1", 1, "org/nome-antigo", dias_atras(400), False),
                      ("v2", 1, "org/nome-novo", dias_atras(10), True)],
    )
    por_repo = {l["repo"]: l for l in spark.sql(analises.historico_preservado(**t)).collect()}

    assert por_repo["org/nome-antigo"]["divergentes"] == 1
    assert por_repo["org/nome-novo"]["divergentes"] == 0


def test_sem_mudanca_nenhum_commit_diverge(spark, cenario):
    t = cenario(
        commits=[commit("a", "au1", 10), commit("b", "au1", 20)],
        autores=[("au1", "user")],
    )
    assert spark.sql(analises.historico_preservado(**t)).collect()[0]["divergentes"] == 0


# --------------------------------------------------------------------------
# O painel
# --------------------------------------------------------------------------

def test_painel_junta_os_sinais(spark, cenario):
    t = cenario(
        commits=[commit(f"d{i}", "dono", 10) for i in range(6)]
        + [commit("o", "outro", 10)],
        autores=[("dono", "user"), ("outro", "user")],
        issues=[(1, "skr1", "dono", True, None, 500)],
    )
    linha = spark.sql(analises.painel_de_saude(**t)).collect()[0]

    assert linha["commits_45d"] == 7
    assert linha["autores_45d"] == 2
    assert linha["bus_factor"] == 1
    assert linha["issues_em_aberto"] == 1
    assert linha["idade_mediana_em_aberto"] == 500.0


def test_painel_responde_com_fct_issue_vazia(spark, cenario):
    """Enquanto o notebook 10 nao rodar, as demais colunas continuam valendo."""
    t = cenario(
        commits=[commit("a", "au1", 10)],
        autores=[("au1", "user")],
    )
    linha = spark.sql(analises.painel_de_saude(**t)).collect()[0]

    assert linha["commits_45d"] == 1
    assert linha["bus_factor"] == 1
    assert linha["issues_em_aberto"] is None
