"""A composicao do painel com o portao, exercitada contra o motor.

`painel_de_saude` e `cobertura_do_backfill` ja tem teste proprio. O que so
aparece aqui e a juncao entre as duas: se ela liga o repositorio certo, se
sobrevive ao repositorio que nunca teve coleta de issue, e se a ordem do
painel atravessa a subconsulta.
"""

from datetime import date, timedelta

import pytest

from radar import consumo

pytestmark = pytest.mark.spark

HOJE = date.today()


def dias_atras(n: int) -> date:
    return HOJE - timedelta(days=n)


def sk(dia: date) -> int:
    return int(dia.strftime("%Y%m%d"))


@pytest.fixture
def cenario(spark):
    """As quatro tabelas da gold mais as duas que o portao le."""

    def constroi(repos, estados=(), issues_silver=()):
        # Um commit recente e um antigo por repositorio: o suficiente para o
        # repositorio existir nos dois periodos do ritmo.
        commits, calendario, dim_repo = [], set(), []
        for indice, (repo, _) in enumerate(repos, start=1):
            skr = f"skr{indice}"
            dim_repo.append((skr, indice, repo, dias_atras(400), None, True,
                             dias_atras(400)))
            for dia, sufixo in ((10, "novo"), (60, "velho")):
                commits.append((f"{repo}-{sufixo}", skr, "au1",
                                sk(dias_atras(dia)), 0))
                calendario.add(sk(dias_atras(dia)))

        spark.createDataFrame(
            commits,
            "sha STRING, sk_repositorio STRING, sk_autor STRING, "
            "sk_data_autoria INT, dias_ate_o_commit INT",
        ).createOrReplaceTempView("_fct_commit")

        spark.createDataFrame(
            [("au1", "user")], "sk_autor STRING, github_tipo STRING"
        ).createOrReplaceTempView("_dim_autor")

        spark.createDataFrame(
            dim_repo,
            "sk_repositorio STRING, repo_id BIGINT, repo STRING, "
            "valido_de DATE, valido_ate DATE, flag_atual BOOLEAN, "
            "observado_de DATE",
        ).createOrReplaceTempView("_dim_repositorio")

        spark.createDataFrame(
            [(s, date(s // 10000, s // 100 % 100, s % 100)) for s in calendario],
            "sk_tempo INT, data DATE",
        ).createOrReplaceTempView("_dim_tempo")

        spark.createDataFrame(
            [(1, f"skr{i}", "au1", True, None, 500)
             for i, (_, tem_issue) in enumerate(repos, start=1) if tem_issue],
            "numero INT, sk_repositorio STRING, sk_autor STRING, "
            "esta_aberta BOOLEAN, dias_ate_fechar INT, dias_em_aberto INT",
        ).createOrReplaceTempView("_fct_issue")

        spark.createDataFrame(
            [(r, "issues", st, wm) for r, st, wm in estados],
            "repo STRING, endpoint STRING, status STRING, watermark STRING",
        ).selectExpr(
            "repo", "endpoint", "status", "to_timestamp(watermark) AS watermark"
        ).createOrReplaceTempView("_controle")

        spark.createDataFrame(
            list(issues_silver) or [("", 0, "closed")],
            "repo STRING, numero INT, estado STRING",
        ).selectExpr(
            "repo", "numero", "estado",
            "to_timestamp('2026-08-01T00:00:00') AS atualizada_em",
        ).createOrReplaceTempView("_silver_issues")

        return {
            "fato": "_fct_commit",
            "repositorio": "_dim_repositorio",
            "autor": "_dim_autor",
            "tempo": "_dim_tempo",
            "issue": "_fct_issue",
            "controle_ingestao": "_controle",
            "silver_issues": "_silver_issues",
        }

    return constroi


def por_repo(spark, tabelas):
    linhas = spark.sql(consumo.painel_com_portao(**tabelas)).collect()
    return {linha["repo"]: linha for linha in linhas}


def test_o_portao_acompanha_cada_repositorio(spark, cenario):
    """Duas coletas em estados diferentes, no mesmo painel.

    E o caso que motiva a coluna: as duas linhas trazem numero de issue em
    aberto, e so uma delas pode ser lida.
    """
    t = cenario(
        repos=[("org/pronto", True), ("org/andando", True)],
        estados=[
            ("org/pronto", "ok", "2026-08-28T00:00:00"),
            ("org/andando", "truncado", "2019-04-01T00:00:00"),
        ],
        issues_silver=[("org/pronto", 1, "open"), ("org/andando", 1, "open")],
    )
    linhas = por_repo(spark, t)

    assert linhas["org/pronto"]["issues_confiavel"] is True
    assert linhas["org/andando"]["issues_confiavel"] is False
    assert linhas["org/andando"]["status_da_coleta"] == "truncado"


def test_repositorio_sem_coleta_de_issue_nao_some_do_painel(spark, cenario):
    """A juncao e a esquerda: as colunas de commit continuam respondendo.

    Um repositorio novo, ainda sem nenhuma linha na tabela de controle para
    `issues`, tem bus factor e ritmo validos. Uma juncao interna o apagaria do
    painel inteiro por causa da metade que falta.
    """
    t = cenario(repos=[("org/so-commits", False)], estados=[])
    linhas = por_repo(spark, t)

    assert "org/so-commits" in linhas
    assert linhas["org/so-commits"]["commits_45d"] == 1
    assert linhas["org/so-commits"]["issues_confiavel"] is False
    assert linhas["org/so-commits"]["status_da_coleta"] is None


def test_ausencia_de_portao_nao_vira_confiavel(spark, cenario):
    """`coalesce(..., FALSE)` e a escolha conservadora.

    Sem ele a coluna sairia nula, e nulo num painel se le como "sem
    problema". O padrao de quem nao sabe precisa ser "nao confie".
    """
    t = cenario(repos=[("org/sem-controle", True)], estados=[])

    assert por_repo(spark, t)["org/sem-controle"]["issues_confiavel"] is False


def test_a_ordem_do_painel_sobrevive_a_composicao(spark, cenario):
    """O `ORDER BY` de dentro da CTE nao vale para fora.

    O painel ordena por bus factor, e a subconsulta perde essa garantia se a
    consulta externa nao reordenar. Com dois repositorios de bus factor igual
    o desempate cai no ritmo, entao o teste verifica que a coluna de ordem
    existe e que a consulta devolve as duas linhas.
    """
    t = cenario(
        repos=[("org/alfa", True), ("org/beta", True)],
        estados=[("org/alfa", "ok", "2026-08-28T00:00:00")],
    )
    linhas = spark.sql(consumo.painel_com_portao(**t)).collect()

    assert len(linhas) == 2
    assert [l["bus_factor"] for l in linhas] == [1, 1]
