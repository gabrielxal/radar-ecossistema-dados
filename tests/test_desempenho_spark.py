"""A replica e a medida de desbalanceamento, contra o motor.

As duas erram em silencio de formas opostas. A replica pode multiplicar linhas
que o `MERGE` colapsa de volta, e ai a escala nao aconteceu embora a contagem
intermediaria diga que sim. A distribuicao pode dar numero plausivel com a
media calculada sobre a base errada.
"""

import pytest

from radar import desempenho

pytestmark = pytest.mark.spark


@pytest.fixture
def commits(spark):
    """Cinco linhas com a concentracao que o dado real tem.

    `org/grande` responde por 60%, que e a forma do `duckdb/duckdb` com 31%:
    poucos valores de chave e um deles dominando.
    """
    return spark.createDataFrame(
        [
            ("sha1", "org/grande"),
            ("sha2", "org/grande"),
            ("sha3", "org/grande"),
            ("sha4", "org/medio"),
            ("sha5", "org/pequeno"),
        ],
        "sha STRING, repo STRING",
    )


# --------------------------------------------------------------------------
# Replica
# --------------------------------------------------------------------------

def test_replicar_multiplica_o_volume(commits):
    assert desempenho.replicar(commits, 3, "sha").count() == 15


def test_as_copias_tem_chave_distinta(commits):
    """E a razao de a chave mudar.

    Replicar sem tocar nela produziria tres copias identicas, que a
    deduplicacao da bronze e o `MERGE` da silver colapsam de volta em uma. O
    volume subiria na leitura e nao no destino, e a medida seria de uma carga
    que nao existe.
    """
    replicado = desempenho.replicar(commits, 3, "sha")

    assert replicado.select("sha").distinct().count() == 15


def test_a_distribuicao_por_repositorio_sobrevive(commits):
    """O desbalanceamento e o objeto da medida, entao ele nao pode ser diluido.

    Se `org/grande` responde por 60% com cinco linhas, precisa responder por
    60% com quinze. Uma escala que distribuisse por igual mediria um dado que
    nao existe.
    """
    replicado = desempenho.replicar(commits, 3, "sha")
    por_repo = {
        linha["repo"]: linha["n"]
        for linha in replicado.groupBy("repo").count()
        .withColumnRenamed("count", "n").collect()
    }

    assert por_repo == {"org/grande": 9, "org/medio": 3, "org/pequeno": 3}


def test_fator_um_preserva_a_chave_original(commits):
    """Com fator 1 a replica precisa ser identidade.

    E o caso que roda em toda execucao de controle: medir a tabela real usa
    fator 1, e se a chave ganhasse sufixo ali, a linha deixaria de casar com o
    que ja esta gravado.
    """
    replicado = desempenho.replicar(commits, 1, "sha")

    assert sorted(l["sha"] for l in replicado.collect()) == [
        "sha1", "sha2", "sha3", "sha4", "sha5"
    ]


def test_a_coluna_de_controle_nao_vaza_para_o_resultado(commits):
    """`_copia` e andaime da transformacao e nao pertence ao esquema de saida.

    Deixa-la passar quebraria o `MERGE`, que espera a forma da tabela alvo.
    """
    assert desempenho.replicar(commits, 2, "sha").columns == commits.columns


# --------------------------------------------------------------------------
# Distribuicao
# --------------------------------------------------------------------------

def test_a_concentracao_aparece_como_multiplo_da_media(spark, commits):
    """Tres repositorios, cinco linhas: a media e 1,67 por chave.

    `org/grande` tem 3, ou seja 1,8 vezes a media, e 60% do total. E o numero
    que traduz "esta tabela e desbalanceada" em quanto a task mais lenta vai
    atrasar o estagio.
    """
    commits.createOrReplaceTempView("_commits")
    linhas = spark.sql(desempenho.sql_distribuicao("_commits", "repo")).collect()

    maior = linhas[0]
    assert maior["chave"] == "org/grande"
    assert maior["linhas"] == 3
    assert maior["pct_do_total"] == 60.0
    assert maior["vezes_a_media"] == 1.8
    assert maior["chaves_distintas"] == 3


def test_chave_unica_nao_concentra(spark, commits):
    """O contraste que decide a chave de shuffle.

    Por `sha` toda chave tem uma linha, entao `vezes_a_media` e 1 em todas: e
    a distribuicao que `(repo, sha)` produz na deduplicacao da bronze, e o
    motivo de aquele shuffle nao ter desbalanceamento apesar de ser sobre o
    historico inteiro.
    """
    commits.createOrReplaceTempView("_commits")
    linhas = spark.sql(desempenho.sql_distribuicao("_commits", "sha")).collect()

    assert {linha["vezes_a_media"] for linha in linhas} == {1.0}


def test_a_ordem_poe_o_gargalo_primeiro(spark, commits):
    commits.createOrReplaceTempView("_commits")
    linhas = spark.sql(desempenho.sql_distribuicao("_commits", "repo")).collect()

    assert [linha["linhas"] for linha in linhas] == [3, 1, 1]


# --------------------------------------------------------------------------
# Plano
# --------------------------------------------------------------------------

def test_o_plano_de_um_agrupamento_tem_shuffle(spark, commits):
    """A ponte entre o texto do plano e o que o motor de fato produz.

    `resumo_do_plano` e testado sobre texto fixo; aqui se confirma que o texto
    que o motor gera contem mesmo os operadores procurados. Sem isto, um
    operador renomeado numa versao nova faria o resumo devolver `{}` para
    sempre, sem erro.
    """
    commits.createOrReplaceTempView("_commits")
    texto = desempenho.plano(spark, "SELECT repo, count(*) FROM _commits GROUP BY repo")

    resumo = desempenho.resumo_do_plano(texto)

    assert resumo.get("Exchange", 0) >= 1
    assert resumo.get("HashAggregate", 0) >= 1


def test_o_numero_de_varreduras_bate_com_o_de_tabelas(spark, commits):
    """O teste que teria pego a contagem em dobro.

    E a conferencia mais simples possivel do resumo: uma consulta que le duas
    tabelas tem de mostrar duas varreduras. A primeira versao mostrava quatro,
    porque contava a arvore e a secao de detalhe do modo `formatted`, e o
    defeito so apareceu ao conferir `Scan` contra o numero de tabelas das
    consultas reais.
    """
    commits.createOrReplaceTempView("_a")
    commits.createOrReplaceTempView("_b")

    texto = desempenho.plano(
        spark,
        "SELECT a.repo, count(*) AS n FROM _a a JOIN _b b USING (sha) GROUP BY a.repo",
    )

    assert desempenho.resumo_do_plano(texto)["Scan"] == 2
