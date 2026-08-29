"""Dimensoes verificadas contra o motor Spark.

**Fora do alcance:** a gravacao em Delta e o `MERGE`, validados no Databricks.
"""

from datetime import date, datetime, timezone

import pytest

from radar import gold

pytestmark = pytest.mark.spark

MOMENTO = datetime(2026, 8, 23, 12, 0, 0, tzinfo=timezone.utc)


def commit(sha, github_id=None, login=None, tipo=None, nome="Ana",
           email="ana@exemplo.com", commitado="2026-08-01T10:00:00Z"):
    return (sha, github_id, login, tipo, nome, email, commitado)


ESQUEMA_COMMITS = (
    "sha STRING, github_id BIGINT, github_login STRING, github_tipo STRING, "
    "autor_nome STRING, autor_email STRING, commitado_em STRING"
)


@pytest.fixture
def commits(spark):
    """DataFrame no formato da silver, com a data ja convertida."""
    from pyspark.sql import functions as F

    def constroi(linhas):
        return spark.createDataFrame(linhas, ESQUEMA_COMMITS).withColumn(
            "commitado_em", F.to_timestamp("commitado_em")
        )

    return constroi


# --------------------------------------------------------------------------
# Chave substituta
# --------------------------------------------------------------------------

def chave(spark, *valores):
    from pyspark.sql import functions as F

    partes = [F.lit(v).cast("string") for v in valores]
    return spark.range(1).select(gold.chave_substituta(*partes).alias("k")).collect()[0]["k"]


def test_mesma_entrada_gera_a_mesma_chave(spark):
    # Reprocessar do zero tem de devolver a chave identica, senao todo fato
    # passa a apontar para a linha errada.
    assert chave(spark, "conta", "42") == chave(spark, "conta", "42")


def test_entradas_diferentes_geram_chaves_diferentes(spark):
    assert chave(spark, "conta", "42") != chave(spark, "conta", "43")
    assert chave(spark, "conta", "42") != chave(spark, "email", "42")


def test_nulo_ocupa_posicao_em_vez_de_desaparecer(spark):
    # `concat_ws` ignora nulos: sem o marcador, ("a", NULL) e ("a") virariam
    # o mesmo texto e a mesma chave para entidades diferentes.
    assert chave(spark, "a", None) != chave(spark, "a")


def test_fronteira_entre_as_partes_e_respeitada(spark):
    # Com separador comum, ("ab", "c") e ("a", "bc") colidiriam.
    assert chave(spark, "ab", "c") != chave(spark, "a", "bc")


def test_chave_tem_o_tamanho_de_um_sha256(spark):
    assert len(chave(spark, "conta", "42")) == 64


# --------------------------------------------------------------------------
# dim_tempo
# --------------------------------------------------------------------------

def test_um_dia_por_linha_sem_lacuna(spark):
    df = gold.gerar_dim_tempo(spark, date(2026, 1, 1), date(2026, 12, 31))
    assert df.count() == 365
    assert df.select("sk_tempo").distinct().count() == 365


def test_ano_bissexto_tem_o_dia_a_mais(spark):
    df = gold.gerar_dim_tempo(spark, date(2028, 1, 1), date(2028, 12, 31))
    assert df.count() == 366


def test_chave_do_tempo_e_aaaammdd(spark):
    df = gold.gerar_dim_tempo(spark, date(2026, 8, 23), date(2026, 8, 23))
    linha = df.collect()[0]
    assert linha["sk_tempo"] == 20260823
    assert linha["data"] == date(2026, 8, 23)


def test_atributos_do_calendario(spark):
    # 2026-08-23 e um domingo.
    linha = gold.gerar_dim_tempo(spark, date(2026, 8, 23), date(2026, 8, 23)).collect()[0]
    assert (linha["ano"], linha["trimestre"], linha["mes"], linha["dia"]) == (2026, 3, 8, 23)
    assert linha["nome_mes"] == "agosto"
    assert linha["nome_dia"] == "domingo"
    assert linha["dia_da_semana"] == 7
    assert linha["e_fim_de_semana"] is True


def test_segunda_feira_abre_a_semana(spark):
    # 2026-08-24 e segunda.
    linha = gold.gerar_dim_tempo(spark, date(2026, 8, 24), date(2026, 8, 24)).collect()[0]
    assert linha["dia_da_semana"] == 1
    assert linha["nome_dia"] == "segunda-feira"
    assert linha["e_fim_de_semana"] is False


def test_nomes_nao_dependem_do_locale(spark):
    df = gold.gerar_dim_tempo(spark, date(2026, 1, 1), date(2026, 12, 31))
    meses = {l["nome_mes"] for l in df.select("nome_mes").distinct().collect()}
    assert meses == set(gold.MESES)


def test_ano_iso_difere_do_calendario_na_virada(spark):
    # 2027-01-01 e sexta e pertence a semana 53 de 2026. Agrupar por
    # (ano, semana_iso) sem `ano_semana_iso` juntaria semanas distintas.
    linha = gold.gerar_dim_tempo(spark, date(2027, 1, 1), date(2027, 1, 1)).collect()[0]
    assert linha["ano"] == 2027
    assert linha["ano_semana_iso"] == 2026
    assert linha["semana_iso"] == 53


def test_colunas_batem_com_o_ddl(spark):
    df = gold.gerar_dim_tempo(spark, date(2026, 8, 23), date(2026, 8, 23))
    for coluna in df.columns:
        assert coluna in gold.ddl_dim_tempo()


# --------------------------------------------------------------------------
# dim_autor
# --------------------------------------------------------------------------

def test_uma_linha_por_autor(commits):
    df = gold.montar_dim_autor(
        commits([
            commit("a", github_id=42, login="ana", tipo="user"),
            commit("b", github_id=42, login="ana", tipo="user"),
            commit("c", github_id=99, login="bia", tipo="user", email="bia@exemplo.com"),
        ]),
        MOMENTO,
    )
    assert df.count() == 2


def test_chave_vem_da_conta_quando_existe(commits):
    linha = gold.montar_dim_autor(
        commits([commit("a", github_id=42, login="ana", tipo="user")]), MOMENTO
    ).collect()[0]
    assert linha["origem_da_chave"] == gold.ORIGEM_CONTA
    assert linha["chave_natural"] == "42"


def test_chave_vem_do_email_sem_conta(commits):
    linha = gold.montar_dim_autor(
        commits([commit("a", github_id=None, email="sem@conta.com")]), MOMENTO
    ).collect()[0]
    assert linha["origem_da_chave"] == gold.ORIGEM_EMAIL
    assert linha["chave_natural"] == "sem@conta.com"
    assert linha["github_id"] is None


def test_conta_unifica_dois_emails_da_mesma_pessoa(commits):
    # E o que a chave por e-mail sozinha nao faria: duas linhas para a mesma
    # pessoa que usa endereco de trabalho e pessoal.
    df = gold.montar_dim_autor(
        commits([
            commit("a", github_id=42, login="ana", email="ana@trabalho.com"),
            commit("b", github_id=42, login="ana", email="ana@pessoal.com"),
        ]),
        MOMENTO,
    )
    assert df.count() == 1


def test_scd1_mantem_o_commit_mais_recente(commits):
    df = gold.montar_dim_autor(
        commits([
            commit("a", github_id=42, login="ana_antiga", commitado="2026-06-01T10:00:00Z"),
            commit("b", github_id=42, login="ana_nova", commitado="2026-08-01T10:00:00Z"),
        ]),
        MOMENTO,
    )
    assert df.collect()[0]["github_login"] == "ana_nova"


def test_escolha_e_determinista_em_empate(commits):
    # Mesmo instante nos dois commits: o desempate por `sha` evita que a
    # linha escolhida dependa da ordem de leitura.
    linhas = [
        commit("zzz", github_id=42, login="pelo_sha_maior"),
        commit("aaa", github_id=42, login="pelo_sha_menor"),
    ]
    primeira = gold.montar_dim_autor(commits(linhas), MOMENTO).collect()[0]
    segunda = gold.montar_dim_autor(commits(linhas), MOMENTO).collect()[0]
    assert primeira["github_login"] == segunda["github_login"] == "pelo_sha_menor"


def test_commit_sem_conta_e_sem_email_fica_de_fora(commits):
    # Nao ha chave natural: ele pertence ao membro desconhecido, nao a uma
    # linha propria sem identidade.
    df = gold.montar_dim_autor(
        commits([commit("a", github_id=None, email=None)]), MOMENTO
    )
    assert df.count() == 0


def test_chaves_substitutas_sao_unicas(commits):
    df = gold.montar_dim_autor(
        commits([
            commit("a", github_id=42, login="ana"),
            commit("b", github_id=None, email="sem@conta.com"),
            commit("c", github_id=99, login="bia", email="bia@exemplo.com"),
        ]),
        MOMENTO,
    )
    assert df.select("sk_autor").distinct().count() == df.count() == 3


def test_colunas_do_autor_batem_com_o_ddl(commits):
    df = gold.montar_dim_autor(commits([commit("a", github_id=42)]), MOMENTO)
    assert tuple(df.columns) == gold.COLUNAS_DIM_AUTOR


# --------------------------------------------------------------------------
# Membro desconhecido
# --------------------------------------------------------------------------

def test_membro_desconhecido_tem_o_mesmo_formato(spark, commits):
    desconhecido = gold.linha_desconhecida(spark, MOMENTO)
    autores = gold.montar_dim_autor(commits([commit("a", github_id=42)]), MOMENTO)

    assert tuple(desconhecido.columns) == tuple(autores.columns)
    assert desconhecido.union(autores).count() == 2


def test_membro_desconhecido_nao_colide_com_autor_real(spark, commits):
    desconhecido = gold.linha_desconhecida(spark, MOMENTO).collect()[0]
    real = gold.montar_dim_autor(
        commits([commit("a", github_id=None, email=gold.CHAVE_DESCONHECIDA)]), MOMENTO
    ).collect()[0]

    # Mesma chave natural, origens diferentes: as substitutas divergem.
    assert desconhecido["chave_natural"] == real["chave_natural"]
    assert desconhecido["sk_autor"] != real["sk_autor"]


# --------------------------------------------------------------------------
# dim_repositorio: SCD2 derivada das fotos diarias
# --------------------------------------------------------------------------

ESQUEMA_FOTOS = (
    "repo_id BIGINT, repo STRING, dt STRING, nome_completo STRING, dono STRING, "
    "dono_tipo STRING, linguagem STRING, licenca STRING, branch_padrao STRING, "
    "arquivado BOOLEAN, e_fork BOOLEAN"
)


def foto(repo_id, dt, licenca="mit", linguagem="c++", arquivado=False,
         dono="duckdb", nome="duckdb/duckdb"):
    return (repo_id, nome, dt, nome, dono, "organization", linguagem, licenca,
            "main", arquivado, False)


@pytest.fixture
def fotos(spark):
    def constroi(linhas):
        return spark.createDataFrame(linhas, ESQUEMA_FOTOS)

    return constroi


def versoes(fotos, linhas):
    return gold.montar_dim_repositorio(fotos(linhas), MOMENTO).orderBy("valido_de")


def test_serie_sem_mudanca_produz_uma_versao(fotos):
    df = versoes(fotos, [foto(1, "2026-08-21"), foto(1, "2026-08-22"), foto(1, "2026-08-23")])
    assert df.count() == 1
    linha = df.collect()[0]
    assert linha["observado_de"] == date(2026, 8, 21)
    assert linha["valido_ate"] is None
    assert linha["flag_atual"] is True


def test_primeira_versao_abre_para_tras(fotos):
    # O fato comeca antes da primeira foto. Sem isto, a juncao por vigencia
    # descartaria todo commit anterior ao dia em que passamos a fotografar.
    linha = versoes(fotos, [foto(1, "2026-08-21")]).collect()[0]
    assert linha["valido_de"] == gold.INICIO_DOS_TEMPOS
    assert linha["observado_de"] == date(2026, 8, 21)


def test_versao_seguinte_vale_do_dia_em_que_foi_vista(fotos):
    # So a primeira e suposicao; a mudanca foi observada e tem data real.
    antiga, nova = versoes(fotos, [
        foto(1, "2026-08-21", licenca="mit"),
        foto(1, "2026-08-23", licenca="apache-2.0"),
    ]).collect()

    assert antiga["valido_de"] == gold.INICIO_DOS_TEMPOS
    assert nova["valido_de"] == nova["observado_de"] == date(2026, 8, 23)


def test_todo_dia_encontra_exatamente_uma_versao(spark, fotos):
    # A propriedade que a juncao do fato depende: qualquer instante cai em
    # uma versao, e em apenas uma.
    versoes(fotos, [
        foto(1, "2026-08-21", licenca="mit"),
        foto(1, "2026-08-23", licenca="apache-2.0"),
    ]).createOrReplaceTempView("_dim")

    for dia in ("2020-01-01", "2026-08-22", "2026-08-23", "2030-01-01"):
        achadas = spark.sql(
            f"""
            SELECT count(*) AS n FROM _dim
            WHERE DATE'{dia}' >= valido_de
              AND (valido_ate IS NULL OR DATE'{dia}' < valido_ate)
            """
        ).collect()[0]["n"]
        assert achadas == 1, dia


def test_mudanca_de_atributo_abre_versao(fotos):
    df = versoes(fotos, [
        foto(1, "2026-08-21", licenca="mit"),
        foto(1, "2026-08-22", licenca="mit"),
        foto(1, "2026-08-23", licenca="apache-2.0"),
    ])
    assert df.count() == 2
    antiga, nova = df.collect()
    assert (antiga["licenca"], nova["licenca"]) == ("mit", "apache-2.0")


def test_fronteira_nao_deixa_dia_sem_versao_nem_em_duas(fotos):
    antiga, nova = versoes(fotos, [
        foto(1, "2026-08-21", licenca="mit"),
        foto(1, "2026-08-23", licenca="apache-2.0"),
    ]).collect()

    # Fechada a esquerda, aberta a direita: `valido_ate` da anterior e o
    # `valido_de` da seguinte.
    assert antiga["valido_ate"] == nova["valido_de"] == date(2026, 8, 23)
    assert antiga["flag_atual"] is False
    assert nova["flag_atual"] is True


def test_apenas_uma_versao_vigente_por_chave_natural(fotos):
    df = versoes(fotos, [
        foto(1, "2026-08-21", licenca="mit"),
        foto(1, "2026-08-22", licenca="apache-2.0"),
        foto(1, "2026-08-23", licenca="bsd-3-clause"),
    ])
    assert df.where("flag_atual").count() == 1


def test_flag_atual_e_valido_ate_contam_a_mesma_historia(fotos):
    df = versoes(fotos, [
        foto(1, "2026-08-21", licenca="mit"),
        foto(1, "2026-08-22", licenca="apache-2.0"),
    ])
    incoerentes = df.where(
        "(flag_atual AND valido_ate IS NOT NULL) OR (NOT flag_atual AND valido_ate IS NULL)"
    )
    assert incoerentes.count() == 0


def test_valor_que_volta_ao_anterior_abre_terceira_versao(fotos):
    # mit -> apache -> mit sao tres periodos distintos, nao dois.
    df = versoes(fotos, [
        foto(1, "2026-08-21", licenca="mit"),
        foto(1, "2026-08-22", licenca="apache-2.0"),
        foto(1, "2026-08-23", licenca="mit"),
    ])
    assert df.count() == 3
    assert df.select("sk_repositorio").distinct().count() == 3


def test_repositorios_diferentes_nao_se_misturam(fotos):
    df = gold.montar_dim_repositorio(
        fotos([
            foto(1, "2026-08-21", licenca="mit"),
            foto(2, "2026-08-21", licenca="apache-2.0", nome="pola-rs/polars"),
            foto(1, "2026-08-22", licenca="bsd-3-clause"),
        ]),
        MOMENTO,
    )
    assert df.count() == 3
    assert df.where("repo_id = 2").count() == 1


def test_renomeacao_versiona_sem_trocar_a_chave_natural(fotos):
    # `repo_id` sobrevive; `nome_completo` e atributo versionado.
    df = versoes(fotos, [
        foto(1, "2026-08-21", nome="antigo/nome"),
        foto(1, "2026-08-22", nome="novo/nome"),
    ])
    assert df.count() == 2
    assert df.select("repo_id").distinct().count() == 1


def test_chave_substituta_e_unica_e_estavel(fotos):
    linhas = [
        foto(1, "2026-08-21", licenca="mit"),
        foto(1, "2026-08-22", licenca="apache-2.0"),
    ]
    primeira = [l["sk_repositorio"] for l in versoes(fotos, linhas).collect()]
    segunda = [l["sk_repositorio"] for l in versoes(fotos, linhas).collect()]

    # Derivada, nao mantida: recalcular do zero devolve as mesmas chaves.
    assert primeira == segunda
    assert len(set(primeira)) == 2


def test_colunas_batem_com_o_ddl(fotos):
    df = versoes(fotos, [foto(1, "2026-08-21")])
    assert tuple(df.columns) == gold.COLUNAS_DIM_REPOSITORIO
    for coluna in df.columns:
        assert coluna in gold.ddl_dim_repositorio()
