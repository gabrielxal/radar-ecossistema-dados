"""Schema da silver verificado contra o motor Spark.

O que estes testes cobrem e o que os testes de string nao alcancam: se o DDL
declarado e sintaticamente aceito, e como o `from_json` responde a payload
incompleto, invalido ou com campo a mais.
"""

from datetime import datetime, timezone

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


# --------------------------------------------------------------------------
# Tipagem e normalizacao
#
# Timestamp e sempre verificado por `date_format` no fuso da sessao, nunca
# pelo datetime colhido em Python: o `collect()` converte para o fuso do
# driver, e o teste passaria ou falharia conforme a maquina.
# --------------------------------------------------------------------------

MOMENTO = datetime(2026, 8, 22, 15, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def tipado(spark, df_payload):
    """Payload cru -> parsear -> tipar, com as colunas da bronze em volta."""
    from pyspark.sql import functions as F

    def constroi(dado, repo="duckdb/duckdb"):
        df = (
            df_payload(dado)
            .withColumn("repo", F.lit(repo))
            .withColumn("_ingerido_em", F.lit(MOMENTO).cast("timestamp"))
            .withColumn("_arquivo_origem", F.lit("/raw/commits/x.jsonl"))
        )
        return silver.tipar(silver.parsear(df), MOMENTO)

    return constroi


def em_utc(df, coluna):
    """Renderiza o timestamp no fuso da sessao, sem passar por Python."""
    from pyspark.sql import functions as F

    return df.select(
        F.date_format(F.col(coluna), "yyyy-MM-dd HH:mm:ss").alias("t")
    ).collect()[0]["t"]


def test_tipos_declarados_por_coluna(tipado):
    tipos = dict(tipado(COMMIT).dtypes)
    assert tipos["sha"] == "string"            # hash, nunca numero
    assert tipos["autorado_em"] == "timestamp"
    assert tipos["commitado_em"] == "timestamp"
    assert tipos["comentarios"] == "int"
    assert tipos["assinatura_verificada"] == "boolean"
    assert tipos["github_id"] == "bigint"
    assert tipos["qtd_pais"] == "int"


def test_data_iso_vira_timestamp_no_instante_certo(tipado):
    df = tipado(COMMIT)
    assert em_utc(df, "autorado_em") == "2026-08-01 10:00:00"
    assert em_utc(df, "commitado_em") == "2026-08-02 10:00:00"


def test_offset_explicito_e_convertido_para_utc(tipado):
    # A API devolve `Z`, mas o cast nao pode depender disso.
    com_offset = {
        **COMMIT,
        "commit": {
            **COMMIT["commit"],
            "author": {**COMMIT["commit"]["author"], "date": "2026-08-01T10:00:00+02:00"},
        },
    }
    assert em_utc(tipado(com_offset), "autorado_em") == "2026-08-01 08:00:00"


def test_data_invalida_vira_nulo_sem_derrubar_a_carga(tipado):
    # `try_to_timestamp`: com ANSI ligado, o cast comum lancaria excecao e
    # um unico registro torto derrubaria a carga inteira.
    torta = {
        **COMMIT,
        "commit": {
            **COMMIT["commit"],
            "author": {**COMMIT["commit"]["author"], "date": "ontem de manha"},
        },
    }
    linha = tipado(torta).collect()[0]
    assert linha["autorado_em"] is None
    assert linha["sha"] == "abc123"


def test_email_normalizado_para_minusculas(tipado):
    barulhento = {
        **COMMIT,
        "commit": {
            **COMMIT["commit"],
            "author": {**COMMIT["commit"]["author"], "email": "  Ana@Exemplo.COM  "},
        },
    }
    assert tipado(barulhento).collect()[0]["autor_email"] == "ana@exemplo.com"


def test_categorica_normalizada(tipado):
    variado = {
        **COMMIT,
        "author": {**COMMIT["author"], "type": " Bot "},
        "commit": {
            **COMMIT["commit"],
            "verification": {"verified": False, "reason": "UNSIGNED"},
        },
    }
    linha = tipado(variado).collect()[0]
    assert linha["github_tipo"] == "bot"
    assert linha["assinatura_motivo"] == "unsigned"
    assert linha["github_tipo"] in silver.TIPOS_DE_AUTOR
    assert linha["assinatura_motivo"] in silver.MOTIVOS_DE_ASSINATURA


def test_string_vazia_vira_nulo(tipado):
    # `''` e NULL sao a mesma ausencia e comparam diferente.
    vazio = {**COMMIT, "commit": {**COMMIT["commit"], "message": "   "}}
    assert tipado(vazio).collect()[0]["mensagem"] is None


def test_mensagem_preserva_o_conteudo_interno(tipado):
    # Aparar as pontas nao pode reescrever o corpo da mensagem.
    texto = {**COMMIT, "commit": {**COMMIT["commit"], "message": "  fix: a\n\n  b  "}}
    assert tipado(texto).collect()[0]["mensagem"] == "fix: a\n\n  b"


def test_commit_sem_conta_do_github_mantem_a_identidade_do_git(tipado):
    linha = tipado({**COMMIT, "author": None}).collect()[0]
    assert linha["github_login"] is None
    assert linha["github_id"] is None
    assert linha["autor_nome"] == "Ana"        # a identidade do git sobrevive
    assert linha["autor_email"] == "ana@exemplo.com"


def test_quantidade_de_pais_identifica_merge(tipado):
    assert tipado(COMMIT).collect()[0]["qtd_pais"] == 2

    simples = {**COMMIT, "parents": [{"sha": "p1"}]}
    assert tipado(simples).collect()[0]["qtd_pais"] == 1


def test_parents_ausente_nao_vira_contagem_falsa(tipado):
    # `size(NULL)` devolve -1 em modo legado: contagem negativa entraria na
    # tabela como se fosse dado.
    sem_pais = {k: v for k, v in COMMIT.items() if k != "parents"}
    assert tipado(sem_pais).collect()[0]["qtd_pais"] is None


def test_proveniencia_atravessa_a_camada(tipado):
    linha = tipado(COMMIT).collect()[0]
    assert linha["_arquivo_origem"] == "/raw/commits/x.jsonl"
    assert linha["_ingerido_em"] is not None
    assert linha["_processado_em"] is not None


def test_repo_vem_da_bronze_e_nao_do_payload(tipado):
    # O payload de commit nao carrega o repositorio; ele vem do caminho.
    assert tipado(COMMIT, repo="pola-rs/polars").collect()[0]["repo"] == "pola-rs/polars"


# --------------------------------------------------------------------------
# Quarentena
# --------------------------------------------------------------------------

@pytest.fixture
def classificado(spark, df_payload):
    """Payload cru -> parsear -> classificar, com as colunas da bronze."""
    from pyspark.sql import functions as F

    def constroi(dados_brutos, repo="duckdb/duckdb"):
        linhas = dados_brutos if isinstance(dados_brutos, list) else [dados_brutos]
        df = None
        for dado in linhas:
            parte = (
                df_payload(dado)
                .withColumn("repo", F.lit(repo))
                .withColumn("_ingerido_em", F.lit(MOMENTO).cast("timestamp"))
                .withColumn("_arquivo_origem", F.lit("/raw/commits/x.jsonl"))
            )
            df = parte if df is None else df.union(parte)
        return silver.classificar(silver.parsear(df), MOMENTO)

    return constroi


def motivo_de(classificado, dado):
    return classificado(dado).collect()[0][silver.COLUNA_MOTIVO]


# --------------------------------------------------------------------------
# Roteamento
# --------------------------------------------------------------------------

def test_commit_completo_passa_sem_motivo(classificado):
    assert motivo_de(classificado, COMMIT) is None


def test_json_invalido_e_rotulado_ilegivel(classificado):
    assert motivo_de(classificado, "{isso nao e json") == "payload_ilegivel"


def test_payload_sem_a_chave_e_rotulado_chave_ausente(classificado):
    # JSON valido, campos presentes, mas sem `sha`: diagnostico diferente de
    # payload ilegivel, e por isso um motivo proprio.
    sem_sha = {k: v for k, v in COMMIT.items() if k != "sha"}
    assert motivo_de(classificado, sem_sha) == "chave_ausente"


def test_data_do_commit_invalida_e_desviada(classificado):
    torta = {
        **COMMIT,
        "commit": {
            **COMMIT["commit"],
            "committer": {**COMMIT["commit"]["committer"], "date": "ontem de manha"},
        },
    }
    assert motivo_de(classificado, torta) == "data_do_commit_ausente"


def test_motivo_pertence_ao_dominio_declarado(classificado):
    for dado in ["{invalido", {k: v for k, v in COMMIT.items() if k != "sha"}]:
        assert motivo_de(classificado, dado) in silver.MOTIVOS_DE_REJEICAO


def test_ordem_dos_motivos_da_o_diagnostico_mais_geral(classificado):
    # Payload ilegivel tambem nao tem `sha`. Se a ordem se invertesse, todo
    # JSON quebrado seria rotulado `chave_ausente` e o diagnostico se perderia.
    assert motivo_de(classificado, "{invalido") == "payload_ilegivel"


# --------------------------------------------------------------------------
# A invariante: nada se perde, nada duplica
# --------------------------------------------------------------------------

def test_aprovados_e_rejeitados_particionam_a_entrada(classificado):
    sem_sha = {k: v for k, v in COMMIT.items() if k != "sha"}
    df = classificado([COMMIT, sem_sha, "{invalido"])

    assert df.count() == 3
    assert silver.aprovados(df).count() == 1
    assert silver.rejeitados(df).count() == 2


def test_nenhuma_linha_cai_nos_dois_lados(classificado):
    df = classificado([COMMIT, "{invalido"])
    aprovadas = {l["sha"] for l in silver.aprovados(df).collect()}
    rejeitadas = {l["sha"] for l in silver.rejeitados(df).collect()}
    assert aprovadas & rejeitadas == set()


# --------------------------------------------------------------------------
# Formato das duas saidas
# --------------------------------------------------------------------------

def test_tabela_silver_nao_carrega_o_payload(classificado):
    # Guardar o payload aqui duplicaria o que a bronze ja armazena.
    colunas = silver.aprovados(classificado(COMMIT)).columns
    assert "payload" not in colunas
    assert silver.COLUNA_MOTIVO not in colunas
    assert tuple(colunas) == silver.COLUNAS_SILVER


def test_quarentena_guarda_o_original_intacto(classificado):
    import json

    ruim = {k: v for k, v in COMMIT.items() if k != "sha"}
    linha = silver.rejeitados(classificado(ruim)).collect()[0]

    assert json.loads(linha["payload"]) == ruim
    assert linha["motivo"] == "chave_ausente"
    assert linha["repo"] == "duckdb/duckdb"
    assert tuple(silver.rejeitados(classificado(ruim)).columns) == silver.COLUNAS_REJEITADOS


def test_proveniencia_acompanha_a_linha_rejeitada(classificado):
    # Sem ela nao da para voltar ao arquivo que trouxe o registro torto.
    linha = silver.rejeitados(classificado("{invalido")).collect()[0]
    assert linha["_arquivo_origem"] == "/raw/commits/x.jsonl"
    assert linha["_ingerido_em"] is not None
    assert linha["_processado_em"] is not None


def test_colunas_das_saidas_batem_com_os_ddl(classificado):
    df = classificado(COMMIT)
    for coluna in silver.aprovados(df).columns:
        assert coluna in silver.ddl_commits()
    for coluna in silver.rejeitados(df).columns:
        assert coluna in silver.ddl_rejeitados()


# --------------------------------------------------------------------------
# Carga incremental: filtro por watermark
#
# O MERGE e o append exigem Delta e seguem validados no Databricks. O que se
# testa aqui e a decisao de quais linhas entram no lote.
# --------------------------------------------------------------------------

from radar.controle import Checkpoint  # noqa: E402

ONTEM = datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc)
HOJE = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def lote(spark):
    """DataFrame no formato da tabela bronze, com `_ingerido_em` variando."""
    from pyspark.sql import functions as F

    def constroi(momentos):
        linhas = [(f"sha{i}", m) for i, m in enumerate(momentos)]
        return (
            spark.createDataFrame(linhas, "sha STRING, _ingerido_em TIMESTAMP")
            .withColumn("repo", F.lit("duckdb/duckdb"))
            .withColumn("payload", F.lit("{}"))
        )

    return constroi


def test_sem_checkpoint_o_lote_e_a_bronze_inteira(lote):
    df = lote([ONTEM, HOJE])
    assert silver.filtrar_novos(df, None).count() == 2


def test_checkpoint_sem_watermark_tambem_le_tudo(lote):
    # Primeira execucao registrada, porem sem lote anterior processado.
    vazio = Checkpoint(repo="b", endpoint="commits@silver", watermark=None)
    assert silver.filtrar_novos(lote([ONTEM, HOJE]), vazio).count() == 2


def test_watermark_corta_o_que_ja_foi_processado(lote):
    checkpoint = Checkpoint(repo="b", endpoint="commits@silver", watermark=ONTEM)
    novos = silver.filtrar_novos(lote([ONTEM, HOJE]), checkpoint)

    assert novos.count() == 1
    assert novos.collect()[0]["sha"] == "sha1"


def test_comparacao_e_estrita_e_nao_reprocessa_a_fronteira(lote):
    # Linha da bronze nao muda depois de gravada: reler a fronteira so
    # gastaria trabalho para reescrever o mesmo valor.
    checkpoint = Checkpoint(repo="b", endpoint="commits@silver", watermark=HOJE)
    assert silver.filtrar_novos(lote([ONTEM, HOJE]), checkpoint).count() == 0
