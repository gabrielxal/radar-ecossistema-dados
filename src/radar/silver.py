"""Camada silver: o payload da bronze vira colunas tipadas.

O schema e declarado aqui, nunca inferido. Inferencia e funcao dos dados: o
mesmo payload pode produzir schemas diferentes entre duas execucoes, conforme
o que aparecer na amostra lida.

O schema descreve o JSON como ele chega. Data ISO fica STRING, porque em JSON
ela e string; a conversao para TIMESTAMP e explicita, coluna a coluna, no
passo de tipagem. Assim o cast falha onde ha regra e log, nao dentro da
leitura.

Declarado em DDL e nao em StructType: o modulo segue importavel sem pyspark, o
que mantem os testes de contrato rodando sem subir JVM, e o texto declarado e
exatamente o que o Spark recebe. `from_json` aceita as duas formas.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from radar import bronze, controle
from radar.config import SILVER, fqn
from radar.ingestao import Endpoint

TABELA_COMMITS = fqn(SILVER, "commits")

# Coluna que recebe o payload ja estruturado.
COLUNA_DADOS = "dados"

# Schema parcial de proposito: campo do JSON que nao esta aqui e ignorado pelo
# `from_json`. A silver declara o que promete entregar; o resto continua
# guardado no payload da bronze.
#
# `html_url` fica de fora por ser derivavel de `repo` e `sha`.
#
# Os dois `author` sao entidades diferentes e nao intercambiaveis:
#   commit.author -> identidade do git, digitada na maquina de quem commitou
#   author        -> usuario do GitHub, resolvido pela plataforma; pode ser NULL
SCHEMA_COMMIT = """
    sha STRING,
    commit STRUCT<
        author: STRUCT<name: STRING, email: STRING, date: STRING>,
        committer: STRUCT<name: STRING, email: STRING, date: STRING>,
        message: STRING,
        comment_count: BIGINT,
        verification: STRUCT<verified: BOOLEAN, reason: STRING>
    >,
    author STRUCT<login: STRING, id: BIGINT, type: STRING>,
    committer STRUCT<login: STRING, id: BIGINT, type: STRING>,
    parents ARRAY<STRUCT<sha: STRING>>
"""


# Dominios conhecidos das colunas categoricas, ja normalizados. Valor fora
# da lista nao e descartado nem corrigido: a bateria de qualidade reporta.
TIPOS_DE_AUTOR = ("user", "bot", "organization")

MOTIVOS_DE_ASSINATURA = (
    "valid",
    "unsigned",
    "expired_key",
    "not_signing_key",
    "unknown_key",
    "unknown_signature_type",
    "unverified_email",
    "bad_email",
    "no_user",
    "malformed_signature",
    "invalid",
    "gpgverify_error",
    "gpgverify_unavailable",
    "bad_cert",
    "ocsp_pending",
    "ocsp_error",
    "ocsp_revoked",
)


# --------------------------------------------------------------------------
# Aplicacao do schema
# --------------------------------------------------------------------------

def parsear(df, coluna: str = "payload"):
    """Aplica o schema declarado ao payload, preservando as demais colunas.

    Campo do JSON ausente no schema e ignorado; campo do schema ausente no
    JSON vira NULL. Nenhum dos dois interrompe a leitura: o que nao couber no
    contrato e tratado no passo de quarentena.
    """
    from pyspark.sql import functions as F

    return df.withColumn(COLUNA_DADOS, F.from_json(F.col(coluna), SCHEMA_COMMIT))


# --------------------------------------------------------------------------
# Tipagem e normalizacao
# --------------------------------------------------------------------------

def _texto(coluna):
    """Apara espacos e transforma string vazia em NULL.

    `''` e NULL significam a mesma ausencia, mas comparam diferente: um
    `WHERE campo IS NULL` deixaria as vazias de fora, sem aviso.
    """
    from pyspark.sql import functions as F

    return F.nullif(F.trim(coluna), F.lit(""))


def _categoria(coluna):
    """Texto normalizado para minusculas. Vale para coluna de dominio fechado.

    Sem isso `User`, `user` e `USER` viram tres categorias da mesma coisa, e
    nenhum GROUP BY corrige depois.
    """
    from pyspark.sql import functions as F

    return F.lower(_texto(coluna))


def _instante(coluna):
    """Data ISO da API em TIMESTAMP.

    `try_to_timestamp` e nao `to_timestamp`: com ANSI ligado -- o padrao em
    runtime recente do Databricks -- o cast comum lanca excecao e derruba a
    carga inteira por causa de um registro. A versao `try_` devolve NULL, e o
    NULL e o que a quarentena procura.
    """
    from pyspark.sql import functions as F

    return F.try_to_timestamp(coluna)


def tipar(df, momento: datetime):
    """Projeta o struct `dados` em colunas tipadas, uma decisao por coluna.

    Espera as colunas da bronze (`repo`, `_ingerido_em`, `_arquivo_origem`) e
    a coluna `dados`, produzida por `parsear`.
    """
    from pyspark.sql import functions as F

    dados = F.col(COLUNA_DADOS)
    autoria = dados["commit"]["author"]
    commit = dados["commit"]["committer"]
    verificacao = dados["commit"]["verification"]
    usuario = dados["author"]

    return df.select(
        # Chave natural: hash, nunca convertido para numero.
        _texto(dados["sha"]).alias("sha"),
        _texto(F.col("repo")).alias("repo"),

        # Identidade do git: digitada na maquina de quem commitou, sempre
        # presente, sem garantia de corresponder a uma conta.
        _texto(autoria["name"]).alias("autor_nome"),
        _categoria(autoria["email"]).alias("autor_email"),
        _texto(commit["name"]).alias("committer_nome"),
        _categoria(commit["email"]).alias("committer_email"),

        # Duas datas distintas: quando o codigo foi escrito e quando entrou no
        # repositorio. Em rebase elas se afastam por meses.
        _instante(autoria["date"]).alias("autorado_em"),
        _instante(commit["date"]).alias("commitado_em"),

        _texto(dados["commit"]["message"]).alias("mensagem"),
        dados["commit"]["comment_count"].cast("int").alias("comentarios"),

        verificacao["verified"].alias("assinatura_verificada"),
        _categoria(verificacao["reason"]).alias("assinatura_motivo"),

        # Usuario do GitHub: resolvido pela plataforma, ausente quando o
        # e-mail do commit nao esta associado a conta nenhuma. `id` e a chave
        # estavel -- login muda quando a pessoa renomeia a conta.
        _texto(usuario["login"]).alias("github_login"),
        usuario["id"].alias("github_id"),
        _categoria(usuario["type"]).alias("github_tipo"),

        # A lista de pais vira cardinalidade: mais de um identifica merge.
        # Os sha dos pais nao respondem nenhuma pergunta do projeto e seguem
        # guardados no payload da bronze.
        F.when(dados["parents"].isNull(), None)
        .otherwise(F.size(dados["parents"]))
        .cast("int")
        .alias("qtd_pais"),

        F.col("_ingerido_em"),
        F.col("_arquivo_origem"),
        F.lit(momento).cast("timestamp").alias("_processado_em"),

        # Segue adiante para a quarentena poder guardar o original. Nao entra
        # na tabela silver: duplicaria o que a bronze ja armazena.
        F.col("payload"),
    )


# --------------------------------------------------------------------------
# Quarentena
# --------------------------------------------------------------------------

TABELA_REJEITADOS = fqn(SILVER, "commits_rejeitados")

COLUNA_MOTIVO = "_motivo"

# O que entra na tabela silver. `payload` e `_motivo` ficam de fora: um
# duplicaria a bronze, o outro so existe para rotear.
COLUNAS_SILVER = (
    "sha",
    "repo",
    "autor_nome",
    "autor_email",
    "committer_nome",
    "committer_email",
    "autorado_em",
    "commitado_em",
    "mensagem",
    "comentarios",
    "assinatura_verificada",
    "assinatura_motivo",
    "github_login",
    "github_id",
    "github_tipo",
    "qtd_pais",
    "_ingerido_em",
    "_arquivo_origem",
    "_processado_em",
)

COLUNAS_REJEITADOS = (
    "repo",
    "sha",
    "motivo",
    "payload",
    "_ingerido_em",
    "_arquivo_origem",
    "_processado_em",
)

# Avaliados em ordem: o primeiro que casar nomeia a rejeicao. Do mais geral
# para o mais especifico, senao `chave_ausente` engoliria `payload_ilegivel`.
MOTIVOS_DE_REJEICAO = (
    "payload_ilegivel",
    "chave_ausente",
    "repo_ausente",
    "data_do_commit_ausente",
)


def ddl_commits() -> str:
    """DDL da tabela silver. Aqui o tipo e contrato, nao mais texto."""
    return f"""
CREATE TABLE IF NOT EXISTS {TABELA_COMMITS} (
    sha                   STRING    NOT NULL COMMENT 'chave natural, hash do commit',
    repo                  STRING    NOT NULL COMMENT 'owner/nome',
    autor_nome            STRING             COMMENT 'identidade do git de quem escreveu',
    autor_email           STRING             COMMENT 'e-mail do git, normalizado',
    committer_nome        STRING             COMMENT 'identidade do git de quem aplicou',
    committer_email       STRING             COMMENT 'e-mail do git, normalizado',
    autorado_em           TIMESTAMP          COMMENT 'quando o codigo foi escrito',
    commitado_em          TIMESTAMP NOT NULL COMMENT 'quando entrou no repositorio; sustenta o watermark',
    mensagem              STRING             COMMENT 'mensagem do commit, sem alteracao interna',
    comentarios           INT                COMMENT 'comentarios no commit',
    assinatura_verificada BOOLEAN            COMMENT 'assinatura conferida pelo GitHub',
    assinatura_motivo     STRING             COMMENT 'dominio conhecido em MOTIVOS_DE_ASSINATURA',
    github_login          STRING             COMMENT 'muda quando a conta e renomeada',
    github_id             BIGINT             COMMENT 'chave estavel do usuario; nulo sem conta associada',
    github_tipo           STRING             COMMENT 'dominio conhecido em TIPOS_DE_AUTOR',
    qtd_pais              INT                COMMENT 'mais de um identifica merge commit',
    _ingerido_em          TIMESTAMP          COMMENT 'quando a linha entrou na bronze',
    _arquivo_origem       STRING             COMMENT 'arquivo da landing zone de onde veio',
    _processado_em        TIMESTAMP          COMMENT 'quando a silver processou'
)
USING DELTA
COMMENT 'Commits tipados e normalizados. Uma linha por (repo, sha).'
"""


def ddl_rejeitados() -> str:
    """DDL da quarentena. O payload vem junto: sem ele nao ha investigacao."""
    return f"""
CREATE TABLE IF NOT EXISTS {TABELA_REJEITADOS} (
    repo            STRING    COMMENT 'owner/nome, quando conhecido',
    sha             STRING    COMMENT 'chave natural, quando presente no payload',
    motivo          STRING    COMMENT 'qual regra do contrato a linha violou',
    payload         STRING    COMMENT 'a linha original, intacta',
    _ingerido_em    TIMESTAMP COMMENT 'quando a linha entrou na bronze',
    _arquivo_origem STRING    COMMENT 'arquivo da landing zone de onde veio',
    _processado_em  TIMESTAMP COMMENT 'quando a silver rejeitou'
)
USING DELTA
COMMENT 'Registros que nao couberam no contrato da silver, com motivo e original.'
"""


def _motivo():
    """Coluna com o motivo da rejeicao; NULL quando a linha cabe no contrato.

    Um registro fora do contrato nao e descartado nem interrompe a carga: e
    desviado com o motivo anotado. Descartar quebraria a contagem de controle
    sem deixar rastro; abortar deixaria treze repositorios reféns de um.
    """
    from pyspark.sql import functions as F

    # Nenhum campo do contrato preenchido: JSON invalido ou objeto vazio.
    ilegivel = (
        F.col("sha").isNull()
        & F.col("commitado_em").isNull()
        & F.col("autor_nome").isNull()
        & F.col("github_id").isNull()
        & F.col("qtd_pais").isNull()
    )

    return (
        F.when(ilegivel, F.lit("payload_ilegivel"))
        .when(F.col("sha").isNull(), F.lit("chave_ausente"))
        .when(F.col("repo").isNull(), F.lit("repo_ausente"))
        # Sem data nao ha watermark nem lugar na linha do tempo: a linha nao
        # responderia nenhuma pergunta do projeto.
        .when(F.col("commitado_em").isNull(), F.lit("data_do_commit_ausente"))
        .otherwise(F.lit(None).cast("string"))
    )


def classificar(df, momento: datetime):
    """Tipa e marca cada linha com o motivo da rejeicao, ou NULL se aprovada."""
    return tipar(df, momento).withColumn(COLUNA_MOTIVO, _motivo())


def aprovados(classificado):
    """As linhas que cabem no contrato, no formato da tabela silver."""
    from pyspark.sql import functions as F

    return classificado.where(F.col(COLUNA_MOTIVO).isNull()).select(*COLUNAS_SILVER)


def rejeitados(classificado):
    """As linhas desviadas, no formato da quarentena."""
    from pyspark.sql import functions as F

    return classificado.where(F.col(COLUNA_MOTIVO).isNotNull()).select(
        "repo",
        "sha",
        F.col(COLUNA_MOTIVO).alias("motivo"),
        "payload",
        "_ingerido_em",
        "_arquivo_origem",
        "_processado_em",
    )


# --------------------------------------------------------------------------
# Carga incremental bronze -> silver
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class ResultadoSilver:
    """O que uma carga da silver produziu."""

    lidos: int
    aprovados: int
    rejeitados: int
    watermark: datetime | None

    @property
    def fecha(self) -> bool:
        """Nenhuma linha se perdeu nem foi contada duas vezes."""
        return self.lidos == self.aprovados + self.rejeitados


def nome_processo(endpoint: Endpoint) -> str:
    """Identificador do processo na tabela de controle."""
    return f"{endpoint.nome}@silver"


def criar_tabelas(spark) -> None:
    """Cria a tabela silver e a quarentena, se ainda nao existirem."""
    spark.sql(ddl_commits())
    spark.sql(ddl_rejeitados())


def filtrar_novos(df, checkpoint):
    """So o que entrou na bronze depois do ultimo processamento.

    Comparacao estrita, sem janela de sobreposicao: linha da bronze nao muda
    depois de gravada -- o MERGE de la nao tem ramo de UPDATE -- entao
    reprocessar a fronteira nao traria nada de novo.
    """
    from pyspark.sql import functions as F

    if checkpoint is None or checkpoint.watermark is None:
        return df
    return df.where(F.col("_ingerido_em") > F.lit(checkpoint.watermark))


def maior_ingestao(df) -> datetime | None:
    """Maior `_ingerido_em` do lote; vira o proximo watermark."""
    from pyspark.sql import functions as F

    return df.select(F.max("_ingerido_em").alias("m")).collect()[0]["m"]


def _gravar_aprovados(spark, df) -> None:
    """Upsert por (repo, sha).

    Aqui existe `WHEN MATCHED THEN UPDATE`, e a bronze nao tem. A diferenca e
    de natureza: linha de bronze e copia da origem, e corrigi-la destruiria a
    evidencia. Linha de silver e derivada -- se a regra de normalizacao
    melhorar, reprocessar deve substituir o valor antigo pelo novo.
    """
    df.createOrReplaceTempView("_silver_aprovados")
    spark.sql(
        f"""
        MERGE INTO {TABELA_COMMITS} AS alvo
        USING _silver_aprovados AS fonte
           ON alvo.repo = fonte.repo AND alvo.sha = fonte.sha
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _gravar_rejeitados(spark, df) -> None:
    """Append: rejeicao e evento de uma execucao, nao entidade.

    Nao ha MERGE possivel -- `sha` pode ser nulo, que e justamente um dos
    motivos de rejeicao. E manter a tentativa anterior mostra desde quando o
    registro vem falhando.
    """
    df.write.mode("append").saveAsTable(TABELA_REJEITADOS)


def carregar(spark, endpoint: Endpoint, momento: datetime) -> ResultadoSilver:
    """Le o que e novo na bronze, tipa, roteia e grava nas duas tabelas."""
    origem = bronze.nome_tabela(endpoint)
    processo = nome_processo(endpoint)

    anterior = controle.ler(spark, origem, processo)
    novos = filtrar_novos(spark.table(origem), anterior)

    # Classificado alimenta duas saidas; sem cache, todo o `from_json` e os
    # casts rodariam duas vezes.
    classificado = classificar(parsear(novos), momento).cache()

    try:
        ok = aprovados(classificado)
        ruins = rejeitados(classificado)

        total = classificado.count()
        n_ok = ok.count()
        n_ruins = ruins.count()

        if n_ok:
            _gravar_aprovados(spark, ok)
        if n_ruins:
            _gravar_rejeitados(spark, ruins)

        watermark = maior_ingestao(classificado) if total else (
            anterior.watermark if anterior else None
        )
    finally:
        classificado.unpersist()

    controle.salvar(
        spark,
        controle.Checkpoint(
            repo=origem,
            endpoint=processo,
            watermark=watermark,
            ultima_execucao=momento,
            status="ok",
            registros=total,
        ),
    )

    return ResultadoSilver(
        lidos=total, aprovados=n_ok, rejeitados=n_ruins, watermark=watermark
    )
