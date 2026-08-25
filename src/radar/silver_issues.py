"""Camada silver do endpoint de issues.

Grao: uma linha por (repo, numero), com o estado mais recente conhecido.

A bronze de issues e um log de versoes, com uma linha por issue por dia de
coleta, porque issue muda depois de criada. A silver colapsa esse log no
estado corrente. E o que faz a camada gold virar projecao pura: como ela
reconstroi tudo por `overwrite`, o estado atual de cada issue sai de graca e o
fato de snapshot acumulado nao precisa de mecanismo de escrita proprio.

Tres destinos, e nao dois. O endpoint `/issues` da API devolve pull requests
misturados as issues, porque um PR e uma issue com um campo a mais. Eles nao
sao descartados: vao para tabela propria. Tempo ate merge e volume de PR
externo dizem tanto sobre a saude de um projeto quanto issue, e o dado ja
chega no mesmo payload, sem requisicao extra.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from radar import bronze, controle, silver_comum
from radar.config import SILVER, fqn
from radar.ingestao import Endpoint

TABELA_ISSUES = fqn(SILVER, "issues")
TABELA_PULL_REQUESTS = fqn(SILVER, "pull_requests")
TABELA_REJEITADOS = fqn(SILVER, "issues_rejeitados")

# Lote classificado, materializado antes de ser roteado para os tres destinos.
TABELA_LOTE = fqn(SILVER, "lote_issues")

COLUNA_MOTIVO = "_motivo"
COLUNA_PR = "e_pull_request"

# Schema parcial, como em commits: campo que nao esta aqui e ignorado pelo
# `from_json` e continua no payload da bronze.
#
# `pull_request` entra so para ser testado quanto a presenca. Quando o
# registro e uma issue de verdade, a chave nao existe no JSON e o struct vem
# NULL; quando e um PR, vem preenchido. E o unico campo que distingue os dois.
SCHEMA_ISSUE = """
    id BIGINT,
    number BIGINT,
    title STRING,
    state STRING,
    state_reason STRING,
    comments BIGINT,
    created_at STRING,
    updated_at STRING,
    closed_at STRING,
    author_association STRING,
    user STRUCT<login: STRING, id: BIGINT, type: STRING>,
    labels ARRAY<STRUCT<name: STRING>>,
    assignees ARRAY<STRUCT<login: STRING>>,
    pull_request STRUCT<url: STRING>
"""

# Dominios conhecidos, ja normalizados. Valor fora da lista nao e descartado
# nem corrigido: a bateria de qualidade reporta.
ESTADOS = ("open", "closed")

# `state_reason` so existe em issue fechada, e nem sempre.
MOTIVOS_DE_ESTADO = ("completed", "not_planned", "reopened", "duplicate")

ASSOCIACOES = (
    "owner",
    "member",
    "collaborator",
    "contributor",
    "first_time_contributor",
    "first_timer",
    "mannequin",
    "none",
)

COLUNAS_ISSUES = (
    "id",
    "repo",
    "numero",
    "titulo",
    "estado",
    "motivo_estado",
    "comentarios",
    "rotulos",
    "qtd_rotulos",
    "qtd_responsaveis",
    "autor_login",
    "autor_id",
    "autor_tipo",
    "associacao_autor",
    "aberta_em",
    "atualizada_em",
    "fechada_em",
    "_ingerido_em",
    "_arquivo_origem",
    "_processado_em",
)

COLUNAS_REJEITADOS = (
    "repo",
    "id",
    "motivo",
    "payload",
    "_ingerido_em",
    "_arquivo_origem",
    "_processado_em",
)

# Avaliados em ordem: o primeiro que casar nomeia a rejeicao.
MOTIVOS_DE_REJEICAO = (
    "payload_ilegivel",
    "chave_ausente",
    "repo_ausente",
    "numero_ausente",
    "data_de_abertura_ausente",
)


def parsear(df, coluna: str = "payload"):
    """O parser generico, fixado no schema de issue."""
    return silver_comum.parsear(df, SCHEMA_ISSUE, coluna)


# --------------------------------------------------------------------------
# Tipagem
# --------------------------------------------------------------------------

def _tamanho(coluna):
    """Tamanho do array, com zero para ausente. Nunca -1."""
    from pyspark.sql import functions as F

    return F.when(coluna.isNull(), F.lit(0)).otherwise(F.size(coluna)).cast("int")


def tipar(df, momento: datetime):
    """Payload estruturado em colunas tipadas, com a proveniencia preservada."""
    from pyspark.sql import functions as F

    dados = F.col(silver_comum.COLUNA_DADOS)
    usuario = dados["user"]

    return df.select(
        dados["id"].cast("bigint").alias("id"),
        silver_comum.texto(F.col("repo")).alias("repo"),
        dados["number"].cast("int").alias("numero"),
        silver_comum.texto(dados["title"]).alias("titulo"),
        silver_comum.categoria(dados["state"]).alias("estado"),
        silver_comum.categoria(dados["state_reason"]).alias("motivo_estado"),
        dados["comments"].cast("int").alias("comentarios"),
        # `transform` sobre array de struct devolve array de string. O nome do
        # rotulo e o que classifica a issue; a cor e o id nao respondem nada.
        F.transform(dados["labels"], lambda r: r["name"]).alias("rotulos"),
        # `coalesce` sobre `size` nao protege: em modo legado `size(NULL)` da
        # -1, e nao NULL. Diario de bordo 10, repetido aqui e pego pelo teste.
        # Zero e nao NULL porque a medida e aditiva.
        _tamanho(dados["labels"]).alias("qtd_rotulos"),
        _tamanho(dados["assignees"]).alias("qtd_responsaveis"),
        silver_comum.texto(usuario["login"]).alias("autor_login"),
        usuario["id"].cast("bigint").alias("autor_id"),
        silver_comum.categoria(usuario["type"]).alias("autor_tipo"),
        silver_comum.categoria(dados["author_association"]).alias("associacao_autor"),
        silver_comum.instante(dados["created_at"]).alias("aberta_em"),
        silver_comum.instante(dados["updated_at"]).alias("atualizada_em"),
        silver_comum.instante(dados["closed_at"]).alias("fechada_em"),
        # O unico campo que separa issue de pull request.
        dados["pull_request"]["url"].isNotNull().alias(COLUNA_PR),
        F.col("payload"),
        F.col("_ingerido_em"),
        F.col("_arquivo_origem"),
        F.lit(momento).cast("timestamp").alias("_processado_em"),
    )


# --------------------------------------------------------------------------
# Quarentena
# --------------------------------------------------------------------------

def _motivo():
    """Qual regra do contrato a linha violou, ou NULL se ela cabe."""
    from pyspark.sql import functions as F

    # Nenhum campo do contrato preenchido: JSON invalido ou objeto vazio.
    # `from_json` em modo permissivo devolve struct de nulos, nao NULL.
    ilegivel = (
        F.col("id").isNull()
        & F.col("numero").isNull()
        & F.col("aberta_em").isNull()
        & F.col("titulo").isNull()
        & F.col("autor_id").isNull()
    )

    return (
        F.when(ilegivel, F.lit("payload_ilegivel"))
        .when(F.col("id").isNull(), F.lit("chave_ausente"))
        .when(F.col("repo").isNull(), F.lit("repo_ausente"))
        # Sem numero nao ha grao: (repo, numero) e o que identifica a issue.
        .when(F.col("numero").isNull(), F.lit("numero_ausente"))
        # Sem data de abertura nao ha primeiro marco, e o fato de ciclo de
        # vida perde o ponto de partida.
        .when(F.col("aberta_em").isNull(), F.lit("data_de_abertura_ausente"))
        .otherwise(F.lit(None).cast("string"))
    )


def classificar(df, momento: datetime):
    """Tipa e marca cada linha com o motivo da rejeicao, ou NULL se aprovada."""
    return tipar(df, momento).withColumn(COLUNA_MOTIVO, _motivo())


def ddl_issues() -> str:
    """DDL da tabela silver de issues."""
    return _ddl_entidade(
        TABELA_ISSUES,
        "Issues tipadas, no estado mais recente. Uma linha por (repo, numero).",
    )


def ddl_pull_requests() -> str:
    """DDL da tabela de pull requests, com as mesmas colunas.

    Mesmo payload, mesma forma, entidades diferentes. Separar em duas tabelas
    e nao usar uma coluna de tipo evita que toda consulta sobre issues precise
    lembrar de filtrar, que e o tipo de filtro esquecido em silencio.
    """
    return _ddl_entidade(
        TABELA_PULL_REQUESTS,
        "Pull requests devolvidos pelo endpoint /issues. Uma linha por (repo, numero).",
    )


def _ddl_entidade(tabela: str, comentario: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {tabela} (
    id               BIGINT    NOT NULL COMMENT 'chave natural global do registro na API',
    repo             STRING    NOT NULL COMMENT 'owner/nome',
    numero           INT       NOT NULL COMMENT 'numero dentro do repositorio; com repo forma o grao',
    titulo           STRING             COMMENT 'titulo, sem alteracao interna',
    estado           STRING             COMMENT 'dominio conhecido em ESTADOS',
    motivo_estado    STRING             COMMENT 'dominio conhecido em MOTIVOS_DE_ESTADO; so em fechadas',
    comentarios      INT                COMMENT 'MEDIDA aditiva: comentarios recebidos',
    rotulos          ARRAY<STRING>      COMMENT 'nomes dos rotulos aplicados',
    qtd_rotulos      INT                COMMENT 'MEDIDA aditiva',
    qtd_responsaveis INT                COMMENT 'MEDIDA aditiva',
    autor_login      STRING             COMMENT 'muda quando a conta e renomeada',
    autor_id         BIGINT             COMMENT 'chave estavel do usuario',
    autor_tipo       STRING             COMMENT 'dominio conhecido em TIPOS_DE_AUTOR',
    associacao_autor STRING             COMMENT 'vinculo com o projeto; dominio em ASSOCIACOES',
    aberta_em        TIMESTAMP NOT NULL COMMENT 'primeiro marco do ciclo de vida',
    atualizada_em    TIMESTAMP          COMMENT 'sustenta o watermark; e por ele que a API filtra em since',
    fechada_em       TIMESTAMP          COMMENT 'marco final; NULL enquanto aberta',
    _ingerido_em     TIMESTAMP          COMMENT 'quando a linha entrou na bronze',
    _arquivo_origem  STRING             COMMENT 'arquivo da landing zone de onde veio',
    _processado_em   TIMESTAMP          COMMENT 'quando a silver processou'
)
USING DELTA
COMMENT '{comentario}'
"""


def ddl_rejeitados() -> str:
    """DDL da quarentena. O payload vem junto: sem ele nao ha investigacao."""
    return f"""
CREATE TABLE IF NOT EXISTS {TABELA_REJEITADOS} (
    repo            STRING    COMMENT 'owner/nome, quando conhecido',
    id              BIGINT    COMMENT 'chave natural, quando presente no payload',
    motivo          STRING    COMMENT 'qual regra do contrato a linha violou',
    payload         STRING    COMMENT 'a linha JSON como a API devolveu',
    _ingerido_em    TIMESTAMP COMMENT 'quando a linha entrou na bronze',
    _arquivo_origem STRING    COMMENT 'arquivo da landing zone de onde veio',
    _processado_em  TIMESTAMP COMMENT 'quando a silver processou'
)
USING DELTA
COMMENT 'Issues fora do contrato, desviadas em vez de descartadas.'
"""


def criar_tabelas(spark) -> None:
    """Cria os tres destinos, se ainda nao existirem."""
    spark.sql(ddl_issues())
    spark.sql(ddl_pull_requests())
    spark.sql(ddl_rejeitados())


# --------------------------------------------------------------------------
# Roteamento
# --------------------------------------------------------------------------

_SELECT_REJEITADOS = ", ".join(
    f"{COLUNA_MOTIVO} AS motivo" if c == "motivo" else c for c in COLUNAS_REJEITADOS
)


def sql_fonte_entidade(origem: str, pull_request: bool) -> str:
    """As linhas aprovadas de um dos dois tipos, ja colapsadas no grao.

    A deduplicacao acontece aqui, e nao depois: o lote pode trazer duas
    versoes da mesma issue, quando ela foi atualizada mais de uma vez desde a
    ultima carga. O MERGE recusa fonte com chave repetida, e mesmo que
    aceitasse a versao vencedora seria indefinida.

    `atualizada_em DESC` implementa a regra da versao mais recente; o
    `_ingerido_em` desempata quando o mesmo instante aparece duas vezes.
    """
    colunas = ", ".join(COLUNAS_ISSUES)
    negacao = "" if pull_request else "NOT "
    return f"""
        SELECT {colunas}
        FROM (
            SELECT {colunas},
                   row_number() OVER (
                       PARTITION BY repo, numero
                       ORDER BY atualizada_em DESC, _ingerido_em DESC
                   ) AS _ordem
            FROM {origem}
            WHERE {COLUNA_MOTIVO} IS NULL AND {negacao}{COLUNA_PR}
        )
        WHERE _ordem = 1
    """


def sql_fonte_rejeitados(origem: str) -> str:
    """As linhas desviadas, no formato da quarentena."""
    return (
        f"SELECT {_SELECT_REJEITADOS} "
        f"FROM {origem} WHERE {COLUNA_MOTIVO} IS NOT NULL"
    )


def sql_merge_entidade(origem: str, pull_request: bool) -> str:
    """Upsert por (repo, numero), com a versao mais recente vencendo.

    A guarda no `WHEN MATCHED` e o que implementa a decisao do grao. Sem ela,
    reprocessar um lote antigo sobrescreveria o estado atual com um estado
    anterior, e a silver andaria para tras sem nenhum erro aparecer.

    A comparacao usa `>=` e nao `>`: reprocessar a mesma versao precisa
    continuar sendo idempotente, e nao virar operacao ignorada.
    """
    tabela = TABELA_PULL_REQUESTS if pull_request else TABELA_ISSUES
    return f"""
        MERGE INTO {tabela} AS alvo
        USING ({sql_fonte_entidade(origem, pull_request)}) AS fonte
           ON alvo.repo = fonte.repo AND alvo.numero = fonte.numero
        WHEN MATCHED AND fonte.atualizada_em >= alvo.atualizada_em
            THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """


def sql_inserir_rejeitados(origem: str) -> str:
    """Insercao simples: rejeicao e evento de uma execucao, nao entidade."""
    return f"INSERT INTO {TABELA_REJEITADOS} {sql_fonte_rejeitados(origem)}"


# --------------------------------------------------------------------------
# Carga incremental bronze -> silver
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class ResultadoIssues:
    """O que uma carga da silver de issues produziu."""

    lidos: int
    issues: int
    pull_requests: int
    rejeitados: int
    watermark: datetime | None

    @property
    def fecha(self) -> bool:
        """Nenhuma linha se perdeu nem foi contada duas vezes.

        A conta e sobre versoes lidas da bronze, nao sobre entidades gravadas:
        duas versoes da mesma issue contam duas vezes aqui e viram uma linha
        na tabela final.
        """
        return self.lidos == self.issues + self.pull_requests + self.rejeitados


def _preparar_lote(spark, classificado) -> None:
    """Grava o lote classificado numa tabela, antes de rotear.

    Sem `cache()`, que o Serverless nao oferece, cada acao reprocessaria o
    `from_json` e os casts desde a bronze. E o MERGE precisa de uma origem
    estavel, que uma tabela e e um DataFrame calculado nao e.
    """
    (
        classificado.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABELA_LOTE)
    )


def carregar(spark, endpoint: Endpoint, momento: datetime) -> ResultadoIssues:
    """Le o que e novo na bronze, tipa, roteia e grava nos tres destinos."""
    from pyspark.sql import functions as F

    origem = bronze.nome_tabela(endpoint)
    processo = silver_comum.nome_processo(endpoint)

    anterior = controle.ler(spark, origem, processo)
    novos = silver_comum.filtrar_novos(spark.table(origem), anterior)
    _preparar_lote(spark, classificar(parsear(novos), momento))

    lote = spark.table(TABELA_LOTE)

    # Uma agregacao unica sobre o lote ja materializado: as quatro contagens
    # vem da mesma varredura, entao a invariante nao depende de leituras
    # separadas que poderiam ver estados diferentes.
    aprovada = F.col(COLUNA_MOTIVO).isNull()
    medida = lote.select(
        F.count(F.lit(1)).alias("lidos"),
        F.count(F.when(aprovada & ~F.col(COLUNA_PR), 1)).alias("issues"),
        F.count(F.when(aprovada & F.col(COLUNA_PR), 1)).alias("pull_requests"),
        F.count(F.when(F.col(COLUNA_MOTIVO).isNotNull(), 1)).alias("rejeitados"),
        F.max("_ingerido_em").alias("watermark"),
    ).collect()[0]

    lidos = medida["lidos"]
    watermark = medida["watermark"] or (anterior.watermark if anterior else None)

    if medida["issues"]:
        spark.sql(sql_merge_entidade(TABELA_LOTE, pull_request=False))
    if medida["pull_requests"]:
        spark.sql(sql_merge_entidade(TABELA_LOTE, pull_request=True))
    if medida["rejeitados"]:
        spark.sql(sql_inserir_rejeitados(TABELA_LOTE))

    controle.salvar(
        spark,
        controle.Checkpoint(
            repo=origem,
            endpoint=processo,
            watermark=watermark,
            ultima_execucao=momento,
            status="ok",
            registros=lidos,
        ),
    )

    return ResultadoIssues(
        lidos=lidos,
        issues=medida["issues"],
        pull_requests=medida["pull_requests"],
        rejeitados=medida["rejeitados"],
        watermark=watermark,
    )
