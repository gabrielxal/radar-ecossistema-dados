"""Camada silver do endpoint de repositorios.

Grao: **uma linha por repositorio por dia de coleta**. Cada carga acrescenta
uma foto; o historico se constroi acumulando fotos, nao lendo o passado --
a API so devolve o estado de agora.

Sem tabela de quarentena, ao contrario da silver de commits, e a diferenca e
proposital: sao catorze linhas por dia vindas de recurso unico. Payload
invalido aqui nao e defeito de um registro entre milhares, e sim sinal de que
a API mudou -- e o payload continua inteiro na bronze, que ja e o lugar de
investiga-lo. O registro descartado e contado e reportado, nunca silencioso.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from radar import bronze, controle, silver
from radar.config import SILVER, fqn
from radar.ingestao import Endpoint

TABELA_REPOSITORIOS = fqn(SILVER, "repositorios")

# Mesma regra da silver de commits: o schema descreve o JSON como ele chega.
# Data ISO fica STRING; a conversao e explicita, coluna a coluna.
SCHEMA_REPOSITORIO = """
    id BIGINT,
    full_name STRING,
    description STRING,
    language STRING,
    default_branch STRING,
    archived BOOLEAN,
    fork BOOLEAN,
    created_at STRING,
    updated_at STRING,
    pushed_at STRING,
    stargazers_count BIGINT,
    forks_count BIGINT,
    open_issues_count BIGINT,
    subscribers_count BIGINT,
    size BIGINT,
    owner STRUCT<login: STRING, id: BIGINT, type: STRING>,
    license STRUCT<spdx_id: STRING, name: STRING>,
    topics ARRAY<STRING>
"""

COLUNAS_REPOSITORIOS = (
    "repo",
    "dt",
    "repo_id",
    "nome_completo",
    "dono",
    "dono_id",
    "dono_tipo",
    "descricao",
    "linguagem",
    "licenca",
    "branch_padrao",
    "arquivado",
    "e_fork",
    "criado_em",
    "atualizado_em",
    "push_em",
    "stars",
    "forks",
    "issues_abertas",
    "observadores",
    "tamanho_kb",
    "topicos",
    "_ingerido_em",
    "_arquivo_origem",
    "_processado_em",
)


@dataclass(frozen=True)
class ResultadoRepositorios:
    lidos: int
    aprovados: int
    descartados: int
    dias: int


def ddl() -> str:
    """DDL da silver de repositorios. Grao: um repositorio por dia."""
    return f"""
CREATE TABLE IF NOT EXISTS {TABELA_REPOSITORIOS} (
    repo            STRING    NOT NULL COMMENT 'owner/nome, vindo do caminho da landing zone',
    dt              STRING    NOT NULL COMMENT 'dia da coleta; parte do grao',
    repo_id         BIGINT    NOT NULL COMMENT 'id numerico da origem; estavel a renomeacao',
    nome_completo   STRING             COMMENT 'owner/nome segundo a API; muda em renomeacao',
    dono            STRING             COMMENT 'login do dono',
    dono_id         BIGINT             COMMENT 'id do dono',
    dono_tipo       STRING             COMMENT 'user | organization',
    descricao       STRING             COMMENT 'texto livre',
    linguagem       STRING             COMMENT 'linguagem predominante segundo a API',
    licenca         STRING             COMMENT 'identificador SPDX, normalizado',
    branch_padrao   STRING             COMMENT 'branch padrao',
    arquivado       BOOLEAN            COMMENT 'projeto arquivado',
    e_fork          BOOLEAN            COMMENT 'e fork de outro repositorio',
    criado_em       TIMESTAMP          COMMENT 'criacao do repositorio',
    atualizado_em   TIMESTAMP          COMMENT 'ultima alteracao de metadados',
    push_em         TIMESTAMP          COMMENT 'ultimo push',
    stars           INT                COMMENT 'MEDIDA: vira fato na etapa 5',
    forks           INT                COMMENT 'MEDIDA: vira fato na etapa 5',
    issues_abertas  INT                COMMENT 'MEDIDA: vira fato na etapa 5',
    observadores    INT                COMMENT 'MEDIDA: vira fato na etapa 5',
    tamanho_kb      INT                COMMENT 'MEDIDA: vira fato na etapa 5',
    topicos         ARRAY<STRING>      COMMENT 'rotulos declarados pelo projeto',
    _ingerido_em    TIMESTAMP          COMMENT 'quando entrou na bronze',
    _arquivo_origem STRING             COMMENT 'arquivo da landing zone',
    _processado_em  TIMESTAMP          COMMENT 'quando a silver processou'
)
USING DELTA
COMMENT 'Foto diaria dos metadados de cada repositorio. Uma linha por (repo, dia).'
"""


def criar_tabela(spark) -> None:
    spark.sql(ddl())


def tipar(df, momento: datetime):
    """Projeta o payload em colunas tipadas, uma decisao por coluna.

    As contagens (`stars`, `forks`, ...) sao **medidas**, nao atributos: elas
    mudam todo dia e viram `fct_repo_snapshot` na Etapa 5. Ficam aqui porque
    a foto e a mesma; o que muda e onde cada campo vai parar na gold.
    """
    from pyspark.sql import functions as F

    dados = F.from_json(F.col("payload"), SCHEMA_REPOSITORIO)
    dono = dados["owner"]

    return df.select(
        F.col("repo"),
        F.col("dt"),
        dados["id"].alias("repo_id"),
        silver._texto(dados["full_name"]).alias("nome_completo"),
        silver._texto(dono["login"]).alias("dono"),
        dono["id"].alias("dono_id"),
        silver._categoria(dono["type"]).alias("dono_tipo"),
        silver._texto(dados["description"]).alias("descricao"),
        silver._texto(dados["language"]).alias("linguagem"),
        # SPDX ja e identificador padronizado; normalizar so a caixa evita
        # `MIT` e `mit` virarem duas licencas.
        silver._categoria(dados["license"]["spdx_id"]).alias("licenca"),
        silver._texto(dados["default_branch"]).alias("branch_padrao"),
        dados["archived"].alias("arquivado"),
        dados["fork"].alias("e_fork"),
        silver._instante(dados["created_at"]).alias("criado_em"),
        silver._instante(dados["updated_at"]).alias("atualizado_em"),
        silver._instante(dados["pushed_at"]).alias("push_em"),
        dados["stargazers_count"].cast("int").alias("stars"),
        dados["forks_count"].cast("int").alias("forks"),
        dados["open_issues_count"].cast("int").alias("issues_abertas"),
        dados["subscribers_count"].cast("int").alias("observadores"),
        dados["size"].cast("int").alias("tamanho_kb"),
        dados["topics"].alias("topicos"),
        F.col("_ingerido_em"),
        F.col("_arquivo_origem"),
        F.lit(momento).cast("timestamp").alias("_processado_em"),
    )


def aprovados(tipado):
    """As fotos utilizaveis: sem `repo_id` nao ha o que identificar."""
    from pyspark.sql import functions as F

    return tipado.where(F.col("repo_id").isNotNull())


def carregar(spark, endpoint: Endpoint, momento: datetime) -> ResultadoRepositorios:
    """Le o que e novo na bronze e faz upsert por (repo, dia)."""
    from pyspark.sql import functions as F

    origem = bronze.nome_tabela(endpoint)
    processo = f"{endpoint.nome}@silver"

    anterior = controle.ler(spark, origem, processo)
    novos = silver.filtrar_novos(spark.table(origem), anterior)
    tipado = tipar(novos, momento)

    medida = tipado.select(
        F.count(F.lit(1)).alias("lidos"),
        F.count(F.col("repo_id")).alias("aprovados"),
        F.max("_ingerido_em").alias("watermark"),
    ).collect()[0]

    lidos, ok = medida["lidos"], medida["aprovados"]
    watermark = medida["watermark"] or (anterior.watermark if anterior else None)

    if ok:
        aprovados(tipado).createOrReplaceTempView("_repos_fonte")
        spark.sql(
            f"""
            MERGE INTO {TABELA_REPOSITORIOS} AS alvo
            USING (SELECT {", ".join(COLUNAS_REPOSITORIOS)} FROM _repos_fonte) AS fonte
               ON alvo.repo = fonte.repo AND alvo.dt = fonte.dt
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

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

    dias = spark.table(TABELA_REPOSITORIOS).select("dt").distinct().count()
    return ResultadoRepositorios(
        lidos=lidos, aprovados=ok, descartados=lidos - ok, dias=dias
    )
