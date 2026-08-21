"""Memoria do pipeline: onde cada ingestao parou.

A tabela de controle guarda, por (repositorio, endpoint), o watermark e o
ETag da ultima execucao bem-sucedida. E ela que transforma um script que
roda uma vez num processo que roda todo dia sem refazer trabalho.

DECISAO: o objeto `spark` e INJETADO em toda funcao, nunca importado no
topo deste modulo. Motivos:
  - o modulo continua importavel fora do Databricks (pytest, IDE, CI);
  - as funcoes puras podem ser testadas sem cluster nenhum;
  - e o mesmo padrao de injecao de dependencia usado no GitHubClient.
Os imports de pyspark ficam DENTRO das funcoes que realmente precisam dele.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from radar.config import BRONZE, fqn

TABELA_CONTROLE = fqn(BRONZE, "controle_ingestao")

# Quantos dias reprocessar de proposito para capturar dado que chega com
# data retroativa (rebase, merge de branch antiga, fuso horario do autor).
DIAS_SOBREPOSICAO = 1


@dataclass(frozen=True)
class Checkpoint:
    """Estado de uma ingestao. Uma linha da tabela de controle.

    Grao: um par (repo, endpoint). Nao ha duas linhas para o mesmo par.
    """

    repo: str
    endpoint: str
    watermark: datetime | None = None
    etag: str | None = None
    ultima_execucao: datetime | None = None
    status: str = "ok"
    mensagem: str | None = None
    registros: int = 0


DDL_CONTROLE = f"""
CREATE TABLE IF NOT EXISTS {TABELA_CONTROLE} (
    repo            STRING    NOT NULL COMMENT 'owner/repo',
    endpoint        STRING    NOT NULL COMMENT 'commits, issues, repo, ...',
    watermark       TIMESTAMP          COMMENT 'maior data ingerida, com sobreposicao aplicada',
    etag            STRING             COMMENT 'ETag da sentinela',
    ultima_execucao TIMESTAMP          COMMENT 'quando a ingestao rodou',
    status          STRING             COMMENT 'ok | erro',
    mensagem        STRING             COMMENT 'detalhe do erro, se houver',
    registros       BIGINT             COMMENT 'quantos registros a ultima carga trouxe'
)
USING DELTA
COMMENT 'Memoria do pipeline: watermark e ETag por repositorio e endpoint.'
"""


# --------------------------------------------------------------------------
# Funcoes puras -- testaveis sem Spark
# --------------------------------------------------------------------------

def calcular_watermark(
    maior_data: datetime | None,
    dias_sobreposicao: int = DIAS_SOBREPOSICAO,
) -> datetime | None:
    """Watermark a gravar: a maior data ingerida MENOS a janela de sobreposicao.

    Recuar de proposito e o que captura dado que chega com data retroativa.
    Se avancassemos ate o maximo exato, um commit datado de ontem que
    aparecesse amanha nunca seria pego pelo filtro `since` -- perda
    silenciosa, sem erro nenhum.

    So e seguro porque a carga e idempotente (MERGE pela chave natural).
    Sobreposicao e idempotencia sao um par: uma sem a outra nao funciona.
    """
    if maior_data is None:
        return None
    return maior_data - timedelta(days=dias_sobreposicao)


def para_iso(momento: datetime | None) -> str | None:
    """Formata para o parametro `since` da API: ISO 8601 em UTC.

    A API do GitHub espera `2026-08-19T03:00:00Z`. Datas sem fuso sao
    tratadas como UTC -- nunca como o fuso local da maquina, que mudaria
    o resultado dependendo de onde o job roda.
    """
    if momento is None:
        return None
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=timezone.utc)
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parametros_de_busca(checkpoint: Checkpoint | None, per_page: int) -> dict:
    """Monta os parametros da chamada de coleta a partir do checkpoint.

    Sem checkpoint (primeira execucao), nao ha `since`: a carga e completa.
    """
    params: dict = {"per_page": per_page}
    if checkpoint is not None:
        desde = para_iso(checkpoint.watermark)
        if desde:
            params["since"] = desde
    return params


# --------------------------------------------------------------------------
# Acesso a tabela -- exige Spark, injetado
# --------------------------------------------------------------------------

def _schema():
    """Schema explicito: nao deixamos o Spark inferir tipo de checkpoint."""
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("repo", StringType(), False),
            StructField("endpoint", StringType(), False),
            StructField("watermark", TimestampType(), True),
            StructField("etag", StringType(), True),
            StructField("ultima_execucao", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("mensagem", StringType(), True),
            StructField("registros", LongType(), True),
        ]
    )


def criar_tabela(spark) -> None:
    """Cria a tabela de controle se ela ainda nao existir. Idempotente."""
    spark.sql(DDL_CONTROLE)


def ler(spark, repo: str, endpoint: str) -> Checkpoint | None:
    """Le o checkpoint de um par (repo, endpoint). None na primeira execucao."""
    from pyspark.sql import functions as F

    linhas = (
        spark.table(TABELA_CONTROLE)
        .where((F.col("repo") == repo) & (F.col("endpoint") == endpoint))
        .collect()
    )
    if not linhas:
        return None

    linha = linhas[0]
    return Checkpoint(
        repo=linha["repo"],
        endpoint=linha["endpoint"],
        watermark=linha["watermark"],
        etag=linha["etag"],
        ultima_execucao=linha["ultima_execucao"],
        status=linha["status"],
        mensagem=linha["mensagem"],
        registros=linha["registros"] or 0,
    )


def salvar(spark, checkpoint: Checkpoint) -> None:
    """Grava o checkpoint via MERGE pela chave (repo, endpoint).

    MERGE e nao INSERT: rodar a ingestao duas vezes no mesmo dia atualiza a
    linha existente em vez de criar uma segunda. E a mesma idempotencia que
    exigimos dos dados, aplicada ao proprio controle.
    """
    df = spark.createDataFrame(
        [
            (
                checkpoint.repo,
                checkpoint.endpoint,
                checkpoint.watermark,
                checkpoint.etag,
                checkpoint.ultima_execucao,
                checkpoint.status,
                checkpoint.mensagem,
                int(checkpoint.registros),
            )
        ],
        schema=_schema(),
    )
    df.createOrReplaceTempView("_novo_checkpoint")

    spark.sql(
        f"""
        MERGE INTO {TABELA_CONTROLE} AS alvo
        USING _novo_checkpoint AS novo
           ON alvo.repo = novo.repo AND alvo.endpoint = novo.endpoint
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def listar(spark):
    """DataFrame com todos os checkpoints, para inspecao no notebook."""
    from pyspark.sql import functions as F

    return spark.table(TABELA_CONTROLE).orderBy(F.col("repo"), F.col("endpoint"))
