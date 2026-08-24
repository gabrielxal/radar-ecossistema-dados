"""Tabela de controle da ingestao: watermark e ETag por (repo, endpoint).

O objeto `spark` e recebido como parametro e os imports de pyspark ficam
dentro das funcoes, para o modulo continuar importavel fora do Databricks.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from radar.config import BRONZE, fqn

TABELA_CONTROLE = fqn(BRONZE, "controle_ingestao")

# Dias reprocessados de proposito para capturar dado com data retroativa.
DIAS_SOBREPOSICAO = 1


@dataclass(frozen=True)
class Checkpoint:
    """Uma linha da tabela de controle. Grao: um par (repo, endpoint)."""

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
# Funcoes puras
# --------------------------------------------------------------------------

def calcular_watermark(
    maior_data: datetime | None,
    dias_sobreposicao: int = DIAS_SOBREPOSICAO,
) -> datetime | None:
    """Watermark a gravar: a maior data ingerida menos a janela de sobreposicao."""
    if maior_data is None:
        return None
    return maior_data - timedelta(days=dias_sobreposicao)


def para_iso(momento: datetime | None) -> str | None:
    """Formata para o parametro `since` da API: ISO 8601 em UTC."""
    if momento is None:
        return None
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=timezone.utc)  # nunca assumir fuso local
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parametros_de_janela(
    desde: datetime | None,
    ate: datetime | None,
    per_page: int,
    extras: dict | None = None,
) -> dict:
    """Parametros de uma chamada de coleta, com limite inferior e superior.

    Os parametros do endpoint entram primeiro e os do mecanismo depois, e a
    ordem e deliberada: `per_page`, `since` e `until` sustentam a paginacao e o
    watermark. Um endpoint que declarasse `since` nos seus parametros fixos
    quebraria a incrementalidade em silencio, e aqui ele nao consegue.
    """
    params: dict = dict(extras or {})
    params["per_page"] = per_page

    inicio = para_iso(desde)
    if inicio:
        params["since"] = inicio

    fim = para_iso(ate)
    if fim:
        params["until"] = fim

    return params


def parametros_de_busca(
    checkpoint: Checkpoint | None,
    per_page: int,
    ate: datetime | None = None,
    extras: dict | None = None,
) -> dict:
    """Parametros da coleta. Sem checkpoint nao ha `since`: a carga e completa."""
    return parametros_de_janela(
        checkpoint.watermark if checkpoint is not None else None,
        ate,
        per_page,
        extras,
    )


# --------------------------------------------------------------------------
# Acesso a tabela
# --------------------------------------------------------------------------

def _schema():
    """Schema explicito, para o Spark nao inferir os tipos."""
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
    """Cria a tabela de controle se nao existir."""
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
    """Grava o checkpoint via MERGE pela chave (repo, endpoint)."""
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
    """DataFrame com todos os checkpoints."""
    from pyspark.sql import functions as F

    return spark.table(TABELA_CONTROLE).orderBy(F.col("repo"), F.col("endpoint"))
