"""Camada bronze: o JSONL cru da landing zone vira tabela Delta.

Bronze nao interpreta o dado: o payload entra como STRING, identico a linha
que a API devolveu. A chave natural e a unica projetada para fora, porque o
MERGE que garante a idempotencia da carga precisa dela.

O objeto `spark` e recebido como parametro e os imports de pyspark ficam
dentro das funcoes, para o modulo continuar importavel fora do Databricks.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from radar.config import BRONZE, fqn
from radar.ingestao import Endpoint

# Ancorado, e sem `_` na primeira parte: nome de usuario e de organizacao no
# GitHub aceitam apenas letras, numeros e hifen, entao o primeiro `__` do
# diretorio e sempre o separador owner/nome. Preserva o underscore do nome
# do repositorio, como em `great-expectations__great_expectations`.
REGEX_REPO = r"^([^_]+)__"
SUBST_REPO_SQL = r"$1/"


@dataclass(frozen=True)
class ResultadoBronze:
    """O que uma carga da bronze produziu."""

    endpoint: str
    linhas_lidas: int      # linhas no JSONL, antes da deduplicacao
    linhas_novas: int      # o que o MERGE de fato inseriu
    linhas_na_tabela: int


# --------------------------------------------------------------------------
# Funcoes puras
# --------------------------------------------------------------------------

def nome_tabela(endpoint: Endpoint) -> str:
    """Uma tabela bronze por endpoint: workspace.radar_bronze.commits."""
    return fqn(BRONZE, endpoint.nome)


def caminho_endpoint(base_volume: str, endpoint: Endpoint) -> str:
    """Raiz da leitura. As particoes repo=/dt= ficam abaixo dela."""
    return f"{base_volume}/{endpoint.nome}"


def dessanitizar_repo(diretorio: str) -> str:
    """`owner__nome` -> `owner/nome`. Inverso de ingestao.sanitizar_repo.

    Nao viola a regra da camada: o valor restaurado e o que a origem usa; a
    codificacao com `__` foi nossa, imposta pelo nome de diretorio.
    """
    return re.sub(REGEX_REPO, r"\1/", diretorio, count=1)


def ddl(endpoint: Endpoint) -> str:
    """DDL da tabela bronze do endpoint. Todo campo de dado e STRING.

    Sem PARTITIONED BY: o volume atual e de poucos MB, e particionar por
    repositorio geraria arquivos pequenos demais, cujo custo de leitura
    supera o que o filtro economiza. Em escala maior a alternativa e
    CLUSTER BY, nao particao fisica.
    """
    return f"""
CREATE TABLE IF NOT EXISTS {nome_tabela(endpoint)} (
    {endpoint.chave:<15} STRING NOT NULL COMMENT 'chave natural do registro na origem',
    repo            STRING NOT NULL COMMENT 'owner/nome, restaurado do caminho',
    dt              STRING          COMMENT 'data da carga, como veio da particao',
    payload         STRING NOT NULL COMMENT 'a linha JSON exatamente como a API devolveu',
    _ingerido_em    TIMESTAMP       COMMENT 'quando esta linha entrou na bronze',
    _arquivo_origem STRING          COMMENT 'arquivo da landing zone de onde veio',
    _endpoint       STRING          COMMENT 'chamada de API que a produziu'
)
USING DELTA
COMMENT 'Copia fiel do endpoint {endpoint.nome} da API do GitHub, com proveniencia.'
"""


# --------------------------------------------------------------------------
# Acesso ao Delta
# --------------------------------------------------------------------------

def criar_tabela(spark, endpoint: Endpoint) -> None:
    """Cria a tabela bronze do endpoint se nao existir."""
    spark.sql(ddl(endpoint))


def ler_landing(spark, base_volume: str, endpoint: Endpoint, momento: datetime):
    """Le o JSONL cru e devolve o DataFrame no formato da tabela bronze.

    `.text()` e nao `.json()`: o Spark nao infere schema nem tipo, cada linha
    chega como string. Mudanca de formato na origem nao quebra esta camada;
    o contrato de tipos pertence a silver.
    """
    from pyspark.sql import functions as F

    bruto = (
        spark.read
        .option("pathGlobFilter", "*.jsonl")
        .text(caminho_endpoint(base_volume, endpoint))
    )

    return bruto.select(
        F.get_json_object(F.col("value"), f"$.{endpoint.chave}").alias(endpoint.chave),
        # O Spark infere o tipo do valor lido do caminho: `dt=2026-08-21`
        # viraria DATE. Na bronze, particao tambem e STRING. O cast resolve
        # na projecao; desligar a inferencia exigiria uma spark.conf que o
        # Serverless nao permite alterar.
        F.regexp_replace(
            F.col("repo").cast("string"), REGEX_REPO, SUBST_REPO_SQL
        ).alias("repo"),
        F.col("dt").cast("string").alias("dt"),
        F.col("value").alias("payload"),
        F.lit(momento).cast("timestamp").alias("_ingerido_em"),
        # Coluna oculta exposta por todo file source do Spark. Evita gravar
        # o caminho de origem dentro do proprio payload.
        F.col("_metadata.file_path").alias("_arquivo_origem"),
        F.lit(endpoint.nome).alias("_endpoint"),
    )


def deduplicar(df, endpoint: Endpoint):
    """Uma linha por (repo, chave), mantendo a ocorrencia mais antiga.

    A duplicata e esperada: DIAS_SOBREPOSICAO faz cada carga reler um dia ja
    lido. Como o MERGE recusa fonte com chave repetida ("multiple source
    rows matched"), a deduplicacao precede a gravacao.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    janela = (
        Window.partitionBy("repo", endpoint.chave)
        .orderBy(F.col("_arquivo_origem").asc())  # ordem determinista
    )
    return (
        df.withColumn("_ordem", F.row_number().over(janela))
        .where(F.col("_ordem") == 1)
        .drop("_ordem")
    )


def carregar(
    spark, base_volume: str, endpoint: Endpoint, momento: datetime
) -> ResultadoBronze:
    """Carrega a landing zone na bronze. Reexecutar nao duplica linha."""
    tabela = nome_tabela(endpoint)

    lidas = ler_landing(spark, base_volume, endpoint, momento)
    total_lido = lidas.count()

    deduplicar(lidas, endpoint).createOrReplaceTempView("_bronze_fonte")

    antes = spark.table(tabela).count()
    spark.sql(
        f"""
        MERGE INTO {tabela} AS alvo
        USING _bronze_fonte AS fonte
           ON alvo.repo = fonte.repo
          AND alvo.{endpoint.chave} = fonte.{endpoint.chave}
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    depois = spark.table(tabela).count()

    return ResultadoBronze(
        endpoint=endpoint.nome,
        linhas_lidas=total_lido,
        linhas_novas=depois - antes,
        linhas_na_tabela=depois,
    )
