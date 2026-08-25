"""Pecas comuns a todas as silvers.

Nasceram em `silver.py` e passaram a ser importadas de la por
`silver_repositorios.py`, momento em que o nome do modulo de origem passou a
mentir sobre o alcance delas.

Fica aqui o que vale para qualquer endpoint. Schema e classificacao para
quarentena dependem do payload e ficam no modulo do endpoint.
"""

from __future__ import annotations

from radar.ingestao import Endpoint

# Coluna que recebe o payload ja estruturado.
COLUNA_DADOS = "dados"


def parsear(df, schema: str, coluna: str = "payload", destino: str = COLUNA_DADOS):
    """Aplica o schema do endpoint ao payload, preservando as demais colunas.

    Leitura permissiva de proposito: o que nao couber no contrato vira NULL e
    e desviado depois, no passo de quarentena, em vez de interromper a carga.
    """
    from pyspark.sql import functions as F

    return df.withColumn(destino, F.from_json(F.col(coluna), schema))


# --------------------------------------------------------------------------
# Tipagem e normalizacao
# --------------------------------------------------------------------------

def texto(coluna):
    """Apara espacos e transforma string vazia em NULL.

    As duas dizem a mesma ausencia, e um `WHERE campo IS NULL` deixaria as
    vazias de fora sem avisar.
    """
    from pyspark.sql import functions as F

    return F.nullif(F.trim(coluna), F.lit(""))


def categoria(coluna):
    """Minusculas, para coluna de dominio fechado.

    `User` e `user` viram duas categorias da mesma coisa, e nenhum GROUP BY
    corrige depois.
    """
    from pyspark.sql import functions as F

    return F.lower(texto(coluna))


def instante(coluna):
    """Data ISO da API em TIMESTAMP.

    `try_to_timestamp` e nao `to_timestamp`: com ANSI ligado, que e o padrao em
    runtime recente do Databricks, o cast comum lanca excecao e derruba a
    carga inteira por causa de um registro. A versao `try_` devolve NULL, e o
    NULL e o que a quarentena procura.
    """
    from pyspark.sql import functions as F

    return F.try_to_timestamp(coluna)


# --------------------------------------------------------------------------
# Leitura incremental da bronze
# --------------------------------------------------------------------------

def nome_processo(endpoint: Endpoint) -> str:
    """Identificador do processo na tabela de controle."""
    return f"{endpoint.nome}@silver"


def filtrar_novos(df, checkpoint):
    """So o que entrou na bronze depois do ultimo processamento.

    Comparacao estrita, sem janela de sobreposicao: linha da bronze nao muda
    depois de gravada, ja que o MERGE de la nao tem ramo de UPDATE, entao
    reprocessar a fronteira nao traria nada de novo.
    """
    from pyspark.sql import functions as F

    if checkpoint is None or checkpoint.watermark is None:
        return df
    return df.where(F.col("_ingerido_em") > F.lit(checkpoint.watermark))
