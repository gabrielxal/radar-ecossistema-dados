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

from radar.config import SILVER, fqn

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
