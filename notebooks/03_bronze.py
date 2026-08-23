# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronze
# MAGIC
# MAGIC Le o JSONL cru da landing zone e carrega na tabela Delta bronze.
# MAGIC
# MAGIC Regra da camada: nao se limpa nada. O payload entra como STRING,
# MAGIC exatamente como a API devolveu. Tipagem e contrato sao da silver.
# MAGIC
# MAGIC Idempotente: a carga e um MERGE por chave natural, entao reexecutar
# MAGIC sobre os mesmos arquivos insere zero linha.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import bronze, ingestao
from radar.config import BRONZE, CATALOG, VOLUME

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

dbutils.widgets.text("endpoint", "commits", "Endpoint a carregar")

ENDPOINT = ingestao.ENDPOINTS[dbutils.widgets.get("endpoint").strip()]

CAMINHO_VOLUME = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"
ORIGEM = bronze.caminho_endpoint(CAMINHO_VOLUME, ENDPOINT)
TABELA = bronze.nome_tabela(ENDPOINT)
agora = datetime.now(timezone.utc)

print("origem  :", ORIGEM)
print("destino :", TABELA)
print("chave   :", ENDPOINT.chave)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-voo
# MAGIC
# MAGIC A bronze le o que a ingestao aterrissou. Sem arquivo, o `spark.read`
# MAGIC falha com "Path does not exist", mensagem que nao identifica a
# MAGIC dependencia ausente.

# COMMAND ----------

try:
    arquivos = dbutils.fs.ls(ORIGEM)
except Exception as erro:
    raise RuntimeError(
        f"landing zone vazia em {ORIGEM} "
        "(preenchida por notebooks/02_ingestao.py)"
    ) from erro

print("particoes de repositorio na origem:", len(arquivos))
for arquivo in arquivos:
    print(" -", arquivo.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela

# COMMAND ----------

bronze.criar_tabela(spark, ENDPOINT)
display(spark.sql(f"DESCRIBE TABLE {TABELA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga

# COMMAND ----------

resultado = bronze.carregar(spark, CAMINHO_VOLUME, ENDPOINT, agora)

print(f"linhas lidas no JSONL : {resultado.linhas_lidas}")
print(f"linhas novas (MERGE)  : {resultado.linhas_novas}")
print(f"total na tabela       : {resultado.linhas_na_tabela}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prova de idempotencia
# MAGIC
# MAGIC A mesma carga, de novo, sobre os mesmos arquivos. `linhas novas` tem
# MAGIC que ser 0: o MERGE so tem `WHEN NOT MATCHED`, entao chave que ja
# MAGIC existe e ignorada.

# COMMAND ----------

repetida = bronze.carregar(spark, CAMINHO_VOLUME, ENDPOINT, agora)

print(f"linhas novas na 2a execucao: {repetida.linhas_novas}")
assert repetida.linhas_novas == 0, "a carga nao e idempotente"
print("idempotencia confirmada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferencia

# COMMAND ----------

display(spark.sql(f"SELECT repo, count(*) AS linhas FROM {TABELA} GROUP BY repo ORDER BY repo"))

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT {ENDPOINT.chave}, repo, dt, _ingerido_em, _arquivo_origem
        FROM {TABELA}
        LIMIT 5
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### O payload continua inteiro
# MAGIC
# MAGIC Os campos continuam no payload e sao lidos aqui sem alterar a
# MAGIC tabela. Na silver eles viram colunas tipadas, com cast explicito.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            {ENDPOINT.chave},
            get_json_object(payload, '$.commit.author.name')  AS autor,
            get_json_object(payload, '$.commit.committer.date') AS data_commit,
            left(get_json_object(payload, '$.commit.message'), 60) AS mensagem
        FROM {TABELA}
        LIMIT 10
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### O transaction log
# MAGIC
# MAGIC Cada carga e uma versao. E o `_delta_log` que faz um monte de Parquet
# MAGIC virar tabela com transacao e historico.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABELA}"))
