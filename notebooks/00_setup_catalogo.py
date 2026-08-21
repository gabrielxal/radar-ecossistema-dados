# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup do catalogo
# MAGIC
# MAGIC Cria os schemas das tres camadas e o Volume da landing zone.
# MAGIC Roda **uma vez**. E idempotente: pode rodar de novo sem quebrar nada.

# COMMAND ----------

import os
import sys

# Torna o pacote `radar` (em src/) importavel dentro do Databricks Git folder.
# O notebook roda com o diretorio dele como cwd, entao a raiz do repo e o pai.
REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar.config import BRONZE, CATALOG, GOLD, SILVER, VOLUME

print("catalogo:", CATALOG)
print("schemas :", BRONZE, SILVER, GOLD)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas
# MAGIC
# MAGIC Um schema por camada, todos no catalogo `workspace`.
# MAGIC
# MAGIC Alternativa rejeitada: um catalogo por camada. O Free Edition nao
# MAGIC permite criar catalogos, e separar camadas em catalogos diferentes
# MAGIC dificultaria joins entre elas.

# COMMAND ----------

for schema in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"schema pronto: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume da landing zone
# MAGIC
# MAGIC Volume do Unity Catalog, nao DBFS: o DBFS esta em desuso e nao tem
# MAGIC governanca. Volume aparece no Catalog Explorer, tem permissao e
# MAGIC participa do lineage.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE}.{VOLUME}")
CAMINHO_VOLUME = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"
print("volume pronto:", CAMINHO_VOLUME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferencia

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

print("conteudo do volume:", dbutils.fs.ls(CAMINHO_VOLUME))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste do Secret Scope
# MAGIC
# MAGIC Define de onde o token vai ser lido no Databricks.
# MAGIC
# MAGIC - **Retornou uma lista** (mesmo vazia) -> Secret Scope disponivel. Caminho certo.
# MAGIC - **Deu erro de permissao** -> Free Edition bloqueia. Plano B com widget.

# COMMAND ----------

try:
    escopos = dbutils.secrets.listScopes()
    print("SECRET SCOPE DISPONIVEL. Escopos existentes:", escopos)
except Exception as erro:
    print("SECRET SCOPE INDISPONIVEL:", type(erro).__name__)
    print(erro)