# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup do catalogo
# MAGIC
# MAGIC Cria schemas, Volume da landing zone e tabela de controle.
# MAGIC Idempotente: pode ser reexecutado. Nao exige interacao humana.
# MAGIC
# MAGIC Credenciais ficam em `01_setup_credenciais`.

# COMMAND ----------

import os
import sys

# Torna o pacote `radar` (em src/) importavel dentro do Git folder.
REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar.config import BRONZE, CATALOG, GOLD, SILVER, VOLUME

print("catalogo:", CATALOG)
print("schemas :", BRONZE, SILVER, GOLD)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas

# COMMAND ----------

for schema in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"schema pronto: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume da landing zone

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE}.{VOLUME}")
CAMINHO_VOLUME = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"
print("volume pronto:", CAMINHO_VOLUME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela de controle

# COMMAND ----------

from radar import controle

controle.criar_tabela(spark)
print("tabela pronta:", controle.TABELA_CONTROLE)

# COMMAND ----------

# Constraint informativa: o Unity Catalog registra a chave primaria mas nao
# a impoe. A unicidade e garantida pelo MERGE em controle.salvar().
try:
    spark.sql(
        f"ALTER TABLE {controle.TABELA_CONTROLE} "
        "ADD CONSTRAINT pk_controle PRIMARY KEY (repo, endpoint)"
    )
    print("constraint de chave primaria registrada")
except Exception as erro:
    print("constraint nao aplicada:", type(erro).__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferencia

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

print("conteudo do volume:", dbutils.fs.ls(CAMINHO_VOLUME))

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {controle.TABELA_CONTROLE}"))

# COMMAND ----------

# Vazia na primeira execucao.
display(controle.listar(spark))
