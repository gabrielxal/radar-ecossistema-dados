# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup do catalogo
# MAGIC
# MAGIC Cria a **infraestrutura de dados** do projeto: schemas das tres camadas,
# MAGIC Volume da landing zone e tabela de controle.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Quando rodar** | Ao criar o projeto, e sempre que a estrutura mudar |
# MAGIC | **Idempotente** | Sim -- `CREATE ... IF NOT EXISTS` em tudo |
# MAGIC | **Precisa de humano** | Nao. Pode ser automatizado num job |
# MAGIC | **Toca segredo** | Nao. Credenciais ficam em `01_setup_credenciais` |
# MAGIC
# MAGIC A separacao e proposital: este notebook e automatizavel porque nao
# MAGIC depende de ninguem digitar nada.

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
# MAGIC Um schema por camada, todos no catalogo `workspace`, com o prefixo do
# MAGIC projeto (`radar_`).
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
# MAGIC governanca. Volume aparece no Catalog Explorer, tem controle de
# MAGIC permissao e participa do lineage.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE}.{VOLUME}")
CAMINHO_VOLUME = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"
print("volume pronto:", CAMINHO_VOLUME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela de controle
# MAGIC
# MAGIC A memoria do pipeline. Grao: um par (repositorio, endpoint).
# MAGIC Guarda o watermark e o ETag da ultima execucao bem-sucedida --
# MAGIC e o que evita recomecar do zero a cada madrugada.

# COMMAND ----------

from radar import controle

controle.criar_tabela(spark)
print("tabela pronta:", controle.TABELA_CONTROLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chave primaria informativa
# MAGIC
# MAGIC O Unity Catalog aceita declarar chave primaria, mas **nao a impoe**:
# MAGIC a constraint documenta a intencao e ajuda ferramentas de BI e de
# MAGIC lineage. Quem realmente garante uma linha por par (repo, endpoint)
# MAGIC e o `MERGE` em `controle.salvar()`.

# COMMAND ----------

try:
    spark.sql(
        f"ALTER TABLE {controle.TABELA_CONTROLE} "
        "ADD CONSTRAINT pk_controle PRIMARY KEY (repo, endpoint)"
    )
    print("constraint de chave primaria registrada")
except Exception as erro:
    print("constraint nao aplicada:", type(erro).__name__, "-- o pipeline segue sem ela")

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

# Tabela vazia na primeira execucao -- esperado.
display(controle.listar(spark))
