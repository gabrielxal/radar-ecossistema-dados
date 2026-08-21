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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secret Scope
# MAGIC
# MAGIC O token do GitHub vai para um Secret Scope, nao para o codigo nem
# MAGIC para uma variavel de ambiente do cluster. O Databricks **redige**
# MAGIC automaticamente o valor em qualquer saida de celula -- por isso
# MAGIC abaixo so imprimimos o tamanho, nunca o conteudo.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

ESCOPO = "radar"
CHAVE = "github_token"

w = WorkspaceClient()
existentes = [e.name for e in w.secrets.list_scopes()]

if ESCOPO not in existentes:
    w.secrets.create_scope(scope=ESCOPO)
    print("escopo criado:", ESCOPO)
else:
    print("escopo ja existe:", ESCOPO)

# COMMAND ----------

# MAGIC %md
# MAGIC Rode a celula abaixo, cole o token no campo que aparece no topo do
# MAGIC notebook, rode a celula seguinte **uma vez**, e depois **limpe o campo**.
# MAGIC
# MAGIC O valor digitado no widget nao fica no arquivo `.py` versionado no Git.

# COMMAND ----------

dbutils.widgets.text("github_token", "", "Token do GitHub (limpar apos gravar)")

# COMMAND ----------

_token = dbutils.widgets.get("github_token").strip()

if _token:
    w.secrets.put_secret(scope=ESCOPO, key=CHAVE, string_value=_token)
    print("segredo gravado. LIMPE O CAMPO DO WIDGET agora.")
else:
    print("campo vazio -- nada gravado (esperado se o segredo ja existe).")

del _token

# COMMAND ----------

# Verificacao: le o segredo e mostra apenas o tamanho.
_t = dbutils.secrets.get(scope=ESCOPO, key=CHAVE)
print("segredo acessivel | tamanho:", len(_t), "caracteres")
del _t

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela de controle
# MAGIC
# MAGIC A memoria do pipeline. Grao: um par (repositorio, endpoint).
# MAGIC Guarda o watermark e o ETag da ultima execucao bem-sucedida.

# COMMAND ----------

from radar import controle

controle.criar_tabela(spark)
print("tabela pronta:", controle.TABELA_CONTROLE)

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {controle.TABELA_CONTROLE}"))

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

# Tabela vazia na primeira execucao -- esperado.
display(controle.listar(spark))
