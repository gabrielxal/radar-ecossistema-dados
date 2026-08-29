# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup de credenciais
# MAGIC
# MAGIC Grava o token do GitHub num Secret Scope.
# MAGIC
# MAGIC Escrita unica por workspace, repetida apenas em rotacao de token.
# MAGIC Exige entrada humana, por isso nao vai para job agendado.
# MAGIC
# MAGIC Pre-requisito: token fine-grained, read-only sobre repositorios publicos.

# COMMAND ----------

ESCOPO = "radar"
CHAVE = "github_token"

print(f"destino: escopo '{ESCOPO}', chave '{CHAVE}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar o escopo

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
existentes = [e.name for e in w.secrets.list_scopes()]

if ESCOPO not in existentes:
    w.secrets.create_scope(scope=ESCOPO)
    print("escopo criado:", ESCOPO)
else:
    print("escopo ja existe:", ESCOPO)

print("escopos no workspace:", [e.name for e in w.secrets.list_scopes()])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Campo de entrada
# MAGIC
# MAGIC O widget aparece no topo do notebook. O valor digitado nele fica
# MAGIC na sessao e nao entra no arquivo versionado no Git.

# COMMAND ----------

dbutils.widgets.text("github_token", "", "Token do GitHub (limpar apos gravar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gravar
# MAGIC
# MAGIC Le o widget e grava o valor no Secret Scope. Campo vazio nao
# MAGIC grava nada.

# COMMAND ----------

_token = dbutils.widgets.get("github_token").strip()

if _token:
    w.secrets.put_secret(scope=ESCOPO, key=CHAVE, string_value=_token)
    print("segredo gravado. o valor segue visivel no widget ate ser apagado.")
else:
    print("campo vazio: nada gravado.")

del _token

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar

# COMMAND ----------

# Imprime so o tamanho: nunca o valor.
_t = dbutils.secrets.get(scope=ESCOPO, key=CHAVE)
print("segredo acessivel | tamanho:", len(_t), "caracteres")
del _t

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Teste de ponta a ponta
# MAGIC
# MAGIC O limite por hora identifica o estado da autenticacao:
# MAGIC 5000 com token aceito, 60 sem autenticacao.

# COMMAND ----------

import os
import sys

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar.github_client import GitHubClient

cliente = GitHubClient(dbutils.secrets.get(scope=ESCOPO, key=CHAVE))
resposta = cliente.get("/rate_limit")

limite = resposta.dados["resources"]["core"]["limit"]
print("limite de requisicoes por hora:", limite)
print("AUTENTICADO" if limite > 1000 else "NAO AUTENTICADO: verifique o token")
