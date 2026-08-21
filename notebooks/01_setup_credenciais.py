# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup de credenciais
# MAGIC
# MAGIC Grava o token do GitHub num Secret Scope.
# MAGIC
# MAGIC Rodar uma vez por workspace e a cada rotacao do token.
# MAGIC **Exige entrada humana** -- por isso nao vai para job agendado.
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
# MAGIC Rode a celula abaixo; o campo aparece no topo do notebook.
# MAGIC O valor digitado nao vai para o arquivo versionado no Git.

# COMMAND ----------

dbutils.widgets.text("github_token", "", "Token do GitHub (limpar apos gravar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gravar
# MAGIC
# MAGIC Cole o token no campo do topo, rode esta celula e limpe o campo.
# MAGIC Campo vazio nao grava nada.

# COMMAND ----------

_token = dbutils.widgets.get("github_token").strip()

if _token:
    w.secrets.put_secret(scope=ESCOPO, key=CHAVE, string_value=_token)
    print("segredo gravado. LIMPE O CAMPO DO WIDGET agora.")
else:
    print("campo vazio -- nada gravado.")

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
# MAGIC Limite 5000 = token aceito. Limite 60 = nao autenticado.

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
print("AUTENTICADO" if limite > 1000 else "NAO AUTENTICADO -- verifique o token")
