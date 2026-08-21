# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup de credenciais
# MAGIC
# MAGIC Provisiona o token do GitHub num **Secret Scope** do Databricks.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Quando rodar** | Uma vez por workspace, e a cada rotacao do token (~90 dias) |
# MAGIC | **Idempotente** | Sim -- escopo existente e reaproveitado; campo vazio nao grava nada |
# MAGIC | **Precisa de humano** | **Sim.** Alguem digita o token. Por isso NAO pode ir para um job |
# MAGIC | **Toca segredo** | Sim. E o unico notebook que toca |
# MAGIC
# MAGIC ## Por que separado do `00_setup_catalogo`
# MAGIC
# MAGIC 1. **Ciclo de vida diferente.** O catalogo muda quando a estrutura do
# MAGIC    projeto muda; o token muda quando expira. Nao ha razao para os dois
# MAGIC    rodarem juntos.
# MAGIC 2. **Automatizavel vs. manual.** Este notebook exige alguem digitando.
# MAGIC    Se estivesse junto com o setup do catalogo, tornaria aquele
# MAGIC    inautomatizavel tambem.
# MAGIC 3. **Raio de alcance.** Um cria estrutura vazia e inofensiva; o outro
# MAGIC    manipula credencial. Misturar dificulta auditar quem tocou o que.
# MAGIC 4. **Quem executa.** O catalogo e do projeto, compartilhado. A
# MAGIC    credencial e de cada pessoa, com o token dela.
# MAGIC
# MAGIC ## Antes de rodar
# MAGIC
# MAGIC Tenha em maos um token *fine-grained*, **read-only** sobre repositorios
# MAGIC publicos. Gere em: GitHub > Settings > Developer settings >
# MAGIC Fine-grained tokens.

# COMMAND ----------

ESCOPO = "radar"
CHAVE = "github_token"

print(f"destino do segredo: escopo '{ESCOPO}', chave '{CHAVE}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar o escopo (se ainda nao existir)

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
# MAGIC ## 2. Declarar o campo de entrada
# MAGIC
# MAGIC Rode a celula abaixo. Um campo **"Token do GitHub"** aparece no topo
# MAGIC do notebook.
# MAGIC
# MAGIC O valor digitado ali **nao vai para o arquivo `.py`** versionado no
# MAGIC Git -- so a declaracao do widget vai.

# COMMAND ----------

dbutils.widgets.text("github_token", "", "Token do GitHub (limpar apos gravar)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gravar o segredo
# MAGIC
# MAGIC Cole o token no campo do topo e rode esta celula **uma vez**.
# MAGIC Depois **limpe o campo**.
# MAGIC
# MAGIC Campo vazio nao grava nada -- entao reexecutar o notebook inteiro sem
# MAGIC digitar nada e seguro e nao apaga o segredo existente.

# COMMAND ----------

_token = dbutils.widgets.get("github_token").strip()

if _token:
    w.secrets.put_secret(scope=ESCOPO, key=CHAVE, string_value=_token)
    print("segredo gravado. LIMPE O CAMPO DO WIDGET agora.")
else:
    print("campo vazio -- nada gravado (esperado se o segredo ja existe).")

del _token

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar
# MAGIC
# MAGIC O Databricks **redige automaticamente** o valor de um segredo em
# MAGIC qualquer saida de celula. Ainda assim, aqui so imprimimos o tamanho:
# MAGIC nunca escreva codigo que dependa dessa redacao para nao vazar.

# COMMAND ----------

_t = dbutils.secrets.get(scope=ESCOPO, key=CHAVE)
print("segredo acessivel | tamanho:", len(_t), "caracteres")
del _t

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Teste de ponta a ponta
# MAGIC
# MAGIC Usa o segredo para uma chamada real e confirma a quota de 5.000/hora.
# MAGIC Se aparecer 60, o token nao foi aceito.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro de decisao
# MAGIC
# MAGIC Em producao, o segredo seria provisionado **fora da aplicacao** --
# MAGIC CLI, Terraform ou pipeline de CI -- e nunca passaria por um notebook.
# MAGIC O widget e aceitavel aqui porque o token e *read-only* sobre
# MAGIC repositorios publicos: no pior cenario, o estrago e nulo.
# MAGIC
# MAGIC Limitacao registrada, nao escondida. Ver `docs/PROJETO.md`, secao 8.7.
