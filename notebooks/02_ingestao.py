# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Ingestao para a landing zone
# MAGIC
# MAGIC Para cada repositorio: le o checkpoint, consulta a sentinela, coleta o
# MAGIC que e novo e grava o JSON cru no Volume.
# MAGIC
# MAGIC Idempotente: reexecutar no mesmo dia sobrescreve o arquivo do horario
# MAGIC e atualiza o checkpoint via MERGE.
# MAGIC
# MAGIC ## Parametros
# MAGIC
# MAGIC - **dias_historico** — janela da primeira carga. Sem isso, o primeiro
# MAGIC   `paginar` percorre o historico inteiro e estoura a quota.
# MAGIC - **limite_paginas** — teto de paginas por repositorio. Valvula de seguranca.
# MAGIC - **repos** — lista separada por virgula. Vazio = todos do `config.py`.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import controle, ingestao
from radar.config import BRONZE, CATALOG, REPOS, VOLUME
from radar.github_client import GitHubClient

# Fixa o fuso da sessao: sem isso, timestamps lidos do Delta mudariam de
# valor conforme a configuracao do workspace.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

dbutils.widgets.text("dias_historico", "90", "Dias de historico na 1a carga")
dbutils.widgets.text("limite_paginas", "5", "Maximo de paginas por repositorio")
dbutils.widgets.text("repos", "", "Repositorios (vazio = todos)")

DIAS_HISTORICO = int(dbutils.widgets.get("dias_historico"))
LIMITE_PAGINAS = int(dbutils.widgets.get("limite_paginas")) or None

_repos = dbutils.widgets.get("repos").strip()
REPOS_ALVO = tuple(r.strip() for r in _repos.split(",") if r.strip()) or REPOS

print(f"repositorios     : {len(REPOS_ALVO)}")
print(f"dias_historico   : {DIAS_HISTORICO}")
print(f"limite_paginas   : {LIMITE_PAGINAS}")

# COMMAND ----------

CAMINHO_VOLUME = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"
ENDPOINT = ingestao.ENDPOINTS["commits"]

cliente = GitHubClient(dbutils.secrets.get(scope="radar", key="github_token"))
agora = datetime.now(timezone.utc)

print("destino :", CAMINHO_VOLUME)
print("endpoint:", ENDPOINT.nome)
print("execucao:", agora.isoformat())

# COMMAND ----------

# Sonda com chamada real: /rate_limit devolve valor em cache.
def sonda() -> int:
    return cliente.get(f"/repos/{REPOS_ALVO[0]}").rate_remaining


quota_inicial = sonda()
print("quota antes:", quota_inicial)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execucao
# MAGIC
# MAGIC O `try/except` por repositorio e proposital: falha em um nao pode
# MAGIC derrubar os outros treze. O erro vai para a tabela de controle.

# COMMAND ----------

resultados = []

for repo in REPOS_ALVO:
    anterior = controle.ler(spark, repo, ENDPOINT.nome)

    if anterior is None:
        anterior = ingestao.checkpoint_inicial(
            repo, ENDPOINT.nome, agora, DIAS_HISTORICO
        )

    try:
        resultado = ingestao.ingerir(
            cliente=cliente,
            endpoint=ENDPOINT,
            repo=repo,
            checkpoint=anterior,
            base_volume=CAMINHO_VOLUME,
            momento=agora,
            limite_paginas=LIMITE_PAGINAS,
        )
    except Exception as erro:
        resultado = ingestao.ResultadoIngestao(
            repo=repo,
            endpoint=ENDPOINT.nome,
            registros=0,
            arquivo=None,
            etag=anterior.etag if anterior else None,
            maior_data=None,
            pulado=False,
            erro=f"{type(erro).__name__}: {erro}"[:500],
        )

    controle.salvar(spark, ingestao.proximo_checkpoint(anterior, resultado, agora))
    resultados.append(resultado)

    if resultado.erro:
        marca = "ERRO "
    elif resultado.pulado:
        marca = "pulado"
    else:
        marca = "ok   "
    print(f"[{marca}] {repo:<40} {resultado.registros:>6} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

quota_final = sonda()

pulados = sum(1 for r in resultados if r.pulado)
com_erro = sum(1 for r in resultados if r.erro)
total = sum(r.registros for r in resultados)

print(f"repositorios      : {len(resultados)}")
print(f"  pulados (304)   : {pulados}")
print(f"  com erro        : {com_erro}")
print(f"registros gravados: {total:,}")
print(f"quota consumida   : {quota_inicial - quota_final - 1}")
print(f"quota restante    : {quota_final}")

for r in resultados:
    if r.erro:
        print(f"\nERRO em {r.repo}: {r.erro}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela de controle

# COMMAND ----------

display(controle.listar(spark))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquivos gravados

# COMMAND ----------

import os

encontrados = []
for raiz, _, nomes in os.walk(f"{CAMINHO_VOLUME}/{ENDPOINT.nome}"):
    for nome in nomes:
        if nome.endswith(".jsonl"):
            caminho = os.path.join(raiz, nome)
            encontrados.append((caminho, os.path.getsize(caminho)))

print(f"{len(encontrados)} arquivo(s) na landing zone")
print()
for caminho, tamanho in sorted(encontrados):
    print(f"  {tamanho / 1024:>9,.1f} KB  {caminho}")

# COMMAND ----------

# Amostra do primeiro arquivo, para conferir que o payload chegou intacto.
arquivos = [r.arquivo for r in resultados if r.arquivo]
if arquivos:
    with open(arquivos[0], encoding="utf-8") as f:
        primeira = f.readline()
    print(arquivos[0])
    print(primeira[:600])
else:
    print("nenhum arquivo gravado nesta execucao")
