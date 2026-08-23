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

# MAGIC %md
# MAGIC ## Pre-voo
# MAGIC
# MAGIC A tabela de controle e o Volume sao criados pelo `00_setup_catalogo`.
# MAGIC A conferencia vem antes da primeira requisicao: dependencia ausente
# MAGIC falha aqui, nomeada, em vez de virar um `TABLE_OR_VIEW_NOT_FOUND`
# MAGIC no meio do laco, com quota ja consumida.

# COMMAND ----------

CRIADO_POR = "criado por notebooks/00_setup_catalogo.py"

if not spark.catalog.tableExists(controle.TABELA_CONTROLE):
    raise RuntimeError(
        f"tabela {controle.TABELA_CONTROLE} nao existe ({CRIADO_POR})"
    )

try:
    dbutils.fs.ls(CAMINHO_VOLUME)
except Exception as erro:
    raise RuntimeError(
        f"volume {CAMINHO_VOLUME} nao existe ({CRIADO_POR})"
    ) from erro

print("pre-voo ok:", controle.TABELA_CONTROLE, "|", CAMINHO_VOLUME)

# COMMAND ----------

# Chamada a um endpoint de dados, e nao a /rate_limit: aquele nao consome
# quota e pode responder de cache, o que falsearia a medicao.
def sonda() -> int:
    return cliente.get(f"/repos/{REPOS_ALVO[0]}").rate_remaining


quota_inicial = sonda()
print("quota antes:", quota_inicial)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execucao
# MAGIC
# MAGIC O `try/except` por repositorio isola a falha: um repositorio que
# MAGIC quebra nao interrompe os demais, e o erro fica registrado na
# MAGIC tabela de controle com `status='erro'`.

# COMMAND ----------

resultados = []

for repo in REPOS_ALVO:
    # Inicializado antes do try: o except abaixo usa `anterior`, que pode
    # nao ter sido atribuido se a propria leitura falhar.
    anterior = None

    try:
        anterior = controle.ler(spark, repo, ENDPOINT.nome)

        if anterior is None:
            anterior = ingestao.checkpoint_inicial(
                repo, ENDPOINT.nome, agora, DIAS_HISTORICO
            )

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
    elif resultado.truncado:
        marca = "TRUNC"
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
truncados = sum(1 for r in resultados if r.truncado)
total = sum(r.registros for r in resultados)

print(f"repositorios      : {len(resultados)}")
print(f"  pulados (304)   : {pulados}")
print(f"  com erro        : {com_erro}")
print(f"  truncados       : {truncados}")
print(f"registros gravados: {total:,}")
# O -1 desconta a sonda final, que tambem consome uma requisicao.
print(f"quota consumida   : {quota_inicial - quota_final - 1}")
print(f"quota restante    : {quota_final}")

for r in resultados:
    if r.erro:
        print(f"\nERRO em {r.repo}: {r.erro}")

# COMMAND ----------

# Truncagem nao interrompe nada, e por isso precisa ser dita: o watermark
# avanca por cima do que ficou para tras, tornando a falta permanente.
for r in resultados:
    if r.truncado:
        print(
            f"TRUNCADO {r.repo}: parou no teto de {LIMITE_PAGINAS} paginas; "
            "ha historico anterior nao coletado"
        )

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
