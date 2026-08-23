# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Repositorios
# MAGIC
# MAGIC Segundo endpoint do pipeline: `/repos/{repo}`, recurso unico em vez de
# MAGIC lista paginada. Atravessa as tres camadas num notebook so, porque sao
# MAGIC catorze linhas por dia.
# MAGIC
# MAGIC **Grao**: uma foto por repositorio por dia. A API devolve o estado de
# MAGIC agora e nao tem historico -- ele se constroi acumulando fotos.
# MAGIC
# MAGIC Alimenta a `dim_repositorio` (Etapa 4) e a `fct_repo_snapshot` (Etapa 5).

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import bronze, controle, ingestao, silver_repositorios
from radar.config import BRONZE, CATALOG, REPOS, VOLUME
from radar.github_client import GitHubClient

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

dbutils.widgets.text("repos", "", "Repositorios (vazio = todos)")

_repos = dbutils.widgets.get("repos").strip()
REPOS_ALVO = tuple(r.strip() for r in _repos.split(",") if r.strip()) or REPOS

ENDPOINT = ingestao.ENDPOINTS["repositorios"]
CAMINHO_VOLUME = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"
cliente = GitHubClient(dbutils.secrets.get(scope="radar", key="github_token"))
agora = datetime.now(timezone.utc)

print("endpoint     :", ENDPOINT.caminho)
print("snapshot     :", ENDPOINT.snapshot)
print("chave bronze :", ENDPOINT.chaves)
print("repositorios :", len(REPOS_ALVO))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Coleta
# MAGIC
# MAGIC Sem paginacao, sem watermark e sem sentinela. A ausencia da sentinela e
# MAGIC deliberada: um `304` economizaria uma requisicao e deixaria um dia sem
# MAGIC foto -- e dia sem foto nao se distingue de dia sem mudanca.

# COMMAND ----------

resultados = []

for repo in REPOS_ALVO:
    try:
        resultado = ingestao.ingerir_snapshot(
            cliente=cliente,
            endpoint=ENDPOINT,
            repo=repo,
            base_volume=CAMINHO_VOLUME,
            momento=agora,
        )
    except Exception as erro:
        resultado = ingestao.ResultadoIngestao(
            repo=repo,
            endpoint=ENDPOINT.nome,
            registros=0,
            arquivo=None,
            etag=None,
            maior_data=None,
            pulado=False,
            erro=f"{type(erro).__name__}: {erro}"[:500],
        )

    controle.salvar(spark, ingestao.proximo_checkpoint(None, resultado, agora))
    resultados.append(resultado)

    marca = "ERRO " if resultado.erro else "ok   "
    print(f"[{marca}] {repo:<40} {resultado.registros} foto(s)")

print()
print(f"coletadas : {sum(r.registros for r in resultados)}")
print(f"com erro  : {sum(1 for r in resultados if r.erro)}")

for r in resultados:
    if r.erro:
        print(f"ERRO em {r.repo}: {r.erro}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze
# MAGIC
# MAGIC O mesmo codigo da bronze de commits, parametrizado pelo endpoint. A
# MAGIC chave do MERGE vem de `endpoint.chaves`: aqui e `(repo, dt)`, o que
# MAGIC reduz varias coletas do mesmo dia a uma foto.

# COMMAND ----------

bronze.criar_tabela(spark, ENDPOINT)
carga = bronze.carregar(spark, CAMINHO_VOLUME, ENDPOINT, agora)

print(f"linhas lidas no JSONL : {carga.linhas_lidas}")
print(f"linhas novas (MERGE)  : {carga.linhas_novas}")
print(f"total na tabela       : {carga.linhas_na_tabela}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver

# COMMAND ----------

silver_repositorios.criar_tabela(spark)
tipagem = silver_repositorios.carregar(spark, ENDPOINT, agora)

print(f"lidos da bronze : {tipagem.lidos}")
print(f"aprovados       : {tipagem.aprovados}")
print(f"descartados     : {tipagem.descartados}")
print(f"dias com foto   : {tipagem.dias}")

assert tipagem.descartados == 0, "payload sem repo_id; investigar na bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferencia
# MAGIC
# MAGIC Atributos a esquerda, medidas a direita. Os primeiros viram
# MAGIC `dim_repositorio` com SCD2; os segundos, `fct_repo_snapshot`.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT repo, linguagem, licenca, dono_tipo, arquivado,
               stars, forks, issues_abertas, observadores
        FROM {silver_repositorios.TABELA_REPOSITORIOS}
        WHERE dt = (SELECT max(dt) FROM {silver_repositorios.TABELA_REPOSITORIOS})
        ORDER BY stars DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A serie de fotos
# MAGIC
# MAGIC Com um dia so, a `dim_repositorio` tera uma versao por repositorio. O
# MAGIC comportamento tipo 2 aparece quando um atributo mudar entre duas fotos
# MAGIC -- o que, para licenca e linguagem, leva meses. O mecanismo esta
# MAGIC coberto por teste com mudanca sintetica.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT dt, count(*) AS repositorios, count(DISTINCT licenca) AS licencas,
               count(DISTINCT linguagem) AS linguagens
        FROM {silver_repositorios.TABELA_REPOSITORIOS}
        GROUP BY dt ORDER BY dt DESC
        """
    )
)
