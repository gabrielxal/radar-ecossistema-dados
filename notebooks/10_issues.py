# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Issues
# MAGIC
# MAGIC Terceiro endpoint do pipeline: `/repos/{repo}/issues`. Atravessa as tres
# MAGIC camadas num notebook so, como o de repositorios.
# MAGIC
# MAGIC Grao da silver: uma linha por (repo, numero), no estado mais recente
# MAGIC conhecido. A bronze guarda uma linha por issue por dia de coleta, porque
# MAGIC issue muda depois de criada; a silver colapsa esse log no estado atual.
# MAGIC
# MAGIC Alimenta a `fct_issue` (Etapa 6) e acrescenta autores a `dim_autor`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duas particularidades deste endpoint
# MAGIC
# MAGIC **A API devolve pull requests junto.** Um PR e uma issue com um campo a
# MAGIC mais. A bronze guarda os dois, porque e copia fiel do endpoint; a silver
# MAGIC separa em duas tabelas.
# MAGIC
# MAGIC **A coleta e crescente.** `/issues` nao aceita `until`, entao o backfill
# MAGIC em janelas usado em commits nao serve aqui. No lugar dele vem
# MAGIC `direction=asc`: a lista comeca no registro mais antigo depois do
# MAGIC watermark e caminha para frente, entao o teto de paginas corta os mais
# MAGIC recentes, que sao exatamente os que a execucao seguinte busca.
# MAGIC
# MAGIC Isso torna a primeira carga um backfill retomavel: se ela nao terminar,
# MAGIC a proxima continua de onde parou, sem deixar buraco.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import bronze, controle, ingestao, silver_issues
from radar.config import BRONZE, CATALOG, REPOS, VOLUME
from radar.github_client import GitHubClient

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# `dias_historico` em zero significa carga completa: sem `since`, a coleta
# comeca na issue mais antiga do repositorio. E o backfill do estoque, que
# uma janela de 90 dias nao alcancaria, porque `since` filtra por
# `updated_at` e issue parada nao tem atualizacao recente.
dbutils.widgets.text("dias_historico", "0", "Dias de historico na 1a carga (0 = tudo)")
dbutils.widgets.text("limite_paginas", "40", "Maximo de paginas por repositorio")
dbutils.widgets.text("repos", "", "Repositorios (vazio = todos)")

DIAS_HISTORICO = int(dbutils.widgets.get("dias_historico"))
LIMITE_PAGINAS = int(dbutils.widgets.get("limite_paginas")) or None

_repos = dbutils.widgets.get("repos").strip()
REPOS_ALVO = tuple(r.strip() for r in _repos.split(",") if r.strip()) or REPOS

ENDPOINT = ingestao.ENDPOINTS["issues"]
CAMINHO_VOLUME = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"
cliente = GitHubClient(dbutils.secrets.get(scope="radar", key="github_token"))
agora = datetime.now(timezone.utc)

print("endpoint        :", ENDPOINT.caminho)
print("parametros fixos:", ENDPOINT.extras)
print("chave bronze    :", ENDPOINT.chaves)
print("ordem crescente :", ENDPOINT.ordem_crescente)
print("dias_historico  :", DIAS_HISTORICO)
print("limite_paginas  :", LIMITE_PAGINAS)
print("repositorios    :", len(REPOS_ALVO))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-voo

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

def sonda() -> int:
    return cliente.get(f"/repos/{REPOS_ALVO[0]}").rate_remaining


quota_inicial = sonda()
print("quota antes:", quota_inicial)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Coleta

# COMMAND ----------

resultados = []

for repo in REPOS_ALVO:
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

    # O endpoint entra aqui porque a regra da truncagem depende do sentido da
    # coleta: em ordem crescente o watermark avanca mesmo truncado, senao o
    # backfill repetiria as mesmas paginas para sempre.
    controle.salvar(
        spark, ingestao.proximo_checkpoint(anterior, resultado, agora, ENDPOINT)
    )
    resultados.append(resultado)

    if resultado.erro:
        marca = "ERRO "
    elif resultado.pulado:
        marca = "pulado"
    elif resultado.truncado:
        marca = "PARC "
    else:
        marca = "ok   "
    print(f"[{marca}] {repo:<40} {resultado.registros:>6} registros")

print()
print(f"coletados : {sum(r.registros for r in resultados)}")
print(f"truncados : {sum(1 for r in resultados if r.truncado)}")
print(f"com erro  : {sum(1 for r in resultados if r.erro)}")
print(f"quota gasta: {quota_inicial - sonda()}")

for r in resultados:
    if r.erro:
        print(f"ERRO em {r.repo}: {r.erro}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Truncagem aqui nao e a mesma da secao 5.7
# MAGIC
# MAGIC Em commits, uma coleta truncada deixa historico inalcancavel atras do
# MAGIC watermark. Em issues, o que ficou de fora esta a frente, e a proxima
# MAGIC execucao alcanca. `PARC` significa backfill em andamento, nao perda.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT repo, status, registros, watermark, ultima_execucao
        FROM {controle.TABELA_CONTROLE}
        WHERE endpoint = '{ENDPOINT.nome}'
        ORDER BY status DESC, repo
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze
# MAGIC
# MAGIC A chave do MERGE inclui `dt`, o que faz da tabela um log de versoes:
# MAGIC cada dia de coleta acrescenta a versao daquele dia em vez de
# MAGIC sobrescrever, e a camada continua sendo so insercao.

# COMMAND ----------

bronze.criar_tabela(spark, ENDPOINT)
carga = bronze.carregar(spark, CAMINHO_VOLUME, ENDPOINT, agora)

print(f"linhas lidas no JSONL : {carga.linhas_lidas}")
print(f"linhas novas (MERGE)  : {carga.linhas_novas}")
print(f"total na tabela       : {carga.linhas_na_tabela}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver
# MAGIC
# MAGIC Tres destinos. A conta fecha sobre versoes lidas da bronze: duas
# MAGIC versoes da mesma issue contam duas vezes aqui e viram uma linha na
# MAGIC tabela final.

# COMMAND ----------

silver_issues.criar_tabelas(spark)
tipagem = silver_issues.carregar(spark, ENDPOINT, agora)

print(f"lidos da bronze : {tipagem.lidos}")
print(f"issues          : {tipagem.issues}")
print(f"pull requests   : {tipagem.pull_requests}")
print(f"rejeitados      : {tipagem.rejeitados}")

assert tipagem.fecha, "a conta nao fechou: linha perdida entre bronze e silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferencia
# MAGIC
# MAGIC O estoque por repositorio: quantas abertas, quantas fechadas e a idade
# MAGIC da mais velha ainda em aberto. E o recorte que a janela de 90 dias nao
# MAGIC alcancaria, porque issue parada nao tem `updated_at` recente.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT repo,
               count(*)                                        AS issues,
               count_if(estado = 'open')                       AS abertas,
               count_if(estado = 'closed')                     AS fechadas,
               max(CASE WHEN fechada_em IS NULL
                        THEN datediff(current_date(), aberta_em) END) AS dias_da_mais_velha,
               round(avg(CASE WHEN fechada_em IS NOT NULL
                        THEN datediff(fechada_em, aberta_em) END), 1) AS media_dias_ate_fechar
        FROM {silver_issues.TABELA_ISSUES}
        GROUP BY repo
        ORDER BY abertas DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue e pull request, lado a lado
# MAGIC
# MAGIC Os PRs nao foram descartados. Sao entidade propria, no mesmo payload,
# MAGIC sem requisicao extra.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT 'issues' AS entidade, repo, count(*) AS linhas
        FROM {silver_issues.TABELA_ISSUES} GROUP BY repo
        UNION ALL
        SELECT 'pull_requests', repo, count(*)
        FROM {silver_issues.TABELA_PULL_REQUESTS} GROUP BY repo
        ORDER BY repo, entidade
        """
    )
)
