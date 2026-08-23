# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Silver
# MAGIC
# MAGIC Le o que e novo na bronze, aplica o schema declarado, tipa cada coluna
# MAGIC e roteia: o que cabe no contrato vai para `radar_silver.commits`, o que
# MAGIC nao cabe vai para `radar_silver.commits_rejeitados` com o motivo.
# MAGIC
# MAGIC Incremental por watermark sobre `_ingerido_em`, com checkpoint proprio
# MAGIC (`commits@silver`) na mesma tabela de controle da ingestao.
# MAGIC
# MAGIC A invariante da camada: bronze = silver + quarentena.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import bronze, controle, ingestao, silver

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

dbutils.widgets.text("endpoint", "commits", "Endpoint a processar")

ENDPOINT = ingestao.ENDPOINTS[dbutils.widgets.get("endpoint").strip()]

ORIGEM = bronze.nome_tabela(ENDPOINT)
PROCESSO = silver.nome_processo(ENDPOINT)
agora = datetime.now(timezone.utc)

print("origem     :", ORIGEM)
print("destino    :", silver.TABELA_COMMITS)
print("quarentena :", silver.TABELA_REJEITADOS)
print("processo   :", PROCESSO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-voo

# COMMAND ----------

if not spark.catalog.tableExists(ORIGEM):
    raise RuntimeError(
        f"tabela {ORIGEM} nao existe (criada por notebooks/03_bronze.py)"
    )

silver.criar_tabelas(spark)
print("pre-voo ok")

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {silver.TABELA_COMMITS}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint anterior
# MAGIC
# MAGIC Vazio na primeira execucao: sem watermark, o lote e a bronze inteira.

# COMMAND ----------

anterior = controle.ler(spark, ORIGEM, PROCESSO)
print("watermark anterior:", anterior.watermark if anterior else None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga

# COMMAND ----------

resultado = silver.carregar(spark, ENDPOINT, agora)

print(f"lidos da bronze : {resultado.lidos}")
print(f"aprovados       : {resultado.aprovados}")
print(f"rejeitados      : {resultado.rejeitados}")
print(f"novo watermark  : {resultado.watermark}")
print()
print("fecha (lidos = aprovados + rejeitados):", resultado.fecha)

assert resultado.fecha, "linha perdida entre a bronze e as duas saidas"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prova de idempotencia
# MAGIC
# MAGIC A segunda execucao seguida encontra o watermark ja avancado e le zero
# MAGIC linhas da bronze. Nao e o mesmo tipo de prova da bronze: la o MERGE
# MAGIC rodava e nao inseria nada; aqui o lote sequer se forma.

# COMMAND ----------

repetida = silver.carregar(spark, ENDPOINT, agora)

print(f"lidos na 2a execucao: {repetida.lidos}")
assert repetida.lidos == 0, "o watermark nao avancou"
print("idempotencia confirmada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagem de controle

# COMMAND ----------

na_bronze = spark.table(ORIGEM).count()
na_silver = spark.table(silver.TABELA_COMMITS).count()
em_quarentena = spark.table(silver.TABELA_REJEITADOS).count()

print(f"bronze              : {na_bronze}")
print(f"silver              : {na_silver}")
print(f"quarentena          : {em_quarentena}")
print(f"silver + quarentena : {na_silver + em_quarentena}")
print()
print("igualdade fecha:", na_bronze == na_silver + em_quarentena)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferencia

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT repo,
               count(*)                        AS commits,
               count(github_id)                AS com_conta_github,
               min(commitado_em)               AS mais_antigo,
               max(commitado_em)               AS mais_recente
        FROM {silver.TABELA_COMMITS}
        GROUP BY repo
        ORDER BY commits DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uma linha tipada
# MAGIC
# MAGIC O que na bronze era uma string JSON unica.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT sha, repo, autor_nome, autor_email, autorado_em, commitado_em,
               comentarios, assinatura_verificada, assinatura_motivo,
               github_login, github_id, github_tipo, qtd_pais
        FROM {silver.TABELA_COMMITS}
        LIMIT 5
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que a tipagem revela
# MAGIC
# MAGIC Agregacoes que a bronze nao permitiria: `avg` sobre texto nao existe, e
# MAGIC ordenar data em string ordena alfabeticamente.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            count(*)                                          AS commits,
            round(avg(qtd_pais), 2)                           AS media_de_pais,
            sum(CASE WHEN qtd_pais > 1 THEN 1 ELSE 0 END)     AS merges,
            sum(CASE WHEN assinatura_verificada THEN 1 ELSE 0 END) AS assinados,
            count(DISTINCT autor_email)                       AS autores_distintos,
            sum(CASE WHEN github_id IS NULL THEN 1 ELSE 0 END) AS sem_conta_github,
            round(avg(datediff(commitado_em, autorado_em)), 2) AS dias_ate_o_commit
        FROM {silver.TABELA_COMMITS}
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarentena

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT motivo, count(*) AS linhas, min(_processado_em) AS desde
        FROM {silver.TABELA_REJEITADOS}
        GROUP BY motivo
        ORDER BY linhas DESC
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT repo, sha, motivo, left(payload, 200) AS inicio_do_payload,
               _arquivo_origem
        FROM {silver.TABELA_REJEITADOS}
        LIMIT 10
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint e historico

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT repo AS origem, endpoint AS processo, watermark,
               ultima_execucao, status, registros
        FROM {controle.TABELA_CONTROLE}
        ORDER BY endpoint, repo
        """
    )
)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {silver.TABELA_COMMITS}"))
