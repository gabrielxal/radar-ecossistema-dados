# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Qualidade da silver
# MAGIC
# MAGIC As mesmas duas perguntas da bronze, com respostas diferentes:
# MAGIC
# MAGIC 1. **Chegou tudo?** — `bronze = silver + quarentena`
# MAGIC 2. **Chegou integro?** — bateria sobre colunas tipadas
# MAGIC
# MAGIC A diferenca esta na segunda. Comparar duas datas exige que elas sejam
# MAGIC datas: sao verificacoes que a bronze nao teria como fazer.
# MAGIC
# MAGIC Compartilha a tabela de historico com a bateria da bronze; a coluna
# MAGIC `tabela` separa as duas series.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import bronze, ingestao, qualidade, silver

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

dbutils.widgets.text("endpoint", "commits", "Endpoint a verificar")

ENDPOINT = ingestao.ENDPOINTS[dbutils.widgets.get("endpoint").strip()]
TABELA = silver.TABELA_COMMITS
agora = datetime.now(timezone.utc)

print("tabela verificada :", TABELA)
print("quarentena        :", silver.TABELA_REJEITADOS)
print("historico         :", qualidade.TABELA_QUALIDADE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-voo

# COMMAND ----------

for tabela in (bronze.nome_tabela(ENDPOINT), TABELA, silver.TABELA_REJEITADOS):
    if not spark.catalog.tableExists(tabela):
        raise RuntimeError(
            f"tabela {tabela} nao existe (criada por notebooks/05_silver.py)"
        )

qualidade.criar_tabela(spark)
print("pre-voo ok")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Contagem de controle
# MAGIC
# MAGIC A igualdade so fecha porque registro fora do contrato e desviado, nunca
# MAGIC descartado. Descarte silencioso apareceria aqui como diferenca, sem
# MAGIC nenhuma pista de onde as linhas foram parar.

# COMMAND ----------

recon = qualidade.reconciliar_silver(spark, ENDPOINT)

print(f"na bronze             : {recon.na_origem}")
print(f"silver + quarentena   : {recon.no_destino}")
print(f"diferenca             : {recon.diferenca}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bateria

# COMMAND ----------

bateria = qualidade.verificacoes_silver(ENDPOINT)

for v in bateria:
    print(f"[{v.severidade:<8}] {v.nome}")
    print(f"           {v.descricao}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Executar e registrar
# MAGIC
# MAGIC `avaliar` grava o historico antes de devolver. A interrupcao vem
# MAGIC depois, na celula seguinte: execucao reprovada que nao entra no
# MAGIC historico e justamente a que faria falta na investigacao.

# COMMAND ----------

resultados = qualidade.avaliar(spark, TABELA, bateria, recon, agora)

for r in resultados:
    marca = "ok  " if r.passou else "FALHA"
    linha = f"[{marca}] {r.nome:<38} violacoes: {r.violacoes}"
    if r.esperado is not None:
        linha += f"  (esperado {r.esperado}, obtido {r.obtido})"
    print(linha)

bloqueios, avisos = qualidade.resumo(resultados)
print()
print(f"bloqueios: {bloqueios} | avisos: {avisos}")

# COMMAND ----------

qualidade.levantar_se_bloqueou(resultados)
print("nenhuma regra bloqueante falhou")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. O que os avisos apontam
# MAGIC
# MAGIC Aviso nao interrompe, mas nomeia o que investigar. Categoria fora do
# MAGIC dominio costuma ser a origem tendo criado um valor novo.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT github_tipo, count(*) AS linhas
        FROM {TABELA}
        GROUP BY github_tipo
        ORDER BY linhas DESC
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT assinatura_motivo, assinatura_verificada, count(*) AS linhas
        FROM {TABELA}
        GROUP BY assinatura_motivo, assinatura_verificada
        ORDER BY linhas DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Historico das duas camadas
# MAGIC
# MAGIC A mesma tabela guarda as duas baterias; `tabela` separa as series.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT executado_em, tabela, verificacao, severidade,
               violacoes, passou, esperado, obtido
        FROM {qualidade.TABELA_QUALIDADE}
        ORDER BY executado_em DESC, tabela, severidade, verificacao
        LIMIT 60
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### As duas contagens de controle no tempo
# MAGIC
# MAGIC Uma serie por camada: quanto entrou e quanto saiu, a cada execucao.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT executado_em, verificacao, esperado AS na_origem,
               obtido AS no_destino, esperado - obtido AS diferenca
        FROM {qualidade.TABELA_QUALIDADE}
        WHERE verificacao IN (
            '{qualidade.RECONCILIACAO_BRONZE}', '{qualidade.RECONCILIACAO_SILVER}'
        )
        ORDER BY executado_em DESC
        LIMIT 40
        """
    )
)
