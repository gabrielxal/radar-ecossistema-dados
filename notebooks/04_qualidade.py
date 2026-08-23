# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Qualidade da bronze
# MAGIC
# MAGIC Duas perguntas, nesta ordem:
# MAGIC
# MAGIC 1. Chegou tudo? A contagem de controle compara a landing zone com a bronze
# MAGIC 2. Chegou integro? A bateria de verificacoes roda sobre a tabela
# MAGIC
# MAGIC O resultado de cada execucao e gravado em
# MAGIC `radar_bronze.qualidade_execucao`. Verificacao sem historico nao
# MAGIC responde "isso ja estava errado ontem?".
# MAGIC
# MAGIC Roda depois do `03_bronze`. Falha bloqueante interrompe o notebook.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import bronze, controle, ingestao, qualidade
from radar.config import BRONZE, CATALOG, VOLUME

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

dbutils.widgets.text("endpoint", "commits", "Endpoint a verificar")

ENDPOINT = ingestao.ENDPOINTS[dbutils.widgets.get("endpoint").strip()]

CAMINHO_VOLUME = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"
TABELA = bronze.nome_tabela(ENDPOINT)
agora = datetime.now(timezone.utc)

print("tabela verificada :", TABELA)
print("historico         :", qualidade.TABELA_QUALIDADE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-voo

# COMMAND ----------

if not spark.catalog.tableExists(TABELA):
    raise RuntimeError(
        f"tabela {TABELA} nao existe (criada por notebooks/03_bronze.py)"
    )

qualidade.criar_tabela(spark)
print("pre-voo ok")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Contagem de controle
# MAGIC
# MAGIC O numero de linhas da landing zone (ja deduplicada) tem que ser igual
# MAGIC ao da bronze. Diferenca positiva significa linha que existe no arquivo
# MAGIC e nao chegou na tabela, perda silenciosa, o pior tipo.
# MAGIC
# MAGIC Ela vira uma regra da bateria (`reconciliacao_landing_bronze`), com as
# MAGIC contagens gravadas em `esperado` e `obtido`. E o que permite perguntar
# MAGIC depois quantas linhas a landing zone tinha numa data passada.

# COMMAND ----------

recon = qualidade.reconciliar(spark, CAMINHO_VOLUME, ENDPOINT, agora)

print(f"na landing zone (deduplicada) : {recon.na_origem}")
print(f"na bronze                     : {recon.no_destino}")
print(f"diferenca                     : {recon.diferenca}")

# Sem `assert` aqui de proposito: a reconciliacao entra na bateria como as
# demais regras e e cobrada no fim, depois de gravada. Interromper agora
# deixaria a execucao reprovada fora do historico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bateria de verificacoes

# COMMAND ----------

bateria = qualidade.verificacoes_bronze(ENDPOINT)

for v in bateria:
    print(f"[{v.severidade:<8}] {v.nome}")
    print(f"           {v.descricao}")
    print()

# COMMAND ----------

# `avaliar` poe a contagem de controle na frente da bateria e grava o
# historico antes de devolver; a interrupcao fica para a celula seguinte.
resultados = qualidade.avaliar(spark, TABELA, bateria, recon, agora)

for r in resultados:
    marca = "ok  " if r.passou else "FALHA"
    linha = f"[{marca}] {r.nome:<28} violacoes: {r.violacoes}"
    if r.esperado is not None:
        linha += f"  (esperado {r.esperado}, obtido {r.obtido})"
    print(linha)

bloqueios, avisos = qualidade.resumo(resultados)
print()
print(f"bloqueios: {bloqueios} | avisos: {avisos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cobrar o resultado
# MAGIC
# MAGIC O historico ja foi gravado por `avaliar`, na celula anterior. A ordem e
# MAGIC deliberada: se a excecao viesse primeiro, a execucao reprovada nao
# MAGIC entraria no historico, e e ela que faria falta na investigacao.

# COMMAND ----------

print("execucao registrada em", qualidade.TABELA_QUALIDADE)

qualidade.levantar_se_bloqueou(resultados)
print("nenhuma regra bloqueante falhou")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cobertura contra a tabela de controle
# MAGIC
# MAGIC Cruzamento entre o que a ingestao diz ter gravado e o que a bronze
# MAGIC tem. Nao e uma igualdade: `registros` guarda a ultima carga, nao o
# MAGIC acumulado. O que se procura aqui e o caso gritante, repositorio com
# MAGIC carga bem-sucedida e nenhuma linha na bronze.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            c.repo,
            c.status,
            c.registros            AS registros_ultima_carga,
            c.watermark,
            count(b.repo)          AS linhas_na_bronze
        FROM {controle.TABELA_CONTROLE} c
        LEFT JOIN {TABELA} b ON b.repo = c.repo
        WHERE c.endpoint = '{ENDPOINT.nome}'
        GROUP BY c.repo, c.status, c.registros, c.watermark
        ORDER BY linhas_na_bronze ASC, c.repo
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Historico

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT executado_em, verificacao, severidade,
               violacoes, passou, esperado, obtido
        FROM {qualidade.TABELA_QUALIDADE}
        WHERE tabela = '{TABELA}'
        ORDER BY executado_em DESC, severidade, verificacao
        LIMIT 50
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evolucao por regra
# MAGIC
# MAGIC Uma regra que passa ha semanas e comeca a falhar diz mais do que o
# MAGIC estado de hoje: alguma coisa mudou, e da para datar quando.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            verificacao,
            count(*)                                  AS execucoes,
            sum(CASE WHEN passou THEN 0 ELSE 1 END)   AS reprovacoes,
            max(executado_em)                         AS ultima_execucao
        FROM {qualidade.TABELA_QUALIDADE}
        WHERE tabela = '{TABELA}'
        GROUP BY verificacao
        ORDER BY reprovacoes DESC, verificacao
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Volume ao longo do tempo
# MAGIC
# MAGIC A serie que so existe porque a contagem de controle passou a ser
# MAGIC gravada. Queda de volume entre duas execucoes e sintoma, de ETag
# MAGIC pulando o que nao devia, de watermark adiantado, de arquivo removido
# MAGIC da landing zone.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT executado_em, esperado AS na_landing_zone, obtido AS na_bronze,
               esperado - obtido AS diferenca
        FROM {qualidade.TABELA_QUALIDADE}
        WHERE tabela = '{TABELA}'
          AND verificacao = '{qualidade.RECONCILIACAO_BRONZE}'
        ORDER BY executado_em DESC
        LIMIT 30
        """
    )
)
