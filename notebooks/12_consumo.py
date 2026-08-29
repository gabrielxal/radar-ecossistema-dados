# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Camada de consumo
# MAGIC
# MAGIC Cria as visoes sobre as quais o painel e construido.
# MAGIC
# MAGIC O pipeline terminava na gold sem consumidor. O que faltava nao era mais
# MAGIC uma consulta, porque elas ja existem em `src/radar/analises.py`, e sim um
# MAGIC lugar estavel de onde um dashboard possa le-las sem carregar copia do
# MAGIC SQL. E o que `consumo.py` resolve, e o raciocinio esta no docstring do
# MAGIC modulo.
# MAGIC
# MAGIC A tarefa e barata: visao nao le dado, so guarda a definicao. Roda em
# MAGIC segundos e pode ser reexecutada a vontade.

# COMMAND ----------

import os
import sys

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import consumo, gold

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-voo
# MAGIC
# MAGIC Visao sobre tabela que nao existe e aceita na criacao e falha so na
# MAGIC consulta, o que empurra o erro para quem abrir o painel. Conferir aqui
# MAGIC troca isso por uma mensagem que diz qual notebook falta rodar.

# COMMAND ----------

OBRIGATORIAS = {
    gold.TABELA_FCT_COMMIT: "notebooks/09_fatos.py",
    gold.TABELA_REPOSITORIO: "notebooks/08_dimensoes.py",
    gold.TABELA_AUTOR: "notebooks/08_dimensoes.py",
    gold.TABELA_TEMPO: "notebooks/08_dimensoes.py",
    gold.TABELA_FCT_ISSUE: "notebooks/10_issues.py",
}

faltando = [
    f"{tabela} (criada por {notebook})"
    for tabela, notebook in OBRIGATORIAS.items()
    if not spark.catalog.tableExists(tabela)
]

if faltando:
    raise RuntimeError("tabelas ausentes:\n  " + "\n  ".join(faltando))

print("pre-voo ok")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criacao

# COMMAND ----------

criadas = consumo.criar(spark)

for nome in criadas:
    print(nome)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferencia
# MAGIC
# MAGIC Criar a visao nao valida a consulta: o `CREATE OR REPLACE VIEW` aceita
# MAGIC SQL que so falha quando alguem seleciona dela. Rodar cada uma com
# MAGIC `LIMIT 1` e o que transforma isso em erro aqui, e nao no painel.

# COMMAND ----------

for nome in criadas:
    linhas = spark.sql(f"SELECT * FROM {nome} LIMIT 1").count()
    print(f"{nome:<45} ok ({linhas} linha de amostra)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## O painel
# MAGIC
# MAGIC A visao principal, com o portao na ultima coluna. As colunas de issue
# MAGIC so podem ser lidas onde `issues_confiavel` e verdadeira: em coleta
# MAGIC crescente, o que chega primeiro e a parte velha e ja fechada do
# MAGIC backlog, e `em_aberto` sai baixa demais justamente onde o backfill
# MAGIC ainda nao terminou.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {consumo.nome_visao('painel_de_saude')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Como o dashboard e montado
# MAGIC
# MAGIC O layout, widget a widget, esta em `dashboards/painel_de_saude.md`.
# MAGIC Cada visual consulta uma visao criada aqui, sem SQL proprio.
