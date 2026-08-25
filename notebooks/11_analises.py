# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Analises
# MAGIC
# MAGIC As perguntas da secao 2 do `PROJETO.md`, respondidas.
# MAGIC
# MAGIC As consultas moram em `src/radar/analises.py`, com o raciocinio de cada
# MAGIC uma no docstring, e sao exercitadas contra o motor em
# MAGIC `tests/test_analises_spark.py` com cenarios de resposta conhecida. Aqui
# MAGIC elas rodam contra o dado real.
# MAGIC
# MAGIC Tres filtros atravessam toda pergunta sobre atividade humana: data de
# MAGIC autoria, atraso de ate 7 dias e bot de fora. Estao declarados uma vez em
# MAGIC `analises.py` e explicados na secao 10.6.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import analises, gold

spark.conf.set("spark.sql.session.timeZone", "UTC")
agora = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-voo

# COMMAND ----------

OBRIGATORIAS = {
    gold.TABELA_FCT_COMMIT: "notebooks/09_fatos.py",
    gold.TABELA_REPOSITORIO: "notebooks/08_dimensoes.py",
    gold.TABELA_AUTOR: "notebooks/08_dimensoes.py",
    gold.TABELA_TEMPO: "notebooks/08_dimensoes.py",
}

for tabela, notebook in OBRIGATORIAS.items():
    if not spark.catalog.tableExists(tabela):
        raise RuntimeError(f"tabela {tabela} nao existe (criada por {notebook})")

# `fct_issue` e opcional: sem ela a pergunta 3 fica sem resposta e as outras
# tres continuam valendo.
TEM_ISSUES = spark.catalog.tableExists(gold.TABELA_FCT_ISSUE) and (
    spark.table(gold.TABELA_FCT_ISSUE).limit(1).count() > 0
)

print("pre-voo ok")
print("issues disponiveis:", TEM_ISSUES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1: o projeto acelera ou desacelera?
# MAGIC
# MAGIC Duas colunas, e a comparacao entre elas e a resposta. Volume subindo com
# MAGIC producao por pessoa caindo e time crescendo, nao projeto acelerando.

# COMMAND ----------

display(spark.sql(analises.ritmo_por_autor()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2: bus factor
# MAGIC
# MAGIC Quantas pessoas concentram metade dos commits. `1` significa ponto unico
# MAGIC de falha; `concentracao_pct` poe o numero em escala do tamanho do time.
# MAGIC
# MAGIC E a pergunta que justifica `dim_autor` ser conformada.

# COMMAND ----------

display(spark.sql(analises.bus_factor()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3: quanto tempo uma issue leva para ser fechada?
# MAGIC
# MAGIC Antes da resposta, onde ela vale. A coleta de issues e crescente, e num
# MAGIC repositorio ainda truncado o que chegou e a parte velha e ja fechada do
# MAGIC backlog. Use so as linhas com `confiavel`.

# COMMAND ----------

display(spark.sql(analises.cobertura_do_backfill()))

# COMMAND ----------

# MAGIC %md
# MAGIC Duas medidas com significados opostos: `mediana_dias_ate_fechar` mede
# MAGIC vazao e `mediana_idade_em_aberto` mede backlog. Fechar rapido o que e
# MAGIC facil e ignorar o resto produz a primeira boa e a segunda pessima.

# COMMAND ----------

if TEM_ISSUES:
    display(spark.sql(analises.ciclo_de_issues()))
else:
    print("fct_issue vazia: rode notebooks/10_issues.py antes desta pergunta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4: o historico antigo muda junto?
# MAGIC
# MAGIC E a pergunta que justifica a SCD2. Uma versao por repositorio significa
# MAGIC que nenhum atributo mudou desde a primeira foto, e nao defeito: licenca e
# MAGIC linguagem mudam em escala de meses.

# COMMAND ----------

display(spark.sql(analises.versoes_do_repositorio()))

# COMMAND ----------

# MAGIC %md
# MAGIC A segunda tabela compara, para cada commit, o estado a que ele esta
# MAGIC ligado com o estado vigente hoje. O dia em que `divergentes` deixar de
# MAGIC ser zero e o dia em que a SCD2 paga o custo dela.

# COMMAND ----------

display(spark.sql(analises.historico_preservado()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## A pergunta central
# MAGIC
# MAGIC Os sinais lado a lado, sem coluna de veredito. Bus factor 1 com ritmo
# MAGIC alto e um risco diferente de bus factor 12 com ritmo caindo, e um indice
# MAGIC unico igualaria os dois.

# COMMAND ----------

display(spark.sql(analises.painel_de_saude()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que estes numeros nao sustentam
# MAGIC
# MAGIC Leitura sem limite declarado e pior que leitura ausente.
# MAGIC
# MAGIC | Limite | Consequencia |
# MAGIC |---|---|
# MAGIC | Janela de 90 dias | "morrendo" e tendencia longa; projeto maduro e estavel se parece com projeto desacelerando num trecho curto |
# MAGIC | As duas janelas de 45 dias caem em agosto | ferias no hemisferio norte deslocam a segunda metade para baixo em quase todos |
# MAGIC | Serie de fotos curta | stars e forks ao longo do tempo precisam de calendario, nao de codigo |
# MAGIC | Commit nao e a unica forma de manter | revisao e triagem sustentam projeto e nao aparecem em `fct_commit` |
# MAGIC | Nem todo projeto usa o GitHub para tudo | os Apache conduzem discussao no JIRA, e a pergunta 3 mede outra coisa neles |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro da execucao
# MAGIC
# MAGIC O painel muda a cada execucao: `dias_em_aberto` cresce e a janela anda.
# MAGIC Guardar a data e o que permite comparar duas leituras depois.

# COMMAND ----------

print("leitura de:", agora.date().isoformat())
print()
for nome, tabela in (
    ("commits", gold.TABELA_FCT_COMMIT),
    ("fotos", gold.TABELA_FCT_SNAPSHOT),
    ("issues", gold.TABELA_FCT_ISSUE),
):
    if spark.catalog.tableExists(tabela):
        print(f"{nome:<10}: {spark.table(tabela).count()}")
