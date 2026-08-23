# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Fatos
# MAGIC
# MAGIC Dois dos tres tipos de fato do Kimball. O terceiro -- snapshot
# MAGIC acumulado, `fct_issue` -- exige o endpoint de issues e fica para a
# MAGIC etapa seguinte.
# MAGIC
# MAGIC | Fato | Tipo | Grao | Medidas |
# MAGIC |---|---|---|---|
# MAGIC | `fct_commit` | transacao | um commit | aditivas, e uma **nao aditiva** |
# MAGIC | `fct_repo_snapshot` | snapshot periodico | um repositorio por dia | **semi-aditivas** |
# MAGIC
# MAGIC Como as dimensoes, os dois sao derivados da silver e reconstruidos por
# MAGIC inteiro. A gold toda e derivada: nenhuma tabela guarda estado proprio.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import gold, qualidade, silver, silver_repositorios

spark.conf.set("spark.sql.session.timeZone", "UTC")
agora = datetime.now(timezone.utc)

print("fct_commit        :", gold.TABELA_FCT_COMMIT)
print("fct_repo_snapshot :", gold.TABELA_FCT_SNAPSHOT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-voo

# COMMAND ----------

for tabela in (gold.TABELA_TEMPO, gold.TABELA_AUTOR, gold.TABELA_REPOSITORIO):
    if not spark.catalog.tableExists(tabela):
        raise RuntimeError(
            f"dimensao {tabela} nao existe (criada por notebooks/08_dimensoes.py)"
        )

gold.criar_fatos(spark)
qualidade.criar_tabela(spark)
print("pre-voo ok")

# COMMAND ----------

dim_tempo = spark.table(gold.TABELA_TEMPO)
dim_autor = spark.table(gold.TABELA_AUTOR)
dim_repositorio = spark.table(gold.TABELA_REPOSITORIO)
commits = spark.table(silver.TABELA_COMMITS)
repositorios = spark.table(silver_repositorios.TABELA_REPOSITORIOS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. fct_commit
# MAGIC
# MAGIC A juncao com `dim_repositorio` e **por vigencia**, nao por `flag_atual`:
# MAGIC um commit de junho pertence ao estado que o repositorio tinha em junho.
# MAGIC Usar a versao atual jogaria fora a historia que a SCD2 guarda.

# COMMAND ----------

fato_commit = gold.montar_fct_commit(commits, dim_repositorio, dim_autor, agora)
linhas_commit = gold.escrever(spark, fato_commit, gold.TABELA_FCT_COMMIT)

na_silver = commits.count()
print(f"commits na silver : {na_silver}")
print(f"linhas no fato    : {linhas_commit}")
print(f"diferenca         : {na_silver - linhas_commit}")

assert linhas_commit == na_silver, "o fato nao corresponde a silver"

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT sha, sk_repositorio, sk_autor, sk_data_commit, sk_data_autoria,
               qtd_pais, e_merge, assinatura_verificada, dias_ate_o_commit
        FROM {gold.TABELA_FCT_COMMIT}
        WHERE dias_ate_o_commit > 100
        LIMIT 10
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. fct_repo_snapshot
# MAGIC
# MAGIC Com uma foto, sao 14 linhas. A serie se constroi rodando o `07`
# MAGIC diariamente: este e o fato que precisa de **tempo**, nao de codigo.

# COMMAND ----------

fato_snapshot = gold.montar_fct_repo_snapshot(repositorios, dim_repositorio, agora)
linhas_snapshot = gold.escrever(spark, fato_snapshot, gold.TABELA_FCT_SNAPSHOT)

nas_fotos = repositorios.count()
print(f"fotos na silver : {nas_fotos}")
print(f"linhas no fato  : {linhas_snapshot}")

assert linhas_snapshot == nas_fotos, "o fato nao corresponde as fotos"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bateria dos fatos
# MAGIC
# MAGIC O Unity Catalog registra chave estrangeira mas **nao a impoe**. Estas
# MAGIC verificacoes substituem a imposicao do banco: sem elas, um fato
# MAGIC apontando para dimensao inexistente nao gera erro -- a linha apenas
# MAGIC some da consulta com juncao.

# COMMAND ----------

bateria = qualidade.verificacoes_fatos()

for v in bateria:
    print(f"[{v.severidade:<8}] {v.nome}")
    print(f"           {v.descricao}")
    print()

# COMMAND ----------

recon = qualidade.Reconciliacao(
    nome="reconciliacao_silver_fct_commit",
    na_origem=na_silver,
    no_destino=linhas_commit,
)

resultados = qualidade.avaliar(spark, gold.TABELA_FCT_COMMIT, bateria, recon, agora)

for r in resultados:
    marca = "ok  " if r.passou else "FALHA"
    linha = f"[{marca}] {r.nome:<40} violacoes: {r.violacoes}"
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
# MAGIC ## 4. O modelo respondendo
# MAGIC
# MAGIC Consultas que exigem o star schema inteiro -- fato, tres dimensoes e,
# MAGIC na primeira delas, a dimensao de tempo nos **dois papeis**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atividade por dia da semana
# MAGIC
# MAGIC Uma das perguntas filhas do projeto: o ecossistema e mantido por
# MAGIC trabalho remunerado em horario comercial, ou por voluntariado?

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT t.dia_da_semana, t.nome_dia, t.e_fim_de_semana,
               count(*) AS commits,
               round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS percentual
        FROM {gold.TABELA_FCT_COMMIT} f
        JOIN {gold.TABELA_TEMPO} t ON t.sk_tempo = f.sk_data_commit
        GROUP BY t.dia_da_semana, t.nome_dia, t.e_fim_de_semana
        ORDER BY t.dia_da_semana
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### A mesma pergunta, com as decisoes analiticas explicitas
# MAGIC
# MAGIC A consulta acima usa `sk_data_commit` e nao filtra nada -- e por isso
# MAGIC responde errado. Tres correcoes, cada uma com motivo:
# MAGIC
# MAGIC | Correcao | Por que |
# MAGIC |---|---|
# MAGIC | `sk_data_autoria` | a pergunta e *quando a pessoa trabalhou*, nao quando o codigo entrou |
# MAGIC | `dias_ate_o_commit <= 7` | remove a migracao do `dbt-core` (2.919 commits num unico dia) e o rebase do `trino` |
# MAGIC | `github_tipo <> 'bot'` | automacao roda em agenda e nao tem fim de semana |
# MAGIC
# MAGIC A diferenca entre as duas saidas e a demonstracao de que **filtro e
# MAGIC escolha de chave de data sao decisoes analiticas, nao detalhes
# MAGIC tecnicos**. O modelo dimensional as torna explicitas em vez de
# MAGIC acidentais.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT t.dia_da_semana, t.nome_dia, t.e_fim_de_semana,
               count(*) AS commits,
               round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS percentual
        FROM {gold.TABELA_FCT_COMMIT} f
        JOIN {gold.TABELA_TEMPO} t ON t.sk_tempo = f.sk_data_autoria
        JOIN {gold.TABELA_AUTOR} a USING (sk_autor)
        WHERE f.dias_ate_o_commit <= 7
          AND (a.github_tipo IS NULL OR a.github_tipo <> 'bot')
        GROUP BY t.dia_da_semana, t.nome_dia, t.e_fim_de_semana
        ORDER BY t.dia_da_semana
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A dimensao de tempo nos dois papeis
# MAGIC
# MAGIC A mesma tabela, referenciada duas vezes pelo mesmo fato. Sem isto, a
# MAGIC pergunta "escrito num mes, absorvido em outro" nao teria resposta.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            autoria.ano || '-' || lpad(autoria.mes, 2, '0') AS mes_de_autoria,
            entrada.ano || '-' || lpad(entrada.mes, 2, '0') AS mes_de_entrada,
            count(*) AS commits
        FROM {gold.TABELA_FCT_COMMIT} f
        JOIN {gold.TABELA_TEMPO} autoria ON autoria.sk_tempo = f.sk_data_autoria
        JOIN {gold.TABELA_TEMPO} entrada ON entrada.sk_tempo = f.sk_data_commit
        WHERE autoria.ano || autoria.mes <> entrada.ano || entrada.mes
        GROUP BY 1, 2
        ORDER BY commits DESC
        LIMIT 15
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bot contra pessoa, por linguagem
# MAGIC
# MAGIC Fato, `dim_autor` e `dim_repositorio` na mesma consulta.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT r.linguagem,
               sum(CASE WHEN a.github_tipo = 'bot' THEN 1 ELSE 0 END) AS de_bot,
               sum(CASE WHEN a.github_tipo = 'user' THEN 1 ELSE 0 END) AS de_pessoa,
               round(100.0 * sum(CASE WHEN a.github_tipo = 'bot' THEN 1 ELSE 0 END)
                     / count(*), 1) AS percentual_bot
        FROM {gold.TABELA_FCT_COMMIT} f
        JOIN {gold.TABELA_AUTOR} a       USING (sk_autor)
        JOIN {gold.TABELA_REPOSITORIO} r USING (sk_repositorio)
        GROUP BY r.linguagem
        ORDER BY percentual_bot DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### O snapshot e a semi-aditividade
# MAGIC
# MAGIC Somar estrelas **entre repositorios num dia** produz o total do
# MAGIC ecossistema. Somar o mesmo repositorio ao longo de dias contaria a
# MAGIC mesma estrela varias vezes -- por isso a agregacao no tempo e `max`,
# MAGIC nunca `sum`.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT t.data,
               sum(f.stars)          AS estrelas_no_ecossistema,
               sum(f.forks)          AS forks,
               sum(f.issues_abertas) AS issues_abertas,
               count(*)              AS repositorios
        FROM {gold.TABELA_FCT_SNAPSHOT} f
        JOIN {gold.TABELA_TEMPO} t ON t.sk_tempo = f.sk_data
        GROUP BY t.data
        ORDER BY t.data DESC
        """
    )
)
