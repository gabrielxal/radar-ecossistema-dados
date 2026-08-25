# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Dimensoes
# MAGIC
# MAGIC As tres dimensoes da gold, mais a bateria que verifica as invariantes
# MAGIC do modelo.
# MAGIC
# MAGIC | Dimensao | Origem | Estrategia |
# MAGIC |---|---|---|
# MAGIC | `dim_tempo` | gerada | um dia por linha, sem lacuna |
# MAGIC | `dim_autor` | silver de commits | SCD1, chave hibrida |
# MAGIC | `dim_repositorio` | silver de repositorios | **SCD2 derivada** das fotos |
# MAGIC
# MAGIC As tres sao reconstruidas por inteiro a cada execucao. Isso so e seguro
# MAGIC porque a chave substituta e um hash: recalcular gera as mesmas chaves, e
# MAGIC os fatos que apontam para elas continuam validos.

# COMMAND ----------

import os
import sys
from datetime import datetime, timedelta, timezone

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import gold, qualidade, silver, silver_issues, silver_repositorios

spark.conf.set("spark.sql.session.timeZone", "UTC")
agora = datetime.now(timezone.utc)

print("dim_tempo       :", gold.TABELA_TEMPO)
print("dim_autor       :", gold.TABELA_AUTOR)
print("dim_repositorio :", gold.TABELA_REPOSITORIO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-voo

# COMMAND ----------

ORIGENS = {
    silver.TABELA_COMMITS: "notebooks/05_silver.py",
    silver_repositorios.TABELA_REPOSITORIOS: "notebooks/07_repositorios.py",
    silver_issues.TABELA_ISSUES: "notebooks/10_issues.py",
}

for tabela, notebook in ORIGENS.items():
    if not spark.catalog.tableExists(tabela):
        raise RuntimeError(f"tabela {tabela} nao existe (criada por {notebook})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold.TABELA_TEMPO.rsplit('.', 1)[0]}")
gold.criar_tabelas(spark)
qualidade.criar_tabela(spark)
print("pre-voo ok")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_tempo
# MAGIC
# MAGIC O intervalo cobre **as tres silvers**, e nao so commits.
# MAGIC
# MAGIC Duas razoes se somam. Dentro de commits, `autorado_em` e `commitado_em`
# MAGIC chegam a 562 dias de distancia no dado real, entao um calendario
# MAGIC derivado so da data de commit ja deixaria orfao o papel de autoria.
# MAGIC
# MAGIC E `fct_issue` desloca o inicio em anos: issue aberta muito antes da
# MAGIC janela de 90 dias de commits produz uma chave que nenhum calendario
# MAGIC derivado de commits teria.
# MAGIC
# MAGIC Chave de tempo e calculada e nao buscada, decidido em 4.2. O preco e
# MAGIC que a juncao perde a linha em silencio quando a data cai fora, e so a
# MAGIC bateria dos fatos acusa.

# COMMAND ----------

primeiro, ultimo = gold.limites_do_calendario(
    spark.table(silver.TABELA_COMMITS),
    issues=spark.table(silver_issues.TABELA_ISSUES),
    repositorios=spark.table(silver_repositorios.TABELA_REPOSITORIOS),
)

# Folga para tras absorve dado mais antigo numa carga futura; para a frente,
# permite que o fato de amanha encontre sua linha.
INICIO = primeiro - timedelta(days=30)
FIM = ultimo + timedelta(days=365)

print("data mais antiga :", primeiro)
print("data mais recente:", ultimo)
print("intervalo gerado :", INICIO, "a", FIM)

# COMMAND ----------

linhas_tempo = gold.escrever(
    spark, gold.gerar_dim_tempo(spark, INICIO, FIM), gold.TABELA_TEMPO
)
print(f"dias na dimensao: {linhas_tempo}")

display(spark.sql(f"SELECT * FROM {gold.TABELA_TEMPO} ORDER BY sk_tempo LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_autor
# MAGIC
# MAGIC Chave hibrida: `github_id` quando existe, e-mail do git quando nao.
# MAGIC Mais o membro desconhecido, para o fato que nao resolve nenhuma das
# MAGIC duas encontrar uma linha.
# MAGIC
# MAGIC A dimensao le das duas silvers porque os dois fatos apontam para ela.
# MAGIC Quem abre issue nem sempre commita: so com commits, todo relator
# MAGIC externo cairia no membro desconhecido e a pergunta sobre concentracao
# MAGIC de manutencao perderia justamente a parte de fora do nucleo.

# COMMAND ----------

autores = gold.montar_dim_autor(
    spark.table(silver.TABELA_COMMITS),
    agora,
    issues=spark.table(silver_issues.TABELA_ISSUES),
)
com_desconhecido = gold.linha_desconhecida(spark, agora).union(autores)

linhas_autor = gold.escrever(spark, com_desconhecido, gold.TABELA_AUTOR)
print(f"autores na dimensao: {linhas_autor}")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT origem_da_chave, github_tipo, count(*) AS autores
        FROM {gold.TABELA_AUTOR}
        GROUP BY origem_da_chave, github_tipo
        ORDER BY autores DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contagem de controle da dim_autor
# MAGIC
# MAGIC A conta e contra **as duas silvers**, e nao so contra commits. A
# MAGIC dimensao e conformada: ela serve `fct_commit` e `fct_issue`, entao a
# MAGIC referencia e a uniao das chaves naturais dos dois lados, mais o membro
# MAGIC desconhecido.
# MAGIC
# MAGIC Quem commita e abre issue aparece uma vez so, porque a chave por conta e
# MAGIC a mesma nos dois. E a interseccao que mostra isso funcionando.

# COMMAND ----------

composicao = spark.sql(
    f"""
    WITH de_commits AS (
        SELECT DISTINCT coalesce(cast(github_id AS STRING), autor_email) AS chave
        FROM {silver.TABELA_COMMITS}
    ),
    de_issues AS (
        SELECT DISTINCT cast(autor_id AS STRING) AS chave
        FROM {silver_issues.TABELA_ISSUES}
        WHERE autor_id IS NOT NULL
    ),
    commits_validos AS (
        SELECT chave FROM de_commits WHERE chave IS NOT NULL
    )
    SELECT
        (SELECT count(*) FROM commits_validos) AS em_commits,
        (SELECT count(*) FROM de_issues)       AS em_issues,
        (SELECT count(*) FROM (
            SELECT chave FROM commits_validos INTERSECT SELECT chave FROM de_issues
        )) AS em_ambos,
        (SELECT count(*) FROM (
            SELECT chave FROM commits_validos UNION SELECT chave FROM de_issues
        )) AS uniao
    """
).collect()[0]

esperado = composicao["uniao"]

print(f"chaves vindas de commits : {composicao['em_commits']}")
print(f"chaves vindas de issues  : {composicao['em_issues']}")
print(f"presentes nos dois       : {composicao['em_ambos']}")
print(f"uniao (a referencia)     : {esperado}")
print()
print(f"linhas na dimensao       : {linhas_autor}")
print(f"diferenca (esperada: 1)  : {linhas_autor - esperado}")

assert linhas_autor - esperado == 1, "a dimensao nao corresponde as duas silvers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_repositorio
# MAGIC
# MAGIC SCD2 derivada: as versoes sao recalculadas comparando cada foto com
# MAGIC a do dia anterior. Sem estado mantido, sem risco de uma execucao
# MAGIC perdida deixar a tabela torta.

# COMMAND ----------

repositorios = gold.montar_dim_repositorio(
    spark.table(silver_repositorios.TABELA_REPOSITORIOS), agora
)
linhas_repo = gold.escrever(spark, repositorios, gold.TABELA_REPOSITORIO)

print(f"versoes na dimensao: {linhas_repo}")

display(
    spark.sql(
        f"""
        SELECT repo, linguagem, licenca, arquivado,
               valido_de, valido_ate, flag_atual
        FROM {gold.TABELA_REPOSITORIO}
        ORDER BY repo, valido_de
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bateria da gold
# MAGIC
# MAGIC As tres primeiras sao as invariantes da SCD2 declaradas na secao 6.4 do
# MAGIC documento, afirmacoes que a modelagem faz e que so um teste de fora
# MAGIC comprova.

# COMMAND ----------

bateria = qualidade.verificacoes_gold()

for v in bateria:
    print(f"[{v.severidade:<8}] {v.nome}")
    print(f"           {v.descricao}")
    print()

# COMMAND ----------

recon = qualidade.Reconciliacao(
    nome="reconciliacao_silver_dim_autor",
    na_origem=esperado + 1,
    no_destino=linhas_autor,
)

resultados = qualidade.avaliar(spark, gold.TABELA_AUTOR, bateria, recon, agora)

for r in resultados:
    marca = "ok  " if r.passou else "FALHA"
    linha = f"[{marca}] {r.nome:<34} violacoes: {r.violacoes}"
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
# MAGIC ## O modelo, montado
# MAGIC
# MAGIC A juncao que a Etapa 5 vai usar, exercitada aqui sem fato nenhum.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            r.repo,
            r.linguagem,
            r.licenca,
            count(DISTINCT a.sk_autor) AS autores,
            count(*)                   AS commits
        FROM {silver.TABELA_COMMITS} c
        JOIN {gold.TABELA_AUTOR} a
          ON a.chave_natural = coalesce(cast(c.github_id AS STRING), c.autor_email)
        JOIN {gold.TABELA_REPOSITORIO} r
          ON r.repo = c.repo AND r.flag_atual
        JOIN {gold.TABELA_TEMPO} t
          ON t.data = to_date(c.commitado_em)
        GROUP BY r.repo, r.linguagem, r.licenca
        ORDER BY commits DESC
        """
    )
)
