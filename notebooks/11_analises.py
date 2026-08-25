# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Análises
# MAGIC
# MAGIC O modelo dimensional foi construído para responder as perguntas da seção
# MAGIC 2 do `PROJETO.md`. Este notebook é onde elas são respondidas.
# MAGIC
# MAGIC As consultas moram em `src/radar/analises.py` e são exercitadas contra o
# MAGIC motor em `tests/test_analises_spark.py`, com cenários pequenos de resposta
# MAGIC conhecida. Aqui elas rodam contra o dado real.

# COMMAND ----------

# MAGIC %md
# MAGIC ## As três correções que atravessam tudo
# MAGIC
# MAGIC A seção 10.6 mostrou que a mesma pergunta, sobre o mesmo fato, admite duas
# MAGIC respostas, e que três decisões separam uma da outra:
# MAGIC
# MAGIC | Correção | Por quê |
# MAGIC |---|---|
# MAGIC | `sk_data_autoria` no lugar de `sk_data_commit` | a pergunta é quando a pessoa trabalhou |
# MAGIC | `dias_ate_o_commit <= 7` | descarta história anterior absorvida de uma vez |
# MAGIC | bot fora | automação roda em agenda e não tem ritmo humano |
# MAGIC
# MAGIC Elas estão declaradas uma vez em `analises.py` e valem para toda pergunta
# MAGIC sobre atividade humana recente. É o modelo dimensional expondo a decisão
# MAGIC analítica em vez de embuti-la.

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
# MAGIC ## Pré-voo

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

# `fct_issue` é opcional aqui. Enquanto o notebook 10 não rodar, a pergunta 3
# fica sem resposta e as outras três continuam valendo.
TEM_ISSUES = spark.catalog.tableExists(gold.TABELA_FCT_ISSUE) and (
    spark.table(gold.TABELA_FCT_ISSUE).limit(1).count() > 0
)

print("pre-voo ok")
print("issues disponiveis:", TEM_ISSUES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1: o projeto acelera ou desacelera?
# MAGIC
# MAGIC > Commits crescem, mas por contribuidor ativo também?
# MAGIC
# MAGIC A segunda metade da pergunta é a que importa. Volume total subindo pode
# MAGIC significar time crescendo, e não projeto acelerando. Pode até esconder o
# MAGIC contrário, se o volume por pessoa caiu enquanto entrava gente.
# MAGIC
# MAGIC **Como ler:** `variacao_volume_pct` e `variacao_por_autor_pct` com sinais
# MAGIC opostos é o caso interessante. Volume subindo com produtividade por pessoa
# MAGIC caindo é time crescendo; o inverso é time encolhendo e sobrando trabalho
# MAGIC para quem ficou.

# COMMAND ----------

display(spark.sql(analises.ritmo_por_autor()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2: bus factor
# MAGIC
# MAGIC > Quantas pessoas concentram 50% dos commits?
# MAGIC
# MAGIC O nome vem de "quantas pessoas precisariam ser atropeladas por um ônibus
# MAGIC para o projeto parar".
# MAGIC
# MAGIC **Como ler:** `bus_factor = 1` significa que uma pessoa sozinha responde
# MAGIC por metade do trabalho, e a saída dela é um evento de sobrevivência.
# MAGIC `concentracao_pct` põe o número em escala: bus factor 3 num projeto de 5
# MAGIC pessoas é distribuição plana; bus factor 3 num de 200 é concentração
# MAGIC extrema.
# MAGIC
# MAGIC Esta é a pergunta que justifica `dim_autor` ser conformada. Ela responde
# MAGIC sobre commits, e a mesma dimensão serve `fct_issue` sem chave nova.

# COMMAND ----------

display(spark.sql(analises.bus_factor()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3: quanto tempo uma issue leva para ser fechada?
# MAGIC
# MAGIC Duas medidas com significados opostos, e é a diferença entre elas que
# MAGIC responde.
# MAGIC
# MAGIC `mediana_dias_ate_fechar` olha o que já terminou, e mede vazão. Sozinha
# MAGIC engana: um projeto que fecha rápido o que é fácil e ignora o resto parece
# MAGIC saudável.
# MAGIC
# MAGIC `mediana_idade_em_aberto` olha o que não terminou, e mede backlog. Número
# MAGIC alto aqui é a assinatura de projeto morrendo, e nenhuma medida sobre issue
# MAGIC fechada mostra isso.
# MAGIC
# MAGIC O marco do meio, `primeira_resposta_em`, não existe: não vem no payload da
# MAGIC issue. Está registrado nas melhorias planejadas, com o caminho barato.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Antes da resposta: em quais repositórios ela vale
# MAGIC
# MAGIC A coleta de issues é crescente, e o backfill leva várias execuções nos
# MAGIC repositórios grandes. Enquanto ele não termina, o que chegou é a parte
# MAGIC mais velha e já fechada do backlog: issue aberta recebe comentário, tem
# MAGIC `updated_at` recente, e está no fim da caminhada.
# MAGIC
# MAGIC Ler a pergunta 3 num repositório truncado dá a mesma forma de erro da
# MAGIC seção 5.7. O dado que chegou está correto e a conclusão tirada dele não.
# MAGIC
# MAGIC A coluna `confiavel` separa os dois casos. Use só as linhas com `true`.

# COMMAND ----------

display(spark.sql(analises.cobertura_do_backfill()))

# COMMAND ----------

if TEM_ISSUES:
    display(spark.sql(analises.ciclo_de_issues()))
else:
    print("fct_issue vazia: rode notebooks/10_issues.py antes desta pergunta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4: o histórico antigo muda junto?
# MAGIC
# MAGIC > Quando um repositório muda de linguagem, licença ou dono, o histórico
# MAGIC > antigo muda junto?
# MAGIC
# MAGIC É a pergunta que justifica a SCD2. Sem ela, versionar a dimensão seria
# MAGIC enfeite.
# MAGIC
# MAGIC **Como ler a primeira tabela:** uma versão por repositório significa que
# MAGIC nenhum atributo mudou desde a primeira foto. Não é defeito. Licença e
# MAGIC linguagem mudam em escala de meses, e a série tem poucos dias.

# COMMAND ----------

display(spark.sql(analises.versoes_do_repositorio()))

# COMMAND ----------

# MAGIC %md
# MAGIC **Como ler a segunda tabela:** para cada commit, ela compara o nome do
# MAGIC repositório na versão a que ele está ligado com o nome da versão vigente
# MAGIC hoje.
# MAGIC
# MAGIC Enquanto nada mudou, `divergentes` fica em zero, e isso mostra que a
# MAGIC junção por vigência está ligada corretamente. O dia em que a coluna
# MAGIC deixar de ser zero é o dia em que a SCD2 passa a pagar o custo dela: o
# MAGIC commit antigo continuará apontando para o estado da época, e não para o
# MAGIC de agora.

# COMMAND ----------

display(spark.sql(analises.historico_preservado()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## A pergunta central
# MAGIC
# MAGIC > Quais ferramentas do ecossistema estão saudáveis, e quais estão
# MAGIC > morrendo? Onde há risco de concentração de manutenção?
# MAGIC
# MAGIC Os sinais lado a lado, um repositório por linha. Não há coluna de
# MAGIC veredito, e a ausência é deliberada: um índice único de saúde esconderia
# MAGIC a informação que importa. Bus factor 1 com ritmo alto é um risco
# MAGIC diferente de bus factor 12 com ritmo caindo, e um número só os igualaria.
# MAGIC
# MAGIC | Coluna | O que denuncia |
# MAGIC |---|---|
# MAGIC | `ritmo_por_autor_pct` negativo | quem ficou está produzindo menos |
# MAGIC | `bus_factor` igual a 1 ou 2 | manutenção concentrada em poucas pessoas |
# MAGIC | `idade_mediana_em_aberto` alta | backlog envelhecendo sem resposta |

# COMMAND ----------

display(spark.sql(analises.painel_de_saude()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que estes números não sustentam
# MAGIC
# MAGIC Vale registrar junto da resposta, porque leitura sem limite declarado é
# MAGIC pior que leitura ausente.
# MAGIC
# MAGIC **A janela é de 90 dias.** "Morrendo" é um julgamento sobre tendência
# MAGIC longa, e três meses mostram um trecho. Um projeto maduro e estável se
# MAGIC parece com um projeto desacelerando quando só se olha o trecho.
# MAGIC
# MAGIC **A série de fotos é curta.** `fct_repo_snapshot` acumula um ponto por
# MAGIC dia desde que o job diário começou. Stars e forks ao longo do tempo, que
# MAGIC são o sinal mais direto de interesse, precisam de calendário e não de
# MAGIC código.
# MAGIC
# MAGIC **Commit não é a única forma de manter um projeto.** Revisão de código,
# MAGIC triagem de issue e resposta em discussão sustentam um projeto e não
# MAGIC aparecem em `fct_commit`. O bus factor daqui é o bus factor de quem
# MAGIC escreve código, que é mais estreito que o de quem mantém.
# MAGIC
# MAGIC **Parte do ecossistema não usa o GitHub para tudo.** O `apache/spark`
# MAGIC conduz discussão de issue no JIRA, então a pergunta 3 mede outra coisa
# MAGIC nele e nos projetos Apache em geral.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro da execução
# MAGIC
# MAGIC O painel muda a cada execução, porque `dias_em_aberto` cresce e a janela
# MAGIC de 45 dias anda. Guardar a data da leitura é o que permite comparar duas
# MAGIC leituras depois.

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
