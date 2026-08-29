# Databricks notebook source
# MAGIC %md
# MAGIC # 14 - Desempenho
# MAGIC
# MAGIC O projeto tem 18.673 commits. Nesse volume nenhuma decisao de custo se
# MAGIC prova: tudo cabe numa particao, nenhum shuffle vai a disco, e o motor
# MAGIC esconde qualquer escolha ruim atras de dados pequenos.
# MAGIC
# MAGIC Varios comentarios do codigo afirmam coisas sobre custo. O de
# MAGIC `bronze.ddl` diz que particionar por repositorio geraria arquivos
# MAGIC pequenos demais. O de `consumo` diz que recalcular a visao e
# MAGIC irrelevante. Nenhum tinha numero atras.
# MAGIC
# MAGIC Este notebook troca as afirmacoes por medidas, em cinco passos:
# MAGIC
# MAGIC | Passo | Pergunta |
# MAGIC |---|---|
# MAGIC | 1 | Como as tabelas estao guardadas hoje? |
# MAGIC | 2 | O dado e desbalanceado, e por qual chave? |
# MAGIC | 3 | O que o motor decide fazer nas consultas do painel? |
# MAGIC | 4 | O que muda quando o volume cresce? |
# MAGIC | 5 | `OPTIMIZE` e `CLUSTER BY` pagam? |
# MAGIC
# MAGIC **Custo.** O passo 4 grava uma tabela de escala. Com o fator padrao sao
# MAGIC cerca de 900 mil linhas, alguns minutos de execucao e dezenas de MB. Em
# MAGIC Free Edition isso cabe; antes de subir o fator, olhe o tamanho medido
# MAGIC no passo 1 e multiplique.
# MAGIC
# MAGIC **O que a medida nao cobre.** Tempo de parede varia com o estado do
# MAGIC cluster. Comparacao vale entre casos medidos na mesma sessao, nunca
# MAGIC entre execucoes de dias diferentes.

# COMMAND ----------

import os
import sys

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import analises, bronze, config, consumo, desempenho, gold, ingestao, silver

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.text("fator", "50", "Fator de replicacao")
dbutils.widgets.dropdown("limpar", "nao", ["nao", "sim"], "Apagar as tabelas de escala")

FATOR = int(dbutils.widgets.get("fator"))
LIMPAR = dbutils.widgets.get("limpar") == "sim"

# A tabela de escala e rascunho: prefixo proprio, fora da lista de retencao de
# `manutencao.tabelas_gerenciadas()`, e apagavel pelo widget.
TABELA_ESCALA = config.fqn(config.BRONZE, "escala_commits")
TABELA_AGRUPADA = config.fqn(config.BRONZE, "escala_commits_agrupada")

print(f"fator = {FATOR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Como as tabelas estao guardadas
# MAGIC
# MAGIC `DESCRIBE DETAIL` sai do metadado, sem ler dado nenhum. A coluna que
# MAGIC importa e o tamanho medio por arquivo: a referencia usual do Delta e a
# MAGIC faixa de dezenas a centenas de MB, e bem abaixo disso o custo de abrir
# MAGIC e listar arquivo passa a dominar o de ler conteudo.

# COMMAND ----------

ALVOS = [
    bronze.nome_tabela(ingestao.ENDPOINTS["commits"]),
    bronze.nome_tabela(ingestao.ENDPOINTS["issues"]),
    silver.TABELA_COMMITS,
    gold.TABELA_FCT_COMMIT,
    gold.TABELA_FCT_ISSUE,
]

print(f"{'tabela':<45} {'arquivos':>9} {'MB total':>10} {'MB/arquivo':>11}")
print("-" * 78)

for tabela in ALVOS:
    if not spark.catalog.tableExists(tabela):
        continue
    d = desempenho.detalhe_da_tabela(spark, tabela)
    mb = d.bytes_totais / (1024 * 1024)
    print(f"{tabela:<45} {d.arquivos:>9} {mb:>10.1f} {d.mb_por_arquivo:>11.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Medido em 2026-08-29, e o resultado refutou a hipotese que estava
# MAGIC escrita aqui.** O texto anterior dizia que, se o tamanho medio
# MAGIC estivesse abaixo da faixa util, a carga incremental semanal seria a
# MAGIC causa e a resposta seria compactar.
# MAGIC
# MAGIC | tabela | arquivos | MB | MB/arquivo |
# MAGIC |---|---|---|---|
# MAGIC | `bronze.commits` | 5 | 15,7 | 3,15 |
# MAGIC | `bronze.issues` | 8 | 144,1 | 18,01 |
# MAGIC | `silver.commits` | 4 | 4,9 | 1,22 |
# MAGIC | `gold.fct_commit` | 1 | 0,5 | 0,48 |
# MAGIC
# MAGIC Nao ha fragmentacao: sao de 1 a 8 arquivos por tabela. O tamanho medio
# MAGIC e pequeno porque **a tabela inteira e menor que um arquivo ideal**, e
# MAGIC nao porque foi quebrada em pedacos. Compactar nao tem o que juntar.
# MAGIC
# MAGIC Licao sobre o proprio instrumento: `mb_por_arquivo` sozinho engana.
# MAGIC `fct_commit` mostra 0,48 e parece problema classico de arquivo pequeno;
# MAGIC e um arquivo com a tabela toda dentro. A metrica so significa alguma
# MAGIC coisa lida junto com a contagem.
# MAGIC
# MAGIC O que a medida confirma e o comentario de `bronze.ddl`: 15,7 MB em 14
# MAGIC repositorios dariam ~1,1 MB por particao, contra os 3,15 atuais.
# MAGIC Particionar de fato triplicaria a contagem. A afirmacao estava certa e
# MAGIC agora tem numero.
# MAGIC
# MAGIC E aparece uma assimetria que ninguem tinha notado: `bronze.issues` tem
# MAGIC 9x o tamanho de `bronze.commits`. Payload maior, e log de versoes com
# MAGIC `dt` na chave. E ali que o crescimento vai doer primeiro.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Onde o dado e desbalanceado
# MAGIC
# MAGIC Cada valor distinto da chave vira uma particao de shuffle. A particao
# MAGIC com cinco vezes a media leva cinco vezes o tempo, e o estagio inteiro
# MAGIC so termina quando ela terminar: desbalanceamento vira tempo de parede,
# MAGIC nao apenas memoria.
# MAGIC
# MAGIC As duas chaves abaixo estao no mesmo pipeline e se comportam de formas
# MAGIC opostas.

# COMMAND ----------

# MAGIC %md
# MAGIC **Por `repo`.** E a chave de todo agrupamento das analises: bus factor,
# MAGIC ritmo, ciclo de issues. Aqui deve aparecer concentracao.

# COMMAND ----------

display(spark.sql(desempenho.sql_distribuicao(silver.TABELA_COMMITS, "repo")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Por `sha`.** E a chave da deduplicacao da bronze, em
# MAGIC `Window.partitionBy(repo, sha)`. Cardinalidade altissima, uma linha por
# MAGIC chave, `vezes_a_media` em 1: shuffle sobre o historico inteiro, e sem
# MAGIC desbalanceamento nenhum.
# MAGIC
# MAGIC A conclusao de projeto e que os dois shuffles do pipeline sao caros por
# MAGIC motivos diferentes. O da deduplicacao e caro por volume; o das analises
# MAGIC e caro por concentracao. Otimizacao que sirva para um nao serve para o
# MAGIC outro.

# COMMAND ----------

display(spark.sql(desempenho.sql_distribuicao(silver.TABELA_COMMITS, "sha", limite=5)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. O que o motor decide fazer
# MAGIC
# MAGIC `resumo_do_plano` conta os operadores que respondem as perguntas de
# MAGIC custo. `Exchange` e o shuffle; `BroadcastHashJoin` contra
# MAGIC `SortMergeJoin` diz se o motor conseguiu transmitir o lado pequeno ou
# MAGIC teve de ordenar os dois.

# COMMAND ----------

CONSULTAS = {
    "bus_factor": analises.bus_factor(),
    "ritmo_por_autor": analises.ritmo_por_autor(),
    "ciclo_de_issues": analises.ciclo_de_issues(),
    "painel (as tres juntas)": consumo.painel_com_portao(),
}

planos = {}
for nome, sql in CONSULTAS.items():
    planos[nome] = desempenho.plano(spark, sql)
    print(f"{nome:<26} {desempenho.resumo_do_plano(planos[nome])}")

# COMMAND ----------

# MAGIC %md
# MAGIC O painel compoe tres consultas, entao a soma dos operadores dele diz se
# MAGIC o motor reaproveita alguma coisa ou paga cada uma inteira. Se o total
# MAGIC for a soma exata das partes, nao ha reaproveitamento, e materializar as
# MAGIC visoes menores passa a ser uma opcao real.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. O que muda quando o volume cresce
# MAGIC
# MAGIC `replicar` multiplica as linhas preservando a distribuicao por
# MAGIC repositorio, e muda a chave natural para as copias nao colapsarem na
# MAGIC deduplicacao. Se `duckdb/duckdb` responde por 31% hoje, responde por
# MAGIC 31% depois: e o desbalanceamento que se quer medir, e diluir seria
# MAGIC medir um dado que nao existe.

# COMMAND ----------

origem = spark.table(silver.TABELA_COMMITS)

(
    desempenho.replicar(origem, FATOR, "sha")
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_ESCALA)
)

detalhe = desempenho.detalhe_da_tabela(spark, TABELA_ESCALA, contar=True)
print(f"linhas    : {detalhe.linhas:,}")
print(f"arquivos  : {detalhe.arquivos}")
print(f"MB total  : {detalhe.bytes_totais / (1024 * 1024):.1f}")
print(f"MB/arquivo: {detalhe.mb_por_arquivo:.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC A distribuicao precisa ter sobrevivido. Se `pct_do_total` do maior
# MAGIC repositorio mudou, a replica diluiu o desbalanceamento e as medidas
# MAGIC seguintes nao valem.

# COMMAND ----------

display(spark.sql(desempenho.sql_distribuicao(TABELA_ESCALA, "repo", limite=5)))

# COMMAND ----------

# MAGIC %md
# MAGIC Agora a mesma consulta nos dois volumes. O que interessa nao e o tempo
# MAGIC absoluto: e se ele cresce junto com o dado ou mais rapido que ele.
# MAGIC Crescimento proporcional e varredura; crescimento maior que
# MAGIC proporcional e shuffle desbalanceado ou derrame para disco.

# COMMAND ----------

def agrupamento(tabela):
    """A forma das analises: agregacao por repositorio, que e a chave concentrada."""
    return f"""
        SELECT repo, count(*) AS commits, count(DISTINCT autor_email) AS autores
        FROM {tabela}
        GROUP BY repo
    """


medicoes = desempenho.comparar(
    spark,
    {
        f"agrupar por repo ({origem.count():,} linhas)":
            agrupamento(silver.TABELA_COMMITS),
        f"agrupar por repo ({detalhe.linhas:,} linhas)":
            agrupamento(TABELA_ESCALA),
    },
)

for medicao in medicoes:
    print(medicao)

razao_tempo = medicoes[1].mediana / medicoes[0].mediana
print(f"\nvolume x{FATOR}, tempo x{razao_tempo:.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `OPTIMIZE` e `CLUSTER BY` pagam?
# MAGIC
# MAGIC Duas coisas diferentes que costumam ser confundidas.
# MAGIC
# MAGIC | | O que faz | Ajuda quando |
# MAGIC |---|---|---|
# MAGIC | `OPTIMIZE` | junta arquivos pequenos em maiores | ha muitos arquivos pequenos |
# MAGIC | `CLUSTER BY` | agrupa linhas da mesma chave nos mesmos arquivos | ha filtro por essa chave |
# MAGIC
# MAGIC O comentario de `bronze.ddl` ja aponta para `CLUSTER BY` em vez de
# MAGIC particao fisica, e a diferenca esta em quem decide o corte: particao
# MAGIC fisica fixa um diretorio por valor, e o clustering deixa o motor
# MAGIC agrupar sem se comprometer com o numero de arquivos.

# COMMAND ----------

antes_arquivos = desempenho.detalhe_da_tabela(spark, TABELA_ESCALA).arquivos
antes_leitura = desempenho.medir(
    spark, "leitura filtrada, antes",
    f"SELECT * FROM {TABELA_ESCALA} WHERE repo = 'duckdb/duckdb'",
)

spark.sql(f"OPTIMIZE {TABELA_ESCALA}")

depois_arquivos = desempenho.detalhe_da_tabela(spark, TABELA_ESCALA).arquivos
depois_leitura = desempenho.medir(
    spark, "leitura filtrada, depois",
    f"SELECT * FROM {TABELA_ESCALA} WHERE repo = 'duckdb/duckdb'",
)

print(f"arquivos: {antes_arquivos} -> {depois_arquivos}")
print(antes_leitura)
print(depois_leitura)

# COMMAND ----------

# MAGIC %md
# MAGIC Agora com clustering pela chave do filtro. `OPTIMIZE` sozinho junta
# MAGIC arquivo sem olhar conteudo; com `CLUSTER BY`, as linhas do mesmo
# MAGIC repositorio ficam juntas e o motor pode pular arquivo inteiro na
# MAGIC leitura filtrada.

# COMMAND ----------

# Tabela separada, e nao `ALTER` na existente, por dois motivos. Nem todo
# runtime aceita ligar clustering numa tabela criada sem ele, e manter as duas
# permite medir a mesma consulta nas duas formas na mesma sessao, que e a
# unica comparacao valida de tempo de parede.
spark.sql(f"""
    CREATE OR REPLACE TABLE {TABELA_AGRUPADA}
    CLUSTER BY (repo)
    AS SELECT * FROM {TABELA_ESCALA}
""")
spark.sql(f"OPTIMIZE {TABELA_AGRUPADA}")

agrupado_arquivos = desempenho.detalhe_da_tabela(spark, TABELA_AGRUPADA).arquivos
agrupado_leitura = desempenho.medir(
    spark, "leitura filtrada, agrupada",
    f"SELECT * FROM {TABELA_AGRUPADA} WHERE repo = 'duckdb/duckdb'",
)

print(f"arquivos: {depois_arquivos} -> {agrupado_arquivos}")
print(agrupado_leitura)

# COMMAND ----------

# MAGIC %md
# MAGIC As tres leituras medem a mesma consulta sobre o mesmo dado, mudando so
# MAGIC como ele esta guardado. O que separa os dois ganhos:
# MAGIC
# MAGIC | Comparacao | O que ela isola |
# MAGIC |---|---|
# MAGIC | antes vs depois | so compactacao: menos arquivo para abrir |
# MAGIC | depois vs agrupada | data skipping: arquivo inteiro pulado pelo filtro |
# MAGIC
# MAGIC Se a segunda nao melhorar, a conclusao nao e que clustering nao serve:
# MAGIC e que neste volume o filtro por `repo` ja custava pouco. Registre isso
# MAGIC em vez de repetir a recomendacao generica.

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que registrar
# MAGIC
# MAGIC Os numeros desta execucao vao para a secao 8.13 do `PROJETO.md`. O que
# MAGIC vale registrar nao e o tempo, que muda a cada sessao, e sim:
# MAGIC
# MAGIC - o tamanho medio de arquivo das tabelas reais, que decide se ha
# MAGIC   problema de arquivo pequeno hoje
# MAGIC - a concentracao por `repo`, que e uma propriedade do ecossistema e nao
# MAGIC   do cluster
# MAGIC - a **razao** entre volume e tempo, que e comparavel entre execucoes
# MAGIC - o que mudou no plano, se algo mudou
# MAGIC
# MAGIC Se uma medida contradisser um comentario do codigo, o comentario e que
# MAGIC esta errado. Corrija-o citando o numero.

# COMMAND ----------

if LIMPAR:
    for tabela in (TABELA_ESCALA, TABELA_AGRUPADA):
        spark.sql(f"DROP TABLE IF EXISTS {tabela}")
        print(f"{tabela} apagada")
else:
    print("tabelas de escala mantidas; use o widget `limpar` para apagar")
