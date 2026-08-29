# Databricks notebook source
# MAGIC %md
# MAGIC # 13 - Manutencao
# MAGIC
# MAGIC Restricoes do motor, politica de retencao e leitura do historico.
# MAGIC
# MAGIC O raciocinio esta no docstring de `src/radar/manutencao.py`. O resumo:
# MAGIC
# MAGIC | Camada | Mecanismo | Porque |
# MAGIC |---|---|---|
# MAGIC | bronze e silver | quarentena | a violacao vem da origem, e uma linha suja nao pode derrubar a carga |
# MAGIC | gold | `CHECK` | a violacao e defeito de derivacao nosso, e gravar defeito deve abortar |
# MAGIC
# MAGIC Esta tarefa e idempotente e barata: nao le dado, so metadado.

# COMMAND ----------

import os
import sys

REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
if f"{REPO}/src" not in sys.path:
    sys.path.insert(0, f"{REPO}/src")

from radar import bronze, ingestao, manutencao

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.dropdown("recriar", "nao", ["nao", "sim"], "Recriar restricoes")
RECRIAR = dbutils.widgets.get("recriar") == "sim"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restricoes
# MAGIC
# MAGIC O Delta valida as linhas ja gravadas antes de aceitar a restricao,
# MAGIC entao a primeira execucao tambem e uma auditoria do que esta nas
# MAGIC tabelas hoje. Uma falha aqui nao e problema de configuracao: e uma
# MAGIC invariante do modelo que o dado real viola.
# MAGIC
# MAGIC Use `recriar = sim` quando a expressao de alguma restricao mudar. Sem
# MAGIC isso, a antiga continua valendo enquanto o codigo diz outra coisa.

# COMMAND ----------

resultado = manutencao.aplicar_restricoes(spark, recriar=RECRIAR)

for chave, estado in resultado.items():
    print(f"{estado:<12} {chave}")

falhas = {k: v for k, v in resultado.items() if v.startswith("FALHOU")}

# COMMAND ----------

# MAGIC %md
# MAGIC A falha e bloqueante de proposito. Uma restricao recusada significa que
# MAGIC a gold tem linha que o modelo declara impossivel, e seguir a partir
# MAGIC dali gravaria mais uma camada em cima do defeito.

# COMMAND ----------

if falhas:
    detalhe = "\n  ".join(f"{k}: {v}" for k, v in falhas.items())
    raise RuntimeError(f"{len(falhas)} restricao(oes) recusada(s):\n  {detalhe}")

print(f"{len(resultado)} restricoes no lugar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retencao
# MAGIC
# MAGIC 14 dias de arquivo apagado, contra os 7 do padrao. O pipeline e semanal
# MAGIC (decisao 8.10), entao 7 dariam menos de um ciclo: carga defeituosa no
# MAGIC domingo, notada na leitura seguinte, e a janela para voltar atras ja
# MAGIC teria fechado.
# MAGIC
# MAGIC Retencao e time travel sao a mesma decisao vista de dois lados.

# COMMAND ----------

# As bronzes entram por endpoint, porque os nomes dependem do `Endpoint`.
TABELAS = manutencao.tabelas_gerenciadas() + tuple(
    bronze.nome_tabela(e) for e in ingestao.ENDPOINTS.values()
)

for tabela in manutencao.aplicar_retencao(spark, TABELAS):
    print(tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que o historico mostra
# MAGIC
# MAGIC `DESCRIBE HISTORY` ja sustentou a prova de idempotencia da secao 10.5:
# MAGIC sete versoes da bronze, a primeira inserindo 200 linhas e as seis
# MAGIC seguintes inserindo zero sobre a mesma origem.
# MAGIC
# MAGIC Aqui a leitura fica disponivel para qualquer tabela, com as metricas da
# MAGIC operacao ja extraidas do mapa.

# COMMAND ----------

ALVO = bronze.nome_tabela(ingestao.ENDPOINTS["commits"])

display(spark.sql(manutencao.sql_historico(ALVO)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel: o que a tabela dizia antes
# MAGIC
# MAGIC E a pergunta que o projeto tem e que so o time travel responde. A
# MAGIC recuperacao da secao 5.7 levou a bronze de 5.646 para 18.537 linhas, e
# MAGIC a proporcao de commits de bot caiu de 10,5% para 4,9%: as linhas
# MAGIC antigas estavam todas certas e a conclusao tirada delas estava errada
# MAGIC por um fator de dois.
# MAGIC
# MAGIC Enquanto as duas versoes couberem na retencao, as duas leituras existem
# MAGIC ao mesmo tempo, e a comparacao deixa de depender do que ficou anotado
# MAGIC no documento.
# MAGIC
# MAGIC Nao ha versao fixa no codigo de proposito: qual delas interessa depende
# MAGIC de quando se pergunta. A celula abaixo pega as cinco mais recentes.

# COMMAND ----------

versoes = [
    linha["versao"]
    for linha in spark.sql(manutencao.sql_historico(ALVO, limite=5)).collect()
]

display(spark.sql(manutencao.sql_contagem_por_versao(ALVO, versoes)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM
# MAGIC
# MAGIC Nao roda aqui, e a ausencia e a decisao.
# MAGIC
# MAGIC O `VACUUM` apaga exatamente o que o time travel usaria, e nao tem
# MAGIC desfazer. Numa tarefa agendada ele rodaria sem ninguem olhando, na
# MAGIC mesma execucao em que uma carga defeituosa acabou de gravar, e
# MAGIC destruiria a versao boa junto.
# MAGIC
# MAGIC O volume atual tambem nao justifica: sao poucos MB, e arquivo obsoleto
# MAGIC nao chega perto de custar o que custa perder a possibilidade de voltar.
# MAGIC
# MAGIC O comando fica montado para quando houver motivo, e roda a mao:
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(manutencao.sql_vacuum(ALVO))
# MAGIC ```
# MAGIC
# MAGIC Sem `RETAIN`, ele usa a propriedade da tabela, que e onde a politica
# MAGIC esta declarada. Passar horas na chamada contradiria a declaracao.
