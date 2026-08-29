"""Instrumentos para medir o custo de uma consulta, em vez de supor.

O projeto inteiro tem 18.673 commits, e nesse volume nenhuma decisao de
desempenho se prova: tudo cabe numa particao, nenhum shuffle vai a disco, e o
motor esconde qualquer escolha ruim atras de dados pequenos. Varios
comentarios do codigo afirmam coisas sobre custo. O de `bronze.ddl` diz que
particionar geraria arquivos pequenos demais, e o de `consumo` diz que
recalcular a visao e barato. Nenhum tinha numero atras.

Este modulo existe para trocar as afirmacoes por medidas. Ele nao otimiza
nada: ele instrumenta.

## As tres perguntas que ele responde

1. **Como esta guardado?** `detalhe_da_tabela` le numero e tamanho medio de
   arquivo. E o que torna "arquivos pequenos demais" verificavel.
2. **O dado e desbalanceado?** `sql_distribuicao` mede a concentracao por
   chave. Um `duckdb/duckdb` com 31% dos commits nao e curiosidade: e a task
   que vai demorar cinco vezes mais que as outras num agrupamento por `repo`.
3. **Quanto custa, de fato?** `medir` cronometra com o resultado
   materializado, e `resumo_do_plano` diz o que o motor decidiu fazer.

## Por que o `noop` e nao o `count()`

Cronometrar `spark.sql(...).count()` mede a consulta errada. O otimizador sabe
que so a contagem importa e poda projecao, juncao e ate leitura de coluna, e o
tempo que sai e de um plano que ninguem vai executar em producao.

`write.format("noop")` materializa toda linha e todo campo, e descarta na
saida. E o unico jeito de pagar o custo real sem gravar nada.

## O que a medida nao cobre

Tempo de parede num cluster compartilhado varia com vizinho, cache de disco e
estado do motor. Duas execucoes seguidas da mesma consulta nao dao o mesmo
numero, e a primeira quase sempre e a mais lenta. Por isso `medir` repete e
devolve a mediana, e por isso comparacao vale entre casos medidos na mesma
sessao, nunca entre execucoes de dias diferentes.
"""

from __future__ import annotations

import contextlib
import io
import re
import time
from dataclasses import dataclass
from statistics import median

# Sufixo da coluna que a replica acrescenta a chave natural. Precisa ser
# improvavel no dado real: se aparecer num `sha` de verdade, duas linhas
# distintas da replica colidem e a deduplicacao passa a mentir.
MARCA_REPLICA = "--replica-"


# --------------------------------------------------------------------------
# Como a tabela esta guardada
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Detalhe:
    """O que `DESCRIBE DETAIL` diz sobre o armazenamento de uma tabela."""

    tabela: str
    arquivos: int
    bytes_totais: int
    linhas: int | None = None

    @property
    def bytes_por_arquivo(self) -> float:
        """Tamanho medio. E o numero que diz se ha problema de arquivo pequeno.

        A referencia usual do Delta e a faixa de dezenas a centenas de MB. Bem
        abaixo disso, o custo de abrir e listar arquivo passa a dominar o de
        ler conteudo, e nenhuma particao ajuda.
        """
        if self.arquivos == 0:
            return 0.0
        return self.bytes_totais / self.arquivos

    @property
    def mb_por_arquivo(self) -> float:
        return self.bytes_por_arquivo / (1024 * 1024)


def detalhe_da_tabela(spark, tabela: str, contar: bool = False) -> Detalhe:
    """Le `DESCRIBE DETAIL`. Com `contar`, acrescenta a varredura de linhas.

    A contagem e opcional porque e a unica parte cara: o resto sai do
    metadado, sem tocar em dado nenhum.
    """
    linha = spark.sql(f"DESCRIBE DETAIL {tabela}").collect()[0]

    return Detalhe(
        tabela=tabela,
        arquivos=int(linha["numFiles"]),
        bytes_totais=int(linha["sizeInBytes"]),
        linhas=spark.table(tabela).count() if contar else None,
    )


# --------------------------------------------------------------------------
# Desbalanceamento
# --------------------------------------------------------------------------

def sql_distribuicao(tabela: str, chave: str, limite: int = 20) -> str:
    """Concentracao de linhas por valor da chave.

    `vezes_a_media` e a coluna que importa. Num agrupamento ou juncao por essa
    chave, cada valor distinto vira uma particao de shuffle, e a particao que
    tem cinco vezes a media leva cinco vezes o tempo. O estagio inteiro so
    termina quando ela terminar, entao o desbalanceamento vira tempo de
    parede, nao apenas memoria.

    Serve para escolher a chave: `(repo, sha)` distribui bem porque quase toda
    chave e unica; `repo` sozinho concentra, porque um repositorio responde por
    quase um terco do dado.
    """
    return f"""
        WITH por_chave AS (
            SELECT {chave} AS chave, count(*) AS linhas
            FROM {tabela}
            GROUP BY {chave}
        ),
        referencia AS (
            SELECT sum(linhas) AS total, avg(linhas) AS media,
                   max(linhas) AS maior, count(*) AS chaves
            FROM por_chave
        )
        SELECT p.chave                                       AS chave,
               p.linhas                                      AS linhas,
               round(100.0 * p.linhas / r.total, 1)          AS pct_do_total,
               round(p.linhas / nullif(r.media, 0), 1)       AS vezes_a_media,
               r.chaves                                      AS chaves_distintas
        FROM por_chave p CROSS JOIN referencia r
        ORDER BY linhas DESC
        LIMIT {limite}
    """


# --------------------------------------------------------------------------
# Escala sintetica
# --------------------------------------------------------------------------

def replicar(df, fator: int, chave: str):
    """Multiplica o volume preservando a distribuicao por repositorio.

    Transformacao pura de DataFrame, sem I/O, pelo motivo da entrada 4 do
    diario: separar leitura de transformacao e o que torna a regra testavel
    sem tocar o disco.

    **A chave natural muda e o resto nao.** Replicar sem mexer na chave
    produziria N copias da mesma linha, que a deduplicacao e o `MERGE`
    colapsariam de volta em uma: o volume subiria na origem e nao no destino,
    e a medida sairia sem sentido. Acrescentar o numero da copia a chave
    mantem as linhas distintas.

    **`repo` fica intacto de proposito.** E o que preserva o desbalanceamento
    real: se `duckdb/duckdb` responde por 31% das linhas hoje, responde por
    31% depois de replicar. Uma escala que distribui uniformemente mediria um
    dado que nao existe.
    """
    # A validacao vem antes do import para o erro de uso ser alcancavel sem
    # motor: a suite rapida da CI roda sem pyspark (decisao 8.11).
    if fator < 1:
        raise ValueError("fator precisa ser 1 ou mais")

    from pyspark.sql import functions as F

    copias = F.explode(F.sequence(F.lit(1), F.lit(fator))).alias("_copia")

    return (
        df.select("*", copias)
        .withColumn(
            chave,
            F.when(F.col("_copia") == 1, F.col(chave)).otherwise(
                F.concat(F.col(chave), F.lit(MARCA_REPLICA), F.col("_copia"))
            ),
        )
        .drop("_copia")
    )


# --------------------------------------------------------------------------
# Cronometragem
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Medicao:
    """Quanto um caso custou, em segundos."""

    nome: str
    amostras: tuple[float, ...]

    @property
    def mediana(self) -> float:
        return median(self.amostras)

    @property
    def primeira(self) -> float:
        """A execucao fria. Costuma ser a mais lenta, e e a que enganaria."""
        return self.amostras[0]

    def __str__(self) -> str:
        return (
            f"{self.nome:<40} {self.mediana:7.2f}s "
            f"(fria {self.primeira:.2f}s, {len(self.amostras)} amostras)"
        )


def _materializar(spark, sql: str) -> None:
    """Executa a consulta produzindo toda linha e todo campo, sem gravar."""
    spark.sql(sql).write.format("noop").mode("overwrite").save()


def medir(
    spark, nome: str, sql: str, repeticoes: int = 3, aquecer: bool = True
) -> Medicao:
    """Cronometra a consulta com o resultado materializado e descartado.

    O `noop` existe para isto: escrever num destino que nao guarda nada obriga
    o motor a produzir toda linha e todo campo, sem o custo de gravacao e sem
    a poda que um `count()` provocaria.

    Repete porque a primeira execucao paga o que as seguintes nao pagam --
    plano, metadado e cache de arquivo. Devolve todas as amostras, para que
    quem le decida se a diferenca entre a fria e a mediana e o achado.

    **`aquecer` existe por causa de uma medida falsa.** A primeira execucao do
    experimento comparou a mesma leitura em tres formas de armazenamento e
    devolveu 0,92s, 0,89s e 0,83s, sugerindo 10% de ganho. Nao havia ganho: a
    distancia entre a fria e a mediana caiu 0,12, 0,06 e 0,00 na mesma ordem,
    que e a assinatura da sessao esquentando, e a tabela agrupada tinha um
    arquivo so, sem nada para o data skipping pular.

    Uma execucao descartada antes de cronometrar poe todos os casos no mesmo
    regime termico. Sem ela, `comparar` mede a ordem em que os casos foram
    escritos, e o ultimo sempre parece o melhor.
    """
    if repeticoes < 1:
        raise ValueError("repeticoes precisa ser 1 ou mais")

    if aquecer:
        _materializar(spark, sql)

    amostras = []
    for _ in range(repeticoes):
        inicio = time.perf_counter()
        _materializar(spark, sql)
        amostras.append(time.perf_counter() - inicio)

    return Medicao(nome=nome, amostras=tuple(amostras))


def comparar(spark, casos: dict, repeticoes: int = 3) -> tuple[Medicao, ...]:
    """Mede varios casos na mesma sessao, que e a unica comparacao valida.

    Tempo de parede varia com o estado do cluster e com o vizinho, entao
    numero medido hoje nao se compara com numero anotado semana passada. Medir
    os casos em sequencia e o que mantem a comparacao honesta.
    """
    return tuple(
        medir(spark, nome, sql, repeticoes) for nome, sql in casos.items()
    )


# --------------------------------------------------------------------------
# O que o motor decidiu fazer
# --------------------------------------------------------------------------

# Operadores que respondem as perguntas de custo. `Exchange` e o shuffle, que
# e a operacao cara; os dois tipos de juncao dizem se o motor conseguiu
# transmitir o lado pequeno ou teve de ordenar os dois.
OPERADORES = (
    "Exchange",
    "BroadcastHashJoin",
    "SortMergeJoin",
    "BroadcastExchange",
    "HashAggregate",
    "SortAggregate",
    "Sort",
    "Window",
    "Filter",
    "Scan",
)


def plano(spark, sql: str, modo: str = "formatted") -> str:
    """Texto do plano fisico da consulta, sem executa-la.

    `DataFrame.explain` imprime em vez de devolver, e a captura de `stdout` e
    o caminho que usa so API publica. A alternativa seria chamar o objeto Java
    por dentro (`_jdf.queryExecution()`), que da o texto direto e quebra
    quando a versao do motor muda, e o runtime do Databricks e mais novo que
    o pyspark do venv (decisao 8.9).
    """
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        spark.sql(sql).explain(mode=modo)
    return buffer.getvalue()


# No modo `formatted`, o texto tem duas partes: a arvore e, depois, um bloco
# de detalhe por operador, aberto por uma linha como `(7) HashAggregate`.
# Contar o texto inteiro conta cada operador duas vezes.
INICIO_DO_DETALHE = re.compile(r"^\(\d+\) ", re.MULTILINE)


def somente_a_arvore(texto: str) -> str:
    """Corta o texto do plano antes da secao de detalhe.

    Em `simple`, que nao tem secao de detalhe, devolve o texto inteiro. E o
    que deixa `resumo_do_plano` responder o mesmo nos dois modos.
    """
    achado = INICIO_DO_DETALHE.search(texto)
    return texto[: achado.start()] if achado else texto


def resumo_do_plano(texto: str) -> dict:
    """Quantas vezes cada operador aparece no plano.

    Funcao pura sobre o texto, e nao sobre a sessao, por dois motivos. Ela fica
    testavel sem motor, e a leitura do plano deixa de exigir olho treinado:
    `Exchange` em 4 responde "ha quatro shuffles" sem que ninguem precise
    procurar a palavra no meio de duzentas linhas.

    **Conta so a arvore.** A primeira versao contava o texto inteiro e devolvia
    o dobro no modo `formatted`, que imprime a arvore e depois um bloco de
    detalhe por operador. O erro apareceu ao conferir `Scan` contra o numero
    de tabelas de cada consulta: tres tabelas devolviam seis varreduras, nas
    tres consultas medidas.

    Um fator constante se cancelava em `diferenca_de_plano`, que era o uso
    pretendido, entao o defeito so atingia a leitura absoluta,
    que foi exatamente como o resultado acabou sendo apresentado.

    Conta ocorrencia de texto, e nao no de arvore. Serve para comparar dois
    planos da mesma consulta antes e depois de uma mudanca; nao serve como
    analise estrutural.
    """
    arvore = somente_a_arvore(texto)

    return {
        operador: arvore.count(operador)
        for operador in OPERADORES
        if arvore.count(operador) > 0
    }


def diferenca_de_plano(antes: str, depois: str) -> dict:
    """O que mudou entre dois planos, operador a operador.

    E a leitura que interessa numa otimizacao: nao o plano novo, e sim o que
    saiu e o que entrou. Um `Exchange` a menos e o ganho; um
    `BroadcastHashJoin` que virou `SortMergeJoin` e a regressao que passaria
    despercebida se so o tempo fosse olhado.
    """
    a, b = resumo_do_plano(antes), resumo_do_plano(depois)

    return {
        operador: (a.get(operador, 0), b.get(operador, 0))
        for operador in sorted(set(a) | set(b))
        if a.get(operador, 0) != b.get(operador, 0)
    }
