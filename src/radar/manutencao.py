"""Restricoes do motor, retencao e leitura do historico das tabelas.

Tres assuntos que o Delta oferece e o projeto nao usava. O primeiro exigiu uma
decisao de fronteira que vale mais que o recurso.

## Onde uma restricao CHECK cabe, e onde ela e o instrumento errado

A bateria de qualidade ja verifica invariante. A pergunta e por que
acrescentar um mecanismo que faz algo parecido, e a resposta e que os dois
falham de formas opostas:

| | CHECK | bateria |
|---|---|---|
| quando age | na escrita | depois da carga |
| ao violar | aborta a transacao inteira | quarentena a linha e segue |
| granularidade | tudo ou nada | por linha, com motivo |

Abortar tudo por causa de uma linha e o comportamento que a secao 10.4 recusa
quando o dado vem de fora: e o mesmo argumento de `try_to_timestamp` contra
`to_timestamp`, onde um registro torto derrubaria a carga inteira.

Mas ele deixa de ser errado quando a violacao nao pode vir da origem. A gold
nao ingere nada: ela e derivada pelo nosso proprio codigo a partir da silver.
Uma linha invalida ali nao e sujeira que chegou, e defeito de derivacao, e
carga que grava defeito de derivacao deve mesmo abortar inteira.

Dai a fronteira adotada:

    bronze e silver -> quarentena, porque a violacao vem da origem
    gold            -> CHECK, porque a violacao e nossa

E a mesma divisao que ja existia sem nome. As baterias da bronze e da silver
falam de conteudo; as da gold falam de integridade do modelo.

**O que foi deixado de fora de proposito.** `dias_ate_o_commit >= 0` parece
obvio e nao entra: o valor sai de duas datas da origem, e relogio de
contribuidor adiantado produz negativo legitimo. Ali a violacao volta a ser
sujeira, e a resposta certa continua sendo medir e reportar, nao abortar a
carga semanal.

## O que as restricoes escolhidas pegam

As contagens com `>= 0` cobrem uma familia de defeito que ja custou duas
entradas do diario, a 10 e a 24: `size(NULL)` devolve `-1` em modo legado, e
nao `NULL`, entao a guarda por `coalesce` nao dispara e o valor negativo
atravessa qualquer verificacao de nulo. Nas duas vezes quem pegou foi um
teste, nunca o dado. Uma restricao no motor teria pego na primeira gravacao.

## Retencao

O padrao do Delta guarda arquivo apagado por 7 dias, e e esse periodo que
define ate onde o time travel alcanca. Retencao e time travel sao a mesma
decisao vista de dois lados, e quase todo mundo descobre isso apagando o que
precisava.

Sete dias nao servem aqui porque o pipeline e semanal (decisao 8.10). Uma
carga defeituosa no domingo so seria notada na leitura seguinte, e a margem
para voltar atras seria de horas. Catorze dias dao dois ciclos.
"""

from __future__ import annotations

from dataclasses import dataclass

from radar import gold, qualidade, silver, silver_issues, silver_repositorios
from radar.controle import TABELA_CONTROLE

# --------------------------------------------------------------------------
# Restricoes CHECK, so na gold
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Restricao:
    """Uma restricao CHECK, com o motivo de ela ser do motor e nao da bateria.

    `porque` nao e documentacao decorativa: e o criterio de admissao. Se a
    resposta nao for "uma violacao aqui e defeito do nosso codigo", a
    restricao nao pertence a este catalogo.
    """

    tabela: str
    nome: str
    expressao: str
    porque: str


RESTRICOES = (
    # ---- dim_tempo: gerada inteira por nos, entao tudo aqui e nosso ----
    Restricao(
        gold.TABELA_TEMPO,
        "sk_tempo_bate_com_a_data",
        "sk_tempo = year(data) * 10000 + month(data) * 100 + day(data)",
        "A chave inteligente do passo 4.2 e um contrato: os fatos calculam "
        "`sk_tempo` a partir da data em vez de buscar por juncao. Se a "
        "dimensao deixar de honrar a formula, todo fato passa a apontar para "
        "o dia errado sem erro nenhum.",
    ),
    Restricao(
        gold.TABELA_TEMPO,
        "dia_da_semana_no_intervalo_iso",
        "dia_da_semana BETWEEN 1 AND 7",
        "Derivado por nos, e a entrada 11 do diario ja mostrou que funcao de "
        "calendario erra em silencio na virada de ano.",
    ),
    # ---- dim_repositorio: as invariantes da SCD2 ----
    Restricao(
        gold.TABELA_REPOSITORIO,
        "vigencia_nao_invertida",
        "valido_ate IS NULL OR valido_ate > valido_de",
        "Versao que termina antes de comecar e defeito da derivacao da secao "
        "6.4. Nenhuma foto da origem pode produzir isso.",
    ),
    Restricao(
        gold.TABELA_REPOSITORIO,
        "flag_atual_concorda_com_valido_ate",
        "flag_atual = (valido_ate IS NULL)",
        "E a segunda das tres invariantes da SCD2, hoje verificada so depois "
        "da carga. Como restricao, a linha inconsistente nao chega a existir.",
    ),
    Restricao(
        gold.TABELA_REPOSITORIO,
        "observado_nao_precede_a_vigencia",
        "observado_de >= valido_de",
        "A vigencia da primeira versao e aberta para tras (passo 5.3), entao "
        "`valido_de` pode anteceder a primeira foto. O contrario nao: foto "
        "anterior ao inicio da vigencia seria erro de ordenacao.",
    ),
    # ---- dim_autor: o dominio da chave hibrida ----
    Restricao(
        gold.TABELA_AUTOR,
        "origem_da_chave_no_dominio",
        "origem_da_chave IN ('conta', 'email', 'desconhecida')",
        "A origem entra no hash da chave substituta (secao 6.7). Um valor "
        "fora do dominio produz chave que nenhum fato encontra.",
    ),
    # ---- contagens: a familia size(NULL) = -1, do diario 10 e 24 ----
    Restricao(
        gold.TABELA_FCT_COMMIT,
        "contagens_nao_negativas",
        "(comentarios IS NULL OR comentarios >= 0) AND "
        "(qtd_pais IS NULL OR qtd_pais >= 0)",
        "`size(NULL)` devolve -1 em modo legado. Contagem negativa atravessa "
        "qualquer verificacao de nulo, e nas duas vezes que apareceu quem "
        "pegou foi um teste, nao o dado.",
    ),
    Restricao(
        gold.TABELA_FCT_ISSUE,
        "contagens_nao_negativas",
        "(comentarios IS NULL OR comentarios >= 0) AND "
        "(qtd_rotulos IS NULL OR qtd_rotulos >= 0) AND "
        "(qtd_responsaveis IS NULL OR qtd_responsaveis >= 0)",
        "Mesma familia. `qtd_rotulos` e literalmente o caso da entrada 24, "
        "onde a guarda existia e mirava o alvo errado.",
    ),
    Restricao(
        gold.TABELA_FCT_ISSUE,
        "issue_aberta_nao_tem_prazo_de_fechamento",
        "esta_aberta = FALSE OR dias_ate_fechar IS NULL",
        "As duas colunas saem do mesmo marco na derivacao do snapshot "
        "acumulado. Divergirem significa que a projecao da secao 6.8 quebrou.",
    ),
    Restricao(
        gold.TABELA_FCT_SNAPSHOT,
        "medidas_nao_negativas",
        " AND ".join(
            f"({medida} IS NULL OR {medida} >= 0)"
            for medida in gold.MEDIDAS_SNAPSHOT
        ),
        "Stars e forks nao decrescem abaixo de zero na origem. Um negativo "
        "aqui e cast errado, que e o que a secao 7 trata.",
    ),
)


def sql_adicionar(restricao: Restricao) -> str:
    """DDL que acrescenta a restricao.

    O Delta valida as linhas ja gravadas antes de aceitar, entao o comando
    tambem funciona como auditoria do que esta na tabela hoje.
    """
    return (
        f"ALTER TABLE {restricao.tabela} "
        f"ADD CONSTRAINT {restricao.nome} CHECK ({restricao.expressao})"
    )


def sql_remover(restricao: Restricao) -> str:
    """DDL que remove a restricao. Necessario para reaplicar uma expressao."""
    return f"ALTER TABLE {restricao.tabela} DROP CONSTRAINT IF EXISTS {restricao.nome}"


def restricoes_da_tabela(spark, tabela: str) -> frozenset[str]:
    """Nomes das restricoes CHECK ja presentes na tabela.

    O Delta guarda cada uma como propriedade `delta.constraints.<nome>`, com o
    nome em minusculas.
    """
    prefixo = "delta.constraints."
    linhas = spark.sql(f"SHOW TBLPROPERTIES {tabela}").collect()
    return frozenset(
        linha["key"][len(prefixo):]
        for linha in linhas
        if linha["key"].startswith(prefixo)
    )


def aplicar_restricoes(spark, restricoes=RESTRICOES, recriar: bool = False) -> dict:
    """Aplica o catalogo. Reexecutar nao falha.

    `recriar` derruba antes de criar, que e o caminho quando a expressao
    mudou: `ADD CONSTRAINT` com nome existente e erro, e sem ele a restricao
    velha continuaria valendo enquanto o codigo diria outra coisa.

    Devolve `{nome_qualificado: 'criada' | 'ja existia' | mensagem do erro}`.
    O erro entra no resultado em vez de interromper, porque uma restricao que
    a tabela atual viola e justamente o achado que se quer ver por inteiro, e
    parar na primeira esconderia as demais.
    """
    resultado: dict[str, str] = {}

    # Uma consulta de propriedades por tabela, e nao por restricao: seis
    # tabelas respondem pelas dez do catalogo. Reler nao mudaria a decisao,
    # porque os nomes sao unicos dentro de cada tabela.
    ja_presentes = {
        tabela: restricoes_da_tabela(spark, tabela)
        for tabela in {r.tabela for r in restricoes}
    }

    for restricao in restricoes:
        chave = f"{restricao.tabela}.{restricao.nome}"
        presentes = ja_presentes[restricao.tabela]

        if restricao.nome.lower() in presentes and not recriar:
            resultado[chave] = "ja existia"
            continue

        try:
            if recriar:
                spark.sql(sql_remover(restricao))
            spark.sql(sql_adicionar(restricao))
            resultado[chave] = "criada"
        except Exception as erro:  # a mensagem do motor e o diagnostico
            resultado[chave] = f"FALHOU: {erro}"

    return resultado


# --------------------------------------------------------------------------
# Retencao
# --------------------------------------------------------------------------

# 14 dias de arquivo apagado sao dois ciclos do pipeline semanal. O padrao de
# 7 daria menos de um: carga defeituosa no domingo, notada na leitura da
# semana seguinte, e a janela para voltar atras ja teria fechado.
RETENCAO_ARQUIVOS = "interval 14 days"

# O log e barato e e ele que sustenta `DESCRIBE HISTORY`. Trinta dias mantem
# legivel o mes inteiro de operacoes mesmo depois de o arquivo sumir: da para
# saber o que aconteceu, ainda que nao de mais para consultar o conteudo.
RETENCAO_LOG = "interval 30 days"

PROPRIEDADES_RETENCAO = {
    "delta.deletedFileRetentionDuration": RETENCAO_ARQUIVOS,
    "delta.logRetentionDuration": RETENCAO_LOG,
}


def tabelas_gerenciadas() -> tuple[str, ...]:
    """As tabelas Delta persistentes do projeto.

    Duas ausencias sao deliberadas.

    A bronze nao entra pela lista fixa porque os nomes dela dependem do
    `Endpoint`, e quem os conhece e o notebook, que acrescenta os seus.

    As tabelas `lote_` tambem nao. Elas sao materializacao intermediaria do
    MERGE, reescritas inteiras a cada execucao, entao acumulam mais versoes
    que qualquer outra e nenhuma delas tem valor de auditoria. Guardar
    catorze dias de rascunho seria pagar retencao pelo que nunca sera lido.
    """
    return (
        TABELA_CONTROLE,
        qualidade.TABELA_QUALIDADE,
        silver.TABELA_COMMITS,
        silver.TABELA_REJEITADOS,
        silver_issues.TABELA_ISSUES,
        silver_issues.TABELA_PULL_REQUESTS,
        silver_issues.TABELA_REJEITADOS,
        silver_repositorios.TABELA_REPOSITORIOS,
        gold.TABELA_TEMPO,
        gold.TABELA_AUTOR,
        gold.TABELA_REPOSITORIO,
        gold.TABELA_FCT_COMMIT,
        gold.TABELA_FCT_SNAPSHOT,
        gold.TABELA_FCT_ISSUE,
    )


def sql_retencao(tabela: str) -> str:
    """DDL que fixa a politica de retencao da tabela."""
    itens = ", ".join(
        f"'{chave}' = '{valor}'" for chave, valor in PROPRIEDADES_RETENCAO.items()
    )
    return f"ALTER TABLE {tabela} SET TBLPROPERTIES ({itens})"


def aplicar_retencao(spark, tabelas=None) -> tuple[str, ...]:
    """Fixa a politica nas tabelas. Idempotente."""
    alvo = tuple(tabelas) if tabelas is not None else tabelas_gerenciadas()
    for tabela in alvo:
        spark.sql(sql_retencao(tabela))
    return alvo


def sql_vacuum(tabela: str, horas: int | None = None) -> str:
    """DDL do VACUUM.

    Sem `horas`, o motor usa a propriedade da tabela, que e o caminho certo: a
    politica fica declarada num lugar so e o comando nao a contradiz.

    O que o VACUUM apaga e exatamente o que o time travel usaria. Nao ha
    desfazer, e e por isso que o parametro existe mas nao tem padrao curto.
    """
    if horas is None:
        return f"VACUUM {tabela}"
    if horas < 0:
        raise ValueError("horas negativas")
    return f"VACUUM {tabela} RETAIN {horas} HOURS"


# --------------------------------------------------------------------------
# Time travel
# --------------------------------------------------------------------------

def sql_historico(tabela: str, limite: int = 20) -> str:
    """As ultimas operacoes da tabela, com o que cada uma escreveu.

    `operationMetrics` e um mapa de strings, e as chaves mudam conforme a
    operacao. O acesso por chave devolve NULL quando ela nao existe naquela
    linha, entao a consulta serve para MERGE, overwrite e append sem ramo.
    """
    if limite < 1:
        raise ValueError("limite precisa ser positivo")

    return f"""
        SELECT version   AS versao,
               timestamp AS quando,
               operation AS operacao,
               try_cast(operationMetrics['numOutputRows'] AS BIGINT)
                   AS linhas_escritas,
               try_cast(operationMetrics['numTargetRowsInserted'] AS BIGINT)
                   AS inseridas,
               try_cast(operationMetrics['numTargetRowsUpdated'] AS BIGINT)
                   AS atualizadas,
               try_cast(operationMetrics['numTargetFilesAdded'] AS BIGINT)
                   AS arquivos_novos
        FROM (DESCRIBE HISTORY {tabela})
        ORDER BY versao DESC
        LIMIT {limite}
    """


def sql_contagem_por_versao(tabela: str, versoes) -> str:
    """Quantas linhas a tabela tinha em cada versao, lado a lado.

    E a pergunta que o projeto tem e que so o time travel responde: o que esta
    tabela dizia antes daquela carga? A recuperacao da secao 5.7 levou a
    bronze de 5.646 para 18.537 linhas, e a conclusao sobre proporcao de bot
    mudou por um fator de dois. Enquanto as duas versoes couberem na retencao,
    as duas leituras existem ao mesmo tempo, e a comparacao deixa de depender
    do que ficou anotado no documento.

    O custo e uma varredura por versao, entao a lista e explicita: passar o
    historico inteiro leria a tabela dezenas de vezes.
    """
    numeros = [int(v) for v in versoes]
    if not numeros:
        raise ValueError("nenhuma versao pedida")
    if any(v < 0 for v in numeros):
        raise ValueError("versao negativa")

    partes = [
        f"SELECT {v} AS versao, count(*) AS linhas FROM {tabela} VERSION AS OF {v}"
        for v in numeros
    ]
    return "\nUNION ALL\n".join(partes) + "\nORDER BY versao"
