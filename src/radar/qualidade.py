"""Testes de qualidade sobre as tabelas do lakehouse.

Cada verificacao e uma consulta que conta violacoes: zero e aprovacao. O
resultado de cada execucao fica gravado, o que permite comparar o estado de
hoje com o das execucoes anteriores.

A severidade separa a falha que interrompe o pipeline da que apenas informa.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from radar import bronze, silver
from radar.config import BRONZE, REPOS, fqn
from radar.ingestao import Endpoint

TABELA_QUALIDADE = fqn(BRONZE, "qualidade_execucao")

BLOQUEIA = "bloqueia"
AVISA = "avisa"
SEVERIDADES = (BLOQUEIA, AVISA)

# As contagens de controle nao sao SQL sobre uma tabela unica, mas entram na
# bateria com o mesmo formato das demais para compartilhar o historico.
RECONCILIACAO_BRONZE = "reconciliacao_landing_bronze"
RECONCILIACAO_SILVER = "reconciliacao_bronze_silver"


@dataclass(frozen=True)
class Verificacao:
    """Uma regra. O SQL devolve uma coluna `violacoes` com a contagem."""

    nome: str
    descricao: str
    severidade: str
    sql: str


@dataclass(frozen=True)
class Resultado:
    nome: str
    severidade: str
    violacoes: int
    # Preenchidos so pela contagem de controle, onde o par origem/destino e a
    # informacao a guardar. Nulos nas demais regras, em que `esperado` seria
    # sempre 0.
    esperado: int | None = None
    obtido: int | None = None

    @property
    def passou(self) -> bool:
        return self.violacoes == 0


@dataclass(frozen=True)
class Reconciliacao:
    """Contagem de controle entre duas camadas: quanto entrou, quanto saiu."""

    nome: str
    na_origem: int
    no_destino: int

    @property
    def diferenca(self) -> int:
        return self.na_origem - self.no_destino

    @property
    def bate(self) -> bool:
        return self.diferenca == 0

    def como_resultado(self) -> Resultado:
        """Converte a contagem em uma linha da bateria, com historico.

        `abs()` porque o desvio conta nos dois sentidos: destino com menos
        linhas indica perda no caminho; com mais, origem removida ou insercao
        feita por fora do pipeline.
        """
        return Resultado(
            nome=self.nome,
            severidade=BLOQUEIA,
            violacoes=abs(self.diferenca),
            esperado=self.na_origem,
            obtido=self.no_destino,
        )


DDL_QUALIDADE = f"""
CREATE TABLE IF NOT EXISTS {TABELA_QUALIDADE} (
    executado_em TIMESTAMP COMMENT 'quando a bateria rodou',
    tabela       STRING    COMMENT 'tabela verificada',
    verificacao  STRING    COMMENT 'nome da regra',
    severidade   STRING    COMMENT 'bloqueia | avisa',
    violacoes    BIGINT    COMMENT 'linhas que violam a regra; 0 e aprovacao',
    passou       BOOLEAN   COMMENT 'violacoes = 0',
    esperado     BIGINT    COMMENT 'contagem de controle: quanto a origem tinha',
    obtido       BIGINT    COMMENT 'contagem de controle: quanto o destino tem'
)
USING DELTA
COMMENT 'Historico dos testes de qualidade. Uma linha por regra por execucao.'
"""


# --------------------------------------------------------------------------
# Funcoes puras
# --------------------------------------------------------------------------

def _lista_sql(valores) -> str:
    """Tupla Python -> lista literal de SQL."""
    return ", ".join("'" + str(v) + "'" for v in valores)


def verificacoes_bronze(endpoint: Endpoint) -> tuple[Verificacao, ...]:
    """A bateria da bronze. Nenhuma delas olha regra de negocio.

    Bronze nao limpa dado, entao aqui so cabe verificar o que a propria
    camada promete: chave presente, chave unica, proveniencia completa.
    """
    tabela = bronze.nome_tabela(endpoint)
    chave = endpoint.chave

    return (
        Verificacao(
            nome="chave_ausente_no_payload",
            descricao=(
                f"Todo payload contem `{chave}`. Violacao indica JSON "
                "invalido ou mudanca no formato da API."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE get_json_object(payload, '$.{chave}') IS NULL
            """,
        ),
        Verificacao(
            nome="chave_duplicada",
            descricao=(
                "Uma linha por (repo, chave). Verifica de fora a "
                "idempotencia garantida pelo MERGE da carga."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT repo, {chave}
                    FROM {tabela}
                    GROUP BY repo, {chave}
                    HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="proveniencia_incompleta",
            descricao=(
                "Os tres metadados de proveniencia estao preenchidos. Sem "
                "eles nao ha como rastrear a origem de uma linha."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE _ingerido_em IS NULL
                   OR _arquivo_origem IS NULL
                   OR _endpoint IS NULL
            """,
        ),
        Verificacao(
            nome="endpoint_inconsistente",
            descricao="Toda linha da tabela veio do endpoint que ela representa.",
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE _endpoint <> '{endpoint.nome}'
            """,
        ),
        Verificacao(
            nome="repo_fora_do_escopo",
            descricao=(
                "Todo repo pertence a lista do config. Violacao indica "
                "caminho mal formado ou decodificacao errada do diretorio."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE repo NOT IN ({_lista_sql(REPOS)})
            """,
        ),
        Verificacao(
            nome="data_do_registro_ausente",
            descricao=(
                f"O campo `{endpoint.campo_data}` sustenta o watermark e a "
                "tipagem da silver. Avisa em vez de bloquear: a bronze "
                "armazena o registro defeituoso para permitir investiga-lo."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE get_json_object(payload, '$.{endpoint.campo_data}') IS NULL
            """,
        ),
    )


def verificacoes_silver(endpoint: Endpoint) -> tuple[Verificacao, ...]:
    """A bateria da silver. Aqui cabem regras sobre o significado do dado.

    Sao verificacoes que a bronze nao poderia fazer: comparar duas datas
    exige que elas sejam datas, e nao texto.
    """
    tabela = silver.TABELA_COMMITS
    quarentena = silver.TABELA_REJEITADOS

    return (
        Verificacao(
            nome="chave_duplicada",
            descricao=(
                "Uma linha por (repo, sha). Verifica de fora o upsert que a "
                "carga faz por essa mesma chave."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT repo, sha FROM {tabela}
                    GROUP BY repo, sha HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="contagem_de_pais_negativa",
            descricao=(
                "`size(NULL)` devolve -1 em modo legado. Contagem negativa "
                "passaria por qualquer verificacao de nulo sem ser notada."
            ),
            severidade=BLOQUEIA,
            sql=f"SELECT count(*) AS violacoes FROM {tabela} WHERE qtd_pais < 0",
        ),
        Verificacao(
            nome="normalizacao_nao_aplicada",
            descricao=(
                "E-mail gravado exatamente como a normalizacao produziria. "
                "Violacao indica linha que entrou por fora da projecao."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE autor_email <> lower(trim(autor_email))
                   OR committer_email <> lower(trim(committer_email))
            """,
        ),
        Verificacao(
            nome="texto_vazio_em_vez_de_nulo",
            descricao=(
                "String vazia e NULL sao a mesma ausencia e comparam "
                "diferente. A silver converte uma na outra."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE mensagem = '' OR autor_nome = '' OR github_login = ''
            """,
        ),
        Verificacao(
            nome="quarentena_sem_motivo",
            descricao=(
                "Toda linha desviada diz por que foi. Quarentena sem motivo "
                "e um registro perdido com passos extras."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {quarentena}
                WHERE motivo IS NULL OR motivo NOT IN ({_lista_sql(silver.MOTIVOS_DE_REJEICAO)})
            """,
        ),
        Verificacao(
            nome="commit_anterior_a_autoria",
            descricao=(
                "A data de entrada no repositorio nao antecede a de escrita. "
                "Rebase afasta as duas, mas nunca inverte a ordem."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE commitado_em < autorado_em
            """,
        ),
        Verificacao(
            nome="data_no_futuro",
            descricao=(
                "Commit datado depois de agora. Costuma ser relogio errado na "
                "maquina de quem commitou, e distorce qualquer serie temporal."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE commitado_em > current_timestamp()
            """,
        ),
        Verificacao(
            nome="tipo_de_autor_fora_do_dominio",
            descricao=(
                "`github_tipo` pertence ao dominio conhecido. Valor novo "
                "indica categoria criada pela origem, nao defeito nosso."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE github_tipo IS NOT NULL
                  AND github_tipo NOT IN ({_lista_sql(silver.TIPOS_DE_AUTOR)})
            """,
        ),
        Verificacao(
            nome="motivo_de_assinatura_fora_do_dominio",
            descricao=(
                "`assinatura_motivo` pertence ao dominio conhecido, que o "
                "GitHub amplia de tempos em tempos."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE assinatura_motivo IS NOT NULL
                  AND assinatura_motivo NOT IN ({_lista_sql(silver.MOTIVOS_DE_ASSINATURA)})
            """,
        ),
        Verificacao(
            nome="repo_fora_do_escopo",
            descricao=(
                "Todo repo pertence a lista do config, como na bronze. "
                "Divergencia aqui apareceria entre as duas camadas."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE repo NOT IN ({_lista_sql(REPOS)})
            """,
        ),
    )


def resumo(resultados: list[Resultado]) -> tuple[int, int]:
    """(quantos bloqueios falharam, quantos avisos falharam)."""
    bloqueios = sum(1 for r in resultados if not r.passou and r.severidade == BLOQUEIA)
    avisos = sum(1 for r in resultados if not r.passou and r.severidade == AVISA)
    return bloqueios, avisos


def levantar_se_bloqueou(resultados: list[Resultado]) -> None:
    """Interrompe o pipeline se alguma regra bloqueante falhou."""
    falhas = [r for r in resultados if not r.passou and r.severidade == BLOQUEIA]
    if falhas:
        detalhe = ", ".join(f"{r.nome}={r.violacoes}" for r in falhas)
        raise AssertionError(f"qualidade reprovada: {detalhe}")


# --------------------------------------------------------------------------
# Execucao
# --------------------------------------------------------------------------

def criar_tabela(spark) -> None:
    spark.sql(DDL_QUALIDADE)


def executar(spark, verificacoes: tuple[Verificacao, ...]) -> list[Resultado]:
    """Roda a bateria e devolve um Resultado por regra."""
    return [
        Resultado(
            nome=v.nome,
            severidade=v.severidade,
            violacoes=int(spark.sql(v.sql).collect()[0]["violacoes"]),
        )
        for v in verificacoes
    ]


def registrar(
    spark, resultados: list[Resultado], tabela: str, momento: datetime
) -> None:
    """Acrescenta a execucao ao historico. Append: nada e sobrescrito."""
    from pyspark.sql.types import (
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("executado_em", TimestampType(), True),
            StructField("tabela", StringType(), True),
            StructField("verificacao", StringType(), True),
            StructField("severidade", StringType(), True),
            StructField("violacoes", LongType(), True),
            StructField("passou", BooleanType(), True),
            StructField("esperado", LongType(), True),
            StructField("obtido", LongType(), True),
        ]
    )
    linhas = [
        (
            momento,
            tabela,
            r.nome,
            r.severidade,
            int(r.violacoes),
            r.passou,
            None if r.esperado is None else int(r.esperado),
            None if r.obtido is None else int(r.obtido),
        )
        for r in resultados
    ]
    (
        spark.createDataFrame(linhas, schema=schema)
        .write.mode("append")
        # A tabela pode ter sido criada antes de `esperado` e `obtido`
        # existirem; `mergeSchema` acrescenta as colunas e deixa NULL nas
        # linhas antigas. Aceitavel numa tabela de controle com schema
        # declarado no codigo. Em bronze ou silver fica desligado, senao um
        # nome digitado errado cria coluna nova sem revisao.
        .option("mergeSchema", "true")
        .saveAsTable(TABELA_QUALIDADE)
    )


def reconciliar(
    spark, base_volume: str, endpoint: Endpoint, momento: datetime
) -> Reconciliacao:
    """Contagem de controle: o que ha na landing zone chegou inteiro na bronze?

    Compara contra a origem ja deduplicada -- a sobreposicao de dias faz o
    JSONL bruto ter mais linhas do que a bronze deve ter, por desenho.
    """
    fonte = bronze.deduplicar(
        bronze.ler_landing(spark, base_volume, endpoint, momento), endpoint
    )
    return Reconciliacao(
        nome=RECONCILIACAO_BRONZE,
        na_origem=fonte.count(),
        no_destino=spark.table(bronze.nome_tabela(endpoint)).count(),
    )


def reconciliar_silver(spark, endpoint: Endpoint) -> Reconciliacao:
    """Contagem de controle: `bronze = silver + quarentena`.

    A igualdade so fecha porque registro fora do contrato e desviado, nunca
    descartado. Se a silver descartasse em silencio, a diferenca apareceria
    aqui sem nenhuma pista de onde as linhas foram parar.
    """
    na_bronze = spark.table(bronze.nome_tabela(endpoint)).count()
    na_silver = spark.table(silver.TABELA_COMMITS).count()
    em_quarentena = spark.table(silver.TABELA_REJEITADOS).count()

    return Reconciliacao(
        nome=RECONCILIACAO_SILVER,
        na_origem=na_bronze,
        no_destino=na_silver + em_quarentena,
    )


def avaliar(
    spark,
    tabela: str,
    verificacoes: tuple[Verificacao, ...],
    reconciliacao: Reconciliacao,
    momento: datetime,
) -> list[Resultado]:
    """Roda a bateria, grava o historico e devolve os resultados.

    A gravacao acontece aqui, antes de qualquer interrupcao: quem chama e
    quem decide levantar. Execucao reprovada que nao entra no historico e
    justamente a que faria falta na investigacao.
    """
    resultados = [reconciliacao.como_resultado()] + executar(spark, verificacoes)
    registrar(spark, resultados, tabela, momento)
    return resultados
