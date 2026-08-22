"""Testes de qualidade sobre as tabelas do lakehouse.

Cada verificacao e uma consulta que conta violacoes: zero e aprovacao. O
resultado de cada execucao fica gravado, o que permite comparar o estado de
hoje com o das execucoes anteriores.

A severidade separa a falha que interrompe o pipeline da que apenas informa.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from radar import bronze
from radar.config import BRONZE, REPOS, fqn
from radar.ingestao import Endpoint

TABELA_QUALIDADE = fqn(BRONZE, "qualidade_execucao")

BLOQUEIA = "bloqueia"
AVISA = "avisa"
SEVERIDADES = (BLOQUEIA, AVISA)

# A contagem de controle nao e um SQL sobre uma tabela unica, mas entra na
# bateria com o mesmo formato das demais para compartilhar o historico.
RECONCILIACAO = "reconciliacao_landing_bronze"


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
    """Contagem de controle entre a landing zone e a bronze."""

    na_origem: int
    na_bronze: int

    @property
    def diferenca(self) -> int:
        return self.na_origem - self.na_bronze

    @property
    def bate(self) -> bool:
        return self.diferenca == 0

    def como_resultado(self) -> Resultado:
        """Converte a contagem em uma linha da bateria, com historico.

        `abs()` porque o desvio conta nos dois sentidos: bronze com menos
        linhas indica perda no caminho; com mais, arquivo removido da landing
        zone ou insercao feita por fora do pipeline.
        """
        return Resultado(
            nome=RECONCILIACAO,
            severidade=BLOQUEIA,
            violacoes=abs(self.diferenca),
            esperado=self.na_origem,
            obtido=self.na_bronze,
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
        na_origem=fonte.count(),
        na_bronze=spark.table(bronze.nome_tabela(endpoint)).count(),
    )
