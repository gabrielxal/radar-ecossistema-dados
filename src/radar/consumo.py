"""Camada de consumo: as consultas de analise expostas como visao.

O pipeline terminava na gold sem ninguem consumindo. O painel e a peca que
faltava, e a decisao que importa aqui e onde o SQL dele mora.

Colar a consulta dentro de cada widget seria uma segunda copia da logica que
ja existe em `analises.py`, sem teste e livre para divergir no dia em que uma
das duas mudasse. A decisao 8.12 poe consulta de analise em `src/` justamente
para ela ser exercitada contra o motor antes de rodar na plataforma, e um
dashboard com SQL proprio desfaz isso.

A visao resolve. O widget consulta `SELECT * FROM vw_painel_de_saude`, a
logica continua num lugar so, e o dashboard vira layout.

**Visao e nao tabela materializada**, por duas razoes:

- as consultas usam `current_date()`, e a janela precisa andar sozinha; uma
  tabela congelaria a leitura no dia da carga, e o painel passaria a mentir
  entre uma execucao e outra
- visao nao tem carga, entao nao ha etapa nova no DAG nem estado a manter

O custo e recalcular a cada abertura. Com o volume atual e irrelevante, e o
dia em que deixar de ser tem sintoma direto: o painel demora a abrir. A saida
nesse dia e materializar as visoes caras como tabela na tarefa de fatos, e a
troca esta medida em `notebooks/14_desempenho.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from radar import analises
from radar.config import GOLD, fqn

PREFIXO = "vw_"


def nome_visao(sufixo: str) -> str:
    """Nome totalmente qualificado da visao: workspace.radar_gold.vw_<sufixo>."""
    return fqn(GOLD, f"{PREFIXO}{sufixo}")


# --------------------------------------------------------------------------
# O painel, com o portao ao lado
# --------------------------------------------------------------------------

def painel_com_portao(**tabelas) -> str:
    """`painel_de_saude` com a coluna que diz se as issues dele valem.

    O portao existe desde a Etapa 6 e ate agora vivia numa celula separada do
    notebook 11, o que deixa a leitura correta na mao de quem lembrar de rodar
    as duas. Num painel isso nao sobrevive: as colunas de issue aparecem ao
    lado das de commit, com a mesma aparencia de fato consolidado, e nada na
    tela diz que as de um repositorio truncado estao deslocadas.

    Em coleta crescente o que chega primeiro e a parte velha e ja fechada do
    backlog, entao `em_aberto` e `idade_mediana_em_aberto` saem baixas demais
    exatamente onde o backfill ainda nao terminou. E a forma do defeito da
    secao 5.7: o dado que chegou esta certo, e a conclusao tirada dele nao.

    A juncao fica aqui, e nao dentro de `painel_de_saude`, porque a pergunta
    central e sobre saude do projeto e o portao e sobre estado da nossa
    coleta. Sao camadas diferentes: uma responde sobre o mundo, a outra sobre
    o pipeline. Compor as duas e trabalho do consumo.
    """
    painel = analises.painel_de_saude(**tabelas)
    portao = analises.cobertura_do_backfill(**tabelas)

    return f"""
        WITH painel AS ({painel}),
             portao AS ({portao})
        SELECT p.*,
               coalesce(g.confiavel, FALSE) AS issues_confiavel,
               g.status                     AS status_da_coleta,
               g.coletado_ate               AS issues_coletadas_ate
        FROM painel p
        LEFT JOIN portao g ON g.repo = p.repo
        ORDER BY p.bus_factor, p.ritmo_por_autor_pct
    """


# --------------------------------------------------------------------------
# O catalogo de visoes
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Visao:
    """Uma visao da camada de consumo.

    `construir` recebe os nomes das tabelas por palavra-chave e devolve SQL,
    que e a mesma assinatura das funcoes de `analises.py`. E o que permite ao
    teste apontar a visao para dado sintetico.
    """

    sufixo: str
    construir: Callable[..., str]
    comentario: str

    @property
    def nome(self) -> str:
        return nome_visao(self.sufixo)


VISOES = (
    Visao(
        "painel_de_saude",
        painel_com_portao,
        "A pergunta central: os sinais lado a lado, sem coluna de veredito. "
        "`issues_confiavel` marca onde o backfill terminou; as colunas de "
        "issue so valem nas linhas em que ela e verdadeira.",
    ),
    Visao(
        "bus_factor",
        analises.bus_factor,
        "Quantas pessoas concentram metade dos commits. 1 e ponto unico de "
        "falha. Grao: um repositorio.",
    ),
    Visao(
        "ritmo_por_autor",
        analises.ritmo_por_autor,
        "Volume e volume por autor ativo, em dois periodos de 45 dias. As "
        "duas colunas juntas separam time crescendo de projeto acelerando.",
    ),
    Visao(
        "ciclo_de_issues",
        analises.ciclo_de_issues,
        "Vazao e backlog, que sao medidas opostas: mediana ate fechar olha o "
        "que terminou, idade em aberto olha o que nao terminou.",
    ),
    Visao(
        "cobertura_do_backfill",
        analises.cobertura_do_backfill,
        "Estado da coleta de issues por repositorio. `confiavel` e o portao "
        "de toda pergunta sobre issue em aberto.",
    ),
    Visao(
        "versoes_do_repositorio",
        analises.versoes_do_repositorio,
        "Quantas versoes a SCD2 guarda de cada repositorio. Mais de uma "
        "significa que um atributo versionado mudou.",
    ),
)


def ddl(visao: Visao, **tabelas) -> str:
    """DDL da visao. `CREATE OR REPLACE` para a carga ser reexecutavel.

    O comentario entra na definicao porque e o que o Catalog Explorer e o
    Genie leem: visao sem comentario obriga quem consulta a abrir o codigo
    para saber o grao.
    """
    if "'" in visao.comentario:
        raise ValueError(
            f"comentario de {visao.sufixo} tem aspa simples e quebraria o DDL"
        )

    return f"""
CREATE OR REPLACE VIEW {visao.nome}
COMMENT '{visao.comentario}'
AS
{visao.construir(**tabelas)}
"""


def criar(spark, **tabelas) -> tuple[str, ...]:
    """Cria ou substitui todas as visoes. Devolve os nomes, na ordem."""
    for visao in VISOES:
        spark.sql(ddl(visao, **tabelas))
    return tuple(v.nome for v in VISOES)
