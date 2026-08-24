"""Ingestao da API para a landing zone.

Grava o JSON exatamente como veio da API, em arquivos JSONL particionados.
Nao usa Spark: escreve com I/O comum, o que torna o modulo testavel com
`tmp_path` do pytest.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from radar.config import PER_PAGE

# Tamanho da fatia do backfill. Sete dias porque o repositorio mais ativo do
# escopo faz ~64 commits/dia: ~450 por janela, ou cinco paginas de 100.
DIAS_POR_JANELA = 7
from radar.controle import (
    Checkpoint,
    calcular_watermark,
    parametros_de_busca,
    parametros_de_janela,
)


@dataclass(frozen=True)
class Endpoint:
    """Metadados de um endpoint da API.

    Dois formatos convivem aqui. Lista e paginada e incremental: commits
    chegam aos milhares e so os novos interessam. Snapshot e recurso
    unico e sem historico: `/repos/{repo}` devolve o estado de agora, e o
    historico se constroi coletando todo dia.
    """

    nome: str
    caminho: str        # com {repo} a substituir

    # Caminho aninhado ate a data do registro, e a base do proximo watermark.
    #
    # Invariante: precisa ser o mesmo campo pelo qual a API filtra em `since`.
    # O watermark e calculado a partir do maior valor deste campo e vira o
    # `since` da execucao seguinte; se os dois apontarem para campos
    # diferentes, o filtro e o marcador andam em ritmos distintos e a coleta
    # deixa de convergir. Em `/commits` o `since` filtra pela data do commit;
    # em `/issues` filtra por `updated_at`, que nao e a data de criacao.
    campo_data: str

    chave: str          # chave natural do registro, extraida do payload
    # O que identifica uma linha na bronze. Numa lista e a chave natural do
    # registro; num snapshot e o par repositorio + dia, porque o mesmo
    # repositorio coletado em dois dias sao duas fotos, nao uma repetida.
    chaves: tuple[str, ...]
    snapshot: bool = False

    # Parametros fixos da chamada, proprios do endpoint. Par de tuplas e nao
    # dict para o dataclass continuar congelavel e comparavel.
    params_extra: tuple[tuple[str, str], ...] = ()

    # A API aceita `until`? So com ele o backfill pode ser fatiado em janelas.
    # `/commits` aceita; `/issues` nao, e por isso depende de ordenacao
    # ascendente para nao perder historico no teto de paginas.
    aceita_until: bool = False

    # O registro muda na origem depois de criado?
    #
    # Commit e imutavel: coletado duas vezes, as duas copias sao iguais. Issue
    # nao e: titulo, estado, rotulos e contagem de comentarios mudam ao longo
    # da vida dela. A diferenca decide qual copia a bronze mantem quando o
    # mesmo registro aparece em duas cargas, e manter a errada congela a issue
    # no estado em que ela foi vista pela primeira vez.
    mutavel: bool = False

    # A API entrega do mais novo para o mais antigo, ou o contrario?
    #
    # Decide o que fazer com o watermark quando o teto de paginas corta a
    # coleta. Em ordem decrescente o que ficou de fora esta atras do
    # watermark, e avancar tornaria a falta permanente. Em ordem crescente o
    # que ficou de fora esta a frente, e nao avancar faz a coleta repetir
    # eternamente as mesmas primeiras paginas sem nunca chegar ao fim.
    ordem_crescente: bool = False

    @property
    def extras(self) -> dict:
        """Os parametros fixos do endpoint, prontos para a chamada."""
        return dict(self.params_extra)


ENDPOINTS: dict[str, Endpoint] = {
    "commits": Endpoint(
        nome="commits",
        caminho="/repos/{repo}/commits",
        campo_data="commit.committer.date",
        chave="sha",
        chaves=("repo", "sha"),
        aceita_until=True,
    ),
    # A coleta de issues nao usa `until`, que a API nao oferece aqui, e sim
    # ordenacao crescente. Com `direction=asc` a lista comeca no registro mais
    # antigo depois do watermark e caminha para frente: o teto de paginas
    # corta os mais recentes, que sao exatamente os que a execucao seguinte
    # vai buscar. A coleta converge sem deixar buraco atras de si, e a
    # primeira carga, sem `since`, e um backfill retomavel de graca.
    #
    # `state=all` porque sem ele a API devolve so as abertas, e um fato de
    # ciclo de vida sem as fechadas nao tem o marco final.
    #
    # O `dt` na chave da bronze e o que torna a camada um log de versoes: cada
    # dia de coleta acrescenta a versao daquele dia em vez de sobrescrever, e
    # o MERGE continua sem ramo de UPDATE.
    "issues": Endpoint(
        nome="issues",
        caminho="/repos/{repo}/issues",
        campo_data="updated_at",
        chave="id",
        chaves=("repo", "id", "dt"),
        mutavel=True,
        ordem_crescente=True,
        params_extra=(
            ("state", "all"),
            ("sort", "updated"),
            ("direction", "asc"),
        ),
    ),
    "repositorios": Endpoint(
        nome="repositorios",
        caminho="/repos/{repo}",
        campo_data="pushed_at",
        chave="id",
        chaves=("repo", "dt"),
        snapshot=True,
    ),
}


@dataclass(frozen=True)
class ResultadoIngestao:
    """O que uma ingestao produziu, para alimentar a tabela de controle."""

    repo: str
    endpoint: str
    registros: int
    arquivo: str | None
    etag: str | None
    maior_data: datetime | None
    pulado: bool          # True quando a sentinela respondeu 304
    erro: str | None = None
    # True quando o teto de paginas interrompeu a coleta e ficou dado para
    # tras. Nao e erro: a carga foi bem-sucedida, so nao foi completa.
    truncado: bool = False


# --------------------------------------------------------------------------
# Funcoes puras
# --------------------------------------------------------------------------

def sanitizar_repo(repo: str) -> str:
    """`owner/nome` -> `owner__nome`, para caber num nome de diretorio."""
    return repo.replace("/", "__")


def caminho_arquivo(
    base_volume: str, endpoint: str, repo: str, momento: datetime
) -> str:
    """Caminho particionado no estilo Hive, para o Spark inferir as colunas."""
    return (
        f"{base_volume}/{endpoint}"
        f"/repo={sanitizar_repo(repo)}"
        f"/dt={momento.strftime('%Y-%m-%d')}"
        f"/{momento.strftime('%H%M%S')}.jsonl"
    )


def valor_aninhado(dado: dict, caminho: str) -> Any:
    """Navega por chaves separadas por ponto: 'commit.committer.date'."""
    atual: Any = dado
    for parte in caminho.split("."):
        if not isinstance(atual, dict):
            return None
        atual = atual.get(parte)
    return atual


def em_utc(momento: datetime | None) -> datetime | None:
    """Normaliza para UTC, assumindo UTC quando o valor vem sem fuso.

    Nem todo datetime do pipeline carrega fuso: o watermark lido da tabela de
    controle pode voltar naive, enquanto o `momento` da execucao e sempre
    consciente. Comparar os dois direto levanta `TypeError`, e a excecao
    apareceria no meio de uma coleta em producao.
    """
    if momento is None:
        return None
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=timezone.utc)
    return momento.astimezone(timezone.utc)


def para_datetime(texto: str | None) -> datetime | None:
    """Converte data ISO da API em datetime UTC."""
    if not texto:
        return None
    try:
        return em_utc(datetime.fromisoformat(texto.replace("Z", "+00:00")))
    except ValueError:
        return None


def maior_data(registros: list[dict], campo_data: str) -> datetime | None:
    """Maior data entre os registros; base do proximo watermark."""
    datas = [
        d
        for d in (para_datetime(valor_aninhado(r, campo_data)) for r in registros)
        if d is not None
    ]
    return max(datas) if datas else None


def checkpoint_inicial(
    repo: str, endpoint: str, momento: datetime, dias_historico: int
) -> Checkpoint | None:
    """Checkpoint sintetico que limita a primeira carga a uma janela.

    Sem ele, a primeira execucao pagina o historico inteiro do repositorio e
    estoura a quota.
    """
    if dias_historico <= 0:
        return None
    return Checkpoint(
        repo=repo,
        endpoint=endpoint,
        watermark=momento - timedelta(days=dias_historico),
    )


def _status(resultado: "ResultadoIngestao") -> str:
    """Estado da carga na tabela de controle.

    `truncado` fica entre `ok` e `erro`: nada falhou, mas a coleta parou no
    teto de paginas e ficou historico para tras. Sem esse valor proprio, uma
    carga incompleta se registra como `ok` e a falta de dado some.
    """
    if resultado.erro:
        return "erro"
    if resultado.truncado:
        return "truncado"
    return "ok"


def proximo_checkpoint(
    anterior: Checkpoint | None,
    resultado: "ResultadoIngestao",
    momento: datetime,
    endpoint: Endpoint | None = None,
) -> Checkpoint:
    """Checkpoint a gravar depois de uma ingestao.

    O que fazer com o watermark depois de uma coleta truncada depende do
    sentido em que a API entrega os registros, e a mesma regra aplicada aos
    dois sentidos produz defeitos opostos.

    Em ordem decrescente, que e o caso de `/commits`, o que o teto cortou esta
    atras do watermark. `since` so aceita limite inferior, entao nao ha como
    voltar para buscar. Avancar tornaria a falta permanente e invisivel;
    preservar mantem a proxima execucao tentando o mesmo intervalo, recoletando
    sem duplicar, ate o teto ser suficiente.

    Em ordem crescente, que e o caso de `/issues`, o que o teto cortou esta a
    frente. Tudo que ficou para tras do corte foi coletado. Preservar o
    watermark aqui faria a coleta repetir as mesmas primeiras paginas para
    sempre, sem nunca alcancar o fim: o backfill nunca terminaria.

    O `endpoint` e opcional para nao quebrar chamador antigo, e a ausencia
    assume ordem decrescente, que era o unico comportamento existente.
    """
    watermark = calcular_watermark(resultado.maior_data)
    if watermark is None and anterior is not None:
        watermark = anterior.watermark  # nada novo: preserva o watermark anterior

    crescente = bool(endpoint and endpoint.ordem_crescente)
    if resultado.truncado and not crescente:
        watermark = anterior.watermark if anterior else None

    return Checkpoint(
        repo=resultado.repo,
        endpoint=resultado.endpoint,
        watermark=watermark,
        etag=resultado.etag,
        ultima_execucao=momento,
        status=_status(resultado),
        mensagem=resultado.erro,
        registros=resultado.registros,
    )


def gravar_jsonl(caminho: str, registros: list[dict]) -> int:
    """Grava um registro por linha, sem alterar o payload da API.

    JSONL e nao array JSON: o Spark le linha a linha e o arquivo e divisivel.
    """
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as arquivo:
        for registro in registros:
            arquivo.write(json.dumps(registro, ensure_ascii=False) + "\n")
    return len(registros)


# --------------------------------------------------------------------------
# Chamadas a API
# --------------------------------------------------------------------------

def sentinela(cliente, endpoint: Endpoint, repo: str, etag: str | None):
    """Pergunta se houve movimento, numa URL fixa. Devolve (mudou, novo_etag).

    URL fixa de proposito: o ETag e por URL, e a de coleta muda a cada
    execucao por causa do `since`.
    """
    resposta = cliente.get(
        endpoint.caminho.format(repo=repo),
        params={**endpoint.extras, "per_page": 1},
        etag=etag,
    )
    return (not resposta.nao_modificado), (resposta.etag or etag)


def janelas(
    inicio: datetime | None,
    fim: datetime | None,
    dias: int = DIAS_POR_JANELA,
) -> list[tuple[datetime, datetime]]:
    """Fatia o intervalo [inicio, fim] em pedacos de `dias`.

    As bordas se sobrepoem: `since` e `until` da API sao inclusivos, entao o
    registro que cair exatamente no limite aparece nas duas janelas vizinhas.
    A deduplicacao acontece na coleta, antes da gravacao.

    Intervalo vazio ou invertido devolve lista vazia, e quem chama cai na
    coleta direta. E o que acontece quando nao ha watermark: sem limite
    inferior nao ha o que fatiar.
    """
    inicio, fim = em_utc(inicio), em_utc(fim)
    if inicio is None or fim is None or inicio >= fim:
        return []

    passo = timedelta(days=dias)
    intervalos = []
    atual = inicio
    while atual < fim:
        proximo = min(atual + passo, fim)
        intervalos.append((atual, proximo))
        atual = proximo
    return intervalos


def deduplicar_por_chave(registros: list[dict], chave: str) -> list[dict]:
    """Remove repeticao preservando a ordem de chegada.

    Registro sem a chave e mantido: descartar em silencio o que nao se
    consegue identificar e justamente o defeito que a Etapa 3 revelou.
    """
    vistos = set()
    unicos = []
    for registro in registros:
        valor = registro.get(chave)
        if valor is not None and valor in vistos:
            continue
        if valor is not None:
            vistos.add(valor)
        unicos.append(registro)
    return unicos


def coletar(
    cliente,
    endpoint: Endpoint,
    repo: str,
    checkpoint: Checkpoint | None,
    limite_paginas: int | None = None,
    ate: datetime | None = None,
) -> tuple[list[dict], bool]:
    """Coleta os registros novos desde o watermark. Devolve (registros, truncado).

    Quando o endpoint aceita `until` e ha watermark, a coleta e fatiada em
    janelas. E a correcao do defeito da secao 5.7: numa chamada unica de 90
    dias, o teto de paginas corta a lista no meio e o que ficou para tras
    nunca mais e buscado, porque a API entrega do mais novo para o mais antigo
    e `since` so aceita limite inferior. Com janelas de uma semana, cada
    chamada devolve um volume que cabe no teto, e o teto deixa de decidir
    quanto historico se perde.

    Uma janela truncada trunca a coleta inteira. Poderia-se avancar o
    watermark ate o inicio da janela que falhou, mas o ganho seria evitar
    releitura idempotente, e o custo seria um watermark que avanca sobre uma
    coleta incompleta. E exatamente a troca que causou o defeito original.
    """
    def uma_chamada(params: dict) -> tuple[list[dict], bool]:
        estado: dict = {}
        registros = list(
            cliente.paginar(
                endpoint.caminho.format(repo=repo),
                params=params,
                limite_paginas=limite_paginas,
                estado=estado,
            )
        )
        return registros, estado.get("truncado", False)

    desde = checkpoint.watermark if checkpoint is not None else None
    intervalos = janelas(desde, ate) if endpoint.aceita_until else []

    if not intervalos:
        return uma_chamada(
            parametros_de_busca(checkpoint, PER_PAGE, extras=endpoint.extras)
        )

    registros: list[dict] = []
    truncado = False
    for inicio, fim in intervalos:
        da_janela, cortou = uma_chamada(
            parametros_de_janela(inicio, fim, PER_PAGE, endpoint.extras)
        )
        registros.extend(da_janela)
        truncado = truncado or cortou

    return deduplicar_por_chave(registros, endpoint.chave), truncado


def ingerir_snapshot(
    cliente,
    endpoint: Endpoint,
    repo: str,
    base_volume: str,
    momento: datetime,
) -> ResultadoIngestao:
    """Coleta a foto atual de um recurso unico.

    Sem paginacao e sem watermark: nao ha historico a percorrer, o recurso
    devolve o estado de agora.

    Sem sentinela tambem, e isso e deliberado. Um `304` economizaria uma
    requisicao e deixaria um buraco na serie temporal, porque o dia sem foto
    nao e o dia sem mudanca, e quem consulta nao consegue distinguir os
    dois. Catorze requisicoes por dia e preco baixo por uma serie continua.
    """
    resposta = cliente.get(endpoint.caminho.format(repo=repo))
    registros = [resposta.dados] if resposta.dados else []

    if not registros:
        return ResultadoIngestao(
            repo=repo,
            endpoint=endpoint.nome,
            registros=0,
            arquivo=None,
            etag=resposta.etag,
            maior_data=None,
            pulado=False,
        )

    caminho = caminho_arquivo(base_volume, endpoint.nome, repo, momento)
    gravar_jsonl(caminho, registros)

    return ResultadoIngestao(
        repo=repo,
        endpoint=endpoint.nome,
        registros=1,
        arquivo=caminho,
        etag=resposta.etag,
        maior_data=maior_data(registros, endpoint.campo_data),
        pulado=False,
    )


def ingerir(
    cliente,
    endpoint: Endpoint,
    repo: str,
    checkpoint: Checkpoint | None,
    base_volume: str,
    momento: datetime,
    limite_paginas: int | None = None,
) -> ResultadoIngestao:
    """Sentinela, coleta e gravacao de um par (repo, endpoint)."""
    # Checkpoint truncado significa historico por coletar. A sentinela olha
    # apenas o topo da lista: se nada mudou la, ela responde 304 e o
    # repositorio seria pulado, logo aquele que se sabe incompleto.
    # Ignorar o ETag nesse caso e o que permite a coleta continuar.
    truncado_antes = bool(checkpoint and checkpoint.status == "truncado")
    etag_anterior = None if truncado_antes else (checkpoint.etag if checkpoint else None)

    mudou, novo_etag = sentinela(cliente, endpoint, repo, etag_anterior)

    if not mudou:
        return ResultadoIngestao(
            repo=repo,
            endpoint=endpoint.nome,
            registros=0,
            arquivo=None,
            etag=novo_etag,
            maior_data=None,
            pulado=True,
        )

    registros, truncado = coletar(
        cliente, endpoint, repo, checkpoint, limite_paginas, ate=momento
    )

    if not registros:
        return ResultadoIngestao(
            repo=repo,
            endpoint=endpoint.nome,
            registros=0,
            arquivo=None,
            etag=novo_etag,
            maior_data=None,
            pulado=False,
            truncado=truncado,
        )

    caminho = caminho_arquivo(base_volume, endpoint.nome, repo, momento)
    gravar_jsonl(caminho, registros)

    return ResultadoIngestao(
        repo=repo,
        endpoint=endpoint.nome,
        registros=len(registros),
        arquivo=caminho,
        etag=novo_etag,
        maior_data=maior_data(registros, endpoint.campo_data),
        pulado=False,
        truncado=truncado,
    )
