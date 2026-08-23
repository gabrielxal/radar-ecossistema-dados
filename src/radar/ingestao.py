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
from radar.controle import Checkpoint, calcular_watermark, parametros_de_busca


@dataclass(frozen=True)
class Endpoint:
    """Metadados de um endpoint de lista da API."""

    nome: str
    caminho: str        # com {repo} a substituir
    campo_data: str     # caminho aninhado ate a data do registro
    chave: str          # chave natural do registro


ENDPOINTS: dict[str, Endpoint] = {
    "commits": Endpoint(
        nome="commits",
        caminho="/repos/{repo}/commits",
        campo_data="commit.committer.date",
        chave="sha",
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


def para_datetime(texto: str | None) -> datetime | None:
    """Converte data ISO da API em datetime UTC."""
    if not texto:
        return None
    try:
        momento = datetime.fromisoformat(texto.replace("Z", "+00:00"))
    except ValueError:
        return None
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=timezone.utc)
    return momento.astimezone(timezone.utc)


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
) -> Checkpoint:
    """Checkpoint a gravar depois de uma ingestao."""
    watermark = calcular_watermark(resultado.maior_data)
    if watermark is None and anterior is not None:
        watermark = anterior.watermark  # nada novo: preserva o watermark anterior

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
        params={"per_page": 1},
        etag=etag,
    )
    return (not resposta.nao_modificado), (resposta.etag or etag)


def coletar(
    cliente,
    endpoint: Endpoint,
    repo: str,
    checkpoint: Checkpoint | None,
    limite_paginas: int | None = None,
) -> tuple[list[dict], bool]:
    """Coleta os registros novos desde o watermark. Devolve (registros, truncado)."""
    params = parametros_de_busca(checkpoint, PER_PAGE)
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
    etag_anterior = checkpoint.etag if checkpoint else None
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

    registros, truncado = coletar(cliente, endpoint, repo, checkpoint, limite_paginas)

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
