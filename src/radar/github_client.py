"""Cliente HTTP para a API do GitHub.

Este modulo NAO conhece Spark, Delta nem Databricks: ele recebe caminhos e
devolve dicionarios Python. E isso que permite testa-lo no PC, em
milissegundos, sem subir cluster.

Analogia: GitHubClient e a "conexao" (como no DBeaver), get() e a "query",
e Resposta e o "result set".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator

import requests

from radar.config import API_BASE, API_VERSION, PER_PAGE, TIMEOUT, USER_AGENT




class ErroGitHub(RuntimeError):
    """Falha definitiva da API: retentar nao resolve.

    Excecao propria para que quem chama consiga distinguir "a API recusou"
    de "a rede caiu" -- sao problemas diferentes, com tratamentos diferentes.
    """

    def __init__(self, status: int, url: str, corpo: str = "") -> None:
        # corpo[:200] corta a mensagem: erro da API pode ter milhares de
        # caracteres e poluir o log.
        super().__init__(f"HTTP {status} em {url}: {corpo[:200]}")
        self.status = status
        self.url = url


@dataclass(frozen=True)
class Resposta:
    """Contrato do cliente. Quem consome nao precisa conhecer `requests`.

    frozen=True torna a resposta imutavel: ninguem altera um resultado
    depois de recebido.
    """

    status: int
    dados: Any | None          # None quando for 304 (nao ha corpo)
    etag: str | None
    link_next: str | None      # URL da proxima pagina, se houver
    rate_remaining: int | None
    rate_reset: int | None     # epoch em segundos

    @property
    def nao_modificado(self) -> bool:
        """Le melhor que `resp.status == 304` e centraliza o numero magico."""
        return self.status == 304


class GitHubClient:
    """Cliente fino da API do GitHub.

    O token e INJETADO, nunca lido de dentro da classe. Assim ela funciona
    igual com .env local ou com Databricks Secret Scope, e o teste pode
    passar um token falso sem mexer em variavel de ambiente.
    """

    def __init__(
        self,
        token: str,
        *,
        base_url: str = API_BASE,
        session: requests.Session | None = None,
        timeout: int = TIMEOUT,
    ) -> None:
        # Falhe cedo, falhe alto: sem isto, um token vazio viraria um 401
        # obscuro la na frente, longe da causa.
        if not token:
            raise ValueError("token vazio: verifique o .env ou o Secret Scope")

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

        # session or ... : usa a real por padrao, mas permite injetar uma
        # falsa no teste. A Session tambem reaproveita a conexao TCP.
        self.session = session or requests.Session()

        # Headers definidos UMA vez, valem para toda chamada.
        # O token nunca vira atributo (self.token) de proposito: assim ele
        # nao aparece em log, repr() ou stack trace.
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": USER_AGENT,
            }
        )

    def _url(self, caminho_ou_url: str) -> str:
        """Aceita caminho relativo ou URL completa.

        Aceitar URL completa e proposital: o header Link ja devolve a
        proxima pagina pronta, e a paginacao apenas repassa o que veio.
        """
        if caminho_ou_url.startswith("http"):
            return caminho_ou_url
        return f"{self.base_url}/{caminho_ou_url.lstrip('/')}"

    @staticmethod
    def _para_int(valor: str | None) -> int | None:
        """Converte header em int; devolve None se ausente ou invalido.

        Sem o try/except, um header ausente derrubaria a ingestao inteira.
        """
        try:
            return int(valor)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    def get(
        self,
        caminho_ou_url: str,
        params: dict | None = None,
        etag: str | None = None,
    ) -> Resposta:
        """Faz um GET e traduz a resposta HTTP no contrato Resposta."""
        url = self._url(caminho_ou_url)

        # Se temos um ETag guardado, perguntamos "mudou?" em vez de
        # pedir o conteudo. Se nada mudou, a API responde 304 e nao
        # consome quota.
        headers = {"If-None-Match": etag} if etag else None

        r = self.session.get(url, params=params, headers=headers, timeout=self.timeout)

        # 304 e checado ANTES de r.ok: nao e erro, mas tambem nao tem
        # corpo -- chamar r.json() aqui estouraria.
        if r.status_code == 304:
            dados = None
        elif r.ok:
            dados = r.json() if r.content else None
        else:
            # Falha explicita. Devolver None faria o pipeline seguir com
            # dado faltando, em silencio. (Retry vem no passo 1.5.)
            raise ErroGitHub(r.status_code, url, r.text)

        return Resposta(
            status=r.status_code,
            dados=dados,
            etag=r.headers.get("ETag"),
            # requests JA interpreta o header Link -- nao reinvente o parser.
            link_next=r.links.get("next", {}).get("url"),
            rate_remaining=self._para_int(r.headers.get("X-RateLimit-Remaining")),
            rate_reset=self._para_int(r.headers.get("X-RateLimit-Reset")),
        )
    def paginar(
        self,
        caminho: str,
        params: dict | None = None,
        limite_paginas: int | None = None,
    ) -> Iterator[dict]:
        """Percorre todas as paginas seguindo o header Link, item a item.

        E um gerador: entrega um registro por vez, sem segurar todas as
        paginas na memoria. Quem consome pode parar no meio, e as paginas
        restantes nunca sao buscadas -- economia direta de quota.

        Use apenas em endpoints que devolvem LISTA (commits, issues,
        contributors). Para um recurso unico, use get().
        """
        parametros = dict(params or {})
        parametros.setdefault("per_page", PER_PAGE)

        url: str | None = caminho
        pagina = 0

        while url:
            resposta = self.get(url, params=parametros)

            # A partir da 2a pagina, a URL do link_next JA contem todos os
            # parametros. Reenvia-los duplicaria a query string.
            parametros = None

            # Lista vazia = acabou.
            if not resposta.dados:
                break

            yield from resposta.dados

            pagina += 1

            # Valvula de seguranca: sem limite, um bug percorreria 7.498
            # paginas e torraria a quota inteira em segundos.
            if limite_paginas is not None and pagina >= limite_paginas:
                break

            url = resposta.link_next
