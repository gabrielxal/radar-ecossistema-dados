"""Cliente HTTP da API do GitHub. Nao depende de Spark nem do Databricks."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Iterator

import requests

from radar.config import (
    API_BASE,
    API_VERSION,
    BACKOFF_BASE,
    ESPERA_MAXIMA,
    MAX_TENTATIVAS,
    PER_PAGE,
    TIMEOUT,
    USER_AGENT,
)


class ErroGitHub(RuntimeError):
    """Falha da API que nao deve ser retentada."""

    def __init__(self, status: int, url: str, corpo: str = "") -> None:
        # corpo[:200]: resposta de erro da API pode ter milhares de caracteres.
        super().__init__(f"HTTP {status} em {url}: {corpo[:200]}")
        self.status = status
        self.url = url


@dataclass(frozen=True)
class Resposta:
    """Retorno padronizado de uma chamada."""

    status: int
    dados: Any | None          # None quando for 304
    etag: str | None
    link_next: str | None      # URL da proxima pagina, do header Link
    rate_remaining: int | None
    rate_reset: int | None     # epoch em segundos

    @property
    def nao_modificado(self) -> bool:
        return self.status == 304


class GitHubClient:
    """Cliente da API do GitHub com paginacao, ETag e retry."""

    def __init__(
        self,
        token: str,
        *,
        base_url: str = API_BASE,
        session: requests.Session | None = None,
        timeout: int = TIMEOUT,
    ) -> None:
        if not token:
            raise ValueError("token vazio: verifique o .env ou o Secret Scope")

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()

        # Token fica so no header da sessao; nao vira atributo para nao
        # aparecer em log, repr() ou stack trace.
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": USER_AGENT,
            }
        )

    def _url(self, caminho_ou_url: str) -> str:
        """Aceita caminho relativo ou URL completa (usada ao seguir link_next)."""
        if caminho_ou_url.startswith("http"):
            return caminho_ou_url
        return f"{self.base_url}/{caminho_ou_url.lstrip('/')}"

    @staticmethod
    def _para_int(valor: str | None) -> int | None:
        """Converte header em int; None se ausente ou invalido."""
        try:
            return int(valor)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    def _esperar(
        self, tentativa: int, resposta: requests.Response | None = None
    ) -> None:
        """Backoff exponencial com jitter antes da proxima tentativa."""
        espera = BACKOFF_BASE * (2 ** (tentativa - 1))

        if resposta is not None:
            retry_after = self._para_int(resposta.headers.get("Retry-After"))
            if retry_after:
                espera = max(espera, retry_after)
            elif resposta.headers.get("X-RateLimit-Remaining") == "0":
                # Quota zerada: nao adianta tentar antes do reset.
                reset = self._para_int(resposta.headers.get("X-RateLimit-Reset"))
                if reset:
                    espera = max(espera, reset - time.time() + 1)

        espera = min(espera, ESPERA_MAXIMA)
        espera += random.uniform(0, espera * 0.25)  # jitter

        print(f"    [retry] tentativa {tentativa} falhou; aguardando {espera:.1f}s")
        time.sleep(espera)

    def _montar_resposta(self, r: requests.Response, url: str) -> Resposta:
        """Traduz a resposta HTTP no contrato Resposta."""
        if r.status_code == 304:
            dados = None  # 304 nao tem corpo: r.json() estouraria
        elif r.ok:
            dados = r.json() if r.content else None
        else:
            raise ErroGitHub(r.status_code, url, r.text)

        return Resposta(
            status=r.status_code,
            dados=dados,
            etag=r.headers.get("ETag"),
            link_next=r.links.get("next", {}).get("url"),
            rate_remaining=self._para_int(r.headers.get("X-RateLimit-Remaining")),
            rate_reset=self._para_int(r.headers.get("X-RateLimit-Reset")),
        )

    def get(
        self,
        caminho_ou_url: str,
        params: dict | None = None,
        etag: str | None = None,
    ) -> Resposta:
        """GET com retry em falha de rede, 429 e 5xx. Nao retenta 4xx."""
        url = self._url(caminho_ou_url)
        headers = {"If-None-Match": etag} if etag else None
        motivo = "desconhecido"

        for tentativa in range(1, MAX_TENTATIVAS + 1):
            ultima = tentativa == MAX_TENTATIVAS

            try:
                r = self.session.get(
                    url, params=params, headers=headers, timeout=self.timeout
                )
            except (requests.Timeout, requests.ConnectionError) as erro:
                motivo = f"falha de rede ({erro.__class__.__name__})"
                if not ultima:
                    self._esperar(tentativa)
                continue

            if r.status_code == 429 or r.status_code >= 500:
                motivo = f"HTTP {r.status_code}"
                if not ultima:
                    self._esperar(tentativa, r)  # a resposta pode trazer Retry-After
                continue

            return self._montar_resposta(r, url)

        raise ErroGitHub(0, url, f"desisti apos {MAX_TENTATIVAS} tentativas: {motivo}")

    def paginar(
        self,
        caminho: str,
        params: dict | None = None,
        limite_paginas: int | None = None,
        estado: dict | None = None,
    ) -> Iterator[dict]:
        """Gerador que percorre as paginas seguindo link_next.

        Apenas para endpoints que devolvem lista. Para recurso unico, use get().

        `estado`, quando fornecido, recebe `{"truncado": bool}` ao fim do
        percurso. `True` significa que o teto de paginas interrompeu um
        percurso que ainda tinha proxima pagina, ou seja, ficou dado para
        tras. Sem esse canal, quem consome com `list()` nao teria como saber
        por que o gerador parou, e a coleta parcial passaria por completa.

        So e preenchido se o gerador for percorrido ate o fim.
        """
        parametros = dict(params or {})
        parametros.setdefault("per_page", PER_PAGE)

        url: str | None = caminho
        pagina = 0

        while url:
            resposta = self.get(url, params=parametros)
            parametros = None  # a URL de link_next ja carrega os parametros

            if not resposta.dados:
                break

            yield from resposta.dados

            pagina += 1
            if limite_paginas is not None and pagina >= limite_paginas:
                # Havia proxima pagina? Se nao, o teto coincidiu com o fim e
                # nada se perdeu.
                if estado is not None:
                    estado["truncado"] = bool(resposta.link_next)
                return

            url = resposta.link_next

        if estado is not None:
            estado["truncado"] = False
