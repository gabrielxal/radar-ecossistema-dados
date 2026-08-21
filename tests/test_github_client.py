"""Testes do cliente da API. Nenhum toca na rede.

A sessao HTTP e substituida por um duble (SessaoFalsa) que devolve
respostas escritas por nos. Isso permite testar cenarios impossiveis de
reproduzir sob demanda -- tres erros 500 seguidos, por exemplo.
"""

import pytest

from radar.github_client import ErroGitHub, GitHubClient


class RespostaFalsa:
    """Imita o objeto Response do requests, so com o que o cliente usa."""

    def __init__(self, status=200, corpo=None, headers=None, links=None):
        self.status_code = status
        self.headers = headers or {}
        self.links = links or {}
        self.ok = 200 <= status < 400
        self.text = ""
        self._corpo = corpo
        self.content = b"conteudo" if corpo is not None else b""

    def json(self):
        return self._corpo


class SessaoFalsa:
    """Devolve respostas pre-programadas e registra o que foi pedido."""

    def __init__(self, respostas):
        self.respostas = list(respostas)
        self.chamadas = []
        self.headers = {}

    def get(self, url, params=None, headers=None, timeout=None):
        self.chamadas.append({"url": url, "params": params, "headers": headers})
        return self.respostas.pop(0)


def cliente(respostas):
    """Monta um cliente com sessao falsa e sem esperas reais."""
    sessao = SessaoFalsa(respostas)
    c = GitHubClient("token-de-teste", session=sessao)
    c._esperar = lambda *a, **k: None  # nao dorme durante o teste
    return c, sessao


# --------------------------------------------------------------------------
# Construcao do cliente
# --------------------------------------------------------------------------

def test_token_vazio_levanta_erro():
    with pytest.raises(ValueError):
        GitHubClient("")


def test_headers_de_autenticacao_sao_definidos():
    _, sessao = cliente([])
    assert sessao.headers["Authorization"] == "Bearer token-de-teste"
    assert sessao.headers["X-GitHub-Api-Version"]
    assert sessao.headers["User-Agent"]


def test_token_nao_vira_atributo_do_objeto():
    """O token nao pode vazar em log, repr() ou stack trace."""
    c, _ = cliente([])
    assert not hasattr(c, "token")
    assert "token-de-teste" not in repr(c.__dict__)


# --------------------------------------------------------------------------
# Montagem da URL
# --------------------------------------------------------------------------

def test_caminho_relativo_vira_url_absoluta():
    c, _ = cliente([])
    assert c._url("/repos/x/y") == "https://api.github.com/repos/x/y"


def test_url_absoluta_passa_intacta():
    """Necessario porque o link_next ja vem como URL pronta."""
    c, _ = cliente([])
    url = "https://api.github.com/repositories/1/commits?page=2"
    assert c._url(url) == url


# --------------------------------------------------------------------------
# Traducao da resposta
# --------------------------------------------------------------------------

def test_200_devolve_dados_e_metadados():
    resposta = RespostaFalsa(
        200,
        corpo={"full_name": "duckdb/duckdb"},
        headers={"ETag": 'W/"abc"', "X-RateLimit-Remaining": "4999"},
    )
    c, _ = cliente([resposta])

    r = c.get("/repos/duckdb/duckdb")

    assert r.status == 200
    assert r.dados["full_name"] == "duckdb/duckdb"
    assert r.etag == 'W/"abc"'
    assert r.rate_remaining == 4999


def test_304_devolve_dados_none():
    c, _ = cliente([RespostaFalsa(304)])
    r = c.get("/repos/x/y", etag='W/"abc"')
    assert r.nao_modificado
    assert r.dados is None


def test_etag_vira_header_if_none_match():
    c, sessao = cliente([RespostaFalsa(304)])
    c.get("/repos/x/y", etag='W/"abc"')
    assert sessao.chamadas[0]["headers"]["If-None-Match"] == 'W/"abc"'


def test_header_ausente_nao_quebra():
    """Sem X-RateLimit-Remaining, o campo vira None em vez de estourar."""
    c, _ = cliente([RespostaFalsa(200, corpo={})])
    r = c.get("/x")
    assert r.rate_remaining is None


# --------------------------------------------------------------------------
# Retry
# --------------------------------------------------------------------------

def test_retenta_em_500_ate_conseguir():
    c, sessao = cliente(
        [RespostaFalsa(500), RespostaFalsa(503), RespostaFalsa(200, corpo={"ok": True})]
    )
    r = c.get("/x")
    assert r.status == 200
    assert len(sessao.chamadas) == 3


def test_retenta_em_429():
    c, sessao = cliente([RespostaFalsa(429), RespostaFalsa(200, corpo={"ok": True})])
    assert c.get("/x").status == 200
    assert len(sessao.chamadas) == 2


def test_nao_retenta_em_404():
    """Erro do cliente: insistir so gastaria quota."""
    c, sessao = cliente([RespostaFalsa(404)])
    with pytest.raises(ErroGitHub):
        c.get("/repos/nao/existe")
    assert len(sessao.chamadas) == 1


def test_desiste_apos_o_limite_de_tentativas():
    from radar.config import MAX_TENTATIVAS

    c, sessao = cliente([RespostaFalsa(500)] * MAX_TENTATIVAS)
    with pytest.raises(ErroGitHub):
        c.get("/x")
    assert len(sessao.chamadas) == MAX_TENTATIVAS


# --------------------------------------------------------------------------
# Paginacao
# --------------------------------------------------------------------------

def test_paginar_segue_o_link_next():
    pagina1 = RespostaFalsa(
        200,
        corpo=[{"sha": "a"}, {"sha": "b"}],
        links={"next": {"url": "https://api.github.com/x?page=2"}},
    )
    pagina2 = RespostaFalsa(200, corpo=[{"sha": "c"}])  # sem next: acabou
    c, sessao = cliente([pagina1, pagina2])

    itens = list(c.paginar("/x"))

    assert [i["sha"] for i in itens] == ["a", "b", "c"]
    assert sessao.chamadas[1]["url"] == "https://api.github.com/x?page=2"


def test_paginar_respeita_limite_de_paginas():
    def pagina():
        return RespostaFalsa(
            200,
            corpo=[{"sha": "x"}],
            links={"next": {"url": "https://api.github.com/x?p=2"}},
        )

    c, sessao = cliente([pagina() for _ in range(5)])

    itens = list(c.paginar("/x", limite_paginas=2))

    assert len(itens) == 2
    assert len(sessao.chamadas) == 2


def test_paginar_para_em_lista_vazia():
    c, sessao = cliente([RespostaFalsa(200, corpo=[])])
    assert list(c.paginar("/x")) == []
    assert len(sessao.chamadas) == 1
