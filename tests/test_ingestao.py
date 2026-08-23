"""Testes da ingestao. Nao tocam a rede nem o Databricks."""

import json
from datetime import datetime, timezone

from radar.controle import Checkpoint
from radar.ingestao import (
    ENDPOINTS,
    ResultadoIngestao,
    Endpoint,
    caminho_arquivo,
    gravar_jsonl,
    ingerir,
    maior_data,
    para_datetime,
    proximo_checkpoint,
    sanitizar_repo,
    valor_aninhado,
)

COMMITS = ENDPOINTS["commits"]
MOMENTO = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)


def commit(sha: str, data: str) -> dict:
    """Registro no formato que a API devolve."""
    return {"sha": sha, "commit": {"committer": {"date": data}}}


class ClienteFalso:
    """Duble do GitHubClient: devolve o que foi programado e registra chamadas."""

    def __init__(self, sentinela_status=200, etag='W/"novo"', paginas=None,
                 truncado=False):
        self.sentinela_status = sentinela_status
        self.etag = etag
        self.paginas = paginas or []
        self.truncado = truncado
        self.chamadas_get = []
        self.chamadas_paginar = []

    def get(self, caminho, params=None, etag=None):
        from radar.github_client import Resposta

        self.chamadas_get.append({"caminho": caminho, "params": params, "etag": etag})
        return Resposta(
            status=self.sentinela_status,
            dados=None if self.sentinela_status == 304 else [{"sha": "x"}],
            etag=self.etag,
            link_next=None,
            rate_remaining=4999,
            rate_reset=0,
        )

    def paginar(self, caminho, params=None, limite_paginas=None, estado=None):
        self.chamadas_paginar.append({"caminho": caminho, "params": params})
        yield from self.paginas
        if estado is not None:
            estado["truncado"] = self.truncado


# --------------------------------------------------------------------------
# Caminho e particionamento
# --------------------------------------------------------------------------

def test_barra_do_repo_vira_underscore_duplo():
    assert sanitizar_repo("duckdb/duckdb") == "duckdb__duckdb"


def test_caminho_usa_particao_estilo_hive():
    momento = datetime(2026, 8, 21, 3, 5, 9)
    caminho = caminho_arquivo("/Volumes/x/y/raw", "commits", "pola-rs/polars", momento)
    assert caminho == (
        "/Volumes/x/y/raw/commits/repo=pola-rs__polars/dt=2026-08-21/030509.jsonl"
    )


# --------------------------------------------------------------------------
# Extracao de campos
# --------------------------------------------------------------------------

def test_valor_aninhado_navega_por_pontos():
    assert valor_aninhado(commit("a", "2026-08-20T10:00:00Z"), COMMITS.campo_data) == (
        "2026-08-20T10:00:00Z"
    )


def test_valor_aninhado_de_caminho_inexistente_e_none():
    assert valor_aninhado({"a": 1}, "a.b.c") is None


def test_data_iso_com_z_vira_utc():
    esperado = datetime(2026, 8, 20, 10, 0, 0, tzinfo=timezone.utc)
    assert para_datetime("2026-08-20T10:00:00Z") == esperado


def test_data_invalida_vira_none():
    assert para_datetime("nao é data") is None
    assert para_datetime(None) is None


def test_maior_data_ignora_registros_sem_data():
    registros = [
        commit("a", "2026-08-18T10:00:00Z"),
        commit("b", "2026-08-20T10:00:00Z"),
        {"sha": "c"},
    ]
    assert maior_data(registros, COMMITS.campo_data) == datetime(
        2026, 8, 20, 10, 0, 0, tzinfo=timezone.utc
    )


def test_maior_data_de_lista_vazia_e_none():
    assert maior_data([], COMMITS.campo_data) is None


# --------------------------------------------------------------------------
# Gravacao
# --------------------------------------------------------------------------

def test_gravar_jsonl_uma_linha_por_registro(tmp_path):
    destino = tmp_path / "sub" / "dir" / "arquivo.jsonl"
    registros = [commit("a", "2026-08-20T10:00:00Z"), commit("b", "2026-08-21T10:00:00Z")]

    total = gravar_jsonl(str(destino), registros)

    linhas = destino.read_text(encoding="utf-8").strip().split("\n")
    assert total == 2
    assert len(linhas) == 2
    assert json.loads(linhas[0])["sha"] == "a"


def test_gravar_jsonl_nao_altera_o_payload(tmp_path):
    destino = tmp_path / "a.jsonl"
    original = {"sha": "a", "autor": "José", "extra": {"n": 1}}

    gravar_jsonl(str(destino), [original])

    assert json.loads(destino.read_text(encoding="utf-8")) == original


# --------------------------------------------------------------------------
# Orquestracao
# --------------------------------------------------------------------------

def test_sentinela_304_pula_o_repositorio(tmp_path):
    cliente = ClienteFalso(sentinela_status=304)
    ck = Checkpoint(repo="x/y", endpoint="commits", etag='W/"antigo"')

    r = ingerir(cliente, COMMITS, "x/y", ck, str(tmp_path), datetime(2026, 8, 21))

    assert r.pulado is True
    assert r.registros == 0
    assert cliente.chamadas_paginar == []  # coleta nem chegou a acontecer


def test_sentinela_manda_o_etag_guardado():
    cliente = ClienteFalso(sentinela_status=304)
    ck = Checkpoint(repo="x/y", endpoint="commits", etag='W/"antigo"')

    ingerir(cliente, COMMITS, "x/y", ck, "/tmp", datetime(2026, 8, 21))

    assert cliente.chamadas_get[0]["etag"] == 'W/"antigo"'
    assert cliente.chamadas_get[0]["params"] == {"per_page": 1}


def test_coleta_grava_arquivo_e_devolve_maior_data(tmp_path):
    cliente = ClienteFalso(
        paginas=[commit("a", "2026-08-18T10:00:00Z"), commit("b", "2026-08-20T10:00:00Z")]
    )

    r = ingerir(cliente, COMMITS, "x/y", None, str(tmp_path), datetime(2026, 8, 21, 3, 0, 0))

    assert r.pulado is False
    assert r.registros == 2
    assert r.maior_data == datetime(2026, 8, 20, 10, 0, 0, tzinfo=timezone.utc)
    assert (tmp_path / "commits" / "repo=x__y" / "dt=2026-08-21" / "030000.jsonl").exists()


def test_primeira_execucao_nao_manda_since(tmp_path):
    cliente = ClienteFalso(paginas=[commit("a", "2026-08-18T10:00:00Z")])

    ingerir(cliente, COMMITS, "x/y", None, str(tmp_path), datetime(2026, 8, 21))

    assert "since" not in cliente.chamadas_paginar[0]["params"]


def test_execucao_seguinte_manda_since_do_watermark(tmp_path):
    cliente = ClienteFalso(paginas=[commit("a", "2026-08-20T10:00:00Z")])
    ck = Checkpoint(
        repo="x/y",
        endpoint="commits",
        watermark=datetime(2026, 8, 18, 3, 0, 0, tzinfo=timezone.utc),
    )

    ingerir(cliente, COMMITS, "x/y", ck, str(tmp_path), datetime(2026, 8, 21))

    assert cliente.chamadas_paginar[0]["params"]["since"] == "2026-08-18T03:00:00Z"


def test_sem_registros_novos_nao_cria_arquivo(tmp_path):
    cliente = ClienteFalso(paginas=[])

    r = ingerir(cliente, COMMITS, "x/y", None, str(tmp_path), datetime(2026, 8, 21))

    assert r.registros == 0
    assert r.arquivo is None
    assert not any(tmp_path.iterdir())


def test_etag_novo_e_propagado_para_o_resultado(tmp_path):
    cliente = ClienteFalso(etag='W/"recem-vindo"', paginas=[commit("a", "2026-08-20T10:00:00Z")])

    r = ingerir(cliente, COMMITS, "x/y", None, str(tmp_path), datetime(2026, 8, 21))

    assert r.etag == 'W/"recem-vindo"'


# --------------------------------------------------------------------------
# Checkpoint inicial e proximo checkpoint
# --------------------------------------------------------------------------

def test_checkpoint_inicial_limita_a_janela():
    from radar.ingestao import checkpoint_inicial

    ck = checkpoint_inicial("x/y", "commits", datetime(2026, 8, 21), dias_historico=90)
    assert ck.watermark == datetime(2026, 5, 23)


def test_checkpoint_inicial_zero_dias_e_carga_completa():
    from radar.ingestao import checkpoint_inicial

    assert checkpoint_inicial("x/y", "commits", datetime(2026, 8, 21), 0) is None


def test_proximo_checkpoint_avanca_com_sobreposicao(tmp_path):
    from radar.ingestao import proximo_checkpoint

    cliente = ClienteFalso(paginas=[commit("a", "2026-08-20T10:00:00Z")])
    r = ingerir(cliente, COMMITS, "x/y", None, str(tmp_path), datetime(2026, 8, 21))

    ck = proximo_checkpoint(None, r, datetime(2026, 8, 21))

    # maior data 2026-08-20 menos 1 dia de sobreposicao
    assert ck.watermark == datetime(2026, 8, 19, 10, 0, 0, tzinfo=timezone.utc)
    assert ck.registros == 1
    assert ck.status == "ok"


def test_proximo_checkpoint_preserva_watermark_quando_nada_muda(tmp_path):
    from radar.ingestao import proximo_checkpoint

    anterior = Checkpoint(
        repo="x/y",
        endpoint="commits",
        watermark=datetime(2026, 8, 18, 3, 0, 0, tzinfo=timezone.utc),
        etag='W/"antigo"',
    )
    cliente = ClienteFalso(sentinela_status=304)
    r = ingerir(cliente, COMMITS, "x/y", anterior, str(tmp_path), datetime(2026, 8, 21))

    ck = proximo_checkpoint(anterior, r, datetime(2026, 8, 21))

    assert ck.watermark == anterior.watermark  # nao volta para None
    assert ck.registros == 0


# --------------------------------------------------------------------------
# Truncagem: coleta parcial nao pode passar por completa
# --------------------------------------------------------------------------

def test_coleta_completa_nao_marca_truncagem(tmp_path):
    cliente = ClienteFalso(paginas=[{"sha": "a"}])
    resultado = ingerir(
        cliente=cliente,
        endpoint=COMMITS,
        repo="duckdb/duckdb",
        checkpoint=None,
        base_volume=str(tmp_path),
        momento=MOMENTO,
    )
    assert resultado.truncado is False


def test_teto_de_paginas_marca_truncagem(tmp_path):
    cliente = ClienteFalso(paginas=[{"sha": "a"}], truncado=True)
    resultado = ingerir(
        cliente=cliente,
        endpoint=COMMITS,
        repo="duckdb/duckdb",
        checkpoint=None,
        base_volume=str(tmp_path),
        momento=MOMENTO,
        limite_paginas=1,
    )
    assert resultado.truncado is True


def test_truncagem_vira_status_proprio_no_checkpoint():
    # Nem `ok` nem `erro`: a carga funcionou, mas nao foi completa.
    truncada = ResultadoIngestao(
        repo="x", endpoint="commits", registros=500, arquivo="a.jsonl",
        etag=None, maior_data=MOMENTO, pulado=False, truncado=True,
    )
    checkpoint = proximo_checkpoint(None, truncada, MOMENTO)
    assert checkpoint.status == "truncado"


def test_erro_tem_precedencia_sobre_truncagem():
    # Se a carga falhou, o diagnostico util e o erro, nao a truncagem.
    com_erro = ResultadoIngestao(
        repo="x", endpoint="commits", registros=0, arquivo=None,
        etag=None, maior_data=None, pulado=False, truncado=True,
        erro="HTTPError: 502",
    )
    assert proximo_checkpoint(None, com_erro, MOMENTO).status == "erro"


def test_carga_normal_continua_ok():
    normal = ResultadoIngestao(
        repo="x", endpoint="commits", registros=10, arquivo="a.jsonl",
        etag=None, maior_data=MOMENTO, pulado=False,
    )
    assert proximo_checkpoint(None, normal, MOMENTO).status == "ok"
