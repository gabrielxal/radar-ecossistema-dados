"""Testes da ingestao. Nao tocam a rede nem o Databricks."""

import json
from datetime import datetime, timedelta, timezone

from radar.controle import Checkpoint
from radar.ingestao import (
    ENDPOINTS,
    ResultadoIngestao,
    Endpoint,
    caminho_arquivo,
    coletar,
    deduplicar_por_chave,
    gravar_jsonl,
    janelas,
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


def test_truncagem_nao_avanca_o_watermark():
    # A API entrega do mais novo para o mais antigo e `since` so aceita limite
    # inferior: avancar deixaria o buraco para tras, inalcancavel.
    anterior = Checkpoint(
        repo="x", endpoint="commits",
        watermark=datetime(2026, 5, 24, tzinfo=timezone.utc),
    )
    truncada = ResultadoIngestao(
        repo="x", endpoint="commits", registros=500, arquivo="a.jsonl",
        etag=None, maior_data=datetime(2026, 8, 22, tzinfo=timezone.utc),
        pulado=False, truncado=True,
    )
    proximo = proximo_checkpoint(anterior, truncada, MOMENTO)
    assert proximo.watermark == anterior.watermark


def test_coleta_completa_avanca_o_watermark_normalmente():
    anterior = Checkpoint(
        repo="x", endpoint="commits",
        watermark=datetime(2026, 5, 24, tzinfo=timezone.utc),
    )
    completa = ResultadoIngestao(
        repo="x", endpoint="commits", registros=120, arquivo="a.jsonl",
        etag=None, maior_data=datetime(2026, 8, 22, tzinfo=timezone.utc),
        pulado=False, truncado=False,
    )
    proximo = proximo_checkpoint(anterior, completa, MOMENTO)
    assert proximo.watermark > anterior.watermark


def test_truncagem_na_primeira_execucao_nao_inventa_watermark():
    truncada = ResultadoIngestao(
        repo="x", endpoint="commits", registros=500, arquivo="a.jsonl",
        etag=None, maior_data=datetime(2026, 8, 22, tzinfo=timezone.utc),
        pulado=False, truncado=True,
    )
    assert proximo_checkpoint(None, truncada, MOMENTO).watermark is None


def test_checkpoint_truncado_ignora_o_etag(tmp_path):
    # A sentinela olha so o topo da lista. Um 304 pularia o repositorio que
    # justamente tem historico por coletar, e a recuperacao nunca aconteceria.
    cliente = ClienteFalso(paginas=[{"sha": "a"}])
    truncado = Checkpoint(
        repo="x", endpoint="commits", etag='W/"antigo"', status="truncado",
        watermark=datetime(2026, 5, 25, tzinfo=timezone.utc),
    )
    ingerir(
        cliente=cliente, endpoint=COMMITS, repo="x", checkpoint=truncado,
        base_volume=str(tmp_path), momento=MOMENTO,
    )
    assert cliente.chamadas_get[0]["etag"] is None


def test_checkpoint_normal_usa_o_etag(tmp_path):
    cliente = ClienteFalso(paginas=[{"sha": "a"}])
    normal = Checkpoint(
        repo="x", endpoint="commits", etag='W/"antigo"', status="ok",
        watermark=datetime(2026, 5, 25, tzinfo=timezone.utc),
    )
    ingerir(
        cliente=cliente, endpoint=COMMITS, repo="x", checkpoint=normal,
        base_volume=str(tmp_path), momento=MOMENTO,
    )
    assert cliente.chamadas_get[0]["etag"] == 'W/"antigo"'


# --------------------------------------------------------------------------
# O contrato do Endpoint
# --------------------------------------------------------------------------

def test_campo_data_de_commits_e_o_que_a_api_filtra_em_since():
    """Invariante do watermark.

    O `since` de `/commits` filtra pela data do committer, e o watermark e
    calculado a partir de `campo_data`. Apontar `campo_data` para a data de
    autoria faria o marcador andar em desacordo com o filtro: autoria e
    anterior ao commit, e um rebase separa as duas por meses.
    """
    assert COMMITS.campo_data == "commit.committer.date"


def test_so_endpoint_com_until_e_fatiado_em_janelas():
    """`/issues` nao aceita `until`; `/commits` aceita."""
    assert COMMITS.aceita_until is True
    assert ENDPOINTS["repositorios"].aceita_until is False


def test_params_extra_viram_dicionario():
    endpoint = Endpoint(
        nome="issues", caminho="/repos/{repo}/issues", campo_data="updated_at",
        chave="id", chaves=("repo", "id"),
        params_extra=(("state", "all"), ("direction", "asc")),
    )
    assert endpoint.extras == {"state": "all", "direction": "asc"}


def test_endpoint_sem_params_extra_nao_altera_a_sentinela():
    assert COMMITS.extras == {}


# --------------------------------------------------------------------------
# Backfill em janelas -- a correcao do defeito da secao 5.7
# --------------------------------------------------------------------------

def test_noventa_dias_viram_treze_janelas():
    inicio = datetime(2026, 5, 24, tzinfo=timezone.utc)
    fim = inicio + timedelta(days=90)
    intervalos = janelas(inicio, fim, dias=7)
    assert len(intervalos) == 13
    assert intervalos[0][0] == inicio
    assert intervalos[-1][1] == fim


def test_janelas_sao_contiguas():
    """O fim de uma e o inicio da seguinte: nenhum intervalo fica de fora."""
    inicio = datetime(2026, 5, 24, tzinfo=timezone.utc)
    intervalos = janelas(inicio, inicio + timedelta(days=20), dias=7)
    for anterior, seguinte in zip(intervalos, intervalos[1:]):
        assert anterior[1] == seguinte[0]


def test_janela_final_nao_ultrapassa_o_fim():
    inicio = datetime(2026, 5, 24, tzinfo=timezone.utc)
    fim = inicio + timedelta(days=10)
    assert janelas(inicio, fim, dias=7)[-1][1] == fim


def test_watermark_sem_fuso_nao_derruba_a_coleta():
    """O watermark lido do controle pode voltar naive; `momento` nunca e."""
    naive = datetime(2026, 8, 1, 12, 0, 0)
    intervalos = janelas(naive, MOMENTO, dias=7)
    assert len(intervalos) == 3


def test_sem_watermark_nao_ha_o_que_fatiar():
    """Sem limite inferior a coleta e direta, nao em janelas."""
    assert janelas(None, MOMENTO) == []
    assert janelas(MOMENTO, None) == []


def test_intervalo_invertido_nao_gera_janela():
    """Relogio adiantado nao pode virar laco infinito nem chamada invalida."""
    assert janelas(MOMENTO, MOMENTO - timedelta(days=1)) == []
    assert janelas(MOMENTO, MOMENTO) == []


def test_coleta_em_janelas_faz_uma_chamada_por_intervalo():
    cliente = ClienteFalso(paginas=[{"sha": "a"}])
    checkpoint = Checkpoint(
        repo="x", endpoint="commits",
        watermark=MOMENTO - timedelta(days=21),
    )
    coletar(cliente, COMMITS, "x", checkpoint, ate=MOMENTO)

    assert len(cliente.chamadas_paginar) == 3
    for chamada in cliente.chamadas_paginar:
        assert "since" in chamada["params"]
        assert "until" in chamada["params"]


def test_janelas_cobrem_a_borda_antiga_do_intervalo():
    """O defeito da 5.7: a chamada unica nunca chegava ao inicio da janela."""
    cliente = ClienteFalso(paginas=[{"sha": "a"}])
    inicio = MOMENTO - timedelta(days=21)
    coletar(
        cliente, COMMITS, "x",
        Checkpoint(repo="x", endpoint="commits", watermark=inicio),
        ate=MOMENTO,
    )
    assert cliente.chamadas_paginar[0]["params"]["since"] == "2026-08-01T12:00:00Z"


def test_endpoint_sem_until_faz_chamada_unica():
    cliente = ClienteFalso(paginas=[{"id": 1}])
    checkpoint = Checkpoint(
        repo="x", endpoint="repositorios",
        watermark=MOMENTO - timedelta(days=90),
    )
    coletar(cliente, ENDPOINTS["repositorios"], "x", checkpoint, ate=MOMENTO)

    assert len(cliente.chamadas_paginar) == 1
    assert "until" not in cliente.chamadas_paginar[0]["params"]


def test_truncagem_em_qualquer_janela_trunca_a_coleta():
    cliente = ClienteFalso(paginas=[{"sha": "a"}], truncado=True)
    checkpoint = Checkpoint(
        repo="x", endpoint="commits",
        watermark=MOMENTO - timedelta(days=14),
    )
    _, truncado = coletar(cliente, COMMITS, "x", checkpoint, ate=MOMENTO)
    assert truncado is True


def test_borda_repetida_entre_janelas_nao_duplica_registro():
    """`since` e `until` sao inclusivos: o registro do limite vem duas vezes."""
    cliente = ClienteFalso(paginas=[{"sha": "repetido"}])
    checkpoint = Checkpoint(
        repo="x", endpoint="commits",
        watermark=MOMENTO - timedelta(days=21),
    )
    registros, _ = coletar(cliente, COMMITS, "x", checkpoint, ate=MOMENTO)

    assert len(cliente.chamadas_paginar) == 3
    assert len(registros) == 1


# --------------------------------------------------------------------------
# deduplicar_por_chave
# --------------------------------------------------------------------------

def test_deduplicacao_preserva_a_ordem_de_chegada():
    registros = [{"sha": "a"}, {"sha": "b"}, {"sha": "a"}, {"sha": "c"}]
    assert [r["sha"] for r in deduplicar_por_chave(registros, "sha")] == ["a", "b", "c"]


def test_registro_sem_chave_nao_e_descartado():
    """Sumir com o que nao se consegue identificar e o defeito da 5.7 de novo."""
    registros = [{"sha": None}, {"sha": None}, {"sha": "a"}]
    assert len(deduplicar_por_chave(registros, "sha")) == 3


# --------------------------------------------------------------------------
# O endpoint de issues
# --------------------------------------------------------------------------

ISSUES = ENDPOINTS["issues"]


def test_issues_filtra_por_updated_at():
    """O `since` de /issues nao filtra pela data de criacao.

    Apontar `campo_data` para `created_at` faria o watermark andar num ritmo
    e o filtro em outro, e a coleta deixaria de convergir sem erro nenhum.
    """
    assert ISSUES.campo_data == "updated_at"


def test_issues_pede_abertas_e_fechadas():
    """Sem `state=all` a API devolve so as abertas, e o fato perde o marco final."""
    assert ISSUES.extras["state"] == "all"


def test_issues_coleta_em_ordem_crescente():
    """E o que substitui o `until`, que a API nao oferece neste endpoint."""
    assert ISSUES.extras["direction"] == "asc"
    assert ISSUES.extras["sort"] == "updated"
    assert ISSUES.ordem_crescente is True
    assert ISSUES.aceita_until is False


def test_issue_e_registro_mutavel_e_commit_nao():
    assert ISSUES.mutavel is True
    assert COMMITS.mutavel is False


def test_dia_entra_na_chave_da_bronze_de_issues():
    """E o que faz a bronze virar log de versoes em vez de sobrescrever."""
    assert ISSUES.chaves == ("repo", "id", "dt")


# --------------------------------------------------------------------------
# Truncagem: a mesma regra nos dois sentidos produz defeitos opostos
# --------------------------------------------------------------------------

ANTES = datetime(2026, 5, 24, tzinfo=timezone.utc)


def truncada(endpoint, maior_data=None):
    anterior = Checkpoint(repo="x", endpoint=endpoint.nome, watermark=ANTES)
    resultado = ResultadoIngestao(
        repo="x", endpoint=endpoint.nome, registros=500, arquivo="a.jsonl",
        etag=None, maior_data=maior_data, pulado=False, truncado=True,
    )
    return proximo_checkpoint(anterior, resultado, MOMENTO, endpoint)


def test_coleta_decrescente_truncada_nao_avanca_o_watermark():
    """O que o teto cortou esta atras: avancar tornaria a falta permanente."""
    novo = truncada(COMMITS, maior_data=MOMENTO)
    assert novo.watermark == ANTES


def test_coleta_crescente_truncada_avanca_o_watermark():
    """O que o teto cortou esta a frente: nao avancar nunca terminaria o backfill."""
    novo = truncada(ISSUES, maior_data=MOMENTO)
    assert novo.watermark > ANTES


def test_truncagem_fica_registrada_nos_dois_sentidos():
    """Avancar o watermark nao e o mesmo que dizer que a carga foi completa."""
    assert truncada(COMMITS, MOMENTO).status == "truncado"
    assert truncada(ISSUES, MOMENTO).status == "truncado"


def test_sem_endpoint_a_regra_antiga_continua_valendo():
    anterior = Checkpoint(repo="x", endpoint="commits", watermark=ANTES)
    resultado = ResultadoIngestao(
        repo="x", endpoint="commits", registros=500, arquivo="a.jsonl",
        etag=None, maior_data=MOMENTO, pulado=False, truncado=True,
    )
    assert proximo_checkpoint(anterior, resultado, MOMENTO).watermark == ANTES


def test_sentinela_de_issues_usa_os_parametros_do_endpoint(tmp_path):
    """A sentinela precisa observar o mesmo recorte que a coleta.

    Sem `state=all` nela, o ETag descreveria so as issues abertas, e uma
    issue fechada desde a ultima execucao nao contaria como movimento.
    """
    cliente = ClienteFalso(paginas=[])
    ingerir(
        cliente=cliente, endpoint=ISSUES, repo="x", checkpoint=None,
        base_volume=str(tmp_path), momento=MOMENTO,
    )
    assert cliente.chamadas_get[0]["params"]["state"] == "all"
    assert cliente.chamadas_get[0]["params"]["per_page"] == 1
