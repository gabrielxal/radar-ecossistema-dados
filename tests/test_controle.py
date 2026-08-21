"""Testes das funcoes puras do modulo de controle.

Nenhum destes testes precisa de Spark nem de Databricks: as funcoes de
calculo foram separadas do acesso a tabela justamente para isso.
"""

from datetime import datetime, timedelta, timezone

from radar.controle import (
    Checkpoint,
    calcular_watermark,
    para_iso,
    parametros_de_busca,
)


# --------------------------------------------------------------------------
# calcular_watermark -- a janela de sobreposicao
# --------------------------------------------------------------------------

def test_watermark_recua_a_janela_de_sobreposicao():
    maior = datetime(2026, 8, 19, 3, 0, 0)
    assert calcular_watermark(maior, dias_sobreposicao=1) == datetime(2026, 8, 18, 3, 0, 0)


def test_watermark_sem_dado_ingerido_e_none():
    """Primeira execucao: nao ha de onde recuar."""
    assert calcular_watermark(None) is None


def test_watermark_com_sobreposicao_zero_nao_recua():
    maior = datetime(2026, 8, 19, 3, 0, 0)
    assert calcular_watermark(maior, dias_sobreposicao=0) == maior


def test_watermark_nunca_avanca_alem_do_dado():
    """O watermark e sempre <= a maior data ingerida. Nunca o relogio local."""
    maior = datetime(2026, 8, 19, 3, 0, 0)
    assert calcular_watermark(maior) <= maior


# --------------------------------------------------------------------------
# para_iso -- formato aceito pelo parametro `since`
# --------------------------------------------------------------------------

def test_iso_no_formato_da_api():
    momento = datetime(2026, 8, 18, 3, 0, 0, tzinfo=timezone.utc)
    assert para_iso(momento) == "2026-08-18T03:00:00Z"


def test_iso_trata_data_sem_fuso_como_utc():
    """Nunca usar o fuso local: mudaria o resultado conforme onde o job roda."""
    assert para_iso(datetime(2026, 8, 18, 3, 0, 0)) == "2026-08-18T03:00:00Z"


def test_iso_converte_outro_fuso_para_utc():
    momento = datetime(2026, 8, 18, 0, 0, 0, tzinfo=timezone(timedelta(hours=-3)))
    assert para_iso(momento) == "2026-08-18T03:00:00Z"


def test_iso_de_none_e_none():
    assert para_iso(None) is None


# --------------------------------------------------------------------------
# parametros_de_busca -- o checkpoint virando chamada de API
# --------------------------------------------------------------------------

def test_sem_checkpoint_a_carga_e_completa():
    """Primeira execucao nao manda `since`: puxa tudo."""
    params = parametros_de_busca(None, per_page=100)
    assert params == {"per_page": 100}


def test_com_checkpoint_manda_since():
    ck = Checkpoint(
        repo="duckdb/duckdb",
        endpoint="commits",
        watermark=datetime(2026, 8, 18, 3, 0, 0, tzinfo=timezone.utc),
    )
    params = parametros_de_busca(ck, per_page=100)
    assert params["since"] == "2026-08-18T03:00:00Z"
    assert params["per_page"] == 100


def test_checkpoint_sem_watermark_nao_manda_since():
    """Repo ja registrado mas ainda sem carga bem-sucedida."""
    ck = Checkpoint(repo="x/y", endpoint="commits", watermark=None)
    assert "since" not in parametros_de_busca(ck, per_page=50)


# --------------------------------------------------------------------------
# Checkpoint -- o contrato
# --------------------------------------------------------------------------

def test_checkpoint_tem_valores_padrao_seguros():
    ck = Checkpoint(repo="x/y", endpoint="commits")
    assert ck.watermark is None
    assert ck.etag is None
    assert ck.registros == 0
    assert ck.status == "ok"


def test_checkpoint_e_imutavel():
    import pytest

    ck = Checkpoint(repo="x/y", endpoint="commits")
    with pytest.raises(Exception):
        ck.repo = "outro/repo"
