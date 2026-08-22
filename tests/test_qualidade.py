"""Testes das funcoes puras da qualidade. Sem Spark, sem Databricks."""

import pytest

from radar import qualidade
from radar.config import REPOS
from radar.ingestao import ENDPOINTS

COMMITS = ENDPOINTS["commits"]
BATERIA = qualidade.verificacoes_bronze(COMMITS)


def resultado(nome, severidade, violacoes):
    return qualidade.Resultado(nome=nome, severidade=severidade, violacoes=violacoes)


# --------------------------------------------------------------------------
# A bateria
# --------------------------------------------------------------------------

def test_toda_verificacao_tem_severidade_conhecida():
    for v in BATERIA:
        assert v.severidade in qualidade.SEVERIDADES


def test_nomes_das_verificacoes_sao_unicos():
    nomes = [v.nome for v in BATERIA]
    assert len(nomes) == len(set(nomes))


def test_toda_verificacao_consulta_a_tabela_do_endpoint():
    for v in BATERIA:
        assert "workspace.radar_bronze.commits" in v.sql


def test_toda_verificacao_devolve_a_coluna_violacoes():
    # O executor le `violacoes` pelo nome; sem isso a bateria quebra em runtime.
    for v in BATERIA:
        assert "AS violacoes" in v.sql


def test_toda_verificacao_tem_descricao_util():
    for v in BATERIA:
        assert len(v.descricao) > 30


def test_bateria_usa_a_chave_natural_do_endpoint():
    duplicada = next(v for v in BATERIA if v.nome == "chave_duplicada")
    assert f"GROUP BY repo, {COMMITS.chave}" in duplicada.sql


def test_escopo_lista_todos_os_repositorios_do_config():
    escopo = next(v for v in BATERIA if v.nome == "repo_fora_do_escopo")
    for repo in REPOS:
        assert f"'{repo}'" in escopo.sql


def test_existe_regra_bloqueante_e_regra_de_aviso():
    severidades = {v.severidade for v in BATERIA}
    assert severidades == {qualidade.BLOQUEIA, qualidade.AVISA}


# --------------------------------------------------------------------------
# Resultado e resumo
# --------------------------------------------------------------------------

def test_passou_e_zero_violacoes():
    assert resultado("x", qualidade.BLOQUEIA, 0).passou
    assert not resultado("x", qualidade.BLOQUEIA, 1).passou


def test_resumo_separa_bloqueios_de_avisos():
    resultados = [
        resultado("a", qualidade.BLOQUEIA, 0),
        resultado("b", qualidade.BLOQUEIA, 3),
        resultado("c", qualidade.AVISA, 7),
        resultado("d", qualidade.AVISA, 0),
    ]
    assert qualidade.resumo(resultados) == (1, 1)


def test_aviso_que_falha_nao_bloqueia():
    qualidade.levantar_se_bloqueou([resultado("c", qualidade.AVISA, 99)])


def test_bloqueio_que_falha_interrompe_com_o_detalhe():
    with pytest.raises(AssertionError, match="chave_duplicada=2"):
        qualidade.levantar_se_bloqueou(
            [resultado("chave_duplicada", qualidade.BLOQUEIA, 2)]
        )


# --------------------------------------------------------------------------
# Reconciliacao
# --------------------------------------------------------------------------

def test_reconciliacao_bate_quando_as_contagens_sao_iguais():
    r = qualidade.Reconciliacao(na_origem=200, na_bronze=200)
    assert r.bate
    assert r.diferenca == 0


def test_reconciliacao_acusa_linha_perdida_no_caminho():
    r = qualidade.Reconciliacao(na_origem=200, na_bronze=198)
    assert not r.bate
    assert r.diferenca == 2
