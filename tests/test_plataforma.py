"""Restricoes do Databricks Serverless, verificadas no codigo-fonte.

O Serverless gerencia o motor e fecha os controles que o afetam. Cada regra
aqui nasceu de um erro real em execucao, e existe para o proximo aparecer em
0,3s de pytest em vez de no meio de uma carga.

Sao testes de texto, nao de comportamento: verificam o que o codigo escreve,
nao o que ele faz.
"""

from pathlib import Path

import pytest

FONTES = sorted((Path(__file__).resolve().parents[1] / "src" / "radar").glob("*.py"))
NOTEBOOKS = sorted((Path(__file__).resolve().parents[1] / "notebooks").glob("*.py"))


def texto(caminho):
    return caminho.read_text(encoding="utf-8")


def test_existem_fontes_e_notebooks_para_verificar():
    # Sem isto, um glob quebrado faria a suite inteira passar por vazio.
    assert FONTES and NOTEBOOKS


@pytest.mark.parametrize("arquivo", FONTES + NOTEBOOKS, ids=lambda p: p.name)
def test_sem_persistencia_de_dataframe(arquivo):
    # `NOT_SUPPORTED_WITH_SERVERLESS: PERSIST TABLE`. Sem cache, a saida e
    # pedir menos passagens sobre o DataFrame, nao guardar o resultado.
    conteudo = texto(arquivo)
    assert ".cache()" not in conteudo
    assert ".persist(" not in conteudo


@pytest.mark.parametrize("arquivo", FONTES + NOTEBOOKS, ids=lambda p: p.name)
def test_sem_magic_do_ipython(arquivo):
    # `%load_ext` nao e magic do Databricks: vai para o parser do Python e
    # vira SyntaxError na primeira celula.
    assert "%load_ext" not in texto(arquivo)
    assert "%autoreload" not in texto(arquivo)


@pytest.mark.parametrize("arquivo", FONTES + NOTEBOOKS, ids=lambda p: p.name)
def test_so_configuracao_de_sessao_permitida(arquivo):
    # O Serverless aceita alterar uma lista fechada de configuracoes.
    # `spark.sql.session.timeZone` esta nela; a inferencia de tipo de
    # particao, por exemplo, nao esta.
    permitidas = {"spark.sql.session.timeZone"}
    for linha in texto(arquivo).splitlines():
        if "spark.conf.set(" in linha:
            chave = linha.split('"')[1]
            assert chave in permitidas, f"{arquivo.name}: {chave}"


@pytest.mark.parametrize("arquivo", FONTES, ids=lambda p: p.name)
def test_cast_de_data_tolera_valor_invalido(arquivo):
    # Com ANSI ligado -- padrao em runtime recente -- `to_timestamp` lanca
    # excecao e um registro torto derruba a carga inteira.
    conteudo = texto(arquivo)
    for linha in conteudo.splitlines():
        if "to_timestamp(" in linha and "try_to_timestamp(" not in linha:
            assert linha.strip().startswith("#"), f"{arquivo.name}: {linha.strip()}"
