"""A camada de consumo, sem motor.

O que se verifica aqui e o catalogo: nome, unicidade, comentario e a forma do
DDL. Se a consulta responde certo e assunto de `test_analises_spark.py`, que e
onde o SQL ja e exercitado.
"""

import pytest

from radar import consumo
from radar.config import GOLD


TABELAS_FALSAS = {
    "fato": "_f",
    "repositorio": "_r",
    "autor": "_a",
    "tempo": "_t",
    "issue": "_i",
    "silver_issues": "_si",
    "controle_ingestao": "_c",
}


def test_o_catalogo_nao_esta_vazio():
    assert consumo.VISOES


def test_nome_qualificado_com_prefixo():
    assert consumo.nome_visao("x") == f"workspace.{GOLD}.vw_x"


def test_sufixos_sao_unicos():
    """Duas visoes com o mesmo sufixo: a segunda substituiria a primeira, e o
    `CREATE OR REPLACE` faria isso sem erro nenhum."""
    sufixos = [v.sufixo for v in consumo.VISOES]
    assert len(sufixos) == len(set(sufixos))


@pytest.mark.parametrize("visao", consumo.VISOES, ids=lambda v: v.sufixo)
def test_toda_visao_constroi_sql_sobre_as_tabelas_recebidas(visao):
    """Cada construtor aceita os nomes por palavra-chave.

    E o contrato que permite ao teste apontar para dado sintetico. Uma funcao
    que ignorasse os parametros passaria a consultar o catalogo real de dentro
    da suite.
    """
    sql = visao.construir(**TABELAS_FALSAS)

    assert isinstance(sql, str) and sql.strip()
    assert "workspace." not in sql


@pytest.mark.parametrize("visao", consumo.VISOES, ids=lambda v: v.sufixo)
def test_toda_visao_tem_comentario(visao):
    assert len(visao.comentario) > 20


def test_ddl_declara_a_visao_com_comentario():
    visao = consumo.VISOES[0]
    texto = consumo.ddl(visao, **TABELAS_FALSAS)

    assert f"CREATE OR REPLACE VIEW {visao.nome}" in texto
    assert f"COMMENT '{visao.comentario}'" in texto


def test_comentario_com_aspa_simples_e_recusado():
    """A aspa fecharia a string do COMMENT e o resto do texto viraria SQL.

    Recusar na montagem troca um erro de sintaxe no meio da carga por uma
    mensagem que diz qual visao esta errada.
    """
    quebrada = consumo.Visao("x", lambda **_: "SELECT 1", "nao e o 'grao' certo")

    with pytest.raises(ValueError, match="aspa simples"):
        consumo.ddl(quebrada, **TABELAS_FALSAS)


def test_o_painel_expoe_o_portao():
    """A coluna que separa leitura valida de leitura deslocada."""
    sql = consumo.painel_com_portao(**TABELAS_FALSAS)

    assert "issues_confiavel" in sql
    assert TABELAS_FALSAS["controle_ingestao"] in sql
