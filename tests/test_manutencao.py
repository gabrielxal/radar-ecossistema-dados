"""Restricoes, retencao e time travel, sem motor.

Delta nao existe na sessao local, entao `ALTER TABLE ... ADD CONSTRAINT` e
`VERSION AS OF` seguem validados apenas no Databricks, como ja acontece com
`MERGE` e `DESCRIBE HISTORY` (decisao 8.9).

O que da para exercitar aqui e tudo o que nao depende do motor: o catalogo de
restricoes, o texto do DDL, e a logica de aplicacao contra uma sessao duble --
que e onde mora o comportamento interessante, porque e ela que decide o que
pular, o que recriar e o que reportar como falha.
"""

import pytest

from radar import gold, manutencao


# --------------------------------------------------------------------------
# O catalogo
# --------------------------------------------------------------------------

TABELAS_DA_GOLD = {
    gold.TABELA_TEMPO,
    gold.TABELA_AUTOR,
    gold.TABELA_REPOSITORIO,
    gold.TABELA_FCT_COMMIT,
    gold.TABELA_FCT_SNAPSHOT,
    gold.TABELA_FCT_ISSUE,
}


def test_o_catalogo_nao_esta_vazio():
    assert manutencao.RESTRICOES


@pytest.mark.parametrize(
    "restricao", manutencao.RESTRICOES, ids=lambda r: r.nome
)
def test_toda_restricao_e_da_gold(restricao):
    """A fronteira da decisao: CHECK so onde a violacao e defeito nosso.

    Uma restricao sobre bronze ou silver abortaria a carga inteira por causa
    de uma linha suja da origem, que e exatamente o que a quarentena existe
    para evitar. Este teste e o que impede a fronteira de se perder no dia em
    que alguem achar comodo acrescentar uma regra "so para garantir".
    """
    assert restricao.tabela in TABELAS_DA_GOLD


@pytest.mark.parametrize(
    "restricao", manutencao.RESTRICOES, ids=lambda r: r.nome
)
def test_toda_restricao_declara_o_motivo(restricao):
    assert len(restricao.porque) > 40


def test_nomes_sao_unicos_dentro_da_tabela():
    """O Delta guarda a restricao por nome, e `ADD CONSTRAINT` repetido falha.

    Nomes iguais em tabelas diferentes sao validos e usados de proposito:
    `contagens_nao_negativas` existe em `fct_commit` e em `fct_issue`.
    """
    pares = [(r.tabela, r.nome) for r in manutencao.RESTRICOES]
    assert len(pares) == len(set(pares))


def test_nome_repetido_entre_tabelas_e_permitido():
    nomes = [r.nome for r in manutencao.RESTRICOES]
    assert len(nomes) > len(set(nomes))


def test_a_restricao_rejeitada_nao_entrou():
    """`dias_ate_o_commit >= 0` e o contraexemplo da fronteira.

    Relogio de contribuidor adiantado produz negativo legitimo, entao ali a
    violacao e sujeira da origem e a resposta certa e medir, nao abortar a
    carga semanal. O docstring do modulo registra a rejeicao; este teste
    impede que ela seja desfeita sem passar por aqui.
    """
    expressoes = " ".join(r.expressao for r in manutencao.RESTRICOES)
    assert "dias_ate_o_commit" not in expressoes


# --------------------------------------------------------------------------
# O DDL
# --------------------------------------------------------------------------

def test_sql_adicionar():
    restricao = manutencao.RESTRICOES[0]
    sql = manutencao.sql_adicionar(restricao)

    assert sql.startswith(f"ALTER TABLE {restricao.tabela} ADD CONSTRAINT")
    assert f"CHECK ({restricao.expressao})" in sql


def test_sql_remover_tolera_ausencia():
    """`IF EXISTS` porque `recriar` roda antes da primeira criacao existir."""
    assert "DROP CONSTRAINT IF EXISTS" in manutencao.sql_remover(
        manutencao.RESTRICOES[0]
    )


def test_sql_retencao_fixa_as_duas_propriedades():
    sql = manutencao.sql_retencao("cat.esq.t")

    assert "ALTER TABLE cat.esq.t SET TBLPROPERTIES" in sql
    assert "'delta.deletedFileRetentionDuration' = 'interval 14 days'" in sql
    assert "'delta.logRetentionDuration' = 'interval 30 days'" in sql


def test_vacuum_sem_horas_usa_a_propriedade_da_tabela():
    """Sem `RETAIN`, o motor le a politica ja declarada.

    E o caminho certo: a retencao fica num lugar so, e o comando nao pode
    contradizer o que a tabela declara.
    """
    assert manutencao.sql_vacuum("t") == "VACUUM t"


def test_vacuum_com_horas_declara_o_retain():
    assert manutencao.sql_vacuum("t", 336) == "VACUUM t RETAIN 336 HOURS"


def test_vacuum_recusa_horas_negativas():
    with pytest.raises(ValueError):
        manutencao.sql_vacuum("t", -1)


def test_tabelas_gerenciadas_nao_inclui_rascunho():
    """As tabelas `lote_` sao reescritas inteiras a cada execucao.

    Sao as que mais acumulam versao e as unicas sem valor de auditoria.
    Guardar catorze dias delas seria pagar retencao pelo que nunca sera lido.
    """
    assert not [t for t in manutencao.tabelas_gerenciadas() if ".lote_" in t]


def test_tabelas_gerenciadas_inclui_a_quarentena():
    """A quarentena guarda o que a silver recusou, com o motivo.

    E dado de investigacao: se ficasse de fora da politica, o unico registro
    do que foi rejeitado envelheceria sob a retencao padrao.
    """
    from radar import silver

    assert silver.TABELA_REJEITADOS in manutencao.tabelas_gerenciadas()


def test_tabelas_gerenciadas_nao_repete():
    gerenciadas = manutencao.tabelas_gerenciadas()
    assert len(gerenciadas) == len(set(gerenciadas))


# --------------------------------------------------------------------------
# Time travel
# --------------------------------------------------------------------------

def test_historico_le_as_metricas_da_operacao():
    sql = manutencao.sql_historico("t", 5)

    assert "DESCRIBE HISTORY t" in sql
    assert "operationMetrics['numOutputRows']" in sql
    assert "LIMIT 5" in sql


def test_historico_recusa_limite_invalido():
    with pytest.raises(ValueError):
        manutencao.sql_historico("t", 0)


def test_contagem_por_versao_gera_uma_leitura_por_versao():
    sql = manutencao.sql_contagem_por_versao("t", [3, 7])

    assert sql.count("VERSION AS OF") == 2
    assert "VERSION AS OF 3" in sql
    assert "VERSION AS OF 7" in sql
    assert "UNION ALL" in sql


def test_contagem_por_versao_exige_pelo_menos_uma():
    with pytest.raises(ValueError, match="nenhuma versao"):
        manutencao.sql_contagem_por_versao("t", [])


def test_contagem_por_versao_recusa_versao_negativa():
    """A versao entra interpolada no SQL, entao o valor precisa ser validado.

    `int()` ja barra texto; o negativo passaria por ele e produziria SQL que o
    motor recusa longe daqui.
    """
    with pytest.raises(ValueError, match="negativa"):
        manutencao.sql_contagem_por_versao("t", [-1])


# --------------------------------------------------------------------------
# A aplicacao, contra uma sessao duble
# --------------------------------------------------------------------------

class SessaoFalsa:
    """Duble de `spark` que registra o SQL recebido.

    `propriedades` diz quais restricoes cada tabela ja tem, e `falhar_em`
    simula a tabela que viola a regra, que e o caso que importa: e assim que
    uma restricao nova encontra dado velho torto.
    """

    def __init__(self, propriedades=None, falhar_em=()):
        self.propriedades = propriedades or {}
        self.falhar_em = set(falhar_em)
        self.executados = []

    def sql(self, comando):
        self.executados.append(comando)

        if comando.startswith("SHOW TBLPROPERTIES"):
            tabela = comando.split()[-1]
            return _Linhas(
                [
                    {"key": f"delta.constraints.{nome}", "value": "x"}
                    for nome in self.propriedades.get(tabela, ())
                ]
            )

        for trecho in self.falhar_em:
            if trecho in comando:
                raise RuntimeError("DELTA_NEW_CHECK_CONSTRAINT_VIOLATION")

        return _Linhas([])


class _Linhas:
    def __init__(self, linhas):
        self._linhas = linhas

    def collect(self):
        return self._linhas


def uma(tabela=None, nome="regra", expressao="1 = 1"):
    return manutencao.Restricao(
        tabela or gold.TABELA_TEMPO, nome, expressao, "porque " + "x" * 50
    )


def test_restricao_nova_e_criada():
    sessao = SessaoFalsa()
    resultado = manutencao.aplicar_restricoes(sessao, [uma()])

    assert list(resultado.values()) == ["criada"]
    assert any("ADD CONSTRAINT regra" in c for c in sessao.executados)


def test_restricao_existente_e_pulada():
    """Reexecutar a tarefa nao pode falhar: `ADD CONSTRAINT` repetido e erro."""
    sessao = SessaoFalsa(propriedades={gold.TABELA_TEMPO: ["regra"]})
    resultado = manutencao.aplicar_restricoes(sessao, [uma()])

    assert list(resultado.values()) == ["ja existia"]
    assert not any("ADD CONSTRAINT" in c for c in sessao.executados)


def test_o_nome_e_comparado_em_minusculas():
    """O Delta guarda a propriedade com o nome rebaixado.

    Comparar sem normalizar faria uma restricao ja existente parecer ausente,
    e a tarefa passaria a falhar toda semana.
    """
    sessao = SessaoFalsa(propriedades={gold.TABELA_TEMPO: ["regra"]})
    resultado = manutencao.aplicar_restricoes(sessao, [uma(nome="REGRA")])

    assert list(resultado.values()) == ["ja existia"]


def test_recriar_derruba_antes_de_criar():
    """O caminho de quando a expressao muda.

    Sem derrubar, `ADD CONSTRAINT` falharia pelo nome repetido e a restricao
    antiga continuaria valendo enquanto o codigo diria outra coisa.
    """
    sessao = SessaoFalsa(propriedades={gold.TABELA_TEMPO: ["regra"]})
    resultado = manutencao.aplicar_restricoes(sessao, [uma()], recriar=True)

    assert list(resultado.values()) == ["criada"]
    ordem = [c for c in sessao.executados if "CONSTRAINT" in c]
    assert "DROP CONSTRAINT" in ordem[0]
    assert "ADD CONSTRAINT" in ordem[1]


def test_violacao_entra_no_resultado_sem_interromper():
    """Parar na primeira falha esconderia as demais.

    Uma restricao que a tabela atual viola e justamente o achado que se quer
    ver por inteiro: saber que tres regras falharam, e quais, vale mais que
    descobrir uma por execucao.
    """
    sessao = SessaoFalsa(falhar_em=["ADD CONSTRAINT primeira"])
    resultado = manutencao.aplicar_restricoes(
        sessao, [uma(nome="primeira"), uma(nome="segunda")]
    )

    valores = list(resultado.values())
    assert valores[0].startswith("FALHOU")
    assert valores[1] == "criada"


def test_uma_leitura_de_propriedades_por_tabela():
    """Dez restricoes sobre seis tabelas nao justificam dez consultas.

    Os nomes sao unicos dentro de cada tabela, entao reler entre uma e outra
    nao mudaria nenhuma decisao.
    """
    sessao = SessaoFalsa()
    manutencao.aplicar_restricoes(
        sessao,
        [uma(nome="a"), uma(nome="b"), uma(gold.TABELA_AUTOR, nome="c")],
    )

    leituras = [c for c in sessao.executados if c.startswith("SHOW TBLPROPERTIES")]
    assert len(leituras) == 2


def test_retencao_percorre_as_tabelas_pedidas():
    sessao = SessaoFalsa()
    alvo = manutencao.aplicar_retencao(sessao, ["a", "b"])

    assert alvo == ("a", "b")
    assert len(sessao.executados) == 2
    assert all("SET TBLPROPERTIES" in c for c in sessao.executados)
