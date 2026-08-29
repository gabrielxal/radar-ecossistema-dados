"""Os instrumentos de medida, sem motor.

Medir e uma coisa; ler a medida e outra, e e a segunda que erra em silencio.
Um resumo de plano que conta errado, uma mediana tirada de uma amostra so, um
tamanho medio calculado sobre zero arquivo: nenhum desses quebra, todos
devolvem numero, e numero errado sobre desempenho leva a otimizar o lugar
errado.

Por isso a aritmetica e a leitura de texto ficam separadas da sessao e
exercitadas aqui.
"""

import pytest

from radar import desempenho


# --------------------------------------------------------------------------
# Detalhe do armazenamento
# --------------------------------------------------------------------------

def test_tamanho_medio_de_arquivo():
    detalhe = desempenho.Detalhe("t", arquivos=4, bytes_totais=8 * 1024 * 1024)

    assert detalhe.bytes_por_arquivo == 2 * 1024 * 1024
    assert detalhe.mb_por_arquivo == 2.0


def test_tabela_vazia_nao_divide_por_zero():
    """Tabela recem-criada tem zero arquivo, e a divisao mataria a leitura.

    Devolver 0.0 e a resposta util: nao ha arquivo, logo nao ha problema de
    arquivo pequeno.
    """
    assert desempenho.Detalhe("t", arquivos=0, bytes_totais=0).mb_por_arquivo == 0.0


def test_contagem_de_linhas_e_opcional():
    """E a unica parte cara: o resto sai do metadado."""
    assert desempenho.Detalhe("t", 1, 1).linhas is None


# --------------------------------------------------------------------------
# Leitura do plano
# --------------------------------------------------------------------------

PLANO = """
== Physical Plan ==
AdaptiveSparkPlan
+- HashAggregate(keys=[repo])
   +- Exchange hashpartitioning(repo, 200)
      +- HashAggregate(keys=[repo])
         +- SortMergeJoin [sk_autor], [sk_autor], Inner
            +- Exchange hashpartitioning(sk_autor, 200)
               +- Scan parquet radar_gold.fct_commit
"""


def test_resumo_conta_os_operadores():
    resumo = desempenho.resumo_do_plano(PLANO)

    assert resumo["Exchange"] == 2
    assert resumo["SortMergeJoin"] == 1
    assert resumo["HashAggregate"] == 2


def test_resumo_omite_o_que_nao_aparece():
    """Chave com zero poluiria a leitura sem acrescentar nada."""
    assert "BroadcastHashJoin" not in desempenho.resumo_do_plano(PLANO)


def test_resumo_de_texto_vazio():
    assert desempenho.resumo_do_plano("") == {}


def test_diferenca_mostra_so_o_que_mudou():
    """Numa otimizacao, o que interessa nao e o plano novo: e o delta."""
    antes = "Exchange\nExchange\nSortMergeJoin\nSort"
    depois = "Exchange\nBroadcastHashJoin\nSort"

    mudou = desempenho.diferenca_de_plano(antes, depois)

    assert mudou["Exchange"] == (2, 1)
    assert mudou["SortMergeJoin"] == (1, 0)
    assert mudou["BroadcastHashJoin"] == (0, 1)


def test_diferenca_omite_o_que_ficou_igual():
    """`Sort` aparece uma vez nos dois planos e nao e novidade."""
    antes = "Exchange\nSort"
    depois = "Exchange\nSort"

    assert desempenho.diferenca_de_plano(antes, depois) == {}


def test_a_regressao_de_juncao_aparece_na_diferenca():
    """O caso que so o plano acusa.

    Uma tabela que cresce alem do limiar de transmissao faz o motor trocar
    `BroadcastHashJoin` por `SortMergeJoin`, que acrescenta shuffle. O tempo
    piora sem que nada no SQL tenha mudado, e sem o plano a causa fica
    invisivel.
    """
    mudou = desempenho.diferenca_de_plano(
        "BroadcastHashJoin\nBroadcastExchange",
        "SortMergeJoin\nExchange\nSort",
    )

    assert mudou["BroadcastHashJoin"] == (1, 0)
    assert mudou["SortMergeJoin"] == (0, 1)


# --------------------------------------------------------------------------
# Distribuicao
# --------------------------------------------------------------------------

def test_sql_distribuicao_agrupa_pela_chave_pedida():
    sql = desempenho.sql_distribuicao("radar_gold.fct_commit", "repo", limite=5)

    assert "GROUP BY repo" in sql
    assert "vezes_a_media" in sql
    assert "LIMIT 5" in sql


# --------------------------------------------------------------------------
# Replica
# --------------------------------------------------------------------------

def test_replicar_recusa_fator_invalido():
    """A validacao precede o import de pyspark, e por isso roda sem motor.

    E a mesma invariante da decisao 8.11: o modulo continua utilizavel numa
    maquina sem JVM, ao menos ate a parte que precisa de uma.
    """
    with pytest.raises(ValueError, match="1 ou mais"):
        desempenho.replicar(None, 0, "sha")


def test_a_marca_da_replica_nao_cabe_numa_chave_natural():
    """Se a marca puder ocorrer na chave real, duas linhas distintas colidem.

    As chaves naturais do projeto sao alfanumericas: `sha` e hexadecimal e
    `numero` e inteiro. Basta a marca conter um caractere fora desse conjunto
    para nunca aparecer dentro de uma delas.
    """
    assert any(not c.isalnum() for c in desempenho.MARCA_REPLICA)


# --------------------------------------------------------------------------
# Cronometragem
# --------------------------------------------------------------------------

class SessaoFalsa:
    """Duble que conta execucoes sem executar nada.

    O que se verifica com ele e a mecanica da medicao -- quantas vezes roda,
    o que roda, o que devolve -- e nao o tempo, que num duble seria zero.
    """

    def __init__(self):
        self.consultas = []

    def sql(self, texto):
        self.consultas.append(texto)
        return _Escrita(self)


class _Escrita:
    def __init__(self, sessao):
        self.sessao = sessao

    @property
    def write(self):
        return self

    def format(self, nome):
        self.sessao.consultas.append(f"format:{nome}")
        return self

    def mode(self, _):
        return self

    def save(self):
        self.sessao.consultas.append("save")


def test_medir_repete_o_numero_pedido():
    sessao = SessaoFalsa()
    medicao = desempenho.medir(sessao, "caso", "SELECT 1", repeticoes=3)

    assert len(medicao.amostras) == 3
    assert sessao.consultas.count("save") == 3


def test_medir_materializa_com_noop():
    """`count()` mediria um plano que ninguem executa em producao.

    O otimizador poda projecao e leitura de coluna quando so a contagem
    importa, e o tempo que sai e de outra consulta.
    """
    sessao = SessaoFalsa()
    desempenho.medir(sessao, "caso", "SELECT 1", repeticoes=1)

    assert "format:noop" in sessao.consultas


def test_medir_recusa_zero_repeticoes():
    with pytest.raises(ValueError):
        desempenho.medir(SessaoFalsa(), "caso", "SELECT 1", repeticoes=0)


def test_mediana_descarta_a_execucao_fria():
    """A primeira execucao paga plano e metadado que as seguintes nao pagam.

    Reportar so a media misturaria os dois regimes; a mediana sobre tres
    amostras deixa a fria de fora, e ela continua acessivel para quem quiser
    saber quanto custou.
    """
    medicao = desempenho.Medicao("x", (10.0, 2.0, 2.5))

    assert medicao.mediana == 2.5
    assert medicao.primeira == 10.0


def test_medicao_se_descreve_em_uma_linha():
    texto = str(desempenho.Medicao("bus_factor", (1.0, 2.0, 3.0)))

    assert "bus_factor" in texto
    assert "2.00s" in texto
    assert "3 amostras" in texto


def test_comparar_mede_todos_os_casos():
    sessao = SessaoFalsa()
    medicoes = desempenho.comparar(
        sessao, {"a": "SELECT 1", "b": "SELECT 2"}, repeticoes=2
    )

    assert [m.nome for m in medicoes] == ["a", "b"]
    assert sessao.consultas.count("save") == 4
