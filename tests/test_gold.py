"""Testes das constantes e DDL da gold. Sem Spark."""

from radar import gold


def test_dimensoes_ficam_no_schema_da_gold():
    assert gold.TABELA_TEMPO == "workspace.radar_gold.dim_tempo"
    assert gold.TABELA_AUTOR == "workspace.radar_gold.dim_autor"


# --------------------------------------------------------------------------
# Nomes em portugues, independentes do locale
# --------------------------------------------------------------------------

def test_doze_meses_e_sete_dias():
    assert len(gold.MESES) == 12
    assert len(gold.DIAS) == 7


def test_semana_comeca_na_segunda():
    # A ordem da tupla e indexada por `dia_da_semana` (ISO): 1 = segunda.
    assert gold.DIAS[0] == "segunda-feira"
    assert gold.DIAS[6] == "domingo"


def test_meses_em_ordem_do_calendario():
    assert gold.MESES[0] == "janeiro"
    assert gold.MESES[11] == "dezembro"


# --------------------------------------------------------------------------
# DDL
# --------------------------------------------------------------------------

def test_ddl_do_tempo_usa_chave_inteligente():
    # Tempo e a excecao: a data nunca muda, entao nao ha versao a
    # identificar, e `aaaammdd` permite ler o fato sem juncao.
    assert "sk_tempo        INT     NOT NULL" in gold.ddl_dim_tempo()


def test_ddl_do_autor_usa_hash():
    assert "sk_autor        STRING  NOT NULL" in gold.ddl_dim_autor()


def test_ddl_do_autor_declara_a_origem_da_chave():
    # A coluna torna a decisao da chave hibrida auditavel de dentro do dado.
    ddl = gold.ddl_dim_autor()
    assert "origem_da_chave STRING  NOT NULL" in ddl
    assert "chave_natural   STRING  NOT NULL" in ddl


def test_ddl_do_autor_declara_todas_as_colunas():
    ddl = gold.ddl_dim_autor()
    for coluna in gold.COLUNAS_DIM_AUTOR:
        assert coluna in ddl


def test_ddl_do_tempo_nao_tem_lacuna_declarada():
    # O comentario da tabela e parte do contrato de quem consulta.
    assert "sem lacunas" in gold.ddl_dim_tempo()


# --------------------------------------------------------------------------
# Separador e marcadores
# --------------------------------------------------------------------------

def test_separador_nao_aparece_em_valor_de_negocio():
    # Com `|` ou `-`, as partes ("a|b") e ("a", "b") gerariam a mesma chave.
    assert gold.SEPARADOR == ""
    assert not gold.SEPARADOR.isprintable()


def test_origens_da_chave_sao_distintas():
    origens = (gold.ORIGEM_CONTA, gold.ORIGEM_EMAIL, gold.ORIGEM_DESCONHECIDA)
    assert len(set(origens)) == 3


# --------------------------------------------------------------------------
# dim_repositorio: o que e versionado e o que nao e
# --------------------------------------------------------------------------

def test_medidas_ficam_fora_dos_atributos_versionados():
    # `stars` muda todo dia: versionar geraria 14 repositorios x 365 dias
    # numa dimensao que deve ter 14 linhas. Vao para o fato na Etapa 5.
    for medida in ("stars", "forks", "issues_abertas", "observadores", "tamanho_kb"):
        assert medida not in gold.ATRIBUTOS_VERSIONADOS


def test_atributos_versionados_mudam_raramente():
    esperados = {"nome_completo", "dono", "dono_tipo", "linguagem",
                 "licenca", "branch_padrao", "arquivado", "e_fork"}
    assert set(gold.ATRIBUTOS_VERSIONADOS) == esperados


def test_ddl_do_repositorio_declara_o_intervalo_de_validade():
    ddl = gold.ddl_dim_repositorio()
    assert "valido_de       DATE      NOT NULL" in ddl
    assert "valido_ate      DATE" in ddl
    assert "flag_atual      BOOLEAN   NOT NULL" in ddl


def test_chave_natural_do_repositorio_e_o_id_numerico():
    # `repo_id` sobrevive a renomeacao; `nome_completo` e atributo versionado.
    ddl = gold.ddl_dim_repositorio()
    assert "repo_id         BIGINT    NOT NULL" in ddl
    assert "nome_completo" in gold.ATRIBUTOS_VERSIONADOS


def test_ddl_do_repositorio_declara_todas_as_colunas():
    for coluna in gold.COLUNAS_DIM_REPOSITORIO:
        assert coluna in gold.ddl_dim_repositorio()
