"""Contrato da silver de issues. Nao exige Spark."""

from radar import silver_issues as si


# --------------------------------------------------------------------------
# O contrato entre as colunas e o DDL
# --------------------------------------------------------------------------

def test_todas_as_colunas_declaradas_existem_no_ddl():
    ddl = si.ddl_issues()
    for coluna in si.COLUNAS_ISSUES:
        assert f"    {coluna} " in ddl or f"    {coluna:<16} " in ddl, coluna


def test_issues_e_pull_requests_tem_a_mesma_forma():
    """Mesmo payload, mesma entidade estrutural, tabelas diferentes."""
    def colunas(ddl):
        # So o bloco de colunas: o COMMENT da tabela difere de proposito.
        return ddl.split("(", 1)[1].split("USING DELTA")[0].rsplit(")", 1)[0]

    assert colunas(si.ddl_issues()) == colunas(si.ddl_pull_requests())


def test_as_duas_tabelas_sao_distintas():
    assert si.TABELA_ISSUES != si.TABELA_PULL_REQUESTS
    assert si.TABELA_ISSUES.endswith(".issues")
    assert si.TABELA_PULL_REQUESTS.endswith(".pull_requests")


def test_marcos_obrigatorios_sao_nao_nulos_no_ddl():
    """Sem data de abertura nao ha primeiro marco do ciclo de vida."""
    ddl = si.ddl_issues()
    assert "aberta_em        TIMESTAMP NOT NULL" in ddl
    # O marco final falta enquanto a issue esta aberta, e isso nao e defeito.
    assert "fechada_em       TIMESTAMP  " in ddl


# --------------------------------------------------------------------------
# O SQL de roteamento
# --------------------------------------------------------------------------

def test_fonte_de_issues_exclui_pull_request():
    sql = si.sql_fonte_entidade("lote", pull_request=False)
    assert f"NOT {si.COLUNA_PR}" in sql


def test_fonte_de_pull_request_nao_nega_o_filtro():
    sql = si.sql_fonte_entidade("lote", pull_request=True)
    assert f"NOT {si.COLUNA_PR}" not in sql
    assert si.COLUNA_PR in sql


def test_fonte_deduplica_o_lote_antes_do_merge():
    """Duas versoes da mesma issue no lote fariam o MERGE recusar a fonte."""
    sql = si.sql_fonte_entidade("lote", pull_request=False)
    assert "row_number() OVER" in sql
    assert "PARTITION BY repo, numero" in sql
    assert "atualizada_em DESC" in sql


def test_merge_so_atualiza_com_versao_igual_ou_mais_nova():
    """A guarda e o que impede a silver de andar para tras."""
    sql = si.sql_merge_entidade("lote", pull_request=False)
    assert "WHEN MATCHED AND fonte.atualizada_em >= alvo.atualizada_em" in sql


def test_merge_junta_pelo_grao_declarado():
    sql = si.sql_merge_entidade("lote", pull_request=False)
    assert "alvo.repo = fonte.repo AND alvo.numero = fonte.numero" in sql


def test_merge_de_pull_request_escreve_na_tabela_de_pull_request():
    assert si.TABELA_PULL_REQUESTS in si.sql_merge_entidade("lote", pull_request=True)
    assert si.TABELA_ISSUES in si.sql_merge_entidade("lote", pull_request=False)


def test_rejeitados_renomeia_o_motivo():
    sql = si.sql_fonte_rejeitados("lote")
    assert f"{si.COLUNA_MOTIVO} AS motivo" in sql


def test_rejeitados_e_insercao_e_nao_upsert():
    """Rejeicao e evento de uma execucao, nao entidade."""
    sql = si.sql_inserir_rejeitados("lote")
    assert sql.startswith(f"INSERT INTO {si.TABELA_REJEITADOS}")
    assert "MERGE" not in sql


# --------------------------------------------------------------------------
# A conta da carga
# --------------------------------------------------------------------------

def resultado(lidos, issues, prs, rejeitados):
    return si.ResultadoIssues(
        lidos=lidos, issues=issues, pull_requests=prs,
        rejeitados=rejeitados, watermark=None,
    )


def test_conta_fecha_com_os_tres_destinos():
    assert resultado(10, 6, 3, 1).fecha


def test_conta_nao_fecha_quando_linha_some():
    """Sem o balde de pull requests a conta acusaria perda inexistente."""
    assert not resultado(10, 6, 0, 1).fecha
