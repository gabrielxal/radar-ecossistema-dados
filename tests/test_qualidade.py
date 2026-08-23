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


def test_toda_verificacao_consulta_uma_tabela_da_camada():
    # `carga_truncada` olha a tabela de controle, e nao a bronze: o sintoma
    # dela nao esta no dado que chegou, e sim no registro da carga.
    tabelas = ("workspace.radar_bronze.commits", "controle_ingestao")
    for v in BATERIA:
        assert any(t in v.sql for t in tabelas)


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
    r = qualidade.Reconciliacao(qualidade.RECONCILIACAO_BRONZE, na_origem=200, no_destino=200)
    assert r.bate
    assert r.diferenca == 0


def test_reconciliacao_acusa_linha_perdida_no_caminho():
    r = qualidade.Reconciliacao(qualidade.RECONCILIACAO_BRONZE, na_origem=200, no_destino=198)
    assert not r.bate
    assert r.diferenca == 2


def test_reconciliacao_vira_regra_bloqueante_da_bateria():
    r = qualidade.Reconciliacao(qualidade.RECONCILIACAO_BRONZE, na_origem=200, no_destino=198).como_resultado()
    assert r.nome == qualidade.RECONCILIACAO_BRONZE
    assert r.severidade == qualidade.BLOQUEIA
    assert not r.passou


def test_reconciliacao_guarda_as_duas_contagens():
    # E o que responde "quanto a landing zone tinha naquele dia".
    r = qualidade.Reconciliacao(qualidade.RECONCILIACAO_BRONZE, na_origem=200, no_destino=200).como_resultado()
    assert (r.esperado, r.obtido) == (200, 200)
    assert r.passou


def test_reconciliacao_conta_violacao_nos_dois_sentidos():
    # Bronze com linha a MAIS tambem e defeito: arquivo sumiu da landing zone
    # ou alguem inseriu por fora do pipeline.
    sobra = qualidade.Reconciliacao(qualidade.RECONCILIACAO_BRONZE, na_origem=198, no_destino=200).como_resultado()
    assert sobra.violacoes == 2
    assert not sobra.passou


def test_reconciliacao_reprovada_interrompe_a_execucao():
    recon = qualidade.Reconciliacao(qualidade.RECONCILIACAO_BRONZE, na_origem=200, no_destino=198).como_resultado()
    with pytest.raises(AssertionError, match=qualidade.RECONCILIACAO_BRONZE):
        qualidade.levantar_se_bloqueou([recon])


def test_regra_comum_nao_preenche_as_contagens():
    # `esperado` seria sempre 0 nas regras de violacao e nao diria nada.
    r = resultado("chave_duplicada", qualidade.BLOQUEIA, 0)
    assert r.esperado is None and r.obtido is None


# --------------------------------------------------------------------------
# Bateria da silver
# --------------------------------------------------------------------------

from radar import silver  # noqa: E402

SILVER = qualidade.verificacoes_silver(COMMITS)


def test_bateria_da_silver_tem_a_mesma_forma_da_bronze():
    nomes = [v.nome for v in SILVER]
    assert len(nomes) == len(set(nomes))
    for v in SILVER:
        assert v.severidade in qualidade.SEVERIDADES
        assert "AS violacoes" in v.sql
        assert len(v.descricao) > 30


def test_bateria_da_silver_consulta_as_tabelas_da_camada():
    tabelas = {silver.TABELA_COMMITS, silver.TABELA_REJEITADOS}
    for v in SILVER:
        assert any(t in v.sql for t in tabelas)


def test_silver_verifica_o_que_a_bronze_nao_poderia():
    # Comparar duas datas exige que elas sejam datas, e nao texto: e a prova
    # de que a camada mudou de responsabilidade, nao so de formato.
    ordem = next(v for v in SILVER if v.nome == "commit_anterior_a_autoria")
    assert "commitado_em < autorado_em" in ordem.sql


def test_silver_guarda_a_licao_do_size_null():
    # `size(NULL)` devolve -1; contagem negativa passaria por qualquer
    # verificacao de nulo sem ser notada.
    negativa = next(v for v in SILVER if v.nome == "contagem_de_pais_negativa")
    assert negativa.severidade == qualidade.BLOQUEIA
    assert "qtd_pais < 0" in negativa.sql


def test_silver_confere_a_propria_normalizacao():
    regra = next(v for v in SILVER if v.nome == "normalizacao_nao_aplicada")
    assert "lower(trim(autor_email))" in regra.sql


def test_dominios_entram_nas_verificacoes_de_categoria():
    tipo = next(v for v in SILVER if v.nome == "tipo_de_autor_fora_do_dominio")
    for valor in silver.TIPOS_DE_AUTOR:
        assert f"'{valor}'" in tipo.sql

    motivo = next(v for v in SILVER if v.nome == "motivo_de_assinatura_fora_do_dominio")
    for valor in silver.MOTIVOS_DE_ASSINATURA:
        assert f"'{valor}'" in motivo.sql


def test_quarentena_e_verificada_junto():
    regra = next(v for v in SILVER if v.nome == "quarentena_sem_motivo")
    assert silver.TABELA_REJEITADOS in regra.sql
    for motivo in silver.MOTIVOS_DE_REJEICAO:
        assert f"'{motivo}'" in regra.sql


def test_dominio_fora_da_lista_avisa_em_vez_de_bloquear():
    # Categoria nova e mudanca da origem, nao defeito do pipeline.
    for nome in ("tipo_de_autor_fora_do_dominio", "motivo_de_assinatura_fora_do_dominio"):
        assert next(v for v in SILVER if v.nome == nome).severidade == qualidade.AVISA


def test_as_duas_reconciliacoes_tem_nomes_distintos():
    # Dividem a tabela de historico; nome igual misturaria as series.
    assert qualidade.RECONCILIACAO_BRONZE != qualidade.RECONCILIACAO_SILVER


# --------------------------------------------------------------------------
# Bateria da gold
# --------------------------------------------------------------------------

GOLD = qualidade.verificacoes_gold()


def test_bateria_da_gold_tem_a_mesma_forma_das_outras():
    nomes = [v.nome for v in GOLD]
    assert len(nomes) == len(set(nomes))
    for v in GOLD:
        assert v.severidade in qualidade.SEVERIDADES
        assert "AS violacoes" in v.sql
        assert len(v.descricao) > 30


def test_as_tres_invariantes_da_scd2_estao_cobertas():
    nomes = {v.nome for v in GOLD}
    assert "mais_de_uma_versao_vigente" in nomes
    assert "flag_atual_incoerente" in nomes
    assert "chave_substituta_duplicada" in nomes


def test_invariantes_da_scd2_bloqueiam():
    # Duas versoes vigentes fariam a juncao do fato duplicar a linha.
    for nome in ("mais_de_uma_versao_vigente", "flag_atual_incoerente",
                 "chave_substituta_duplicada"):
        assert next(v for v in GOLD if v.nome == nome).severidade == qualidade.BLOQUEIA


def test_premissa_da_chave_hibrida_virou_verificacao():
    # A medicao que fundamentou a decisao vira vigilancia permanente.
    regra = next(v for v in GOLD if v.nome == "email_em_duas_formas")
    assert regra.severidade == qualidade.BLOQUEIA
    assert "silver.commits" in regra.sql


def test_lacuna_no_tempo_bloqueia():
    regra = next(v for v in GOLD if v.nome == "dim_tempo_com_lacuna")
    assert regra.severidade == qualidade.BLOQUEIA
    assert "datediff" in regra.sql
