"""Testes do schema declarado da silver. Sem Spark, sem Databricks."""

from radar import silver, silver_comum

SCHEMA = silver.SCHEMA_COMMIT


# --------------------------------------------------------------------------
# Nome da tabela
# --------------------------------------------------------------------------

def test_tabela_fica_no_schema_da_silver():
    assert silver.TABELA_COMMITS == "workspace.radar_silver.commits"


# --------------------------------------------------------------------------
# Estrutura do DDL
# --------------------------------------------------------------------------

def test_ddl_tem_delimitadores_balanceados():
    # STRUCT aninhado sem fechar quebra so em runtime, dentro do Spark.
    assert SCHEMA.count("<") == SCHEMA.count(">")


def test_chave_natural_e_string():
    # sha e hash: converter seria erro conceitual, nao otimizacao.
    assert "sha STRING" in SCHEMA


# --------------------------------------------------------------------------
# A decisao central: data continua STRING
# --------------------------------------------------------------------------

def test_nenhuma_data_e_declarada_como_timestamp():
    # Declarar TIMESTAMP faria o `from_json` converter por conta propria, e
    # uma data invalida viraria NULL sem registro. O cast e do passo 3.2.
    assert "TIMESTAMP" not in SCHEMA.upper()


def test_as_duas_datas_do_commit_sao_declaradas():
    assert SCHEMA.count("date: STRING") == 2


# --------------------------------------------------------------------------
# Os dois `author`
# --------------------------------------------------------------------------

def test_identidade_do_git_tem_nome_e_email():
    assert "author: STRUCT<name: STRING, email: STRING, date: STRING>" in SCHEMA


def test_usuario_do_github_tem_login_e_id():
    assert "author STRUCT<login: STRING, id: BIGINT, type: STRING>" in SCHEMA


def test_committer_existe_nos_dois_niveis():
    # Um e a identidade do git, outro e o usuario do GitHub.
    assert "committer: STRUCT<name: STRING" in SCHEMA
    assert "committer STRUCT<login: STRING" in SCHEMA


# --------------------------------------------------------------------------
# Tipos que vem prontos do JSON
# --------------------------------------------------------------------------

def test_contagem_e_numerica():
    assert "comment_count: BIGINT" in SCHEMA


def test_verificacao_de_assinatura_e_booleana():
    assert "verified: BOOLEAN" in SCHEMA


def test_parents_e_lista():
    # Mais de um pai identifica merge commit.
    assert "parents ARRAY<STRUCT<sha: STRING>>" in SCHEMA


# --------------------------------------------------------------------------
# O que ficou de fora
# --------------------------------------------------------------------------

def test_url_derivavel_nao_ocupa_coluna():
    # https://github.com/{repo}/commit/{sha} se monta a partir do que ja existe.
    assert "html_url" not in SCHEMA


# --------------------------------------------------------------------------
# Dominios das colunas categoricas
# --------------------------------------------------------------------------

DOMINIOS = (silver.TIPOS_DE_AUTOR, silver.MOTIVOS_DE_ASSINATURA)


def test_dominios_ja_estao_normalizados():
    # A normalizacao da coluna produz minusculas sem espaco; um dominio fora
    # desse formato nunca casaria, e a verificacao acusaria falso positivo.
    for dominio in DOMINIOS:
        for valor in dominio:
            assert valor == valor.strip().lower()


def test_dominios_nao_tem_repetidos():
    for dominio in DOMINIOS:
        assert len(dominio) == len(set(dominio))


def test_tipos_de_autor_cobrem_conta_e_automacao():
    assert "user" in silver.TIPOS_DE_AUTOR
    assert "bot" in silver.TIPOS_DE_AUTOR


def test_motivos_cobrem_assinado_e_nao_assinado():
    assert "valid" in silver.MOTIVOS_DE_ASSINATURA
    assert "unsigned" in silver.MOTIVOS_DE_ASSINATURA


# --------------------------------------------------------------------------
# Contrato das duas tabelas
# --------------------------------------------------------------------------

def test_tabela_de_quarentena_fica_ao_lado_da_silver():
    assert silver.TABELA_REJEITADOS == "workspace.radar_silver.commits_rejeitados"


def test_payload_nao_entra_na_tabela_silver():
    # Guarda-lo aqui duplicaria a bronze; a quarentena e quem precisa dele.
    assert "payload" not in silver.COLUNAS_SILVER
    assert silver.COLUNA_MOTIVO not in silver.COLUNAS_SILVER


def test_quarentena_guarda_payload_e_motivo():
    assert "payload" in silver.COLUNAS_REJEITADOS
    assert "motivo" in silver.COLUNAS_REJEITADOS


def test_ddl_declara_todas_as_colunas_projetadas():
    for coluna in silver.COLUNAS_SILVER:
        assert coluna in silver.ddl_commits()
    for coluna in silver.COLUNAS_REJEITADOS:
        assert coluna in silver.ddl_rejeitados()


def test_chave_e_data_do_commit_sao_obrigatorias_na_silver():
    # Sao as duas colunas sem as quais a linha nao tem identidade nem lugar
    # no tempo -- e por isso a quarentena existe.
    ddl = silver.ddl_commits()
    assert "sha                   STRING    NOT NULL" in ddl
    assert "commitado_em          TIMESTAMP NOT NULL" in ddl


def test_quarentena_nao_exige_nada():
    # Registro torto precisa caber. Uma coluna NOT NULL aqui recusaria
    # justamente o caso que a tabela existe para acolher.
    assert "NOT NULL" not in silver.ddl_rejeitados()


def test_motivos_sao_unicos_e_normalizados():
    assert len(silver.MOTIVOS_DE_REJEICAO) == len(set(silver.MOTIVOS_DE_REJEICAO))
    for motivo in silver.MOTIVOS_DE_REJEICAO:
        assert motivo == motivo.strip().lower()


# --------------------------------------------------------------------------
# Carga incremental
# --------------------------------------------------------------------------

from radar.ingestao import ENDPOINTS  # noqa: E402

COMMITS = ENDPOINTS["commits"]


def test_processo_da_silver_nao_colide_com_a_ingestao():
    # As duas cargas dividem a tabela de controle; o nome do processo e o que
    # mantem os dois watermarks independentes.
    assert silver_comum.nome_processo(COMMITS) == "commits@silver"
    assert silver_comum.nome_processo(COMMITS) != COMMITS.nome


def test_resultado_fecha_quando_nada_se_perde():
    # A invariante da quarentena: lidos = aprovados + rejeitados.
    completo = silver.ResultadoSilver(lidos=10, aprovados=7, rejeitados=3, watermark=None)
    faltando = silver.ResultadoSilver(lidos=10, aprovados=7, rejeitados=2, watermark=None)
    assert completo.fecha
    assert not faltando.fecha


def test_lote_vazio_fecha_trivialmente():
    assert silver.ResultadoSilver(lidos=0, aprovados=0, rejeitados=0, watermark=None).fecha
