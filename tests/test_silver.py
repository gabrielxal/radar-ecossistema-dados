"""Testes do schema declarado da silver. Sem Spark, sem Databricks."""

from radar import silver

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
