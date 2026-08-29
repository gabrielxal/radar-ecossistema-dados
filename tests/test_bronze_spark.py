"""Projecao e deduplicacao da bronze verificadas contra o motor Spark.

Cobre `projetar` e `deduplicar` a partir de DataFrames em memoria, no mesmo
formato que `spark.read.text()` entrega: `value` com a linha do arquivo,
`repo` e `dt` vindos das particoes do caminho.

A leitura de arquivo (`ler_landing`) e coberta na secao final, que depende de
`winutils.exe` no Windows e e pulada quando ele nao esta configurado.

**Fora do alcance destes testes:** `criar_tabela` (USING DELTA), `carregar`
(MERGE INTO), o Volume e o Unity Catalog, validados apenas no Databricks.
"""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from radar import bronze, ingestao
from radar.config import REPOS

pytestmark = pytest.mark.spark

COMMITS = ingestao.ENDPOINTS["commits"]
AGORA = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)

# Colunas que `spark.read.text()` entrega sobre um caminho particionado, mais
# a proveniencia que `ler_landing` materializa antes de projetar.
SCHEMA_BRUTO = "value STRING, repo STRING, dt STRING, _arquivo_origem STRING"


def commit(sha, mensagem="fix: teste"):
    return {
        "sha": sha,
        "commit": {"message": mensagem, "committer": {"date": "2026-08-01T10:00:00Z"}},
    }


@pytest.fixture
def bruto(spark):
    """Monta o DataFrame cru como ele chega da leitura da landing zone."""

    def constroi(linhas):
        # linhas: (registro, repo_no_caminho, dt, arquivo)
        dados = [
            (json.dumps(registro), repo, dt, arquivo)
            for registro, repo, dt, arquivo in linhas
        ]
        return spark.createDataFrame(dados, SCHEMA_BRUTO)

    return constroi


def projetado(bruto, linhas):
    return bronze.projetar(bruto(linhas), COMMITS, AGORA)


# --------------------------------------------------------------------------
# Decodificacao do repositorio pelo motor
# --------------------------------------------------------------------------

def test_repo_decodificado_pelo_regexp_do_spark(bruto):
    df = projetado(bruto, [(commit("a"), "duckdb__duckdb", "2026-08-22", "f.jsonl")])
    assert df.collect()[0]["repo"] == "duckdb/duckdb"


def test_underscore_do_nome_do_repositorio_sobrevive(bruto):
    linha = (commit("a"), "great-expectations__great_expectations", "2026-08-22", "f")
    df = projetado(bruto, [linha])
    assert df.collect()[0]["repo"] == "great-expectations/great_expectations"


def test_motor_e_python_decodificam_igual(bruto):
    # `regexp_replace` usa `$1`; `re.sub` usa `\1`. Sao motores distintos
    # compartilhando o mesmo padrao: ou concordam, ou ha bug latente.
    linhas = [
        (commit(f"sha{i}"), ingestao.sanitizar_repo(repo), "2026-08-22", f"{i}.jsonl")
        for i, repo in enumerate(REPOS)
    ]
    pelo_spark = {linha["repo"] for linha in projetado(bruto, linhas).collect()}
    pelo_python = {bronze.dessanitizar_repo(ingestao.sanitizar_repo(r)) for r in REPOS}

    assert pelo_spark == pelo_python == set(REPOS)


# --------------------------------------------------------------------------
# Colunas e tipos
# --------------------------------------------------------------------------

def test_particoes_permanecem_string(bruto):
    # Sem o cast, o Spark inferiria DATE a partir de `dt=2026-08-22`.
    df = projetado(bruto, [(commit("a"), "duckdb__duckdb", "2026-08-22", "f.jsonl")])
    tipos = dict(df.dtypes)
    assert tipos["dt"] == "string"
    assert tipos["repo"] == "string"


def test_chave_natural_extraida_do_payload(bruto):
    df = projetado(bruto, [(commit("abc123"), "duckdb__duckdb", "2026-08-22", "f")])
    assert df.collect()[0][COMMITS.chave] == "abc123"


def test_payload_preservado_como_veio_do_arquivo(bruto):
    registro = commit("a", "fix: acentuacao e cedilha -- cao, avaliacao")
    df = projetado(bruto, [(registro, "duckdb__duckdb", "2026-08-22", "f.jsonl")])
    assert json.loads(df.collect()[0]["payload"]) == registro


def test_proveniencia_preenchida(bruto):
    caminho = "/Volumes/x/raw/commits/repo=duckdb__duckdb/dt=2026-08-22/120000.jsonl"
    df = projetado(bruto, [(commit("a"), "duckdb__duckdb", "2026-08-22", caminho)])
    linha = df.collect()[0]

    assert linha["_arquivo_origem"] == caminho
    assert linha["_endpoint"] == COMMITS.nome
    assert linha["_ingerido_em"] is not None


def test_colunas_da_projecao_batem_com_o_ddl(bruto):
    # A tabela e criada pelo DDL e alimentada por INSERT *, que casa por nome:
    # coluna a mais ou a menos so falharia na carga, dentro do Databricks.
    df = projetado(bruto, [(commit("a"), "duckdb__duckdb", "2026-08-22", "f.jsonl")])
    ddl = bronze.ddl(COMMITS)

    for coluna in df.columns:
        assert coluna in ddl


# --------------------------------------------------------------------------
# Deduplicacao
# --------------------------------------------------------------------------

def duas_cargas_do_mesmo_commit(bruto):
    """O cenario que DIAS_SOBREPOSICAO produz de proposito a cada carga."""
    return projetado(
        bruto,
        [
            (commit("x", "primeira"), "duckdb__duckdb", "2026-08-21", "a/21.jsonl"),
            (commit("x", "segunda"), "duckdb__duckdb", "2026-08-22", "b/22.jsonl"),
        ],
    )


def test_sha_repetido_entre_arquivos_vira_uma_linha(bruto):
    df = duas_cargas_do_mesmo_commit(bruto)
    assert df.count() == 2
    assert bronze.deduplicar(df, COMMITS).count() == 1


def test_deduplicacao_mantem_a_ocorrencia_mais_antiga(bruto):
    linha = bronze.deduplicar(duas_cargas_do_mesmo_commit(bruto), COMMITS).collect()[0]
    assert "primeira" in linha["payload"]
    assert linha["_arquivo_origem"] == "a/21.jsonl"


def test_mesmo_sha_em_repos_diferentes_sao_linhas_distintas(bruto):
    # Acontece em fork: o commit carrega o sha do repositorio de origem.
    df = projetado(
        bruto,
        [
            (commit("x"), "duckdb__duckdb", "2026-08-22", "a.jsonl"),
            (commit("x"), "pola-rs__polars", "2026-08-22", "b.jsonl"),
        ],
    )
    assert bronze.deduplicar(df, COMMITS).count() == 2


def test_deduplicacao_e_determinista(bruto):
    primeira = bronze.deduplicar(duas_cargas_do_mesmo_commit(bruto), COMMITS).collect()
    segunda = bronze.deduplicar(duas_cargas_do_mesmo_commit(bruto), COMMITS).collect()
    assert [l["payload"] for l in primeira] == [l["payload"] for l in segunda]


# --------------------------------------------------------------------------
# Leitura da landing zone
#
# Exige `winutils.exe` no Windows; a fixture `hadoop` pula a secao inteira
# quando ele nao esta configurado.
# --------------------------------------------------------------------------

@pytest.fixture
def landing(hadoop, tmp_path):
    """Landing zone escrita pelas funcoes de producao da ingestao.

    Gerar os arquivos com `caminho_arquivo` e `gravar_jsonl`, em vez de montar
    o caminho a mao, faz o teste cobrir o acoplamento entre os dois modulos:
    se o layout de escrita e o de leitura divergirem, isto quebra.
    """
    base = str(tmp_path / "raw").replace("\\", "/")

    def escrever(repo, momento, registros):
        caminho = ingestao.caminho_arquivo(base, COMMITS.nome, repo, momento)
        return ingestao.gravar_jsonl(caminho, registros) and caminho

    return SimpleNamespace(base=base, escrever=escrever)


def test_layout_da_ingestao_e_legivel_pela_bronze(spark, landing):
    landing.escrever("duckdb/duckdb", AGORA, [commit("a"), commit("b")])
    df = bronze.ler_landing(spark, landing.base, COMMITS, AGORA)
    assert df.count() == 2


def test_caminho_vira_coluna_pela_descoberta_de_particoes(spark, landing):
    # `repo=` e `dt=` no diretorio viram colunas sem nenhuma configuracao:
    # e o que torna a proveniencia gratuita, sem grava-la dentro do arquivo.
    landing.escrever("duckdb/duckdb", AGORA, [commit("a")])
    landing.escrever("pola-rs/polars", AGORA.replace(second=1), [commit("b")])

    df = bronze.ler_landing(spark, landing.base, COMMITS, AGORA)
    assert {l["repo"] for l in df.collect()} == {"duckdb/duckdb", "pola-rs/polars"}
    assert {l["dt"] for l in df.collect()} == {"2026-08-22"}


def test_inferencia_tiparia_a_particao_como_date(spark, landing):
    # Prova que o cast de `projetar` carrega peso: lida sem ele, a particao
    # `dt=2026-08-22` chega como DATE, e a bronze exige STRING.
    landing.escrever("duckdb/duckdb", AGORA, [commit("a")])
    caminho = bronze.caminho_endpoint(landing.base, COMMITS)

    inferido = dict(spark.read.option("pathGlobFilter", "*.jsonl").text(caminho).dtypes)
    projetado_ = dict(bronze.ler_landing(spark, landing.base, COMMITS, AGORA).dtypes)

    assert inferido["dt"] == "date"
    assert projetado_["dt"] == "string"


def test_proveniencia_aponta_para_o_arquivo_real(spark, landing):
    caminho = landing.escrever("duckdb/duckdb", AGORA, [commit("a")])
    origem = bronze.ler_landing(spark, landing.base, COMMITS, AGORA).collect()[0]

    assert origem["_arquivo_origem"].endswith(caminho.split("/")[-1])
    assert "repo=duckdb__duckdb" in origem["_arquivo_origem"]
    assert "dt=2026-08-22" in origem["_arquivo_origem"]


def test_arquivo_que_nao_e_jsonl_e_ignorado(spark, landing):
    caminho = landing.escrever("duckdb/duckdb", AGORA, [commit("a")])
    lixo = caminho.replace(".jsonl", ".txt")
    with open(lixo, "w", encoding="utf-8") as f:
        f.write("isto nao deveria entrar na bronze\n")

    df = bronze.ler_landing(spark, landing.base, COMMITS, AGORA)
    assert df.count() == 1
    assert "nao deveria" not in df.collect()[0]["payload"]
