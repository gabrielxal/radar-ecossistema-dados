"""Fixtures compartilhadas pelos testes.

A sessao Spark local existe para responder o que so o motor sabe: se o DDL
declarado e aceito, como o `from_json` se comporta na borda e o que a
descoberta de particoes produz. Ela nao substitui o Databricks: Delta,
Volume e Unity Catalog nao existem aqui.
"""

import json
import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

# HADOOP_HOME e caminho de maquina e vem do .env, que nao e versionado.
load_dotenv()


def hadoop_disponivel() -> bool:
    """No Windows, ler arquivo local pelo Spark exige `winutils.exe`.

    Sem ele a leitura falha com `UnsatisfiedLinkError` na camada nativa do
    Hadoop. Em Linux e macOS a questao nao existe.
    """
    if sys.platform != "win32":
        return True
    raiz = os.environ.get("HADOOP_HOME")
    return bool(raiz) and Path(raiz, "bin", "winutils.exe").is_file()


@pytest.fixture(scope="session")
def spark():
    """Sessao local, uma por execucao da suite. Subir a JVM custa segundos."""
    pytest.importorskip("pyspark", reason="pyspark nao instalado")

    # O JVM procura o interpretador no PATH e acha outro Python, ou nenhum --
    # e o worker falha com "Accept timed out". Apontar para o que roda os
    # testes resolve. `setdefault` preserva quem ja tiver configurado.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # hadoop.dll precisa estar no PATH para a camada nativa carregar.
    if hadoop_disponivel() and sys.platform == "win32":
        binarios = str(Path(os.environ["HADOOP_HOME"], "bin"))
        if binarios not in os.environ["PATH"]:
            os.environ["PATH"] = binarios + os.pathsep + os.environ["PATH"]

    from pyspark.sql import SparkSession

    sessao = (
        SparkSession.builder.appName("radar-testes")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


@pytest.fixture
def hadoop(spark):
    """Porta de entrada dos testes que leem arquivo pelo Spark."""
    if not hadoop_disponivel():
        pytest.skip("HADOOP_HOME com winutils.exe ausente; ver .env")


@pytest.fixture
def df_payload(spark):
    """Constroi um DataFrame de uma linha a partir de um dict, como a bronze."""

    def constroi(dado):
        texto = dado if isinstance(dado, str) else json.dumps(dado)
        return spark.createDataFrame([(texto,)], "payload STRING")

    return constroi
