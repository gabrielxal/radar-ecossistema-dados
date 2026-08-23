"""Camada gold: dimensoes do modelo dimensional.

Aqui os nomes sao de negocio, nao da origem: `autor_nome` e nao
`commit.author.name`. E cada tabela declara seu grao.

O objeto `spark` e recebido como parametro e os imports de pyspark ficam
dentro das funcoes, para o modulo continuar importavel sem JVM.
"""

from __future__ import annotations

from datetime import date, datetime

from radar.config import GOLD, fqn

TABELA_TEMPO = fqn(GOLD, "dim_tempo")
TABELA_AUTOR = fqn(GOLD, "dim_autor")


# --------------------------------------------------------------------------
# Chaves substitutas
# --------------------------------------------------------------------------

# Separador improvavel em qualquer valor de negocio. Com um separador comum
# -- `|`, `-` -- as partes ("a|b") e ("a", "b") produziriam o mesmo texto e,
# portanto, a mesma chave para entidades diferentes.
SEPARADOR = ""

# NULL nao pode simplesmente desaparecer: `concat_ws` ignora nulos, entao
# ("a", NULL) e ("a") virariam o mesmo texto. O marcador preserva a posicao.
MARCA_NULO = "<nulo>"

# Membro para o fato que nao resolve nenhuma chave natural.
CHAVE_DESCONHECIDA = "<desconhecido>"


def chave_substituta(*partes):
    """Hash deterministico das partes que identificam a versao.

    Hash e nao contador: `row_number()` daria chaves diferentes a cada
    reprocessamento, e todo fato passaria a apontar para a linha errada --
    em silencio. Com hash, reprocessar do zero devolve as mesmas chaves.

    Recebe Columns, nunca nomes de coluna: `chave_substituta("email")`
    poderia significar a coluna `email` ou o texto literal, e a ambiguidade
    entre as duas produz chaves diferentes para o mesmo dado.
    """
    from pyspark.sql import functions as F

    preenchidas = [F.coalesce(parte, F.lit(MARCA_NULO)) for parte in partes]
    return F.sha2(F.concat_ws(SEPARADOR, *preenchidas), 256)


# --------------------------------------------------------------------------
# dim_tempo
# --------------------------------------------------------------------------

# Nomes montados aqui, e nao por `date_format(data, 'MMMM')`, que depende do
# locale da sessao: o mesmo codigo produziria "January" num cluster e
# "janeiro" noutro.
MESES = (
    "janeiro", "fevereiro", "marco", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
)

DIAS = (
    "segunda-feira", "terca-feira", "quarta-feira", "quinta-feira",
    "sexta-feira", "sabado", "domingo",
)


def ddl_dim_tempo() -> str:
    """DDL da dimensao de tempo. Grao: um dia."""
    return f"""
CREATE TABLE IF NOT EXISTS {TABELA_TEMPO} (
    sk_tempo        INT     NOT NULL COMMENT 'aaaammdd; chave inteligente, ver comentario do modulo',
    data            DATE    NOT NULL COMMENT 'o dia que a linha representa',
    ano             INT              COMMENT 'ano do calendario',
    trimestre       INT              COMMENT '1 a 4',
    mes             INT              COMMENT '1 a 12',
    nome_mes        STRING           COMMENT 'em portugues, independente do locale',
    dia             INT              COMMENT 'dia do mes',
    semana_iso      INT              COMMENT 'semana ISO 8601, 1 a 53',
    ano_semana_iso  INT              COMMENT 'ano ISO da semana; difere de `ano` na virada',
    dia_da_semana   INT              COMMENT '1=segunda ... 7=domingo (ISO)',
    nome_dia        STRING           COMMENT 'em portugues',
    e_fim_de_semana BOOLEAN          COMMENT 'sabado ou domingo'
)
USING DELTA
COMMENT 'Dimensao de tempo, gerada. Um dia por linha, sem lacunas.'
"""


def gerar_dim_tempo(spark, inicio: date, fim: date):
    """Gera um dia por linha entre as duas datas, inclusive.

    A unica dimensao que nao vem de dado nenhum. Extrai-la dos commits
    deixaria de fora os dias sem commit -- e uma serie temporal com lacunas
    mente por omissao: o grafico pula o dia parado em vez de mostrar zero.

    A chave e `aaaammdd`, e nao um hash. Tempo e a excecao consagrada a
    regra da chave sem significado: a data nunca muda, entao nao ha versao a
    identificar, e uma chave legivel permite ler o fato sem juncao e
    particionar por intervalo.
    """
    from pyspark.sql import functions as F

    dias = spark.sql(
        f"SELECT explode(sequence(DATE'{inicio}', DATE'{fim}', INTERVAL 1 DAY)) AS data"
    )

    # `dayofweek` devolve 1=domingo; a norma ISO comeca na segunda.
    dia_iso = ((F.dayofweek("data") + 5) % 7) + 1

    return dias.select(
        F.date_format("data", "yyyyMMdd").cast("int").alias("sk_tempo"),
        F.col("data"),
        F.year("data").alias("ano"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        F.element_at(F.array(*[F.lit(m) for m in MESES]), F.month("data")).alias("nome_mes"),
        F.dayofmonth("data").alias("dia"),
        F.weekofyear("data").alias("semana_iso"),
        # Ano ISO difere do calendario na virada: 2027-01-01 pertence a
        # semana 53 de 2026. Agrupar por (ano, semana_iso) sem isto junta
        # duas semanas distintas na mesma linha.
        #
        # Calculado pela quinta-feira da semana, e nao por `date_format`
        # com `YYYY`, que o Spark 3+ recusa por ambiguidade historica. A
        # norma ISO define o ano da semana como o ano da sua quinta-feira.
        F.year(F.date_add("data", 4) - dia_iso).alias("ano_semana_iso"),
        dia_iso.alias("dia_da_semana"),
        F.element_at(F.array(*[F.lit(d) for d in DIAS]), dia_iso).alias("nome_dia"),
        dia_iso.isin(6, 7).alias("e_fim_de_semana"),
    )


# --------------------------------------------------------------------------
# dim_autor
# --------------------------------------------------------------------------

COLUNAS_DIM_AUTOR = (
    "sk_autor",
    "chave_natural",
    "origem_da_chave",
    "github_id",
    "github_login",
    "github_tipo",
    "autor_nome",
    "autor_email",
    "_processado_em",
)

ORIGEM_CONTA = "conta"
ORIGEM_EMAIL = "email"
ORIGEM_DESCONHECIDA = "desconhecida"


def ddl_dim_autor() -> str:
    """DDL da dimensao de autor. Grao: uma linha por autor."""
    return f"""
CREATE TABLE IF NOT EXISTS {TABELA_AUTOR} (
    sk_autor        STRING  NOT NULL COMMENT 'hash de (origem, chave natural)',
    chave_natural   STRING  NOT NULL COMMENT 'github_id quando existe, e-mail do git quando nao',
    origem_da_chave STRING  NOT NULL COMMENT 'conta | email | desconhecida',
    github_id       BIGINT           COMMENT 'chave estavel da conta; nulo sem conta associada',
    github_login    STRING           COMMENT 'muda quando a conta e renomeada',
    github_tipo     STRING           COMMENT 'user | bot | organization',
    autor_nome      STRING           COMMENT 'identidade do git no commit mais recente',
    autor_email     STRING           COMMENT 'e-mail do git, normalizado',
    _processado_em  TIMESTAMP        COMMENT 'quando a dimensao foi montada'
)
USING DELTA
COMMENT 'Autores de commit. SCD1: o valor mais recente substitui o anterior.'
"""


def montar_dim_autor(commits, momento: datetime):
    """Uma linha por autor, a partir da silver de commits.

    **Chave hibrida**: `github_id` quando existe, `autor_email` quando nao.
    Nenhum dos dois e chave natural limpa -- `github_id` falta em 1,4% dos
    commits, e o e-mail fragmenta quem usa mais de um endereco. Cada um
    cobre exatamente o buraco do outro.

    A seguranca disso depende de nenhum e-mail aparecer resolvido em um
    commit e nao resolvido noutro: seria a mesma pessoa em duas linhas. Foi
    medido (zero ocorrencias) e virou verificacao bloqueante da bateria.

    **SCD1**: o commit mais recente define os atributos. O projeto pergunta
    sobre atividade, nao sobre historico de nomes -- versionar `login` seria
    complexidade sem demanda.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    chave = F.coalesce(F.col("github_id").cast("string"), F.col("autor_email"))
    origem = F.when(F.col("github_id").isNotNull(), F.lit(ORIGEM_CONTA)).otherwise(
        F.lit(ORIGEM_EMAIL)
    )

    base = (
        commits.where(chave.isNotNull())
        .withColumn("chave_natural", chave)
        .withColumn("origem_da_chave", origem)
    )

    # Desempate por `sha` para a escolha nao depender da ordem de leitura
    # quando dois commits do mesmo autor tem o mesmo instante.
    janela = Window.partitionBy("chave_natural").orderBy(
        F.col("commitado_em").desc(), F.col("sha").asc()
    )

    return (
        base.withColumn("_ordem", F.row_number().over(janela))
        .where(F.col("_ordem") == 1)
        .select(
            chave_substituta(
                F.col("origem_da_chave"), F.col("chave_natural")
            ).alias("sk_autor"),
            F.col("chave_natural"),
            F.col("origem_da_chave"),
            F.col("github_id"),
            F.col("github_login"),
            F.col("github_tipo"),
            F.col("autor_nome"),
            F.col("autor_email"),
            F.lit(momento).cast("timestamp").alias("_processado_em"),
        )
    )


def linha_desconhecida(spark, momento: datetime):
    """O membro que acolhe o fato sem autor identificavel.

    Sem ele, um commit sem `github_id` e sem e-mail nao teria para onde
    apontar, e a juncao do fato deixaria de ser total -- perdendo a linha ou
    exigindo `LEFT JOIN` e nulo espalhado pelas consultas.

    Construida pela mesma `chave_substituta` das demais: um valor fixo
    escrito a mao aqui seria uma segunda implementacao da regra de chave.
    """
    from pyspark.sql import functions as F

    return spark.createDataFrame(
        [(CHAVE_DESCONHECIDA,)], "chave_natural STRING"
    ).select(
        chave_substituta(
            F.lit(ORIGEM_DESCONHECIDA), F.col("chave_natural")
        ).alias("sk_autor"),
        F.col("chave_natural"),
        F.lit(ORIGEM_DESCONHECIDA).alias("origem_da_chave"),
        F.lit(None).cast("bigint").alias("github_id"),
        F.lit(None).cast("string").alias("github_login"),
        F.lit(None).cast("string").alias("github_tipo"),
        F.lit(None).cast("string").alias("autor_nome"),
        F.lit(None).cast("string").alias("autor_email"),
        F.lit(momento).cast("timestamp").alias("_processado_em"),
    )


# --------------------------------------------------------------------------
# dim_repositorio -- SCD2 derivada, nao mantida
# --------------------------------------------------------------------------

TABELA_REPOSITORIO = fqn(GOLD, "dim_repositorio")

# Atributos cuja mudanca abre uma versao nova. As contagens (stars, forks,
# issues) ficam de fora de proposito: mudam todo dia e explodiriam a
# dimensao -- 14 repositorios x 365 dias numa tabela que deve ter 14 linhas.
# Sao medidas, e vao para `fct_repo_snapshot` na Etapa 5.
ATRIBUTOS_VERSIONADOS = (
    "nome_completo",
    "dono",
    "dono_tipo",
    "linguagem",
    "licenca",
    "branch_padrao",
    "arquivado",
    "e_fork",
)

COLUNAS_DIM_REPOSITORIO = (
    ("sk_repositorio", "repo_id", "repo")
    + ATRIBUTOS_VERSIONADOS
    + ("valido_de", "valido_ate", "flag_atual", "_processado_em")
)


def ddl_dim_repositorio() -> str:
    """DDL da dimensao de repositorio. Grao: uma versao por repositorio."""
    atributos = "\n".join(
        f"    {nome:<15} {'BOOLEAN' if nome in ('arquivado', 'e_fork') else 'STRING':<9}"
        f"        COMMENT 'versionado: mudanca abre nova linha',"
        for nome in ATRIBUTOS_VERSIONADOS
    )
    return f"""
CREATE TABLE IF NOT EXISTS {TABELA_REPOSITORIO} (
    sk_repositorio  STRING    NOT NULL COMMENT 'hash de (repo_id, valido_de)',
    repo_id         BIGINT    NOT NULL COMMENT 'chave natural; sobrevive a renomeacao',
    repo            STRING             COMMENT 'owner/nome no dia em que a versao comecou',
{atributos}
    valido_de       DATE      NOT NULL COMMENT 'primeiro dia em que esta versao foi observada',
    valido_ate      DATE               COMMENT 'dia em que outra versao a substituiu; NULL se vigente',
    flag_atual      BOOLEAN   NOT NULL COMMENT 'versao vigente',
    _processado_em  TIMESTAMP          COMMENT 'quando a dimensao foi derivada'
)
USING DELTA
COMMENT 'SCD2 de repositorio, derivada das fotos diarias da silver.'
"""


def montar_dim_repositorio(repositorios, momento: datetime):
    """Deriva a SCD2 a partir da serie de fotos diarias.

    **A dimensao e derivada, nao mantida.** O caminho classico da SCD2 e
    incremental: comparar a carga de hoje com a versao vigente e fechar a
    anterior quando algo muda. Isso guarda estado, e estado errado nao se
    corrige sozinho -- uma execucao perdida deixa a tabela permanentemente
    torta.

    Aqui a silver guarda **todas** as fotos, entao o historico inteiro pode
    ser recalculado do zero a cada execucao. As versoes sao detectadas
    comparando cada foto com a do dia anterior; o resultado e o mesmo
    sempre, e uma execucao perdida se conserta com a proxima.

    O preco e reprocessar tudo. Com 14 repositorios e uma foto por dia,
    isso e irrelevante -- e continua sendo por muitos anos.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    atributos = [F.col(nome) for nome in ATRIBUTOS_VERSIONADOS]
    dia = F.to_date("dt").alias("dia")

    fotos = repositorios.select(
        F.col("repo_id"),
        F.col("repo"),
        dia,
        *atributos,
        chave_substituta(*[F.col(n).cast("string") for n in ATRIBUTOS_VERSIONADOS]).alias(
            "_impressao"
        ),
    )

    linha_do_tempo = Window.partitionBy("repo_id").orderBy("dia")

    # Uma foto abre versao nova quando difere da anterior. A primeira foto
    # de cada repositorio sempre abre -- nao ha anterior com que comparar.
    mudou = (
        F.lag("_impressao").over(linha_do_tempo).isNull()
        | (F.col("_impressao") != F.lag("_impressao").over(linha_do_tempo))
    ).cast("int")

    # A soma acumulada das mudancas numera as versoes.
    versoes = fotos.withColumn(
        "_versao", F.sum(mudou).over(linha_do_tempo.rowsBetween(Window.unboundedPreceding, 0))
    )

    grupo = Window.partitionBy("repo_id", "_versao")
    entre_versoes = Window.partitionBy("repo_id").orderBy("_versao")

    resumo = (
        versoes.groupBy("repo_id", "_versao")
        .agg(
            F.min("dia").alias("valido_de"),
            *[F.first(nome).alias(nome) for nome in ("repo",) + ATRIBUTOS_VERSIONADOS],
        )
        # `valido_ate` e o dia em que a versao seguinte comecou. Fronteira
        # fechada a esquerda e aberta a direita: nenhum dia pertence a duas
        # versoes, e nenhum fica sem versao.
        .withColumn("valido_ate", F.lead("valido_de").over(entre_versoes))
    )

    return resumo.select(
        chave_substituta(
            F.col("repo_id").cast("string"), F.col("valido_de").cast("string")
        ).alias("sk_repositorio"),
        F.col("repo_id"),
        F.col("repo"),
        *[F.col(nome) for nome in ATRIBUTOS_VERSIONADOS],
        F.col("valido_de"),
        F.col("valido_ate"),
        F.col("valido_ate").isNull().alias("flag_atual"),
        F.lit(momento).cast("timestamp").alias("_processado_em"),
    )


# --------------------------------------------------------------------------
# Escrita
# --------------------------------------------------------------------------

def criar_tabelas(spark) -> None:
    """Cria as tres dimensoes, se ainda nao existirem."""
    spark.sql(ddl_dim_tempo())
    spark.sql(ddl_dim_autor())
    spark.sql(ddl_dim_repositorio())


def escrever(spark, df, tabela: str) -> int:
    """Substitui a dimensao inteira e devolve quantas linhas ficaram.

    `overwrite` e nao `MERGE`, nas tres dimensoes. Todas sao pequenas,
    deterministas e derivadas da silver: recalcular do zero e mais barato do
    que manter estado, e elimina a classe inteira de defeito em que uma
    execucao perdida deixa a tabela permanentemente torta.

    Isso so e seguro porque a chave substituta e um hash: reconstruir gera
    exatamente as mesmas chaves, e os fatos que apontam para elas continuam
    validos. Com contador incremental, cada `overwrite` quebraria o modelo.
    """
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela)
    )
    return spark.table(tabela).count()
