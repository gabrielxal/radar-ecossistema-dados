"""Testes de qualidade sobre as tabelas do lakehouse.

Cada verificacao e uma consulta que conta violacoes: zero e aprovacao. O
resultado de cada execucao fica gravado, o que permite comparar o estado de
hoje com o das execucoes anteriores.

A severidade separa a falha que interrompe o pipeline da que apenas informa.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from radar import (
    bronze,
    controle,
    gold,
    silver,
    silver_issues,
    silver_repositorios,
)
from radar.config import BRONZE, REPOS, fqn
from radar.ingestao import Endpoint

TABELA_QUALIDADE = fqn(BRONZE, "qualidade_execucao")

BLOQUEIA = "bloqueia"
AVISA = "avisa"
SEVERIDADES = (BLOQUEIA, AVISA)

# As contagens de controle nao sao SQL sobre uma tabela unica, mas entram na
# bateria com o mesmo formato das demais para compartilhar o historico.
RECONCILIACAO_BRONZE = "reconciliacao_landing_bronze"
RECONCILIACAO_SILVER = "reconciliacao_bronze_silver"


@dataclass(frozen=True)
class Verificacao:
    """Uma regra. O SQL devolve uma coluna `violacoes` com a contagem."""

    nome: str
    descricao: str
    severidade: str
    sql: str


@dataclass(frozen=True)
class Resultado:
    nome: str
    severidade: str
    violacoes: int
    # Preenchidos so pela contagem de controle, onde o par origem/destino e a
    # informacao a guardar. Nulos nas demais regras, em que `esperado` seria
    # sempre 0.
    esperado: int | None = None
    obtido: int | None = None

    @property
    def passou(self) -> bool:
        return self.violacoes == 0


@dataclass(frozen=True)
class Reconciliacao:
    """Contagem de controle entre duas camadas: quanto entrou, quanto saiu."""

    nome: str
    na_origem: int
    no_destino: int

    @property
    def diferenca(self) -> int:
        return self.na_origem - self.no_destino

    @property
    def bate(self) -> bool:
        return self.diferenca == 0

    def como_resultado(self) -> Resultado:
        """Converte a contagem em uma linha da bateria, com historico.

        `abs()` porque o desvio conta nos dois sentidos: destino com menos
        linhas indica perda no caminho; com mais, origem removida ou insercao
        feita por fora do pipeline.
        """
        return Resultado(
            nome=self.nome,
            severidade=BLOQUEIA,
            violacoes=abs(self.diferenca),
            esperado=self.na_origem,
            obtido=self.no_destino,
        )


DDL_QUALIDADE = f"""
CREATE TABLE IF NOT EXISTS {TABELA_QUALIDADE} (
    executado_em TIMESTAMP COMMENT 'quando a bateria rodou',
    tabela       STRING    COMMENT 'tabela verificada',
    verificacao  STRING    COMMENT 'nome da regra',
    severidade   STRING    COMMENT 'bloqueia | avisa',
    violacoes    BIGINT    COMMENT 'linhas que violam a regra; 0 e aprovacao',
    passou       BOOLEAN   COMMENT 'violacoes = 0',
    esperado     BIGINT    COMMENT 'contagem de controle: quanto a origem tinha',
    obtido       BIGINT    COMMENT 'contagem de controle: quanto o destino tem'
)
USING DELTA
COMMENT 'Historico dos testes de qualidade. Uma linha por regra por execucao.'
"""


# --------------------------------------------------------------------------
# Funcoes puras
# --------------------------------------------------------------------------

def _lista_sql(valores) -> str:
    """Tupla Python -> lista literal de SQL."""
    return ", ".join("'" + str(v) + "'" for v in valores)


def verificacoes_bronze(endpoint: Endpoint) -> tuple[Verificacao, ...]:
    """A bateria da bronze. Nenhuma delas olha regra de negocio.

    Bronze nao limpa dado, entao aqui so cabe verificar o que a propria
    camada promete: chave presente, chave unica, proveniencia completa.
    """
    tabela = bronze.nome_tabela(endpoint)
    chave = endpoint.chave

    return (
        Verificacao(
            nome="chave_ausente_no_payload",
            descricao=(
                f"Todo payload contem `{chave}`. Violacao indica JSON "
                "invalido ou mudanca no formato da API."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE get_json_object(payload, '$.{chave}') IS NULL
            """,
        ),
        Verificacao(
            nome="chave_duplicada",
            descricao=(
                "Uma linha por (repo, chave). Verifica de fora a "
                "idempotencia garantida pelo MERGE da carga."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT repo, {chave}
                    FROM {tabela}
                    GROUP BY repo, {chave}
                    HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="proveniencia_incompleta",
            descricao=(
                "Os tres metadados de proveniencia estao preenchidos. Sem "
                "eles nao ha como rastrear a origem de uma linha."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE _ingerido_em IS NULL
                   OR _arquivo_origem IS NULL
                   OR _endpoint IS NULL
            """,
        ),
        Verificacao(
            nome="endpoint_inconsistente",
            descricao="Toda linha da tabela veio do endpoint que ela representa.",
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE _endpoint <> '{endpoint.nome}'
            """,
        ),
        Verificacao(
            nome="repo_fora_do_escopo",
            descricao=(
                "Todo repo pertence a lista do config. Violacao indica "
                "caminho mal formado ou decodificacao errada do diretorio."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE repo NOT IN ({_lista_sql(REPOS)})
            """,
        ),
        Verificacao(
            nome="carga_truncada",
            descricao=(
                "Nenhuma carga parou no teto de paginas. Truncagem nao "
                "corrompe o que chegou, mas registra que a coleta ficou "
                "incompleta. Em coleta decrescente o watermark nao avanca e a "
                "execucao seguinte tenta o mesmo intervalo; em coleta "
                "crescente ele avanca e o backfill continua de onde parou."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {controle.TABELA_CONTROLE}
                WHERE endpoint = '{endpoint.nome}' AND status = 'truncado'
            """,
        ),
        Verificacao(
            nome="data_do_registro_ausente",
            descricao=(
                f"O campo `{endpoint.campo_data}` sustenta o watermark e a "
                "tipagem da silver. Avisa em vez de bloquear: a bronze "
                "armazena o registro defeituoso para permitir investiga-lo."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE get_json_object(payload, '$.{endpoint.campo_data}') IS NULL
            """,
        ),
    )


def verificacoes_silver(endpoint: Endpoint) -> tuple[Verificacao, ...]:
    """A bateria da silver do endpoint.

    Despacha em vez de generalizar: as regras aqui falam do significado do
    dado, e significado nao se parametriza. "Data de fechamento nao pode
    preceder a de abertura" nao tem equivalente em commit, e "contagem de pais
    nao pode ser negativa" nao tem equivalente em issue.
    """
    if endpoint.nome == "issues":
        return verificacoes_silver_issues()
    if endpoint.nome == "commits":
        return verificacoes_silver_commits()
    raise KeyError(
        f"endpoint '{endpoint.nome}' sem bateria de silver declarada em "
        "qualidade.verificacoes_silver"
    )


def verificacoes_silver_issues() -> tuple[Verificacao, ...]:
    """A bateria da silver de issues."""
    tabela = silver_issues.TABELA_ISSUES
    prs = silver_issues.TABELA_PULL_REQUESTS
    dominio_estado = _lista_sql(silver_issues.ESTADOS)
    dominio_motivo = _lista_sql(silver_issues.MOTIVOS_DE_ESTADO)
    dominio_associacao = _lista_sql(silver_issues.ASSOCIACOES)

    return (
        Verificacao(
            nome="chave_duplicada",
            descricao=(
                "Uma linha por (repo, numero). Verifica de fora o upsert que "
                "a carga faz por essa mesma chave, e a deduplicacao do lote "
                "que precede o MERGE."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT repo, numero FROM {tabela}
                    GROUP BY repo, numero HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="numero_em_duas_entidades",
            descricao=(
                "Nenhum (repo, numero) esta ao mesmo tempo em issues e em "
                "pull requests. Os dois compartilham a mesma sequencia de "
                "numeros no repositorio, e a mesma chave nos dois lados "
                "significaria roteamento errado."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT repo, numero FROM {tabela}
                    INTERSECT
                    SELECT repo, numero FROM {prs}
                )
            """,
        ),
        Verificacao(
            nome="fechamento_antes_da_abertura",
            descricao=(
                "Nenhuma issue fecha antes de abrir. Inverteria o sinal de "
                "`dias_ate_fechar` no fato."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM {tabela}
                WHERE fechada_em IS NOT NULL AND fechada_em < aberta_em
            """,
        ),
        Verificacao(
            nome="fechada_sem_data_de_fechamento",
            descricao=(
                "Issue com estado `closed` tem `fechada_em`. Avisa em vez de "
                "bloquear: e inconsistencia da origem, e o fato trata a "
                "ausencia como processo ainda aberto."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes FROM {tabela}
                WHERE estado = 'closed' AND fechada_em IS NULL
            """,
        ),
        Verificacao(
            nome="estado_fora_do_dominio",
            descricao=f"`estado` pertence a {dominio_estado}.",
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes FROM {tabela}
                WHERE estado IS NOT NULL AND estado NOT IN ({dominio_estado})
            """,
        ),
        Verificacao(
            nome="motivo_de_estado_fora_do_dominio",
            descricao=(
                f"`motivo_estado` pertence a {dominio_motivo}. Valor novo "
                "aqui significa que a API passou a classificar de outro jeito."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes FROM {tabela}
                WHERE motivo_estado IS NOT NULL
                  AND motivo_estado NOT IN ({dominio_motivo})
            """,
        ),
        Verificacao(
            nome="associacao_fora_do_dominio",
            descricao=f"`associacao_autor` pertence a {dominio_associacao}.",
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes FROM {tabela}
                WHERE associacao_autor IS NOT NULL
                  AND associacao_autor NOT IN ({dominio_associacao})
            """,
        ),
        Verificacao(
            nome="contagem_negativa",
            descricao=(
                "`size(NULL)` devolve -1 em modo legado, entao contagem "
                "negativa denuncia guarda ausente na tipagem."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM {tabela}
                WHERE qtd_rotulos < 0 OR qtd_responsaveis < 0 OR comentarios < 0
            """,
        ),
        Verificacao(
            nome="repositorio_fora_do_escopo",
            descricao="Todo `repo` da tabela esta na lista configurada.",
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes FROM {tabela}
                WHERE repo NOT IN ({_lista_sql(REPOS)})
            """,
        ),
    )


def verificacoes_silver_commits() -> tuple[Verificacao, ...]:
    """A bateria da silver de commits.

    Sao verificacoes que a bronze nao poderia fazer: comparar duas datas
    exige que elas sejam datas, e nao texto.
    """
    tabela = silver.TABELA_COMMITS
    quarentena = silver.TABELA_REJEITADOS

    return (
        Verificacao(
            nome="chave_duplicada",
            descricao=(
                "Uma linha por (repo, sha). Verifica de fora o upsert que a "
                "carga faz por essa mesma chave."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT repo, sha FROM {tabela}
                    GROUP BY repo, sha HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="contagem_de_pais_negativa",
            descricao=(
                "`size(NULL)` devolve -1 em modo legado. Contagem negativa "
                "passaria por qualquer verificacao de nulo sem ser notada."
            ),
            severidade=BLOQUEIA,
            sql=f"SELECT count(*) AS violacoes FROM {tabela} WHERE qtd_pais < 0",
        ),
        Verificacao(
            nome="normalizacao_nao_aplicada",
            descricao=(
                "E-mail gravado exatamente como a normalizacao produziria. "
                "Violacao indica linha que entrou por fora da projecao."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE autor_email <> lower(trim(autor_email))
                   OR committer_email <> lower(trim(committer_email))
            """,
        ),
        Verificacao(
            nome="texto_vazio_em_vez_de_nulo",
            descricao=(
                "String vazia e NULL sao a mesma ausencia e comparam "
                "diferente. A silver converte uma na outra."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE mensagem = '' OR autor_nome = '' OR github_login = ''
            """,
        ),
        Verificacao(
            nome="quarentena_sem_motivo",
            descricao=(
                "Toda linha desviada diz por que foi. Quarentena sem motivo "
                "e um registro perdido com passos extras."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {quarentena}
                WHERE motivo IS NULL OR motivo NOT IN ({_lista_sql(silver.MOTIVOS_DE_REJEICAO)})
            """,
        ),
        Verificacao(
            nome="commit_anterior_a_autoria",
            descricao=(
                "A data de entrada no repositorio nao antecede a de escrita. "
                "Rebase afasta as duas, mas nunca inverte a ordem."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE commitado_em < autorado_em
            """,
        ),
        Verificacao(
            nome="data_no_futuro",
            descricao=(
                "Commit datado depois de agora. Costuma ser relogio errado na "
                "maquina de quem commitou, e distorce qualquer serie temporal."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE commitado_em > current_timestamp()
            """,
        ),
        Verificacao(
            nome="tipo_de_autor_fora_do_dominio",
            descricao=(
                "`github_tipo` pertence ao dominio conhecido. Valor novo "
                "indica categoria criada pela origem, nao defeito nosso."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE github_tipo IS NOT NULL
                  AND github_tipo NOT IN ({_lista_sql(silver.TIPOS_DE_AUTOR)})
            """,
        ),
        Verificacao(
            nome="motivo_de_assinatura_fora_do_dominio",
            descricao=(
                "`assinatura_motivo` pertence ao dominio conhecido, que o "
                "GitHub amplia de tempos em tempos."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE assinatura_motivo IS NOT NULL
                  AND assinatura_motivo NOT IN ({_lista_sql(silver.MOTIVOS_DE_ASSINATURA)})
            """,
        ),
        Verificacao(
            nome="repo_fora_do_escopo",
            descricao=(
                "Todo repo pertence a lista do config, como na bronze. "
                "Divergencia aqui apareceria entre as duas camadas."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {tabela}
                WHERE repo NOT IN ({_lista_sql(REPOS)})
            """,
        ),
    )


def verificacoes_gold() -> tuple[Verificacao, ...]:
    """A bateria da gold. Aqui as regras sao sobre o modelo, nao sobre o dado.

    As tres primeiras sao as invariantes da SCD2 declaradas na secao 6.4 do
    documento de projeto: sao afirmacoes que a modelagem faz e que so um teste
    de fora comprova.
    """
    tempo = gold.TABELA_TEMPO
    autor = gold.TABELA_AUTOR
    repositorio = gold.TABELA_REPOSITORIO

    return (
        Verificacao(
            nome="mais_de_uma_versao_vigente",
            descricao=(
                "Exatamente uma versao vigente por chave natural. Duas "
                "fariam a juncao do fato duplicar a linha, inflando medida."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT repo_id FROM {repositorio}
                    WHERE flag_atual GROUP BY repo_id HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="flag_atual_incoerente",
            descricao=(
                "`flag_atual` e `valido_ate` contam a mesma historia: "
                "vigente e exatamente a versao sem data de fim."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {repositorio}
                WHERE (flag_atual AND valido_ate IS NOT NULL)
                   OR (NOT flag_atual AND valido_ate IS NULL)
            """,
        ),
        Verificacao(
            nome="chave_substituta_duplicada",
            descricao=(
                "A chave substituta identifica uma versao. Repetida, o fato "
                "passa a apontar para duas linhas ao mesmo tempo."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT sk_repositorio FROM {repositorio}
                    GROUP BY sk_repositorio HAVING count(*) > 1
                    UNION ALL
                    SELECT sk_autor FROM {autor}
                    GROUP BY sk_autor HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="intervalo_de_validade_invertido",
            descricao=(
                "Uma versao nao pode terminar antes de comecar. Fronteira "
                "fechada a esquerda e aberta a direita."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {repositorio}
                WHERE valido_ate IS NOT NULL AND valido_ate <= valido_de
            """,
        ),
        Verificacao(
            nome="email_em_duas_formas",
            descricao=(
                "Nenhum e-mail aparece com conta resolvida em um commit e "
                "sem conta noutro. E a premissa da chave hibrida da "
                "dim_autor: se deixar de valer, a mesma pessoa vira duas "
                "linhas com chaves substitutas diferentes."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT autor_email
                    FROM {silver.TABELA_COMMITS}
                    WHERE autor_email IS NOT NULL
                    GROUP BY autor_email
                    HAVING count(DISTINCT CASE WHEN github_id IS NULL THEN 1 END) > 0
                       AND count(DISTINCT CASE WHEN github_id IS NOT NULL THEN 1 END) > 0
                )
            """,
        ),
        Verificacao(
            nome="dim_tempo_com_lacuna",
            descricao=(
                "Um dia por linha, sem buraco entre o primeiro e o ultimo. "
                "Lacuna faz a serie temporal pular o dia parado."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT datediff(max(data), min(data)) + 1 - count(*) AS violacoes
                    FROM {tempo}
                ) WHERE violacoes <> 0
            """,
        ),
        Verificacao(
            nome="autor_sem_origem_declarada",
            descricao=(
                "Toda linha declara de onde veio a chave. Sem isso a decisao "
                "da chave hibrida deixa de ser auditavel pelo dado."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {autor}
                WHERE origem_da_chave NOT IN (
                    '{gold.ORIGEM_CONTA}', '{gold.ORIGEM_EMAIL}', '{gold.ORIGEM_DESCONHECIDA}'
                )
            """,
        ),
        Verificacao(
            nome="repositorio_fora_do_escopo",
            descricao=(
                "Toda versao pertence a um repositorio da lista do config, "
                "como nas camadas anteriores."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {repositorio}
                WHERE flag_atual AND repo NOT IN ({_lista_sql(REPOS)})
            """,
        ),
    )


def verificacoes_fatos() -> tuple[Verificacao, ...]:
    """A bateria dos fatos: grao e integridade referencial.

    O Unity Catalog registra chave estrangeira mas nao a impoe. Estas
    verificacoes sao o que substitui a imposicao do banco. Sem elas, um
    fato apontando para dimensao inexistente so apareceria como linha que
    some da consulta, sem erro nenhum.
    """
    commit = gold.TABELA_FCT_COMMIT
    snapshot = gold.TABELA_FCT_SNAPSHOT
    issue = gold.TABELA_FCT_ISSUE
    tempo = gold.TABELA_TEMPO
    autor = gold.TABELA_AUTOR
    repositorio = gold.TABELA_REPOSITORIO

    return (
        Verificacao(
            nome="grao_do_fct_commit",
            descricao=(
                "Um commit por linha. Chave repetida significa juncao que "
                "multiplicou linha, e toda medida agregada fica inflada."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT sha, sk_repositorio FROM {commit}
                    GROUP BY sha, sk_repositorio HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="grao_do_fct_issue",
            descricao=(
                "Uma issue por linha. A silver ja colapsa o log de versoes "
                "num estado corrente, entao chave repetida aqui significa "
                "que a juncao com a dimensao por vigencia multiplicou linha."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT numero, sk_repositorio FROM {issue}
                    GROUP BY numero, sk_repositorio HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="marco_final_incoerente",
            descricao=(
                "`esta_aberta` e a ausencia de `sk_data_fechamento` dizem a "
                "mesma coisa. Divergirem significa que os marcos do snapshot "
                "acumulado deixaram de descrever o mesmo processo."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM {issue}
                WHERE esta_aberta <> (sk_data_fechamento IS NULL)
            """,
        ),
        Verificacao(
            nome="duracao_negativa_no_fct_issue",
            descricao=(
                "Nenhuma issue leva tempo negativo para fechar nem tem idade "
                "negativa. Denuncia marco invertido que a silver deixou passar."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM {issue}
                WHERE dias_ate_fechar < 0 OR dias_em_aberto < 0
            """,
        ),
        Verificacao(
            nome="grao_do_fct_repo_snapshot",
            descricao=(
                "Um repositorio por dia. Duas fotos do mesmo dia contariam "
                "as mesmas estrelas duas vezes."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT repo_id, sk_data FROM {snapshot}
                    GROUP BY repo_id, sk_data HAVING count(*) > 1
                )
            """,
        ),
        Verificacao(
            nome="fato_sem_dimensao_de_repositorio",
            descricao=(
                "Toda chave de repositorio existe na dimensao. Orfao nao "
                "gera erro: a linha apenas some da consulta com juncao."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT f.sk_repositorio FROM {commit} f
                    LEFT ANTI JOIN {repositorio} d USING (sk_repositorio)
                    UNION ALL
                    SELECT f.sk_repositorio FROM {snapshot} f
                    LEFT ANTI JOIN {repositorio} d USING (sk_repositorio)
                    UNION ALL
                    SELECT f.sk_repositorio FROM {issue} f
                    LEFT ANTI JOIN {repositorio} d USING (sk_repositorio)
                )
            """,
        ),
        Verificacao(
            nome="fato_sem_dimensao_de_autor",
            descricao=(
                "Toda chave de autor existe na dimensao, inclusive a do "
                "membro desconhecido."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT f.sk_autor FROM {commit} f
                    LEFT ANTI JOIN {autor} d USING (sk_autor)
                    UNION ALL
                    SELECT f.sk_autor FROM {issue} f
                    LEFT ANTI JOIN {autor} d USING (sk_autor)
                )
            """,
        ),
        Verificacao(
            nome="fato_sem_dimensao_de_tempo",
            descricao=(
                "Toda chave de tempo existe na dimensao, nos tres fatos. Sao "
                "calculadas em vez de buscadas, entao esta e a unica rede que "
                "resta. A de fechamento so entra quando existe: issue aberta "
                "nao tem marco final, e isso nao e orfandade."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes FROM (
                    SELECT sk_data_commit AS sk FROM {commit}
                    UNION ALL
                    SELECT sk_data_autoria FROM {commit} WHERE sk_data_autoria IS NOT NULL
                    UNION ALL
                    SELECT sk_data FROM {snapshot}
                    UNION ALL
                    SELECT sk_data_abertura FROM {issue}
                    UNION ALL
                    SELECT sk_data_fechamento FROM {issue}
                    WHERE sk_data_fechamento IS NOT NULL
                ) f
                LEFT ANTI JOIN {tempo} t ON t.sk_tempo = f.sk
            """,
        ),
        Verificacao(
            nome="chave_obrigatoria_nula_no_fato",
            descricao=(
                "Chave nula no fato e juncao perdida em silencio. A data de "
                "autoria pode faltar; repositorio, autor e data do commit nao."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {commit}
                WHERE sk_repositorio IS NULL
                   OR sk_autor IS NULL
                   OR sk_data_commit IS NULL
            """,
        ),
        Verificacao(
            nome="commit_ligado_a_versao_futura",
            descricao=(
                "Nenhum commit aponta para versao que so passou a valer "
                "depois dele. Verifica de fora a juncao por vigencia."
            ),
            severidade=BLOQUEIA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {commit} f
                JOIN {repositorio} d USING (sk_repositorio)
                JOIN {tempo} t ON t.sk_tempo = f.sk_data_commit
                WHERE t.data < d.valido_de
                   OR (d.valido_ate IS NOT NULL AND t.data >= d.valido_ate)
            """,
        ),
        Verificacao(
            nome="desconhecido_usado_com_chave_resolvivel",
            descricao=(
                "O membro desconhecido so acolhe quem nao tem chave natural. "
                "Usado a mais, esconde autor que existia e podia ser ligado."
            ),
            severidade=AVISA,
            sql=f"""
                SELECT count(*) AS violacoes
                FROM {commit} f
                JOIN {autor} d USING (sk_autor)
                WHERE d.origem_da_chave = '{gold.ORIGEM_DESCONHECIDA}'
            """,
        ),
    )


def resumo(resultados: list[Resultado]) -> tuple[int, int]:
    """(quantos bloqueios falharam, quantos avisos falharam)."""
    bloqueios = sum(1 for r in resultados if not r.passou and r.severidade == BLOQUEIA)
    avisos = sum(1 for r in resultados if not r.passou and r.severidade == AVISA)
    return bloqueios, avisos


def levantar_se_bloqueou(resultados: list[Resultado]) -> None:
    """Interrompe o pipeline se alguma regra bloqueante falhou."""
    falhas = [r for r in resultados if not r.passou and r.severidade == BLOQUEIA]
    if falhas:
        detalhe = ", ".join(f"{r.nome}={r.violacoes}" for r in falhas)
        raise AssertionError(f"qualidade reprovada: {detalhe}")


# --------------------------------------------------------------------------
# Execucao
# --------------------------------------------------------------------------

def criar_tabela(spark) -> None:
    spark.sql(DDL_QUALIDADE)


def executar(spark, verificacoes: tuple[Verificacao, ...]) -> list[Resultado]:
    """Roda a bateria e devolve um Resultado por regra."""
    return [
        Resultado(
            nome=v.nome,
            severidade=v.severidade,
            violacoes=int(spark.sql(v.sql).collect()[0]["violacoes"]),
        )
        for v in verificacoes
    ]


def registrar(
    spark, resultados: list[Resultado], tabela: str, momento: datetime
) -> None:
    """Acrescenta a execucao ao historico. Append: nada e sobrescrito."""
    from pyspark.sql.types import (
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("executado_em", TimestampType(), True),
            StructField("tabela", StringType(), True),
            StructField("verificacao", StringType(), True),
            StructField("severidade", StringType(), True),
            StructField("violacoes", LongType(), True),
            StructField("passou", BooleanType(), True),
            StructField("esperado", LongType(), True),
            StructField("obtido", LongType(), True),
        ]
    )
    linhas = [
        (
            momento,
            tabela,
            r.nome,
            r.severidade,
            int(r.violacoes),
            r.passou,
            None if r.esperado is None else int(r.esperado),
            None if r.obtido is None else int(r.obtido),
        )
        for r in resultados
    ]
    (
        spark.createDataFrame(linhas, schema=schema)
        .write.mode("append")
        # A tabela pode ter sido criada antes de `esperado` e `obtido`
        # existirem; `mergeSchema` acrescenta as colunas e deixa NULL nas
        # linhas antigas. Aceitavel numa tabela de controle com schema
        # declarado no codigo. Em bronze ou silver fica desligado, senao um
        # nome digitado errado cria coluna nova sem revisao.
        .option("mergeSchema", "true")
        .saveAsTable(TABELA_QUALIDADE)
    )


def reconciliar(
    spark, base_volume: str, endpoint: Endpoint, momento: datetime
) -> Reconciliacao:
    """Contagem de controle: o que ha na landing zone chegou inteiro na bronze?

    Compara contra a origem ja deduplicada, porque a sobreposicao de dias faz o
    JSONL bruto ter mais linhas do que a bronze deve ter, por desenho.
    """
    fonte = bronze.deduplicar(
        bronze.ler_landing(spark, base_volume, endpoint, momento), endpoint
    )
    return Reconciliacao(
        nome=RECONCILIACAO_BRONZE,
        na_origem=fonte.count(),
        no_destino=spark.table(bronze.nome_tabela(endpoint)).count(),
    )


# Onde cada endpoint deposita o que a silver leu da bronze.
#
# A funcao recebia o endpoint e ignorava, lendo sempre as tabelas de commits.
# Enquanto so o notebook de commits chamava, o defeito nao aparecia; para
# qualquer outro endpoint a conta seria feita contra a tabela errada.
DESTINOS_SILVER: dict[str, tuple[str, ...]] = {
    "commits": (silver.TABELA_COMMITS, silver.TABELA_REJEITADOS),
    "repositorios": (silver_repositorios.TABELA_REPOSITORIOS,),
    # Tres destinos: o endpoint /issues devolve pull requests misturados, e
    # eles vao para tabela propria em vez de sumirem. Sem esse terceiro balde
    # a reconciliacao acusaria como perda o que foi decisao.
    "issues": (
        silver_issues.TABELA_ISSUES,
        silver_issues.TABELA_PULL_REQUESTS,
        silver_issues.TABELA_REJEITADOS,
    ),
}


def destinos_silver(endpoint: Endpoint) -> tuple[str, ...]:
    """Tabelas que recebem o que saiu da bronze deste endpoint.

    Endpoint novo precisa ser declarado aqui de proposito, e a falta levanta
    em vez de devolver vazio: uma reconciliacao que compara com nada aprovaria
    qualquer coisa.

    Endpoint que descarte parte legitima do que leu precisa gravar os
    descartados numa tabela e declara-la junto. E o caso de `/issues`, que
    devolve pull requests misturados as issues: sem uma tabela para eles, a
    diferenca apareceria aqui como perda, sem pista de onde as linhas foram
    parar.
    """
    if endpoint.nome not in DESTINOS_SILVER:
        raise KeyError(
            f"endpoint '{endpoint.nome}' sem destino silver declarado em "
            "qualidade.DESTINOS_SILVER"
        )
    return DESTINOS_SILVER[endpoint.nome]


def contagem_na_bronze(spark, endpoint: Endpoint) -> int:
    """Quantas entidades a bronze do endpoint contem.

    Para endpoint imutavel, linha e entidade e a contagem e direta. Para
    endpoint mutavel a bronze e um log de versoes, com uma linha por dia de
    coleta, e a silver colapsa o log numa linha por entidade. Contar linha dos
    dois lados compararia grandezas diferentes: a reconciliacao acusaria perda
    proporcional a quantas vezes as issues foram atualizadas.

    A pergunta que a reconciliacao faz continua sendo "chegou tudo?", e a
    unidade da resposta e a entidade, nao a versao.
    """
    tabela = bronze.nome_tabela(endpoint)
    if not endpoint.mutavel:
        return spark.table(tabela).count()

    return spark.sql(
        f"SELECT count(DISTINCT repo, {endpoint.chave}) AS total FROM {tabela}"
    ).collect()[0]["total"]


def reconciliar_silver(spark, endpoint: Endpoint) -> Reconciliacao:
    """Contagem de controle: `bronze = soma dos destinos da silver`.

    A igualdade so fecha porque registro fora do contrato e desviado, nunca
    descartado. Se a silver descartasse em silencio, a diferenca apareceria
    aqui sem nenhuma pista de onde as linhas foram parar.
    """
    return Reconciliacao(
        nome=RECONCILIACAO_SILVER,
        na_origem=contagem_na_bronze(spark, endpoint),
        no_destino=sum(
            spark.table(tabela).count() for tabela in destinos_silver(endpoint)
        ),
    )


def avaliar(
    spark,
    tabela: str,
    verificacoes: tuple[Verificacao, ...],
    reconciliacao: Reconciliacao,
    momento: datetime,
) -> list[Resultado]:
    """Roda a bateria, grava o historico e devolve os resultados.

    A gravacao acontece aqui, antes de qualquer interrupcao: quem chama e
    quem decide levantar. Execucao reprovada que nao entra no historico e
    justamente a que faria falta na investigacao.
    """
    resultados = [reconciliacao.como_resultado()] + executar(spark, verificacoes)
    registrar(spark, resultados, tabela, momento)
    return resultados
