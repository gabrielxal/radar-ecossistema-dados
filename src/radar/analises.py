"""As consultas que respondem as perguntas da secao 2.

As quatro primeiras perguntas filhas da secao 2.3, mais a pergunta central da
2.2. A quinta foi respondida na Etapa 5 e esta na secao 10.6.

Consulta mora aqui e nao solta no notebook, pelo motivo da decisao 8.12: o que
esta em `src/` pode ser exercitado contra o motor antes de rodar no Databricks.

Cada funcao devolve SQL como texto e recebe os nomes das tabelas por
parametro, que e o que permite os testes apontarem para views temporarias com
dado sintetico, sem Delta e sem Unity Catalog.
"""

from __future__ import annotations

from radar import controle, gold
from radar import silver_issues as modulo_issues

# --------------------------------------------------------------------------
# As correcoes que separam leitura correta de leitura ingenua
# --------------------------------------------------------------------------
#
# A secao 10.6 mostrou que a mesma pergunta sobre o mesmo fato admite duas
# respostas, e que tres decisoes separam uma da outra. Elas reaparecem em toda
# pergunta sobre atividade humana recente, entao ficam declaradas num lugar so.
#
#   1. `sk_data_autoria` e nao `sk_data_commit`, porque a pergunta e quando a
#      pessoa trabalhou, e nao quando o trabalho entrou no repositorio
#   2. `dias_ate_o_commit <= 7`, que descarta historia anterior absorvida de
#      uma vez; sem isso, um dia de importacao vira um mes de produtividade
#   3. bot fora, porque automacao roda em agenda e nao tem ritmo humano
#
# O valor 7 e folgado de proposito: rebase e branch de longa duracao produzem
# atraso de semanas, e trabalho normal raramente passa de alguns dias.
DIAS_DE_ATRASO_ACEITAVEL = 7

# Autor sem tipo conhecido conta como humano. A alternativa, excluir o
# desconhecido, descartaria os 1,4% de commits sem conta do GitHub associada,
# que sao humanos com certeza.
SEM_BOT = "(a.github_tipo <> 'bot' OR a.github_tipo IS NULL)"


def _tabelas(
    fato=None, repositorio=None, autor=None, tempo=None, issue=None,
    silver_issues=None, controle_ingestao=None,
):
    """Nomes das tabelas, com o catalogo real como padrao."""
    return {
        "fato": fato or gold.TABELA_FCT_COMMIT,
        "repositorio": repositorio or gold.TABELA_REPOSITORIO,
        "autor": autor or gold.TABELA_AUTOR,
        "tempo": tempo or gold.TABELA_TEMPO,
        "issue": issue or gold.TABELA_FCT_ISSUE,
        "silver_issues": silver_issues or modulo_issues.TABELA_ISSUES,
        "controle": controle_ingestao or controle.TABELA_CONTROLE,
    }


# --------------------------------------------------------------------------
# O portao da pergunta 3
# --------------------------------------------------------------------------

def cobertura_do_backfill(**tabelas) -> str:
    """Em quais repositorios o historico de issues ja chegou inteiro.

    A tabela de controle sozinha nao responde isso. A coluna `registros` e da
    ultima execucao, e nao acumulada: um repositorio que a sentinela pulou por
    `304` aparece com zero mesmo tendo milhares de linhas na silver. Lida sem
    esse cuidado, ela sugere perda onde houve conclusao.

    O que decide e o par (`status`, `watermark`). Em coleta crescente, status
    `ok` com watermark no presente significa que a caminhada alcancou o fim.
    `truncado` significa backfill em andamento, e o watermark diz ate onde foi.

    Por que isso e portao e nao curiosidade: issue aberta recebe comentario,
    logo tem `updated_at` recente, logo esta no fim da caminhada ascendente.
    Num repositorio truncado, o que chegou e a parte velha e ja fechada do
    backlog, e toda medida sobre issue em aberto sai deslocada. E a mesma
    forma do defeito da secao 5.7: o dado que chegou esta correto, e a
    conclusao tirada dele nao.
    """
    t = _tabelas(**tabelas)

    return f"""
        WITH estado AS (
            SELECT repo, status, watermark
            FROM {t["controle"]}
            WHERE endpoint = 'issues'
        ),
        acumulado AS (
            SELECT repo,
                   count(*)                AS issues_na_silver,
                   count_if(estado = 'open') AS em_aberto,
                   max(atualizada_em)      AS ultima_atualizacao_vista
            FROM {t["silver_issues"]}
            GROUP BY repo
        )
        SELECT e.repo,
               e.status,
               date(e.watermark) AS coletado_ate,
               coalesce(a.issues_na_silver, 0) AS issues_na_silver,
               coalesce(a.em_aberto, 0)        AS em_aberto,
               e.status = 'ok'                 AS confiavel,
               datediff(current_date(), date(e.watermark)) AS dias_de_atraso
        FROM estado e
        LEFT JOIN acumulado a ON a.repo = e.repo
        ORDER BY confiavel, dias_de_atraso DESC
    """


# --------------------------------------------------------------------------
# Pergunta 1: o projeto acelera ou desacelera?
# --------------------------------------------------------------------------

def ritmo_por_autor(
    dias_por_periodo: int = 45,
    dias_de_atraso: int = DIAS_DE_ATRASO_ACEITAVEL,
    **tabelas,
) -> str:
    """Commits e commits por autor ativo, comparando dois periodos iguais.

    A pergunta da secao 2.3 tem duas partes, e a segunda e a que importa:
    "commits crescem, mas por contribuidor ativo tambem?".

    Volume total subindo pode significar time crescendo, e nao projeto
    acelerando; pode ate esconder o contrario, se o volume por pessoa caiu
    enquanto entrava gente. Sao duas colunas diferentes, e a comparacao entre
    elas e a resposta.

    A janela e de 90 dias, entao o corte natural sao dois periodos de 45.
    """
    t = _tabelas(**tabelas)
    janela_total = dias_por_periodo * 2

    return f"""
        WITH atividade AS (
            SELECT r.repo AS repo,
                   CASE
                       WHEN t.data >= date_sub(current_date(), {dias_por_periodo})
                            THEN 'recente'
                       WHEN t.data >= date_sub(current_date(), {janela_total})
                            THEN 'anterior'
                   END AS periodo,
                   f.sk_autor AS sk_autor
            FROM {t["fato"]} f
            JOIN {t["repositorio"]} r USING (sk_repositorio)
            JOIN {t["autor"]} a USING (sk_autor)
            JOIN {t["tempo"]} t ON t.sk_tempo = f.sk_data_autoria
            WHERE f.dias_ate_o_commit <= {dias_de_atraso}
              AND {SEM_BOT}
        ),
        por_periodo AS (
            SELECT repo, periodo,
                   count(*)                AS commits,
                   count(DISTINCT sk_autor) AS autores
            FROM atividade
            WHERE periodo IS NOT NULL
            GROUP BY repo, periodo
        ),
        lado_a_lado AS (
            SELECT repo,
                   max(CASE WHEN periodo = 'anterior' THEN commits END) AS commits_antes,
                   max(CASE WHEN periodo = 'recente'  THEN commits END) AS commits_depois,
                   max(CASE WHEN periodo = 'anterior' THEN autores END) AS autores_antes,
                   max(CASE WHEN periodo = 'recente'  THEN autores END) AS autores_depois
            FROM por_periodo
            GROUP BY repo
        )
        SELECT repo,
               coalesce(commits_antes, 0)  AS commits_antes,
               coalesce(commits_depois, 0) AS commits_depois,
               coalesce(autores_antes, 0)  AS autores_antes,
               coalesce(autores_depois, 0) AS autores_depois,
               round(commits_antes  / nullif(autores_antes, 0),  1) AS por_autor_antes,
               round(commits_depois / nullif(autores_depois, 0), 1) AS por_autor_depois,
               round(
                   100 * (commits_depois - commits_antes)
                   / nullif(commits_antes, 0)
               ) AS variacao_volume_pct,
               round(
                   100 * (
                       commits_depois / nullif(autores_depois, 0)
                       - commits_antes / nullif(autores_antes, 0)
                   ) / nullif(commits_antes / nullif(autores_antes, 0), 0)
               ) AS variacao_por_autor_pct
        FROM lado_a_lado
        ORDER BY variacao_por_autor_pct
    """


# --------------------------------------------------------------------------
# Pergunta 2: bus factor
# --------------------------------------------------------------------------

def bus_factor(
    limiar: float = 0.5,
    dias_de_atraso: int = DIAS_DE_ATRASO_ACEITAVEL,
    **tabelas,
) -> str:
    """Quantas pessoas concentram metade dos commits de cada repositorio.

    O nome vem de "quantas pessoas precisariam ser atropeladas por um onibus
    para o projeto parar". Um bus factor de 1 significa que uma pessoa sozinha
    responde por metade do trabalho, e a saida dela e um evento de
    sobrevivencia para o projeto.

    A conta e cumulativa: ordena os autores por volume, soma descendo, e
    devolve a posicao em que a soma cruza o limiar.

    `sk_autor` desempata a ordenacao para a resposta nao depender da ordem de
    leitura quando dois autores tem o mesmo numero de commits.

    Esta e a pergunta que justifica `dim_autor` ser conformada. Ela responde
    sobre commits; a mesma dimensao serve `fct_issue` sem chave nova.
    """
    t = _tabelas(**tabelas)

    return f"""
        WITH por_autor AS (
            SELECT r.repo AS repo, f.sk_autor AS sk_autor, count(*) AS commits
            FROM {t["fato"]} f
            JOIN {t["repositorio"]} r USING (sk_repositorio)
            JOIN {t["autor"]} a USING (sk_autor)
            WHERE f.dias_ate_o_commit <= {dias_de_atraso}
              AND {SEM_BOT}
            GROUP BY r.repo, f.sk_autor
        ),
        ranqueado AS (
            SELECT repo, sk_autor, commits,
                   sum(commits) OVER (
                       PARTITION BY repo
                       ORDER BY commits DESC, sk_autor
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS acumulado,
                   sum(commits) OVER (PARTITION BY repo) AS total,
                   row_number() OVER (
                       PARTITION BY repo ORDER BY commits DESC, sk_autor
                   ) AS posicao
            FROM por_autor
        )
        SELECT repo,
               min(CASE WHEN acumulado >= total * {limiar} THEN posicao END) AS bus_factor,
               max(posicao) AS autores,
               max(total)   AS commits,
               round(
                   100.0 * min(CASE WHEN acumulado >= total * {limiar} THEN posicao END)
                   / nullif(max(posicao), 0)
               ) AS concentracao_pct
        FROM ranqueado
        GROUP BY repo
        ORDER BY bus_factor, autores
    """


# --------------------------------------------------------------------------
# Pergunta 3: quanto tempo uma issue leva para ser fechada
# --------------------------------------------------------------------------

def ciclo_de_issues(**tabelas) -> str:
    """Estoque em aberto e tempo de fechamento, por repositorio.

    Duas medidas com significados opostos, e e a diferenca entre elas que
    responde a pergunta.

    `mediana_dias_ate_fechar` olha o que ja terminou, e mede vazao. Sozinha,
    ela engana: um projeto que fecha rapido o que e facil e ignora o resto
    parece saudavel.

    `mediana_idade_em_aberto` olha o que nao terminou, e mede backlog. Um
    numero alto aqui e a assinatura de projeto morrendo, e nenhuma medida
    sobre issue fechada mostra isso.

    Mediana e nao media, porque a distribuicao tem cauda longa: uma issue
    aberta ha tres anos puxaria a media sozinha.
    """
    t = _tabelas(**tabelas)

    return f"""
        SELECT r.repo AS repo,
               count(*)                    AS issues,
               count_if(f.esta_aberta)     AS em_aberto,
               count_if(NOT f.esta_aberta) AS fechadas,
               round(
                   100.0 * count_if(f.esta_aberta) / nullif(count(*), 0)
               ) AS pct_em_aberto,
               round(median(
                   CASE WHEN NOT f.esta_aberta THEN f.dias_ate_fechar END
               ), 1) AS mediana_dias_ate_fechar,
               round(median(
                   CASE WHEN f.esta_aberta THEN f.dias_em_aberto END
               ), 1) AS mediana_idade_em_aberto,
               max(CASE WHEN f.esta_aberta THEN f.dias_em_aberto END) AS idade_da_mais_velha
        FROM {t["issue"]} f
        JOIN {t["repositorio"]} r USING (sk_repositorio)
        GROUP BY r.repo
        ORDER BY mediana_idade_em_aberto DESC NULLS LAST
    """


# --------------------------------------------------------------------------
# Pergunta 4: o historico antigo muda quando o repositorio muda?
# --------------------------------------------------------------------------

def versoes_do_repositorio(**tabelas) -> str:
    """Quantas versoes a SCD2 guarda de cada repositorio.

    Uma versao significa que nenhum atributo mudou desde a primeira foto. Nao
    e defeito: licenca e linguagem mudam em escala de meses, e a serie tem
    poucos dias. Duas ou mais e a SCD2 tendo capturado uma mudanca real.
    """
    t = _tabelas(**tabelas)

    return f"""
        SELECT repo,
               count(*)          AS versoes,
               min(valido_de)    AS primeira_vigencia,
               min(observado_de) AS primeira_foto,
               max(CASE WHEN flag_atual THEN valido_de END) AS versao_vigente_desde
        FROM {t["repositorio"]}
        GROUP BY repo
        ORDER BY versoes DESC, repo
    """


def historico_preservado(**tabelas) -> str:
    """A prova de que o fato aponta para a versao da epoca, e nao para a de hoje.

    Compara, para cada commit, o `repo` da versao a que ele esta ligado com o
    `repo` da versao vigente do mesmo repositorio. Enquanto nada mudou, as
    duas colunas sao iguais em todas as linhas, e `divergentes` fica em zero.

    O valor da consulta e o dia em que `divergentes` deixar de ser zero: e o
    momento em que a SCD2 passa a pagar o custo dela. Ate la, ela mostra que a
    juncao por vigencia esta ligada corretamente.
    """
    t = _tabelas(**tabelas)

    return f"""
        WITH ligado AS (
            SELECT f.sha       AS sha,
                   v.repo_id   AS repo_id,
                   v.repo      AS repo_na_epoca,
                   v.valido_de AS versao_de
            FROM {t["fato"]} f
            JOIN {t["repositorio"]} v USING (sk_repositorio)
        ),
        vigente AS (
            SELECT repo_id, repo AS repo_hoje
            FROM {t["repositorio"]}
            WHERE flag_atual
        )
        SELECT l.repo_na_epoca AS repo,
               count(*)        AS commits,
               count(DISTINCT l.versao_de) AS versoes_referenciadas,
               count_if(l.repo_na_epoca <> g.repo_hoje) AS divergentes
        FROM ligado l
        JOIN vigente g USING (repo_id)
        GROUP BY l.repo_na_epoca
        ORDER BY divergentes DESC, repo
    """


# --------------------------------------------------------------------------
# A pergunta central: quais estao saudaveis, quais estao morrendo
# --------------------------------------------------------------------------

def painel_de_saude(
    dias_por_periodo: int = 45,
    dias_de_atraso: int = DIAS_DE_ATRASO_ACEITAVEL,
    limiar: float = 0.5,
    **tabelas,
) -> str:
    """Os sinais das perguntas filhas lado a lado, um repositorio por linha.

    Nao produz um veredito, e a ausencia e deliberada. Um indice unico de
    saude esconderia a informacao que importa: projeto com bus factor 1 e
    ritmo alto tem risco diferente de projeto com bus factor 12 e ritmo
    caindo, e um numero so os igualaria.

    A juncao com issues e a esquerda de proposito: enquanto `fct_issue`
    estiver vazia as demais colunas continuam respondendo.
    """
    t = _tabelas(**tabelas)
    ritmo = ritmo_por_autor(dias_por_periodo, dias_de_atraso, **tabelas)
    fator = bus_factor(limiar, dias_de_atraso, **tabelas)
    issues = ciclo_de_issues(**tabelas)

    return f"""
        WITH ritmo AS ({ritmo}),
             fator AS ({fator}),
             issues AS ({issues})
        SELECT r.repo AS repo,
               r.commits_depois         AS commits_45d,
               r.autores_depois         AS autores_45d,
               r.variacao_por_autor_pct AS ritmo_por_autor_pct,
               f.bus_factor             AS bus_factor,
               f.autores                AS autores_90d,
               i.em_aberto              AS issues_em_aberto,
               i.mediana_idade_em_aberto AS idade_mediana_em_aberto
        FROM ritmo r
        LEFT JOIN fator f  ON f.repo = r.repo
        LEFT JOIN issues i ON i.repo = r.repo
        ORDER BY f.bus_factor, r.variacao_por_autor_pct
    """
