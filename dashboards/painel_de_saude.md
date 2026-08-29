# Painel de saúde do ecossistema

Especificação do dashboard AI/BI construído sobre `radar_gold`. Um arquivo por
dashboard, versionado pelo mesmo motivo que `orquestracao/*.yml`: a
configuração tem histórico, revisão e caminho de recuperação como qualquer
outro código.

## Pré-requisito

`notebooks/12_consumo.py` executado. Ele cria as seis visões que os widgets
consultam.

## A regra que atravessa o painel

**Nenhum widget tem SQL próprio.** Todo dataset é `SELECT * FROM <visão>`.

A lógica analítica vive em `src/radar/analises.py`, é exercitada contra o motor
em `tests/test_analises_spark.py` e chega ao painel pela visão. Um widget com
consulta escrita dentro dele seria uma segunda cópia da mesma regra, sem teste,
livre para divergir da primeira. É a decisão 8.12 aplicada à camada de consumo,
e está registrada em `docs/PROJETO.md`, seção 8.13.

Se um visual precisar de um recorte que a visão não dá, o caminho é acrescentar
a coluna em `analises.py` — não escrever SQL no widget.

## Datasets

| Nome | Consulta |
|---|---|
| `painel` | `SELECT * FROM workspace.radar_gold.vw_painel_de_saude` |
| `bus_factor` | `SELECT * FROM workspace.radar_gold.vw_bus_factor` |
| `ritmo` | `SELECT * FROM workspace.radar_gold.vw_ritmo_por_autor` |
| `issues` | `SELECT * FROM workspace.radar_gold.vw_ciclo_de_issues` |
| `cobertura` | `SELECT * FROM workspace.radar_gold.vw_cobertura_do_backfill` |

## Layout

Uma página, `Saúde do ecossistema`, em grade de 6 colunas.

### 1. Concentração de manutenção — barras horizontais

| | |
|---|---|
| Dataset | `bus_factor` |
| Y (categoria) | `repo`, ordenado por `bus_factor` crescente |
| X (valor) | `bus_factor` |
| Cor | `bus_factor`, escala sequencial invertida (menor = mais crítico) |
| Posição | linha 1, largura 3 |

Título: *Quantas pessoas concentram metade dos commits*.
Descrição: *1 é ponto único de falha. Janela de 90 dias, bots e histórico
enxertado fora.*

### 2. Acelerando ou desacelerando — dispersão

| | |
|---|---|
| Dataset | `ritmo` |
| X | `variacao_volume_pct` |
| Y | `variacao_por_autor_pct` |
| Rótulo | `repo` |
| Tamanho | `commits_depois` |
| Posição | linha 1, largura 3 |

Título: *Volume contra volume por autor*.
Descrição: *Quadrante inferior esquerdo é queda nos dois. Volume subindo com
produção por pessoa caindo é time crescendo, não projeto acelerando.*

As duas linhas de referência em zero são o que torna o gráfico legível: elas
separam os quatro quadrantes, e é a combinação dos sinais — não cada um
isolado — que responde a pergunta.

### 3. Vazão contra backlog — dispersão

| | |
|---|---|
| Dataset | `issues` |
| X | `mediana_dias_ate_fechar` |
| Y | `mediana_idade_em_aberto` |
| Rótulo | `repo` |
| Tamanho | `issues` |
| Posição | linha 2, largura 3 |

Título: *Fechar rápido não é o mesmo que dar conta*.
Descrição: *Eixo X mede o que terminou; eixo Y mede o que não terminou. Canto
inferior esquerdo é saudável nas duas medidas.*

É o visual que justifica ter as duas colunas: `polars` e `hudi` ficam em
extremos opostos do eixo X e quase juntos no Y.

### 4. Estado da coleta — tabela

| | |
|---|---|
| Dataset | `cobertura` |
| Colunas | `repo`, `status`, `coletado_ate`, `issues_na_silver`, `em_aberto`, `confiavel` |
| Formatação | linha destacada onde `confiavel` é falso |
| Posição | linha 2, largura 3 |

Título: *Onde o histórico de issues já chegou inteiro*.
Descrição: *As medidas de issue só valem nas linhas confiáveis. Em coleta
crescente, o que chega primeiro é a parte velha e já fechada do backlog.*

Este widget não é diagnóstico de infraestrutura: é o rodapé metodológico dos
dois visuais de issue, e por isso fica na mesma página, não numa aba separada.

### 5. O painel — tabela

| | |
|---|---|
| Dataset | `painel` |
| Colunas | todas, na ordem da visão |
| Posição | linha 3, largura 6 |

Título: *Todos os sinais, um repositório por linha*.
Descrição: *Sem coluna de veredito, deliberadamente. Bus factor 1 com ritmo
alto é um risco diferente de bus factor 12 com ritmo caindo, e um índice único
igualaria os dois.*

## O que o painel não tem, de propósito

**Índice de saúde.** A ausência é a decisão registrada em 10.9. Combinar bus
factor, ritmo e backlog num número de 0 a 100 esconderia justamente a
informação que distingue um risco do outro.

**Filtro de período.** As janelas — 90 dias para commits, 45 por período de
comparação — são parâmetros das funções em `analises.py`, com o motivo escrito
no docstring. Expô-las como controle no painel convidaria a mudar o valor sem
o raciocínio junto, e a comparação entre duas leituras deixaria de ser válida.

**Alerta.** Um número que muda toda semana num projeto de 14 repositórios não
sustenta limiar automático. O painel é para leitura, não para vigilância.

## Por que este arquivo é uma especificação, e não um `.lvdash.json`

Um dashboard AI/BI é exportável como JSON e poderia ser versionado direto,
o que seria melhor: reimportar recriaria o painel sem trabalho manual.

Não foi feito porque o formato é do produto e eu não teria como validar o
arquivo sem aplicá-lo num workspace. Um JSON que falha na importação é pior
que uma especificação que se segue em quinze minutos, porque o erro aparece
longe da causa.

O caminho de correção é conhecido e barato: monte o painel uma vez seguindo
este arquivo, exporte o JSON pela interface, e comite o resultado ao lado
deste documento. A partir daí a especificação vira a documentação do que o
JSON contém, e o JSON vira a fonte reaplicável — que é a mesma divisão de
papéis entre `docs/PROJETO.md` e o código.
