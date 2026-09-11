# Indicadores Gold — execução na AWS e reprodução local

Esta pasta reúne duas etapas complementares do projeto:

1. **Execução principal na AWS:** os dados foram organizados no Amazon S3 em
   camadas Bronze, Silver e Gold, processados com AWS Glue e consultados e
   validados com Athena.
2. **Reprodução local:** a base de respondentes corrigida/preparada na AWS foi
   exportada para CSV e colocada em `dados/state_of_data_gold_export.csv`. O
   código desta pasta usa esse retrato para recalcular os indicadores Gold no
   VS Code, com PySpark e Pillow, sem credenciais AWS e sem alterar a
   infraestrutura.

As imagens da pasta `AWS/` são as evidências estáticas da primeira etapa. O
código local e as imagens em `saidas/` pertencem à segunda etapa. Portanto, o
código desta pasta não deve ser interpretado como os scripts originais dos
jobs Glue: ele reproduz localmente as análises Gold a partir do export da base
preparada na AWS.

## Arquitetura visual

O diagrama abaixo resume a execução principal na AWS: ingestão no S3,
processamento das camadas Bronze e Silver com Glue, geração dos produtos Gold,
consulta/validação no Athena e entrega dos artefatos analíticos.

<p align="center">
  <img src="AWS/arquitetura-aws.png" alt="Arquitetura AWS do projeto State of Data Brasil" width="100%">
</p>

O PNG foi incluído no repositório para que o diagrama seja exibido diretamente
no GitHub e no VS Code.

## Fluxo completo

```text
EXECUÇÃO PRINCIPAL — AWS
arquivos das edições
        │
        ▼
S3 / Bronze ── AWS Glue ──> S3 / Silver ── AWS Glue ──> S3 / Gold
                              │                         (produtos Gold)
                              └─ export da base preparada
                                                               │
                                                               ├─ Athena / Glue Catalog
                                                               └─ validação dos produtos Gold
                                                                      │
                                                                      ▼
REPRODUÇÃO LOCAL — VS CODE
export CSV ── PySpark local ── 8 análises ── Pillow ── PNGs e HTML
```

O Athena é usado para consultar e validar as tabelas catalogadas. A execução
local não consulta Athena nem lê o bucket em tempo real; ela usa o CSV salvo
em `dados/`.

## AWS: evidências da execução original

As imagens em `AWS/` documentam os recursos que foram criados e usados na
AWS:

| Arquivo | Evidência |
|---|---|
| `S3-Bucket.png` | bucket e os prefixos principais `bronze/`, `silver/` e `gold/` |
| `S3-Bronze.png` | três arquivos de entrada das edições da pesquisa |
| `S3-Silver.png` | produtos da camada Silver, controle de qualidade e dicionário |
| `S3-Gold.png` | produtos analíticos Gold e a pasta de gráficos |
| `Glue.png` | jobs Glue de Silver, indicadores e gráficos |
| `Athena.png` | catálogo de tabelas e consulta de validação concluída |
| `arquitetura-aws.png` | visão geral do fluxo AWS e da entrega dos artefatos |

Na captura do Athena, a tabela de controle registra `14002` respostas únicas,
valor reproduzido pelo processamento local. As capturas mostram o estado dos
recursos no momento da execução; elas não substituem os scripts/configurações
dos jobs Glue nem são utilizadas como entrada pelo código local.

## Origem dos dados locais

O arquivo de entrada padrão é:

```text
dados/state_of_data_gold_export.csv
```

Ele é um **snapshot/export em nível de respondente da base corrigida/preparada
na AWS**, não uma consulta ao vivo. Ele não é uma das tabelas Gold agregadas
consultadas no Athena; é a base usada pela reprodução local para recalcular os
indicadores Gold. O Spark local valida, deduplica e normaliza essa base antes
das agregações. Se houver um novo export autorizado, ele deve substituir esse
arquivo para atualizar as saídas.

Os caminhos AWS reais ficam nas configurações dos jobs e no ambiente AWS. Os
exemplos abaixo são apenas o formato esperado:

```python
bronze = "s3://<bucket-do-projeto>/bronze/"
silver = "s3://<bucket-do-projeto>/silver/"
gold_produto = "s3://<bucket-do-projeto>/gold/<produto>/"
```

Na execução local, `gerar_todos.py` usa o arquivo em `dados/`. A função
`carregar_dados_spark` também aceita uma URI `s3://` quando for reutilizada em
um ambiente Glue, mas essa não é a execução documentada pelo comando local.

Credenciais, chaves, tokens e permissões IAM não fazem parte do repositório.
As imagens podem mostrar nomes de recursos para comprovar a execução, mas não
concedem acesso ao ambiente AWS.

## Regra de leitura dos períodos

2023 e 2024 são edições encerradas. O campo `ano_pesquisa` usa o primeiro ano
da edição: `2023` corresponde a 2023–2024, `2024` a 2024–2025 e `2025` à
edição em andamento, que deve ser apresentada como **2025–2026 (parcial)**.
Por isso:

- o gráfico de respondentes mostra apenas volume acumulado, sem ranking entre
  edições;
- os demais gráficos que poderiam ser distorcidos pelo tamanho da amostra
  exibem participação percentual dentro de cada edição;
- nenhuma leitura afirma queda, crescimento ou fechamento de 2025–2026 contra
  uma edição completa.

O processamento dos dados é feito com **PySpark**:

- leitura do export CSV da base preparada na AWS com `spark.read.csv`;
- conversão e validação de `ano_pesquisa` e `id_resposta`;
- remoção de duplicidades com `dropDuplicates`;
- padronização semântica das categorias com `pyspark.sql.functions`;
- agregações analíticas com `groupBy`, `count`, `sum` e `Window`.

Depois que o Spark calcula as métricas, o `Pillow` desenha os PNGs. Isso
separa o processamento distribuído da apresentação visual e deixa o código
fácil de auditar. Os PNGs locais são uma reprodução dos indicadores; não são
os mesmos arquivos de artefato que ficam na pasta `Graficos/` da AWS.

## Estrutura da pasta

```text
graficos_gold_3_anos/
├── AWS/                            # evidências estáticas da execução AWS
│   ├── S3-Bucket.png
│   ├── S3-Bronze.png
│   ├── S3-Silver.png
│   ├── S3-Gold.png
│   ├── Glue.png
│   ├── Athena.png
│   └── arquitetura-aws.png          # diagrama exibido neste README
├── gerar_todos.py                  # ponto único de execução
├── requirements.txt                # dependências
├── dados/
│   ├── state_of_data_gold_export.csv
│   └── README.md
├── codigo/
│   ├── spark_comum.py               # leitura, limpeza e agregações Spark
│   ├── comum.py                     # desenho visual com Pillow
│   └── grafico_01 ... grafico_08    # um script por análise
├── saidas/                          # PNGs, HTML e relatório local
└── docs/
    └── MAPA_ENTREGA.md              # guia para auditoria
```

## Bibliotecas

Instale as dependências no ambiente Python do VS Code:

```powershell
python -m pip install -r requirements.txt
```

As versões mínimas estão no arquivo `requirements.txt`.

## Como executar no VS Code

Abra esta pasta no VS Code e execute no terminal:

Antes, salve o export autorizado em `dados/state_of_data_gold_export.csv`.

```powershell
python gerar_todos.py
```

Para gerar apenas um gráfico, use o módulo correspondente a partir da raiz:

```powershell
python -m codigo.grafico_06_tecnologias_principais
```

Os PNGs e o `index.html` ficam na pasta `saidas`. Abra `saidas/index.html`
para visualizar o relatório completo. A AWS não é alterada por estes scripts
locais; eles apenas processam o export disponibilizado localmente.

## Integrantes

| Nome | RM | E-mail |
|---|---|---|
| Felipe Macedo da Silva | RM373436 | felipemsdocs@gmail.com |
| João Pedro Mezzadri Mottin | RM372545 | joaopedromm.construtiva@outlook.com |
| Miguel Fernandes Martins de Bastos | RM373815 | miguelbastospro@gmail.com |
| Thanael Butewicz | RM373935 | zthanaelbutewicz@hotmail.com |
| Veronica de Fatima Machado Silva | RM371976 | v.machado10@hotmail.com |
