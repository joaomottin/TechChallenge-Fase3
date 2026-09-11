# Mapa da entrega e auditoria

> **Distinção importante:** a execução principal do pipeline foi realizada na
> AWS. Esta pasta contém as evidências dessa execução e uma reprodução local
> dos indicadores para execução no VS Code.

## O que foi feito na AWS

Na AWS, o projeto utiliza Amazon S3 para as camadas Bronze, Silver e Gold,
AWS Glue para o processamento e Athena/Glue Catalog para consultas e
validações. As capturas em `../AWS/` registram o bucket, os prefixos, os jobs
Glue e uma consulta Athena concluída.

As cópias dos scripts e exports dos jobs Glue ficam em `../aws_pipeline/`,
organizadas por camada. O notebook e o crosswalk usados no tratamento original
ficam em `../tratamento_dados/`. As imagens continuam sendo evidências do
ambiente AWS; não são lidas pelo código local.

## Objetivo da reprodução local

Gerar indicadores Gold para as edições 2023, 2024 e 2025–2026 da pesquisa
State of Data Brasil, usando PySpark para o processamento e Pillow para a
apresentação visual. A edição 2025–2026 permanece parcial e não pode ser
comparada por volume bruto com as edições encerradas.

Os indicadores comparativos usam percentuais dentro de cada edição sempre
que a contagem absoluta seria afetada pelo fechamento pendente. O volume de
respondentes é apresentado como acumulado observado, sem classificar a
edição parcial como maior ou menor.

## Fluxo da entrega

```text
AWS: S3 Bronze → Glue → S3 Silver → Glue → S3 Gold → Athena/validação
                                                │
                                                └─ export da base de respondentes preparada
                                                       │
VS Code: export CSV → PySpark local → 9 análises → Pillow → PNG/HTML
```

Na reprodução local:

1. `gerar_todos.py` inicia uma sessão Spark local e carrega o export CSV da
   base de respondentes preparada na AWS.
2. `codigo/spark_comum.py` valida as colunas-chave, remove duplicidades por
   `ano_pesquisa + id_resposta`, corrige os nomes semânticos de tecnologia e
   disponibiliza as funções de agregação.
3. Cada `codigo/grafico_*.py` executa uma análise usando `groupBy`, `count`,
   `sum` e `Window`.
4. `codigo/comum.py` transforma as métricas agregadas em imagens PNG legíveis.
5. `saidas/index.html` reúne todos os gráficos em um relatório navegável.

## Arquivos para apresentar

- `codigo/spark_comum.py`: processamento PySpark compartilhado;
- `codigo/grafico_01` a `codigo/grafico_09`: análises independentes;
- `gerar_todos.py`: execução consolidada;
- `requirements.txt`: bibliotecas necessárias;
- `AWS/`: evidências estáticas da execução na AWS;
- `aws_pipeline/`: scripts e exports dos jobs Glue por etapa;
- `tratamento_dados/`: notebook PySpark e crosswalk manual usados na
  preparação da base;
- `saidas/`: evidências visuais geradas;
- `README.md`: instruções de execução.

## Relação entre AWS e execução local

O arquivo `dados/state_of_data_gold_export.csv` é um snapshot autorizado, em
nível de respondente, da base corrigida/preparada na AWS. Ele é a entrada da
reprodução local; não é um arquivo Bronze, não é uma das tabelas Gold
agregadas consultadas no Athena e não é uma consulta ao vivo.

O tratamento que antecede esse snapshot está documentado em
`tratamento_dados/01_pipeline_tratamento_consolidacao.ipynb` e usa o
`tratamento_dados/02_crosswalk_manual.csv` para alinhar manualmente os
cabeçalhos equivalentes das três edições. O job correspondente preservado em
`aws_pipeline/02_silver_tratamento/job_silver_corrigido.py` é a versão executada
no Glue; o notebook é a documentação reproduzível do raciocínio e das
validações.

Os caminhos AWS reais ficam nas configurações dos jobs. O formato genérico é:

```python
bronze = "s3://<bucket-do-projeto>/bronze/"
silver = "s3://<bucket-do-projeto>/silver/"
gold_produto = "s3://<bucket-do-projeto>/gold/<produto>/"
```

Na execução local, `gerar_todos.py` lê sempre
`dados/state_of_data_gold_export.csv`. A função `carregar_dados_spark` aceita
uma URI `s3://` para eventual reutilização em um ambiente Glue, mas o comando
documentado nesta pasta é local e não substitui os jobs originais. Os exports
JSON em `aws_pipeline/` mantêm também a configuração dos jobs e o campo
`script` entregue pelo Glue Studio.

Esta reprodução não executa os jobs Glue, não consulta Athena, não grava no
S3 e não altera a infraestrutura AWS. Credenciais, chaves, tokens e
permissões IAM não fazem parte do repositório.
