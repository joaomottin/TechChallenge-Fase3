# Mapa da entrega e auditoria

> **Distinção importante:** a execução principal do pipeline foi realizada na
> AWS. Esta pasta contém as evidências dessa execução e uma reprodução local
> dos indicadores para execução no VS Code.

## O que foi feito na AWS

Na AWS, o projeto utiliza Amazon S3 para as camadas Bronze, Silver e Gold,
AWS Glue para o processamento e Athena/Glue Catalog para consultas e
validações. As capturas em `../AWS/` registram o bucket, os prefixos, os jobs
Glue e uma consulta Athena concluída.

Os scripts/configurações originais dos jobs Glue não estão nesta pasta. As
imagens são evidências do ambiente AWS; não são lidas pelo código local.

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
VS Code: export CSV → PySpark local → 8 análises → Pillow → PNG/HTML
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
- `codigo/grafico_01` a `codigo/grafico_08`: análises independentes;
- `gerar_todos.py`: execução consolidada;
- `requirements.txt`: bibliotecas necessárias;
- `AWS/`: evidências estáticas da execução na AWS;
- `saidas/`: evidências visuais geradas;
- `README.md`: instruções de execução.

## Relação entre AWS e execução local

O arquivo `dados/state_of_data_gold_export.csv` é um snapshot autorizado, em
nível de respondente, da base corrigida/preparada na AWS. Ele é a entrada da
reprodução local; não é um arquivo Bronze, não é uma das tabelas Gold
agregadas consultadas no Athena e não é uma consulta ao vivo.

Os caminhos AWS reais ficam nas configurações dos jobs. O formato genérico é:

```python
bronze = "s3://<bucket-do-projeto>/bronze/"
silver = "s3://<bucket-do-projeto>/silver/"
gold_produto = "s3://<bucket-do-projeto>/gold/<produto>/"
```

Na execução local, `gerar_todos.py` lê sempre
`dados/state_of_data_gold_export.csv`. A função `carregar_dados_spark` aceita
uma URI `s3://` para eventual reutilização em um ambiente Glue, mas o comando
documentado nesta pasta é local e não substitui os jobs originais.

Esta reprodução não executa os jobs Glue, não consulta Athena, não grava no
S3 e não altera a infraestrutura AWS. Credenciais, chaves, tokens e
permissões IAM não fazem parte do repositório.
