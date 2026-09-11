# Scripts exportados do pipeline AWS

Esta pasta reúne cópias dos scripts e exports dos jobs encontrados na Console
AWS Glue em `us-east-1`, durante a validação final do trabalho. Os arquivos são
referências do pipeline executado na AWS; não são necessários para executar os
gráficos locais em `codigo/`.

## Organização

| Pasta | Artefatos | Papel |
|---|---|---|
| `01_bronze_ingestao/` | `job_landing_to_bronze.py` | Ingestão dos CSVs do Landing para a camada Bronze, com catálogo e dicionário das perguntas. |
| `02_silver_tratamento/` | `job_silver_corrigido.py` e `job_silver_corrigido_export.json` | Tratamento PySpark, validação do crosswalk, padronização e consolidação da Silver. |
| `03_gold_indicadores/` | `job_gold_indicadores.py`, `job_gold_indicadores_export.json` e `job_gold_indicadores_visual.py` | Geração das tabelas Gold e publicação da seleção visual no S3. |
| `04_gold_graficos/` | `job_gold_graficos.py` e `job_gold_graficos_export.json` | Leitura das tabelas Gold e publicação do relatório visual com SVG/HTML. |

Os arquivos `*_export.json` são os downloads nativos do Glue Studio. Eles
mantêm a configuração do job e o campo `script`; os arquivos `.py` de Gold
foram extraídos desse campo para facilitar leitura, revisão e versionamento.

O `job_gold_indicadores_visual.py` é o script gerado pelo job visual do Glue.
Ele lê `silver_silvercorrigido`, seleciona as colunas destinadas à Gold,
aplica a regra básica de qualidade e grava em
`gold/GoldIndicadores/`. O nome do bucket e do banco refletem o ambiente AWS
usado pelo grupo e não devem ser tratados como configuração local universal.

O `job_landing_to_bronze.py` foi preservado como script da etapa Bronze
encontrado nos artefatos baixados. Na lista de jobs exibida na Console durante
a validação, os jobs ativos visíveis foram `GoldGraficosPTBR`,
`GoldIndicadoresPTBR`, `JobSilverCorrigido` e `GoldIndicadoresVisual`.

## Dependências e segurança

Os scripts AWS dependem de AWS Glue, Spark, S3, Glue Data Catalog e permissões
IAM. Eles exigem parâmetros de execução e não devem ser executados no ambiente
local sem adaptação. Nenhuma credencial, chave de acesso, token ou segredo foi
incluído nesta pasta.

O crosswalk usado pelo tratamento fica em
`../tratamento_dados/02_crosswalk_manual.csv`, e o notebook que documenta a
consolidação fica em `../tratamento_dados/01_pipeline_tratamento_consolidacao.ipynb`.
