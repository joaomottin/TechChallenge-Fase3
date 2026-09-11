# Artefatos do tratamento e da consolidação

Esta pasta preserva os artefatos usados pelo grupo antes da geração dos
indicadores Gold. Eles complementam os scripts exportados do AWS Glue em
`../aws_pipeline/`.

| Arquivo | Função |
|---|---|
| `01_pipeline_tratamento_consolidacao.ipynb` | Notebook PySpark/Colab que faz o profiling, valida o crosswalk, renomeia colunas, padroniza tipos, remove duplicidades exatas, une 2023/2024/2025–2026 e exporta a Silver consolidada. |
| `02_crosswalk_manual.csv` | Mapeamento manual de 457 linhas entre os cabeçalhos das três edições e os nomes canônicos. |

## Ordem do tratamento

1. Disponibilizar no diretório de entrada do notebook os três CSVs da pesquisa
   e o `02_crosswalk_manual.csv`.
2. Executar o notebook para fazer o profiling e validar a cobertura das colunas.
3. Aplicar o crosswalk e as regras de tipo/limpeza em PySpark.
4. Consolidar as edições por `UNION`, preservando `ano_pesquisa` e
   `arquivo_origem`.
5. Exportar a base consolidada, o dicionário de dados e os controles de
   qualidade.

O notebook foi escrito para o Google Colab, portanto usa caminhos como
`/content/data` e `/content/output`. A execução local documentada na raiz do
repositório usa o snapshot autorizado em `../dados/` e os módulos em
`../codigo/`; ela não depende do notebook para gerar os PNGs.

O crosswalk é uma decisão manual de equivalência. Ele não deve ser substituído
por similaridade automática sem revisar as perguntas que mudam de significado
entre as edições.
