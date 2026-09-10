# Gráficos Gold — três edições com PySpark

Esta pasta contém os scripts locais da camada Gold. Eles leem o CSV exportado
da Silver e analisam 2023, 2024 e a edição 2025–2026, cuja coleta ainda está
em andamento.

## Regra de leitura dos períodos

2023 e 2024 são edições encerradas. O arquivo registra a edição em andamento
como `ano_pesquisa = 2025`, mas ela deve ser apresentada como **2025–2026
(parcial)**. Por isso:

- o gráfico de respondentes mostra apenas volume acumulado, sem ranking entre
  edições;
- os demais gráficos que poderiam ser distorcidos pelo tamanho da amostra
  exibem participação percentual dentro de cada edição;
- nenhuma leitura afirma queda, crescimento ou fechamento de 2025–2026 contra
  uma edição completa.

O processamento dos dados é feito com **PySpark**:

- leitura do CSV Silver com `spark.read.csv`;
- conversão e validação de `ano_pesquisa` e `id_resposta`;
- remoção de duplicidades com `dropDuplicates`;
- padronização semântica das categorias com `pyspark.sql.functions`;
- agregações analíticas com `groupBy`, `count`, `sum` e `Window`.

Depois que o Spark calcula as métricas, o `Pillow` desenha os PNGs. Isso
separa o processamento distribuído da apresentação visual e deixa o código
fácil de auditar.

## Estrutura da pasta

```text
graficos_gold_3_anos/
├── gerar_todos.py                  # ponto único de execução
├── requirements.txt                # dependências
├── codigo/
│   ├── spark_comum.py               # leitura, limpeza e agregações Spark
│   ├── comum.py                     # desenho visual com Pillow
│   └── grafico_01 ... grafico_08    # um script por análise
├── saidas/                          # PNGs e relatório HTML
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

```powershell
python gerar_todos.py
```

Para gerar apenas um gráfico, use o módulo correspondente a partir da raiz:

```powershell
python -m codigo.grafico_06_tecnologias_principais
```

Os PNGs e o `index.html` ficam na pasta `saidas`. Abra `saidas/index.html`
para visualizar o relatório completo. A AWS não é alterada por estes scripts
locais.

## Integrantes

| Nome | RM | E-mail |
|---|---|---|
| Felipe Macedo da Silva | RM373436 | felipemsdocs@gmail.com |
| João Pedro Mezzadri Mottin | RM372545 | joaopedromm.construtiva@outlook.com |
| Miguel Fernandes Martins de Bastos | RM373815 | miguelbastospro@gmail.com |
| Thanael Butewicz | RM373935 | zthanaelbutewicz@hotmail.com |
| Veronica de Fatima Machado Silva | RM371976 | v.machado10@hotmail.com |
