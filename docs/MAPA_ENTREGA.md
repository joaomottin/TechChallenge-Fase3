# Mapa da entrega e auditoria

## Objetivo

Gerar indicadores Gold comparando as edições 2023, 2024 e 2025 da pesquisa
State of Data Brasil, usando PySpark para o processamento e Pillow para a
apresentação visual.

## Fluxo do código

1. `gerar_todos.py` inicia uma sessão Spark local e carrega a Silver CSV.
2. `codigo/spark_comum.py` valida as colunas-chave, remove duplicidades por
   `ano_pesquisa + id_resposta`, corrige os nomes semânticos de tecnologia e
   disponibiliza as funções de agregação.
3. Cada `codigo/grafico_*.py` executa a análise de uma pergunta ou conjunto de
   perguntas usando `groupBy`, `count`, `sum` e `Window`.
4. `codigo/comum.py` transforma as métricas agregadas em imagens PNG legíveis.
5. `saidas/index.html` reúne todos os gráficos em um relatório navegável.

## Arquivos para apresentar

- `codigo/spark_comum.py`: processamento PySpark compartilhado;
- `codigo/grafico_01` a `codigo/grafico_08`: análises independentes;
- `gerar_todos.py`: execução consolidada;
- `requirements.txt`: bibliotecas necessárias;
- `saidas/`: evidências visuais geradas;
- `README.md`: instruções de execução.

## Observação sobre AWS

Esta pasta é a versão local e reproduzível dos indicadores Gold. Ela não
altera o bucket nem os jobs da AWS. Os scripts AWS Glue, Silver e Gold ficam
entregues separadamente no projeto principal.
