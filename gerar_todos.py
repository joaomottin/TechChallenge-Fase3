"""Executa os oito gráficos locais com 2023, 2024 e 2025.

Uso no VS Code:
    python gerar_todos.py

Para usar outro CSV, defina STATE_DATA_CSV antes de executar.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

from codigo.comum import FUNDO, INPUT_PADRAO, NAVY, OUTPUT_PADRAO, fonte
from codigo.spark_comum import carregar_dados_spark, iniciar_spark
from codigo.grafico_01_respondentes import gerar as gerar_01
from codigo.grafico_02_funcoes_e_niveis import gerar as gerar_02
from codigo.grafico_03_faixa_salarial_por_nivel import gerar as gerar_03
from codigo.grafico_04_perfil_regional import gerar as gerar_04
from codigo.grafico_05_diversidade_genero_senioridade import gerar as gerar_05
from codigo.grafico_06_tecnologias_principais import gerar as gerar_06
from codigo.grafico_07_adocao_prioridade_ia import gerar as gerar_07
from codigo.grafico_08_motivos_nao_ia_resultados_llm import gerar as gerar_08


GERADORES = [gerar_01, gerar_02, gerar_03, gerar_04, gerar_05, gerar_06, gerar_07, gerar_08]


def criar_previa(caminhos: list[Path], pasta_saida: Path) -> Path:
    largura_miniatura, altura_miniatura = 760, 470
    colunas = 2
    linhas = (len(caminhos) + colunas - 1) // colunas
    imagem = Image.new("RGB", (1600, 105 + linhas * altura_miniatura), FUNDO)
    draw = ImageDraw.Draw(imagem)
    draw.text((70, 28), "Prévia dos gráficos — três anos", font=fonte(34, True), fill=NAVY)
    for indice, caminho in enumerate(caminhos):
        miniatura = Image.open(caminho).convert("RGB")
        miniatura.thumbnail((largura_miniatura, altura_miniatura - 25))
        x = 25 + (indice % colunas) * 800
        y = 82 + (indice // colunas) * altura_miniatura
        imagem.paste(miniatura, (x, y))
    destino = pasta_saida / "00_previa_tres_anos.png"
    imagem.save(destino, format="PNG", optimize=True)
    return destino


def criar_index(caminhos: list[Path], pasta_saida: Path, quantidade: int) -> None:
    itens = []
    for caminho in caminhos:
        titulo = caminho.stem.replace("_", " ").title()
        itens.append(f'<section><h2>{titulo}</h2><img src="{caminho.name}" alt="{titulo}"></section>')
    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Gráficos Gold — três anos</title>
  <style>
    body {{ margin: 32px; background: #f8fafc; color: #183557; font-family: Segoe UI, Arial, sans-serif; }}
    h1 {{ margin-bottom: 4px; }}
    p {{ color: #607080; }}
    section {{ margin: 34px 0; }}
    h2 {{ font-size: 20px; text-transform: capitalize; }}
    img {{ max-width: 100%; height: auto; display: block; border: 1px solid #dae1e8; }}
  </style>
</head>
<body>
  <h1>Gráficos Gold — State of Data Brasil</h1>
  <p>Comparação entre 2023, 2024 e 2025. Respostas únicas usadas: {quantidade:,}.</p>
  {''.join(itens)}
</body>
</html>"""
    (pasta_saida / "index.html").write_text(html, encoding="utf-8")


def main() -> None:
    pasta_saida = OUTPUT_PADRAO
    pasta_saida.mkdir(parents=True, exist_ok=True)
    spark = iniciar_spark("GoldGraficosStateOfData")
    try:
        df = carregar_dados_spark(spark)
        caminhos = [gerar(df, pasta_saida) for gerar in GERADORES]
        previa = criar_previa(caminhos, pasta_saida)
        quantidade = df.count()
        criar_index(caminhos, pasta_saida, quantidade)
        leia_me = f"""GRÁFICOS GOLD — TRÊS ANOS

Entrada: {INPUT_PADRAO}
Saída: {pasta_saida}
Anos analisados: 2023, 2024 e 2025
Respostas únicas: {quantidade}
Motor de dados: PySpark (Spark local)
Renderização: Pillow

Cada gráfico tem um script próprio. O arquivo gerar_todos.py executa todos.
A AWS não é alterada por estes scripts locais.
"""
        (pasta_saida / "LEIA-ME.txt").write_text(leia_me, encoding="utf-8")
        print(f"Entrada: {INPUT_PADRAO}")
        print(f"Respostas únicas: {quantidade}")
        print(f"Arquivos gerados: {len(caminhos)}")
        print(f"Prévia: {previa}")
        print(f"Relatório: {pasta_saida / 'index.html'}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
