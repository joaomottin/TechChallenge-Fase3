"""Executa os oito gráficos locais com 2023, 2024 e 2025–2026 parcial.

Uso no VS Code:
    python gerar_todos.py

Para usar outro CSV, defina STATE_DATA_CSV antes de executar.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

from codigo.comum import AVISO_EDICAO_PARCIAL, FUNDO, INPUT_PADRAO, NAVY, OUTPUT_PADRAO, PERIODO_PADRAO, fonte, numero
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
    draw.text((70, 28), "Prévia dos gráficos — três edições", font=fonte(34, True), fill=NAVY)
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
    titulos = {
        "01_respondentes_por_ano": "Respondentes acumulados por edição",
        "02_funcoes_e_niveis": "Funções e níveis profissionais",
        "03_faixa_salarial_por_nivel": "Faixa salarial por nível",
        "04_perfil_regional": "Perfil regional dos respondentes",
        "05_diversidade_genero_senioridade": "Diversidade de gênero por senioridade",
        "06_tecnologias_principais": "Tecnologias mais citadas",
        "07_adocao_prioridade_ia": "Adoção e prioridade de IA",
        "08_motivos_nao_ia_resultados_llm": "Motivos para não usar IA e resultados com LLMs",
    }
    itens = []
    for caminho in caminhos:
        titulo = titulos.get(caminho.stem, caminho.stem.replace("_", " ").title())
        itens.append(f'<article><div class="section-head"><span class="eyebrow">CAMADA GOLD</span><h2>{titulo}</h2></div><img src="{caminho.name}" alt="{titulo}"></article>')
    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="Dashboards Gold com 2023 e 2024 encerrados e 2025–2026 em coleta parcial.">
  <title>Dashboards Gold — três edições</title>
  <style>
    :root {{ color-scheme: light; font-family: "Segoe UI", Arial, sans-serif; background: #f7f9fc; color: #172b4d; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; padding: 48px 32px 72px; background: #f7f9fc; }}
    main {{ max-width: 1520px; margin: 0 auto; }}
    .hero {{ padding: 30px 34px 26px; background: #fff; border: 1px solid #e5eaf2; border-radius: 26px; box-shadow: 0 12px 28px rgba(32, 63, 102, .06); }}
    .eyebrow {{ color: #2684ff; font-size: 12px; font-weight: 700; letter-spacing: .08em; }}
    h1 {{ margin: 10px 0 8px; font-size: clamp(28px, 4vw, 44px); line-height: 1.05; letter-spacing: -.03em; }}
    p {{ margin: 0; color: #667085; font-size: 16px; line-height: 1.5; }}
    .notice {{ margin-top: 12px; color: #8a5a00; font-size: 14px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(480px, 1fr)); gap: 28px; margin-top: 34px; }}
    article {{ padding: 18px; background: #fff; border: 1px solid #e5eaf2; border-radius: 22px; box-shadow: 0 10px 24px rgba(32, 63, 102, .05); }}
    .section-head {{ padding: 2px 6px 15px; }}
    h2 {{ margin: 7px 0 0; font-size: 18px; line-height: 1.2; }}
    img {{ width: 100%; height: auto; display: block; border-radius: 14px; border: 1px solid #e5eaf2; }}
    @media (max-width: 620px) {{ body {{ padding: 20px 14px 40px; }} .grid {{ grid-template-columns: 1fr; gap: 18px; }} article {{ padding: 10px; }} }}
  </style>
</head>
<body>
  <main>
    <header class="hero">
      <div class="eyebrow">STATE OF DATA BRASIL  /  CAMADA GOLD</div>
      <h1>Dashboards de dados — três edições em perspectiva</h1>
      <p>2023 e 2024 encerrados · 2025–2026 em coleta parcial · respostas únicas usadas: {numero(quantidade)}.</p>
      <p class="notice">{AVISO_EDICAO_PARCIAL}</p>
    </header>
    <div class="grid">{''.join(itens)}</div>
  </main>
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
        leia_me = f"""GRÁFICOS GOLD — TRÊS EDIÇÕES

Entrada: {INPUT_PADRAO}
Saída: {pasta_saida}
Período: {PERIODO_PADRAO}
Respostas únicas: {quantidade}
Motor de dados: PySpark (Spark local)
Renderização: Pillow

Cada gráfico tem um script próprio. O arquivo gerar_todos.py executa todos.
As comparações entre edições usam percentuais sempre que o volume bruto seria afetado pela coleta parcial.
{AVISO_EDICAO_PARCIAL}
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
