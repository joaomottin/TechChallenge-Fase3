"""Gráfico 1 — volume acumulado de respondentes por edição."""

from pathlib import Path

from codigo.comum import (
    ANO_EDICAO_PARCIAL,
    AVISO_EDICAO_PARCIAL,
    AZUL,
    PERIODO_PADRAO,
    ROXO,
    TEAL,
    criar_canvas,
    desenhar_colunas,
    desenhar_insight,
    desenhar_kpi,
    numero,
    rotulo_ano,
    salvar,
)
from codigo.spark_comum import obter_dados, respondentes_por_ano


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico01Respondentes")
    valores = respondentes_por_ano(df)
    itens = [(rotulo_ano(ano), valor) for ano, valor in valores.items()]
    total = sum(valores.values())
    valor_parcial = valores.get(ANO_EDICAO_PARCIAL, 0)
    imagem, draw = criar_canvas(
        "Respondentes acumulados por edição",
        "O volume de 2025–2026 é um acumulado de coleta; a barra não representa o fechamento da edição.",
        altura=1000,
        indice=1,
        periodo=PERIODO_PADRAO,
    )
    desenhar_colunas(
        draw,
        (60, 220, 1010, 790),
        itens,
        "Volume observado até o momento",
        cor=AZUL,
        cores=[AZUL, TEAL, ROXO],
    )
    desenhar_kpi(
        draw,
        (1040, 220, 1540, 372),
        "Total da série",
        numero(total),
        "inclui a coleta parcial de 2025–2026",
        AZUL,
    )
    desenhar_kpi(
        draw,
        (1040, 390, 1540, 542),
        "Maior edição",
        numero(max(valores.values(), default=0)),
        f"maior volume em {rotulo_ano(max(valores, key=valores.get, default=0))}",
        TEAL,
    )
    desenhar_kpi(
        draw,
        (1040, 560, 1540, 712),
        "Status da edição",
        rotulo_ano(ANO_EDICAO_PARCIAL),
        "coleta ainda em andamento",
        ROXO,
    )
    desenhar_insight(
        draw,
        (60, 835, 1540, 920),
        "LEITURA PRINCIPAL",
        f"{rotulo_ano(ANO_EDICAO_PARCIAL)} tem {numero(valor_parcial)} respostas acumuladas até agora. {AVISO_EDICAO_PARCIAL}",
        ROXO,
    )
    caminho = salvar(imagem, "01_respondentes_por_ano.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
