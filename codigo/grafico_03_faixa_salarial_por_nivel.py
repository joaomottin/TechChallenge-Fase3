"""Gráfico 3 — distribuição salarial por nível em cada ano."""

from pathlib import Path

from codigo.comum import (
    ANO_EDICAO_PARCIAL,
    AMARELO,
    AVISO_EDICAO_PARCIAL,
    AZUL,
    LARANJA,
    MAGENTA,
    ROXO,
    SLATE,
    TEAL,
    VERDE,
    criar_canvas,
    desenhar_legenda,
    desenhar_insight,
    desenhar_stacked100,
    rotulo_ano,
    rodape,
    salvar,
)
from codigo.spark_comum import anos, contagens_por_ano_e_grupo, obter_dados, padronizar_nivel_col, padronizar_salario_col


NIVEIS = ["Júnior", "Pleno", "Sênior", "Especialista/Staff"]
SALARIOS = [
    "Até R$ 2 mil",
    "R$ 2 a 4 mil",
    "R$ 4 a 6 mil",
    "R$ 6 a 8 mil",
    "R$ 8 a 12 mil",
    "R$ 12 a 16 mil",
    "R$ 16 a 20 mil",
    "Acima de R$ 20 mil",
]
CORES = {
    salario: cor
    for salario, cor in zip(SALARIOS, [AZUL, TEAL, LARANJA, ROXO, VERDE, MAGENTA, AMARELO, SLATE])
}


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico03FaixaSalarial")
    distribuicao = contagens_por_ano_e_grupo(
        df,
        "nivel",
        "faixa_salarial",
        padronizar_nivel_col,
        padronizar_salario_col,
        NIVEIS,
        SALARIOS,
    )
    anos_pesquisa = anos(df)

    def faixa_modal(ano: int, nivel: str) -> tuple[str, float]:
        valores = distribuicao.get(ano, {}).get(nivel, {})
        total = sum(valores.values())
        if not total:
            return "sem respostas", 0.0
        categoria, valor = max(valores.items(), key=lambda item: item[1])
        return categoria, valor / total

    faixa_parcial, percentual_parcial = faixa_modal(ANO_EDICAO_PARCIAL, "Júnior")
    imagem, draw = criar_canvas(
        "Faixa salarial por nível — composição dentro de cada edição",
        f"Cada barra representa 100% das respostas do nível; no recorte parcial, {faixa_parcial.lower()} é a faixa mais citada entre Júniores.",
        altura=1000,
        indice=3,
    )
    caixas = [(55, 220, 545, 690), (575, 220, 1065, 690), (1095, 220, 1545, 690)]
    for ano, caixa in zip(anos_pesquisa, caixas):
        linhas = [(nivel, distribuicao[ano][nivel]) for nivel in NIVEIS]
        desenhar_stacked100(
            draw,
            caixa,
            linhas,
            SALARIOS,
            f"Faixa salarial — {rotulo_ano(ano, detalhado=True)}",
            CORES,
            legenda=False,
            rotulo_largura=130,
            mostrar_rotulos_pequenos=True,
        )
    desenhar_legenda(draw, (75, 725), SALARIOS, CORES, 1450)
    desenhar_insight(
        draw,
        (55, 790, 1545, 875),
        "LEITURA PRINCIPAL",
        f"No recorte parcial de {rotulo_ano(ANO_EDICAO_PARCIAL)}, {percentual_parcial:.0%} dos Júniores estão em {faixa_parcial.lower()}; a composição ainda pode mudar.",
        LARANJA,
    )
    rodape(draw, f"Faixas são categorias declaradas; não foi calculada média salarial nem inferido valor entre as faixas. {AVISO_EDICAO_PARCIAL}")
    caminho = salvar(imagem, "03_faixa_salarial_por_nivel.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
