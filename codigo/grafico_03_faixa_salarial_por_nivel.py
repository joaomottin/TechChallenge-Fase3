"""Gráfico 3 — distribuição salarial por nível em cada ano."""

from pathlib import Path

from codigo.comum import (
    PALETA,
    criar_canvas,
    desenhar_legenda,
    desenhar_stacked100,
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
CORES = {salario: PALETA[indice % len(PALETA)] for indice, salario in enumerate(SALARIOS)}


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
    imagem, draw = criar_canvas(
        "Faixa salarial por nível — comparação entre os três anos",
        "Cada barra representa 100% das respostas de um nível naquele ano; assim a comparação não mistura tamanhos de amostra.",
    )
    caixas = [(70, 190, 550, 700), (590, 190, 1070, 700), (1110, 190, 1590, 700)]
    for ano, caixa in zip(anos(df), caixas):
        linhas = [(nivel, distribuicao[ano][nivel]) for nivel in NIVEIS]
        desenhar_stacked100(draw, caixa, linhas, SALARIOS, f"Faixa salarial — {ano}", CORES, legenda=False, rotulo_largura=130)
    desenhar_legenda(draw, (90, 760), SALARIOS, CORES, 1400)
    rodape(draw, "As faixas são categorias declaradas na pesquisa. Não foi calculada média salarial nem inferido valor entre as faixas.")
    caminho = salvar(imagem, "03_faixa_salarial_por_nivel.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
