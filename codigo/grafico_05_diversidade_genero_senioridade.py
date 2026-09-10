"""Gráfico 5 — gênero por senioridade em cada ano."""

from pathlib import Path

from codigo.comum import PALETA, criar_canvas, desenhar_legenda, desenhar_stacked100, rodape, salvar
from codigo.spark_comum import anos, contagens_por_ano_e_grupo, obter_dados, padronizar_genero_col, padronizar_nivel_col


NIVEIS = ["Júnior", "Pleno", "Sênior", "Especialista/Staff"]
GENEROS = ["Feminino", "Masculino", "Outro", "Prefere não informar"]
CORES = {genero: PALETA[indice + 1] for indice, genero in enumerate(GENEROS)}


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico05Diversidade")
    distribuicao = contagens_por_ano_e_grupo(
        df,
        "nivel",
        "genero",
        padronizar_nivel_col,
        padronizar_genero_col,
        NIVEIS,
        GENEROS,
    )
    imagem, draw = criar_canvas(
        "Diversidade de gênero por senioridade — comparação entre os três anos",
        "Cada painel é um ano; cada barra mostra a composição de gênero dentro de um nível profissional.",
    )
    caixas = [(70, 190, 550, 700), (590, 190, 1070, 700), (1110, 190, 1590, 700)]
    for ano, caixa in zip(anos(df), caixas):
        linhas = [(nivel, distribuicao[ano][nivel]) for nivel in NIVEIS]
        desenhar_stacked100(draw, caixa, linhas, GENEROS, f"Gênero por nível — {ano}", CORES, legenda=False, rotulo_largura=130)
    desenhar_legenda(draw, (300, 765), GENEROS, CORES, 1000)
    rodape(draw, "Grupos pequenos devem ser interpretados com cautela. O percentual é calculado dentro das respostas informadas por nível.")
    caminho = salvar(imagem, "05_diversidade_genero_senioridade.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
