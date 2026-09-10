"""Gráfico 4 — distribuição regional nos três anos."""

from pathlib import Path

from codigo.comum import PALETA, criar_canvas, desenhar_barras_horizontais, rodape, salvar
from codigo.spark_comum import anos, contagens_por_ano, obter_dados, padronizar_regiao_col


REGIOES = ["Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte"]


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico04PerfilRegional")
    contagens = contagens_por_ano(df, "regiao", padronizar_regiao_col, limite=20)
    dados: dict[int, list[tuple[str, int]]] = {
        ano: [(regiao, int(dict(contagens.get(ano, [])).get(regiao, 0))) for regiao in REGIOES]
        for ano in anos(df)
    }
    maior = max((valor for itens in dados.values() for _, valor in itens), default=1)
    imagem, draw = criar_canvas(
        "Perfil regional dos respondentes — comparação entre os três anos",
        "Os três painéis usam a mesma escala para mostrar como a distribuição regional muda entre 2023, 2024 e 2025.",
    )
    caixas = [(70, 205, 550, 735), (590, 205, 1070, 735), (1110, 205, 1590, 735)]
    for ano, caixa in zip(anos(df), caixas):
        desenhar_barras_horizontais(draw, caixa, dados[ano], f"Região — {ano}", cor=PALETA[1], maximo=maior, tamanho_rotulo=15)
    rodape(draw, "A contagem considera as respostas com região informada; não representa a população total de cada região brasileira.")
    caminho = salvar(imagem, "04_perfil_regional.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
