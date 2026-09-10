"""Gráfico 1 — quantidade de respondentes únicos por ano."""

from pathlib import Path

from codigo.comum import criar_canvas, desenhar_colunas, rodape, salvar
from codigo.spark_comum import anos, obter_dados


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico01Respondentes")
    itens = [
        (str(linha["ano_pesquisa"]), int(linha["quantidade"]))
        for linha in (
            df.groupBy("ano_pesquisa")
            .count()
            .withColumnRenamed("count", "quantidade")
            .orderBy("ano_pesquisa")
            .collect()
        )
    ]
    imagem, draw = criar_canvas(
        "Respondentes únicos por ano",
        "Comparação direta das respostas válidas nas edições de 2023, 2024 e 2025.",
        altura=850,
    )
    desenhar_colunas(draw, (170, 205, 1450, 710), itens, "Quantidade de respondentes", cor=(39, 112, 180))
    rodape(draw, "A contagem usa uma resposta por combinação de ano_pesquisa + id_resposta.", altura=850)
    caminho = salvar(imagem, "01_respondentes_por_ano.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
