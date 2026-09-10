"""Gráfico 6 — principais tecnologias por tipo e por ano."""

from pathlib import Path

from codigo.comum import PALETA, criar_canvas, desenhar_tecnologia_painel, rodape, salvar
from codigo.spark_comum import anos, contagens_tecnologia_por_ano, obter_dados


TIPOS = [
    ("Linguagens", "linguagem_corrigida"),
    ("Cloud", "cloud_corrigida"),
    ("Ferramentas de BI", "bi_corrigida"),
    ("Data Lake", "tecnologia_data_lake"),
    ("Data Warehouse", "tecnologia_data_warehouse"),
]


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico06Tecnologias")
    imagem, draw = criar_canvas(
        "Tecnologias mais citadas — comparação entre os três anos",
        "Cada painel mostra o Top 3 de cada tipo de tecnologia. As barras usam uma coluna reservada para o número, evitando sobreposição.",
    )
    caixas = [(45, 190, 545, 850), (555, 190, 1055, 850), (1065, 190, 1565, 850)]
    for indice, (ano, caixa) in enumerate(zip(anos(df), caixas)):
        dados = [
            (tipo, contagens_tecnologia_por_ano(df.filter(df["ano_pesquisa"] == ano), coluna, limite=3).get(ano, []))
            for tipo, coluna in TIPOS
        ]
        desenhar_tecnologia_painel(draw, caixa, dados, f"{ano}", PALETA[indice % len(PALETA)])
    rodape(draw, "As perguntas de tecnologia mudaram de posição nas edições antigas; as colunas foram unificadas semanticamente antes da comparação. Uma resposta pode citar mais de uma tecnologia.")
    caminho = salvar(imagem, "06_tecnologias_principais.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
