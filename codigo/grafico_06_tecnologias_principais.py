"""Gráfico 6 — principais tecnologias por tipo e por ano."""

from pathlib import Path

from codigo.comum import (
    AVISO_EDICAO_PARCIAL,
    AZUL,
    LARANJA,
    TEAL,
    converter_para_percentuais,
    criar_canvas,
    desenhar_tecnologia_painel,
    rotulo_ano,
    rodape,
    salvar,
)
from codigo.spark_comum import anos, contagens_tecnologia_por_ano, obter_dados, respondentes_por_ano


TIPOS = [
    ("Linguagens", "linguagem_corrigida"),
    ("Cloud", "cloud_corrigida"),
    ("Ferramentas de BI", "bi_corrigida"),
    ("Data Lake", "tecnologia_data_lake"),
    ("Data Warehouse", "tecnologia_data_warehouse"),
]


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico06Tecnologias")
    anos_pesquisa = anos(df)
    totais_ano = respondentes_por_ano(df)
    dados_por_ano = {}
    for ano in anos_pesquisa:
        dados_por_ano[ano] = [
            (
                tipo,
                converter_para_percentuais(
                    contagens_tecnologia_por_ano(df.filter(df["ano_pesquisa"] == ano), coluna, limite=3).get(ano, []),
                    totais_ano.get(ano, 0),
                ),
            )
            for tipo, coluna in TIPOS
        ]
    imagem, draw = criar_canvas(
        "Tecnologias mais citadas — participação por edição",
        "Cada painel mostra o Top 3 por categoria em participação dos respondentes; menções múltiplas são permitidas.",
        altura=1000,
        indice=6,
    )
    caixas = [(55, 220, 545, 820), (575, 220, 1065, 820), (1095, 220, 1545, 820)]
    cores_ano = [AZUL, TEAL, LARANJA]
    for indice, (ano, caixa) in enumerate(zip(anos_pesquisa, caixas)):
        desenhar_tecnologia_painel(
            draw,
            caixa,
            dados_por_ano[ano],
            rotulo_ano(ano, detalhado=True),
            cores_ano[indice % len(cores_ano)],
            percentual=True,
        )
    rodape(draw, f"Perguntas foram unificadas semanticamente; uma resposta pode citar mais de uma tecnologia. {AVISO_EDICAO_PARCIAL}")
    caminho = salvar(imagem, "06_tecnologias_principais.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
