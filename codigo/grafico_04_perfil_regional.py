"""Gráfico 4 — distribuição regional nas três edições."""

from pathlib import Path

from codigo.comum import (
    ANO_EDICAO_PARCIAL,
    AVISO_EDICAO_PARCIAL,
    TEAL,
    converter_para_percentuais,
    criar_canvas,
    desenhar_barras_horizontais,
    desenhar_insight,
    rotulo_ano,
    rodape,
    salvar,
)
from codigo.spark_comum import anos, contagens_por_ano, obter_dados, padronizar_regiao_col, totais_por_ano


REGIOES = ["Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte"]


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico04PerfilRegional")
    contagens = contagens_por_ano(df, "regiao", padronizar_regiao_col, limite=20)
    contagens_regiao: dict[int, list[tuple[str, int]]] = {
        ano: [(regiao, int(dict(contagens.get(ano, [])).get(regiao, 0))) for regiao in REGIOES]
        for ano in anos(df)
    }
    anos_pesquisa = anos(df)
    totais_regiao = totais_por_ano(df, "regiao", padronizar_regiao_col, ("Não informado",))
    dados = {
        ano: converter_para_percentuais(itens, totais_regiao.get(ano, 0))
        for ano, itens in contagens_regiao.items()
    }
    participacao_sudeste = dict(dados.get(ANO_EDICAO_PARCIAL, [])).get("Sudeste", 0.0)
    imagem, draw = criar_canvas(
        "Perfil regional dos respondentes — participação por edição",
        "Os percentuais usam apenas respostas com região informada; o recorte 2025–2026 permanece sujeito à coleta.",
        altura=1000,
        indice=4,
    )
    caixas = [(55, 220, 545, 700), (575, 220, 1065, 700), (1095, 220, 1545, 700)]
    for ano, caixa in zip(anos_pesquisa, caixas):
        desenhar_barras_horizontais(
            draw,
            caixa,
            dados[ano],
            f"Região — {rotulo_ano(ano, detalhado=True)}",
            cor=TEAL,
            percentual=True,
            maximo=100,
            tamanho_rotulo=15,
        )
    desenhar_insight(
        draw,
        (55, 745, 760, 875),
        "CONCENTRAÇÃO",
        f"No recorte parcial de {rotulo_ano(ANO_EDICAO_PARCIAL)}, o Sudeste representa {participacao_sudeste:.0f}% das respostas com região informada.",
        TEAL,
    )
    desenhar_insight(
        draw,
        (840, 745, 1545, 875),
        "COMO LER",
        "As barras mostram composição, não crescimento ou queda de volume entre uma edição encerrada e uma coleta em andamento.",
        TEAL,
    )
    rodape(draw, f"A composição não representa a população total de cada região brasileira. {AVISO_EDICAO_PARCIAL}")
    caminho = salvar(imagem, "04_perfil_regional.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
