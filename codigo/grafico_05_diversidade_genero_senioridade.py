"""Gráfico 5 — gênero por senioridade em cada ano."""

from pathlib import Path

from codigo.comum import (
    ANO_EDICAO_PARCIAL,
    AVISO_EDICAO_PARCIAL,
    LARANJA,
    ROXO,
    SLATE,
    TEAL,
    criar_canvas,
    desenhar_insight,
    desenhar_legenda,
    desenhar_stacked100,
    rotulo_ano,
    rodape,
    salvar,
)
from codigo.spark_comum import anos, contagens_por_ano_e_grupo, obter_dados, padronizar_genero_col, padronizar_nivel_col


NIVEIS = ["Júnior", "Pleno", "Sênior", "Especialista/Staff"]
GENEROS = ["Feminino", "Masculino", "Outro", "Prefere não informar"]
CORES = {"Feminino": TEAL, "Masculino": LARANJA, "Outro": ROXO, "Prefere não informar": SLATE}


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
    anos_pesquisa = anos(df)

    def participacao(ano: int, genero: str) -> float:
        total = 0
        valor = 0
        for valores in distribuicao.get(ano, {}).values():
            total += sum(valores.values())
            valor += valores.get(genero, 0)
        return valor / total if total else 0

    feminina_parcial = participacao(ANO_EDICAO_PARCIAL, "Feminino")
    imagem, draw = criar_canvas(
        "Diversidade de gênero por senioridade — composição por edição",
        "As barras mostram a composição de gênero dentro de cada nível; a leitura é proporcional às respostas observadas.",
        altura=1000,
        indice=5,
    )
    caixas = [(55, 220, 545, 700), (575, 220, 1065, 700), (1095, 220, 1545, 700)]
    for ano, caixa in zip(anos_pesquisa, caixas):
        linhas = [(nivel, distribuicao[ano][nivel]) for nivel in NIVEIS]
        desenhar_stacked100(draw, caixa, linhas, GENEROS, f"Gênero por nível — {rotulo_ano(ano, detalhado=True)}", CORES, legenda=False, rotulo_largura=130)
    desenhar_legenda(draw, (300, 735), GENEROS, CORES, 1000)
    desenhar_insight(
        draw,
        (55, 790, 1545, 875),
        "LEITURA PRINCIPAL",
        f"No recorte parcial de {rotulo_ano(ANO_EDICAO_PARCIAL)}, a participação feminina foi de {feminina_parcial:.0%} no conjunto dos níveis informados; isso ainda pode mudar.",
        TEAL,
    )
    rodape(draw, f"Grupos pequenos devem ser interpretados com cautela; percentuais usam apenas respostas de gênero e nível informados. {AVISO_EDICAO_PARCIAL}")
    caminho = salvar(imagem, "05_diversidade_genero_senioridade.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
