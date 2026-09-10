"""Gráfico 2 — principais funções por ano e composição dos níveis."""

from pathlib import Path

from codigo.comum import (
    AVISO_EDICAO_PARCIAL,
    AZUL,
    converter_para_percentuais,
    LARANJA,
    ROXO,
    TEAL,
    criar_canvas,
    desenhar_barras_horizontais,
    desenhar_legenda,
    desenhar_stacked100,
    rotulo_ano,
    rodape,
    salvar,
)
from codigo.spark_comum import (
    anos,
    contagens_por_ano,
    obter_dados,
    padronizar_funcao_col,
    padronizar_nivel_col,
    totais_por_ano,
)


NIVEIS = ["Júnior", "Pleno", "Sênior", "Especialista/Staff"]
CORES_NIVEIS = {nivel: cor for nivel, cor in zip(NIVEIS, [AZUL, TEAL, LARANJA, ROXO])}


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico02FuncoesENiveis")
    anos_pesquisa = anos(df)
    contagens_funcao = contagens_por_ano(df, "funcao_atuacao", padronizar_funcao_col, 5, ("Não informado",))
    totais_funcao = totais_por_ano(df, "funcao_atuacao", padronizar_funcao_col, ("Não informado",))
    por_ano = {
        ano: converter_para_percentuais(itens, totais_funcao.get(ano, 0))
        for ano, itens in contagens_funcao.items()
    }
    df_nivel = df.withColumn("nivel_amigavel", padronizar_nivel_col("nivel"))
    contagens_nivel = contagens_por_ano(df_nivel, "nivel_amigavel", limite=10)
    linhas_nivel = [
        (rotulo_ano(ano), {nivel: int(dict(contagens_nivel.get(ano, [])).get(nivel, 0)) for nivel in NIVEIS})
        for ano in anos_pesquisa
    ]
    imagem, draw = criar_canvas(
        "Funções e níveis profissionais — composição por edição",
        "As funções são exibidas como participação dentro de cada edição; a faixa de nível mostra a composição observada.",
        altura=1000,
        indice=2,
    )
    caixas = [(55, 220, 545, 585), (575, 220, 1065, 585), (1095, 220, 1545, 585)]
    for ano, caixa in zip(anos_pesquisa, caixas):
        desenhar_barras_horizontais(
            draw,
            caixa,
            por_ano[ano],
            f"Principais funções — {rotulo_ano(ano, detalhado=True)}",
            cor=AZUL,
            percentual=True,
            maximo=100,
            tamanho_rotulo=15,
        )

    desenhar_stacked100(draw, (55, 635, 1545, 825), linhas_nivel, NIVEIS, "Nível profissional por ano", CORES_NIVEIS, legenda=False, rotulo_largura=150)
    desenhar_legenda(draw, (310, 842), NIVEIS, CORES_NIVEIS, 1000)
    rodape(draw, f"Percentuais de função usam respostas de função informada. {AVISO_EDICAO_PARCIAL}")
    caminho = salvar(imagem, "02_funcoes_e_niveis.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
