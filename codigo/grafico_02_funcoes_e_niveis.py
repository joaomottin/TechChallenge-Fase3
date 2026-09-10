"""Gráfico 2 — principais funções por ano e composição dos níveis."""

from pathlib import Path

from codigo.comum import (
    PALETA,
    criar_canvas,
    desenhar_barras_horizontais,
    desenhar_legenda,
    desenhar_stacked100,
    rodape,
    salvar,
)
from codigo.spark_comum import (
    anos,
    contagens_por_ano,
    obter_dados,
    padronizar_funcao_col,
    padronizar_nivel_col,
)


NIVEIS = ["Júnior", "Pleno", "Sênior", "Especialista/Staff"]
CORES_NIVEIS = {nivel: PALETA[indice] for indice, nivel in enumerate(NIVEIS)}


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico02FuncoesENiveis")
    anos_pesquisa = anos(df)
    por_ano = contagens_por_ano(df, "funcao_atuacao", padronizar_funcao_col, 5, ("Não informado",))
    maior_funcao = max((valor for itens in por_ano.values() for _, valor in itens), default=1)
    df_nivel = df.withColumn("nivel_amigavel", padronizar_nivel_col("nivel"))
    contagens_nivel = contagens_por_ano(df_nivel, "nivel_amigavel", limite=10)
    linhas_nivel = [
        (str(ano), {nivel: int(dict(contagens_nivel.get(ano, [])).get(nivel, 0)) for nivel in NIVEIS})
        for ano in anos_pesquisa
    ]

    imagem, draw = criar_canvas(
        "Funções e níveis profissionais — comparação entre os três anos",
        "As três colunas mostram as cinco funções mais frequentes de cada edição; abaixo, a composição dos níveis profissionais.",
    )
    caixas = [(70, 190, 550, 690), (590, 190, 1070, 690), (1110, 190, 1590, 690)]
    for ano, caixa in zip(anos_pesquisa, caixas):
        desenhar_barras_horizontais(draw, caixa, por_ano[ano], f"Principais funções — {ano}", cor=PALETA[0], maximo=maior_funcao, tamanho_rotulo=15)

    desenhar_stacked100(draw, (100, 745, 1500, 960), linhas_nivel, NIVEIS, "Nível profissional por ano", CORES_NIVEIS, legenda=False, rotulo_largura=140)
    desenhar_legenda(draw, (310, 947), NIVEIS, CORES_NIVEIS, 1000)
    rodape(draw, "Leitura: percentual de nível calculado dentro das respostas informadas em cada ano; funções mostram quantidade de respondentes.")
    caminho = salvar(imagem, "02_funcoes_e_niveis.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
