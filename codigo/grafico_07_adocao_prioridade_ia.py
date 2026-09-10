"""Gráfico 7 — prioridade e formas de uso de IA nos três anos."""

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
    contagens_padrao_por_ano,
    contagens_por_ano,
    obter_dados,
    padronizar_prioridade_col,
)


PRIORIDADES = [
    "Principal prioridade",
    "Prioridade para os próximos anos",
    "Iniciativa, sem prioridade",
    "Não é prioridade",
    "Não sabe opinar",
    "Outra resposta",
]
CORES_PRIORIDADE = {item: PALETA[indice % len(PALETA)] for indice, item in enumerate(PRIORIDADES)}
PADROES_USO = [
    ("Colaboradores usando IA", ("colaboradores utilizando", "colaboradores usando")),
    ("Desenvolvimento", ("desenvolvimento utilizando", "copilots")),
    ("Processos internos", ("processos internos", "produtos internos")),
    ("Produtos para clientes", ("produtos externos", "clientes finais")),
    ("Direcionamento centralizado", ("direcionamento centralizado",)),
]


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico07AdocaoIA")
    anos_pesquisa = anos(df)
    prioridades_por_ano = contagens_por_ano(df, "idade_q3_e", padronizar_prioridade_col, limite=20)
    linhas_prioridade = [
        (str(ano), {categoria: int(dict(prioridades_por_ano.get(ano, [])).get(categoria, 0)) for categoria in PRIORIDADES})
        for ano in anos_pesquisa
    ]
    usos = contagens_padrao_por_ano(df, "ia_uso_empresa", PADROES_USO)
    maior_uso = max((valor for itens in usos.values() for _, valor in itens), default=1)

    imagem, draw = criar_canvas(
        "Adoção e prioridade de IA — comparação entre os três anos",
        "No topo, a prioridade declarada por edição; abaixo, as principais formas de uso identificadas em cada ano.",
        altura=1200,
    )
    desenhar_stacked100(draw, (90, 190, 1510, 505), linhas_prioridade, PRIORIDADES, "Prioridade declarada para IA", CORES_PRIORIDADE, legenda=False, rotulo_largura=130)
    desenhar_legenda(draw, (250, 475), PRIORIDADES, CORES_PRIORIDADE, 1100)
    caixas = [(55, 600, 535, 1110), (555, 600, 1035, 1110), (1055, 600, 1535, 1110)]
    for ano, caixa in zip(anos_pesquisa, caixas):
        desenhar_barras_horizontais(draw, caixa, usos[ano], f"Formas de uso — {ano}", cor=PALETA[3], maximo=maior_uso, tamanho_rotulo=14)
    rodape(draw, "Uma pessoa pode indicar mais de uma forma de uso. Percentuais de prioridade são calculados dentro das respostas informadas em cada ano.", altura=1200)
    caminho = salvar(imagem, "07_adocao_prioridade_ia.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
