"""Gráfico 7 — prioridade e formas de uso de IA nas três edições."""

from pathlib import Path

from codigo.comum import (
    ANO_EDICAO_PARCIAL,
    AZUL,
    AVISO_EDICAO_PARCIAL,
    converter_para_percentuais,
    LARANJA,
    MAGENTA,
    PALETA,
    ROXO,
    TEAL,
    VERDE,
    criar_canvas,
    desenhar_barras_horizontais,
    desenhar_insight,
    desenhar_legenda,
    desenhar_stacked100,
    rotulo_ano,
    rodape,
    salvar,
)
from codigo.spark_comum import (
    anos,
    contagens_padrao_por_ano,
    contagens_por_ano,
    obter_dados,
    padronizar_prioridade_col,
    respondentes_por_ano,
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
        (rotulo_ano(ano, detalhado=True), {categoria: int(dict(prioridades_por_ano.get(ano, [])).get(categoria, 0)) for categoria in PRIORIDADES})
        for ano in anos_pesquisa
    ]
    usos_contagem = contagens_padrao_por_ano(df, "ia_uso_empresa", PADROES_USO)
    totais_ano = respondentes_por_ano(df)
    usos = {
        ano: converter_para_percentuais(itens, totais_ano.get(ano, 0))
        for ano, itens in usos_contagem.items()
    }
    cores_prioridade = {
        item: cor for item, cor in zip(PRIORIDADES, [AZUL, TEAL, LARANJA, ROXO, VERDE, MAGENTA])
    }

    principal_parcial = usos.get(ANO_EDICAO_PARCIAL, [])[0][0] if usos.get(ANO_EDICAO_PARCIAL) else "o uso mais citado"

    imagem, draw = criar_canvas(
        "Adoção e prioridade de IA — leitura por edição",
        "Prioridades são composição das respostas informadas; formas de uso são participação dos respondentes de cada edição.",
        altura=1000,
        indice=7,
    )
    desenhar_stacked100(draw, (55, 220, 1545, 465), linhas_prioridade, PRIORIDADES, "Prioridade declarada para IA", cores_prioridade, legenda=False, rotulo_largura=150)
    desenhar_legenda(draw, (250, 482), PRIORIDADES, cores_prioridade, 1100)
    caixas = [(55, 535, 545, 835), (575, 535, 1065, 835), (1095, 535, 1545, 835)]
    for ano, caixa in zip(anos_pesquisa, caixas):
        desenhar_barras_horizontais(
            draw,
            caixa,
            usos[ano],
            f"Formas de uso — {rotulo_ano(ano, detalhado=True)}",
            cor=PALETA[3],
            percentual=True,
            maximo=100,
            tamanho_rotulo=14,
        )
    desenhar_insight(
        draw,
        (55, 850, 1545, 910),
        "LEITURA PRINCIPAL",
        f"No recorte parcial de {rotulo_ano(ANO_EDICAO_PARCIAL)}, {principal_parcial.lower()} aparece como a forma de uso mais citada; a leitura pode mudar até o fechamento.",
        ROXO,
    )
    rodape(draw, f"Uma pessoa pode indicar mais de uma forma de uso; os percentuais de uso usam o total de respondentes da edição. {AVISO_EDICAO_PARCIAL}")
    caminho = salvar(imagem, "07_adocao_prioridade_ia.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
