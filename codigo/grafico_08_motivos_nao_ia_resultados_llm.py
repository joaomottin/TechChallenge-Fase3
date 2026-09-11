"""Gráfico 8 — motivos para não usar IA e resultados com LLMs."""

from pathlib import Path

from codigo.comum import (
    ANO_EDICAO_PARCIAL,
    AVISO_EDICAO_PARCIAL,
    LARANJA,
    VERDE,
    converter_para_percentuais,
    criar_canvas,
    desenhar_aviso,
    desenhar_barras_horizontais,
    numero,
    rotulo_ano,
    rodape,
    salvar,
)
from codigo.spark_comum import (
    anos,
    contagens_padrao_por_ano,
    contagens_por_ano,
    normalizar_col,
    obter_dados,
    padronizar_resultado_col,
    totais_por_ano,
)


PADROES_MOTIVOS = [
    ("Falta de expertise ou recursos", ("falta de expertise", "falta de recursos")),
    ("Falta de compreensão dos casos de uso", ("falta de compreensao",)),
    ("Dados ainda não estão prontos", ("dados da empresa nao estao prontos",)),
    ("ROI ainda não comprovado", ("retorno sobre investimento", "roi nao comprovado")),
    ("Segurança e privacidade", ("seguranca e privacidade",)),
    ("Baixa qualidade das respostas", ("baixa qualidade", "alucinacao")),
    ("Propriedade intelectual", ("propriedade intelectual",)),
]

COLUNA_MOTIVOS = "q3_g_motivos_para_nao_usar_ai_generativa_e_llm"
COLUNA_RESULTADOS = "q3_h_empresa_esta_conseguindo_ter_bons_resultados_com_llms"
ROTULOS_RESULTADOS = {
    "Parcialmente": "Piloto (impacto parcial)",
    "Sim": "Produção (com impacto)",
    "Não — em investigação": "Investigação / planejamento",
    "Não — não iniciado": "Não iniciado",
    "Não sabe opinar": "Não sabe opinar",
}


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico08MotivosIA")
    anos_pesquisa = anos(df)
    motivos_contagem = contagens_padrao_por_ano(
        df,
        COLUNA_MOTIVOS,
        PADROES_MOTIVOS,
        5,
    )
    resultados_contagem_bruta = contagens_por_ano(
        df,
        COLUNA_RESULTADOS,
        padronizar_resultado_col,
        5,
        ("Não informado",),
    )
    resultados_contagem = {
        ano: [(ROTULOS_RESULTADOS.get(rotulo, rotulo), quantidade) for rotulo, quantidade in itens]
        for ano, itens in resultados_contagem_bruta.items()
    }
    # Cada pergunta possui uma base válida própria. Usar o total bruto da
    # edição fazia as barras parecerem muito pequenas, pois q3_g e q3_h
    # são perguntas condicionais e têm muitos valores em branco.
    bases_motivos = totais_por_ano(
        df,
        COLUNA_MOTIVOS,
        normalizar_col,
        ("nao informado",),
    )
    bases_resultados = totais_por_ano(
        df,
        COLUNA_RESULTADOS,
        padronizar_resultado_col,
        ("Não informado",),
    )
    motivos = {
        ano: converter_para_percentuais(itens, bases_motivos.get(ano, 0))
        for ano, itens in motivos_contagem.items()
    }
    resultados = {
        ano: converter_para_percentuais(itens, bases_resultados.get(ano, 0))
        for ano, itens in resultados_contagem.items()
    }
    motivo_parcial = motivos.get(ANO_EDICAO_PARCIAL, [])[0][0] if motivos.get(ANO_EDICAO_PARCIAL) else "o motivo mais citado"

    imagem, draw = criar_canvas(
        "Motivos para não usar IA e resultados com LLMs — participação por edição",
        f"Motivos aceitam múltiplas respostas; resultados com LLMs mostram o estágio dos projetos em uma escolha única. No recorte parcial, {motivo_parcial.lower()} lidera as barreiras.",
        altura=1000,
        indice=8,
    )
    caixas_motivos = [(55, 220, 545, 615), (575, 220, 1065, 615), (1095, 220, 1545, 615)]
    for ano, caixa in zip(anos_pesquisa, caixas_motivos):
        desenhar_barras_horizontais(
            draw,
            caixa,
            motivos[ano],
            f"Motivos — {rotulo_ano(ano, detalhado=True)}",
            cor=LARANJA,
            percentual=True,
            maximo=100,
            tamanho_rotulo=14,
            subtitulo=f"entre respostas válidas · n={numero(bases_motivos.get(ano, 0))}",
        )

    caixas_resultados = [(55, 650, 545, 900), (575, 650, 1065, 900), (1095, 650, 1545, 900)]
    for ano, caixa in zip(anos_pesquisa, caixas_resultados):
        if resultados[ano]:
            desenhar_barras_horizontais(
                draw,
                caixa,
                resultados[ano],
                f"Estágio dos projetos com LLMs — {rotulo_ano(ano, detalhado=True)}",
                cor=VERDE,
                percentual=True,
                maximo=100,
                tamanho_rotulo=12,
                subtitulo=f"uma opção por pessoa · respostas válidas n={numero(bases_resultados.get(ano, 0))}",
            )
        else:
            desenhar_aviso(
                draw,
                caixa,
                f"Estágio dos projetos com LLMs — {rotulo_ano(ano, detalhado=True)}",
                "Sem respostas válidas no export desta edição. Não é uma leitura de 0% e não permite comparação histórica neste painel.",
            )
    rodape(draw, f"Motivos aceitam múltiplas respostas e não somam 100%; resultados com LLMs têm uma opção por pessoa. Percentuais usam a base válida. {AVISO_EDICAO_PARCIAL}")
    caminho = salvar(imagem, "08_motivos_nao_ia_resultados_llm.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
