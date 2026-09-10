"""Gráfico 8 — motivos para não usar IA e resultados com LLMs."""

from pathlib import Path

from codigo.comum import (
    PALETA,
    criar_canvas,
    desenhar_aviso,
    desenhar_barras_horizontais,
    rodape,
    salvar,
)
from codigo.spark_comum import (
    anos,
    contagens_padrao_por_ano,
    contagens_por_ano,
    obter_dados,
    padronizar_resultado_col,
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


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico08MotivosIA")
    anos_pesquisa = anos(df)
    motivos = contagens_padrao_por_ano(
        df,
        "q3_g_motivos_para_nao_usar_ai_generativa_e_llm",
        PADROES_MOTIVOS,
        5,
    )
    resultados = contagens_por_ano(
        df,
        "q3_h_empresa_esta_conseguindo_ter_bons_resultados_com_llms",
        padronizar_resultado_col,
        5,
        ("Não informado",),
    )
    maior_motivo = max((valor for itens in motivos.values() for _, valor in itens), default=1)

    imagem, draw = criar_canvas(
        "Motivos para não usar IA e resultados com LLMs — três anos",
        "Os motivos são comparados nas três edições. A parte de resultados deixa explícito quando a pergunta não existia ou não tinha respostas naquele ano.",
        altura=1100,
    )
    caixas_motivos = [(55, 190, 535, 695), (555, 190, 1035, 695), (1055, 190, 1535, 695)]
    for ano, caixa in zip(anos_pesquisa, caixas_motivos):
        desenhar_barras_horizontais(draw, caixa, motivos[ano], f"Motivos — {ano}", cor=PALETA[2], maximo=maior_motivo, tamanho_rotulo=14)

    caixas_resultados = [(55, 770, 535, 1005), (555, 770, 1035, 1005), (1055, 770, 1535, 1005)]
    for ano, caixa in zip(anos_pesquisa, caixas_resultados):
        if resultados[ano]:
            desenhar_barras_horizontais(draw, caixa, resultados[ano], f"Resultados com LLMs — {ano}", cor=PALETA[4], tamanho_rotulo=14)
        else:
            desenhar_aviso(draw, caixa, f"Resultados com LLMs — {ano}", "Pergunta não disponível ou sem respostas registradas nesta edição.")
    rodape(draw, "Uma pessoa pode marcar mais de um motivo; por isso os motivos não somam 100%. A ausência de resultado em um ano é informação do próprio questionário.", altura=1100)
    caminho = salvar(imagem, "08_motivos_nao_ia_resultados_llm.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
