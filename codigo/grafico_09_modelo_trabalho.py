"""Gráfico 9 — modelo de trabalho declarado ou pretendido por edição."""

from pathlib import Path

from codigo.comum import (
    ANO_EDICAO_PARCIAL,
    AVISO_EDICAO_PARCIAL,
    AZUL,
    LARANJA,
    ROXO,
    TEAL,
    converter_para_percentuais,
    criar_canvas,
    desenhar_aviso,
    desenhar_barras_horizontais,
    desenhar_insight,
    desenhar_legenda,
    desenhar_stacked100,
    numero,
    rotulo_ano,
    rodape,
    salvar,
)
from codigo.spark_comum import (
    anos,
    contagens_por_ano,
    obter_dados,
    padronizar_modelo_trabalho_col,
    totais_por_ano,
)


MODELOS_IDEAIS = [
    "Modelo 100% remoto",
    "Modelo híbrido flexível",
    "Modelo híbrido com dias fixos",
    "Modelo 100% presencial",
]
CORES_MODELOS_IDEAIS = {
    "Modelo 100% remoto": TEAL,
    "Modelo híbrido flexível": AZUL,
    "Modelo híbrido com dias fixos": ROXO,
    "Modelo 100% presencial": LARANJA,
}
MODELOS_INTENCAO = [
    "Aceitar e retornar ao modelo 100% presencial",
    "Procurar oportunidade no modelo híbrido ou remoto",
    "Procurar oportunidade no modelo 100% remoto",
]


def gerar(df=None, pasta_saida: Path | None = None) -> Path:
    df, spark_proprio = obter_dados(df, "GoldGrafico09ModeloTrabalho")
    anos_pesquisa = anos(df)
    contagens = contagens_por_ano(
        df,
        "modelo_trabalho_ideal",
        padronizar_modelo_trabalho_col,
        limite=10,
        excluir=("Não informado",),
    )
    totais = totais_por_ano(
        df,
        "modelo_trabalho_ideal",
        padronizar_modelo_trabalho_col,
        ("Não informado",),
    )
    linhas_ideais = [
        (
            f"{rotulo_ano(ano)} · n={numero(totais.get(ano, 0))}",
            {modelo: int(dict(contagens.get(ano, [])).get(modelo, 0)) for modelo in MODELOS_IDEAIS},
        )
        for ano in anos_pesquisa
        if ano != ANO_EDICAO_PARCIAL
    ]
    intencao_parcial = dict(
        converter_para_percentuais(
            [
                (
                    modelo,
                    int(dict(contagens.get(ANO_EDICAO_PARCIAL, [])).get(modelo, 0)),
                )
                for modelo in MODELOS_INTENCAO
            ],
            totais.get(ANO_EDICAO_PARCIAL, 0),
        )
    )

    imagem, draw = criar_canvas(
        "Modelo de trabalho — perguntas diferentes por edição",
        "2023–2024 medem o modelo ideal; 2025–2026 mede a reação diante de um possível retorno ao presencial. Não são comparações diretas.",
        altura=1000,
        indice=9,
    )
    desenhar_stacked100(
        draw,
        (55, 220, 1545, 500),
        linhas_ideais,
        MODELOS_IDEAIS,
        "2023–2024 — modelo ideal declarado",
        CORES_MODELOS_IDEAIS,
        legenda=False,
        rotulo_largura=185,
        mostrar_rotulos_pequenos=True,
    )
    desenhar_legenda(draw, (220, 528), MODELOS_IDEAIS, CORES_MODELOS_IDEAIS, 1160)
    desenhar_barras_horizontais(
        draw,
        (55, 595, 1020, 900),
        intencao_parcial.items(),
        "2025–2026 — intenção diante de retorno ao presencial",
        cor=ROXO,
        percentual=True,
        maximo=100,
        tamanho_rotulo=14,
        subtitulo=f"respostas válidas · n={numero(totais.get(ANO_EDICAO_PARCIAL, 0))}",
    )
    desenhar_aviso(
        draw,
        (1050, 595, 1545, 900),
        "Não comparar diretamente",
        "Os 2% de 2023/2024 são modelo ideal. Os 28% de 2025/2026 são intenção de aceitar o presencial. “Híbrido ou remoto” é uma única opção, não duas. A pergunta mudou.",
    )
    rodape(
        draw,
        "2023–2024: híbrido flexível significa liberdade para escolher quando estar no escritório; híbrido com dias fixos mantém dias presenciais definidos. "
        "2025–2026: as três opções registram intenção de aceitar ou procurar outro modelo. " + AVISO_EDICAO_PARCIAL,
    )
    caminho = salvar(imagem, "09_modelo_trabalho.png", pasta_saida)
    if spark_proprio is not None:
        spark_proprio.stop()
    return caminho


if __name__ == "__main__":
    print(gerar())
