"""Job AWS Glue Spark - relatório visual da camada Gold.

Este job é executado dentro do AWS Glue e não depende de arquivos locais.
Ele lê as tabelas agregadas do Glue Data Catalog, cria gráficos SVG simples
e publica um relatório HTML em português no S3.

Saídas no S3
------------
- gold/Graficos/RelatorioGoldPTBR.html
- gold/Graficos/LEIA-ME.txt
- gold/Graficos/01_respondentes_por_ano.svg
- gold/Graficos/02_funcoes_mais_frequentes.svg
- gold/Graficos/03_faixas_salariais.svg
- gold/Graficos/04_diversidade_genero.svg
- gold/Graficos/05_modelo_trabalho.svg
- gold/Graficos/06_tecnologias_mais_citadas.svg
- gold/Graficos/07_indicadores_ia.svg

O relatório usa apenas as tabelas Gold geradas pelo job GoldIndicadoresPTBR.
As contagens representam os respondentes das pesquisas State of Data Brasil.
"""

from __future__ import annotations

import html
import sys
from typing import Iterable

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

BUCKET = "fiap-techchallenge3-2026"
DATABASE = "fiap_techchallenge3_2026"
PREFIXO = "gold/Graficos/"

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


def ler_tabela(nome: str) -> DataFrame:
    """Lê uma tabela pequena/agregada pelo Glue Data Catalog."""
    return glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE,
        table_name=nome,
        transformation_ctx=f"Leitura_{nome}",
    ).toDF()


def valor_texto(valor: object) -> str:
    if valor is None:
        return "Não informado"
    return str(valor)


def numero(valor: object) -> float:
    try:
        return float(valor)
    except (TypeError, ValueError):
        return 0.0


def escapar(valor: object) -> str:
    return html.escape(valor_texto(valor))


def svg_barras(titulo: str, itens: Iterable[tuple[str, float]], cor: str = "#2563eb") -> str:
    """Cria um gráfico de barras horizontal em SVG, sem bibliotecas externas."""
    itens = list(itens)
    largura = 900
    margem_esquerda = 250
    topo = 70
    altura_barra = 30
    espacamento = 18
    altura = max(180, topo + len(itens) * (altura_barra + espacamento) + 55)
    maior = max([valor for _, valor in itens] or [1.0])
    partes = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {largura} {altura}" role="img" aria-label="{escapar(titulo)}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="24" y="36" font-family="Arial, sans-serif" font-size="22" font-weight="700" fill="#172033">{escapar(titulo)}</text>',
    ]
    for indice, (rotulo, valor) in enumerate(itens):
        y = topo + indice * (altura_barra + espacamento)
        largura_valor = 570 * valor / maior if maior else 0
        partes.append(
            f'<text x="20" y="{y + 21}" font-family="Arial, sans-serif" font-size="15" fill="#334155">{escapar(rotulo[:38])}</text>'
        )
        partes.append(
            f'<rect x="{margem_esquerda}" y="{y}" width="{largura_valor:.1f}" height="{altura_barra}" rx="6" fill="{cor}" opacity="0.9"/>'
        )
        partes.append(
            f'<text x="{margem_esquerda + largura_valor + 10:.1f}" y="{y + 21}" font-family="Arial, sans-serif" font-size="15" font-weight="700" fill="#172033">{valor:.0f}</text>'
        )
    partes.append("</svg>")
    return "".join(partes)


def quebrar_rotulo(rotulo: object, limite: int = 20, max_linhas: int = 2) -> list[str]:
    """Quebra rótulos longos em poucas linhas para impedir sobreposição."""
    palavras = valor_texto(rotulo).replace("/", " / ").split()
    linhas: list[str] = []
    atual = ""
    for palavra in palavras:
        candidato = f"{atual} {palavra}".strip()
        if atual and len(candidato) > limite:
            linhas.append(atual)
            atual = palavra
        else:
            atual = candidato
    if atual:
        linhas.append(atual)
    if len(linhas) <= max_linhas:
        return linhas
    linhas = linhas[:max_linhas]
    linhas[-1] = linhas[-1][: max(1, limite - 1)] + "…"
    return linhas


def svg_colunas(titulo: str, itens: Iterable[tuple[str, float]], cor: str = "#2563eb") -> str:
    """Gráfico de colunas para séries curtas, como anos ou regiões."""
    itens = list(itens)
    largura, altura = 900, 370
    esquerda, topo, area_largura, area_altura = 70, 75, 790, 220
    maior = max([valor for _, valor in itens] or [1.0])
    slot = area_largura / max(len(itens), 1)
    barra_largura = min(100, slot * 0.58)
    partes = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {largura} {altura}" role="img" aria-label="{escapar(titulo)}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="24" y="36" font-family="Arial, sans-serif" font-size="22" font-weight="700" fill="#172033">{escapar(titulo)}</text>',
        '<text x="24" y="58" font-family="Arial, sans-serif" font-size="13" fill="#64748b">Quantidade de respondentes</text>',
    ]
    for nivel in range(4):
        valor_grade = maior * nivel / 3
        y_grade = topo + area_altura - area_altura * nivel / 3
        partes.append(f'<line x1="{esquerda}" y1="{y_grade:.1f}" x2="{esquerda + area_largura}" y2="{y_grade:.1f}" stroke="#d9e2ec" stroke-width="1"/>')
        partes.append(f'<text x="{esquerda - 10}" y="{y_grade + 5:.1f}" text-anchor="end" font-family="Arial, sans-serif" font-size="12" fill="#64748b">{valor_grade:.0f}</text>')
    for indice, (rotulo, valor) in enumerate(itens):
        x = esquerda + indice * slot + (slot - barra_largura) / 2
        altura_barra = area_altura * valor / maior if maior else 0
        y = topo + area_altura - altura_barra
        partes.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{barra_largura:.1f}" height="{altura_barra:.1f}" rx="7" fill="{cor}" opacity="0.9"/>')
        partes.append(f'<text x="{x + barra_largura / 2:.1f}" y="{max(topo - 6, y - 8):.1f}" text-anchor="middle" font-family="Arial, sans-serif" font-size="15" font-weight="700" fill="#172033">{valor:.0f}</text>')
        for linha, texto_linha in enumerate(quebrar_rotulo(rotulo, 14, 2)):
            partes.append(f'<text x="{x + barra_largura / 2:.1f}" y="{topo + area_altura + 28 + linha * 16}" text-anchor="middle" font-family="Arial, sans-serif" font-size="13" fill="#334155">{escapar(texto_linha)}</text>')
    partes.append("</svg>")
    return "".join(partes)


def svg_top3(titulo: str, itens: Iterable[tuple[str, float]], cor: str = "#7c3aed") -> str:
    """Ranking Top 3 com rótulo separado da barra para manter legibilidade."""
    itens = list(itens)[:3]
    largura, altura = 900, 300
    maior = max([valor for _, valor in itens] or [1.0])
    partes = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {largura} {altura}" role="img" aria-label="{escapar(titulo)}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="24" y="36" font-family="Arial, sans-serif" font-size="22" font-weight="700" fill="#172033">{escapar(titulo)}</text>',
        '<text x="24" y="58" font-family="Arial, sans-serif" font-size="13" fill="#64748b">Top 3 entre os respondentes</text>',
    ]
    for indice, (rotulo, valor) in enumerate(itens):
        base = 86 + indice * 68
        linhas = quebrar_rotulo(rotulo, 26, 2)
        partes.append(f'<circle cx="36" cy="{base + 18}" r="17" fill="{cor}" opacity="0.15"/>')
        partes.append(f'<text x="36" y="{base + 23}" text-anchor="middle" font-family="Arial, sans-serif" font-size="16" font-weight="700" fill="{cor}">{indice + 1}</text>')
        for linha, texto_linha in enumerate(linhas):
            partes.append(f'<text x="70" y="{base + 14 + linha * 16}" font-family="Arial, sans-serif" font-size="14" fill="#334155">{escapar(texto_linha)}</text>')
        largura_barra = 420 * valor / maior if maior else 0
        partes.append(f'<rect x="360" y="{base + 4}" width="{largura_barra:.1f}" height="27" rx="7" fill="{cor}" opacity="0.88"/>')
        partes.append(f'<text x="{845:.1f}" y="{base + 23}" font-family="Arial, sans-serif" font-size="15" font-weight="700" fill="#172033">{valor:.0f}</text>')
    partes.append("</svg>")
    return "".join(partes)


def svg_rosca(titulo: str, itens: Iterable[tuple[str, float]], cores: list[str]) -> str:
    """Gráfico de rosca para distribuições de categorias."""
    itens = list(itens)[:6]
    largura, altura = 900, 390
    total = sum(valor for _, valor in itens) or 1.0
    raio, circ = 105, 2 * 3.1415926535 * 105
    partes = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {largura} {altura}" role="img" aria-label="{escapar(titulo)}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="24" y="36" font-family="Arial, sans-serif" font-size="22" font-weight="700" fill="#172033">{escapar(titulo)}</text>',
        '<text x="24" y="58" font-family="Arial, sans-serif" font-size="13" fill="#64748b">Distribuição das categorias exibidas</text>',
        f'<circle cx="230" cy="220" r="{raio}" fill="none" stroke="#e2e8f0" stroke-width="42"/>',
    ]
    deslocamento = 0.0
    for indice, (_, valor) in enumerate(itens):
        segmento = circ * valor / total
        partes.append(f'<circle cx="230" cy="220" r="{raio}" fill="none" stroke="{cores[indice % len(cores)]}" stroke-width="42" stroke-dasharray="{segmento:.2f} {circ:.2f}" stroke-dashoffset="{-deslocamento:.2f}" transform="rotate(-90 230 220)"/>')
        deslocamento += segmento
    partes.append(f'<text x="230" y="215" text-anchor="middle" font-family="Arial, sans-serif" font-size="25" font-weight="700" fill="#172033">{total:.0f}</text>')
    partes.append('<text x="230" y="238" text-anchor="middle" font-family="Arial, sans-serif" font-size="13" fill="#64748b">respostas exibidas</text>')
    for indice, (rotulo, valor) in enumerate(itens):
        y = 100 + indice * 42
        percentual = valor / total * 100
        partes.append(f'<rect x="455" y="{y - 13}" width="14" height="14" rx="3" fill="{cores[indice % len(cores)]}"/>')
        linhas = quebrar_rotulo(rotulo, 28, 2)
        partes.append(f'<text x="480" y="{y}" font-family="Arial, sans-serif" font-size="14" fill="#334155">{escapar(linhas[0])}</text>')
        if len(linhas) > 1:
            partes.append(f'<text x="480" y="{y + 16}" font-family="Arial, sans-serif" font-size="14" fill="#334155">{escapar(linhas[1])}</text>')
        partes.append(f'<text x="845" y="{y}" text-anchor="end" font-family="Arial, sans-serif" font-size="14" font-weight="700" fill="#172033">{valor:.0f} ({percentual:.1f}%)</text>')
    partes.append("</svg>")
    return "".join(partes)


def svg_tecnologias(titulo: str, itens: Iterable[tuple[str, float]]) -> str:
    """Pequenos rankings por tipo de tecnologia, em vez de uma barra única."""
    grupos: dict[str, list[tuple[str, float]]] = {}
    for rotulo, valor in itens:
        tipo, separador, tecnologia = rotulo.partition(" - ")
        grupos.setdefault(tipo if separador else "Tecnologia", []).append((tecnologia if separador else rotulo, valor))
    grupos = dict(sorted(grupos.items(), key=lambda par: sum(v for _, v in par[1]), reverse=True)[:6])
    largura, painel_largura, painel_altura = 900, 280, 210
    colunas = 3
    linhas = (len(grupos) + colunas - 1) // colunas
    altura = max(300, 78 + linhas * painel_altura)
    partes = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {largura} {altura}" role="img" aria-label="{escapar(titulo)}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="24" y="36" font-family="Arial, sans-serif" font-size="22" font-weight="700" fill="#172033">{escapar(titulo)}</text>',
        '<text x="24" y="58" font-family="Arial, sans-serif" font-size="13" fill="#64748b">Top 3 por tipo de tecnologia</text>',
    ]
    cores = ["#0891b2", "#2563eb", "#7c3aed", "#059669", "#ea580c", "#db2777"]
    for indice, (tipo, valores) in enumerate(grupos.items()):
        coluna, linha = indice % colunas, indice // colunas
        x, y = 20 + coluna * 295, 78 + linha * painel_altura
        cor = cores[indice % len(cores)]
        partes.append(f'<rect x="{x}" y="{y}" width="{painel_largura}" height="180" rx="12" fill="#f8fafc" stroke="#d9e2ec"/>')
        partes.append(f'<text x="{x + 14}" y="{y + 25}" font-family="Arial, sans-serif" font-size="15" font-weight="700" fill="#172033">{escapar(tipo[:26])}</text>')
        maior = max([v for _, v in valores[:3]] or [1.0])
        for posicao, (tecnologia, valor) in enumerate(valores[:3]):
            by = y + 48 + posicao * 39
            partes.append(f'<text x="{x + 14}" y="{by + 15}" font-family="Arial, sans-serif" font-size="12" fill="#334155">{escapar(quebrar_rotulo(tecnologia, 19, 1)[0])}</text>')
            largura_barra = 100 * valor / maior if maior else 0
            partes.append(f'<rect x="{x + 130}" y="{by}" width="{largura_barra:.1f}" height="22" rx="5" fill="{cor}" opacity="0.85"/>')
            partes.append(f'<text x="{x + 268}" y="{by + 15}" text-anchor="end" font-family="Arial, sans-serif" font-size="12" font-weight="700" fill="#172033">{valor:.0f}</text>')
    partes.append("</svg>")
    return "".join(partes)


def salvar_objeto(s3, chave: str, conteudo: str, tipo: str) -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{PREFIXO}{chave}",
        Body=conteudo.encode("utf-8"),
        ContentType=tipo,
    )


def top_agrupado(frame: DataFrame, dimensoes: list[str], ano: int, limite: int = 8) -> list[tuple[str, float]]:
    """Soma as quantidades do ano mais recente por uma ou mais dimensões."""
    existente = set(frame.columns)
    dimensoes_validas = [dim for dim in dimensoes if dim in existente]
    if not dimensoes_validas or "quantidade" not in existente or "ano_pesquisa" not in existente:
        return []
    agrupado = (
        frame.filter(F.col("ano_pesquisa") == ano)
        .groupBy(*dimensoes_validas)
        .agg(F.sum(F.col("quantidade")).alias("valor"))
        .orderBy(F.desc("valor"))
        .limit(limite)
    )
    resultado = []
    for linha in agrupado.collect():
        partes = [valor_texto(linha[dim]) for dim in dimensoes_validas]
        resultado.append((" - ".join(partes), numero(linha["valor"])))
    return resultado


def top_agrupado_filtrado(
    frame: DataFrame,
    dimensoes: list[str],
    ano: int,
    limite: int = 8,
    filtros: dict[str, str] | None = None,
) -> list[tuple[str, float]]:
    """Agrupa o ano mais recente aplicando filtros opcionais da tabela Gold."""
    existente = set(frame.columns)
    dimensoes_validas = [dim for dim in dimensoes if dim in existente]
    if not dimensoes_validas or "quantidade" not in existente or "ano_pesquisa" not in existente:
        return []
    filtrado = frame.filter(F.col("ano_pesquisa") == ano)
    for coluna, valor in (filtros or {}).items():
        if coluna in existente:
            filtrado = filtrado.filter(F.col(coluna) == valor)
    agrupado = (
        filtrado.groupBy(*dimensoes_validas)
        .agg(F.sum(F.col("quantidade")).alias("valor"))
        .orderBy(F.desc("valor"))
        .limit(limite)
    )
    resultado = []
    for linha in agrupado.collect():
        partes = [valor_texto(linha[dim]) for dim in dimensoes_validas]
        resultado.append((" - ".join(partes), numero(linha["valor"])))
    return resultado


resumo = ler_tabela("goldresumo")
perfil = ler_tabela("goldperfil")
remuneracao = ler_tabela("goldremuneracao")
diversidade = ler_tabela("golddiversidade")
trabalho = ler_tabela("goldtrabalho")
tecnologias = ler_tabela("goldtecnologias")
ia = ler_tabela("goldia")
motivos = ler_tabela("goldmotivosia")

ano_recente = int(resumo.agg(F.max("ano_pesquisa")).first()[0])

resumo_itens = [
    (valor_texto(linha["ano_pesquisa"]), numero(linha["quantidade"]))
    for linha in resumo.orderBy("ano_pesquisa").collect()
]
funcoes_itens = top_agrupado(perfil, ["funcao_atuacao_amigavel"], ano_recente, 3)
salarios_itens = top_agrupado(remuneracao, ["nivel_amigavel", "faixa_salarial_amigavel"], ano_recente, 3)
diversidade_itens = top_agrupado(diversidade, ["genero_amigavel"], ano_recente, 6)
trabalho_itens = top_agrupado(trabalho, ["modelo_trabalho_amigavel"], ano_recente, 6)
tecnologia_itens = top_agrupado(tecnologias, ["tipo_tecnologia", "tecnologia"], ano_recente, 18)
ia_prioridade_itens = top_agrupado_filtrado(
    ia,
    ["categoria"],
    ano_recente,
    6,
    {"tipo_indicador": "Prioridade de IA"},
)
ia_itens = ia_prioridade_itens or top_agrupado(ia, ["tipo_indicador", "categoria"], ano_recente, 6)
regioes_itens = top_agrupado(perfil, ["regiao_amigavel"], ano_recente, 3)
motivos_itens = top_agrupado(motivos, ["motivo"], ano_recente, 3)

cores_rosca = ["#2563eb", "#7c3aed", "#db2777", "#059669", "#ea580c", "#0891b2"]

graficos = {
    "01_respondentes_por_ano.svg": svg_colunas("Respondentes por ano", resumo_itens, "#2563eb"),
    "02_funcoes_mais_frequentes.svg": svg_top3(f"Top 3 funções - {ano_recente}", funcoes_itens, "#7c3aed"),
    "03_faixas_salariais.svg": svg_top3(f"Top 3 combinações salariais - {ano_recente}", salarios_itens, "#059669"),
    "04_diversidade_genero.svg": svg_rosca(f"Distribuição por gênero - {ano_recente}", diversidade_itens, cores_rosca),
    "05_modelo_trabalho.svg": svg_rosca(f"Modelo de trabalho - {ano_recente}", trabalho_itens, cores_rosca),
    "06_tecnologias_mais_citadas.svg": svg_tecnologias(f"Tecnologias mais citadas - {ano_recente}", tecnologia_itens),
    "07_indicadores_ia.svg": svg_rosca(f"Prioridade para IA - {ano_recente}", ia_itens, cores_rosca),
    "08_regioes_respondentes.svg": svg_colunas(f"Top 3 regiões - {ano_recente}", regioes_itens, "#0f766e"),
    "09_motivos_nao_ia.svg": svg_top3(f"Top 3 motivos para não usar IA - {ano_recente}", motivos_itens, "#b45309"),
}

s3 = boto3.client("s3")
for nome, conteudo in graficos.items():
    salvar_objeto(s3, nome, conteudo, "image/svg+xml; charset=utf-8")

total_anos = sum(valor for _, valor in resumo_itens)
cartoes = "".join(
    [
        f'<div class="card"><span>Respondentes no ano mais recente</span><strong>{numero(dict(resumo_itens).get(str(ano_recente), 0)):.0f}</strong><small>{ano_recente}</small></div>',
        f'<div class="card"><span>Total de respondentes nas três edições</span><strong>{total_anos:.0f}</strong><small>2023 a {ano_recente}</small></div>',
        '<div class="card"><span>Fonte</span><strong>Glue + Athena</strong><small>Camada Gold agregada</small></div>',
    ]
)

cards_graficos = "".join([
    f'<figure>{graficos["01_respondentes_por_ano.svg"]}<figcaption>Evolução do número de respondentes.</figcaption></figure>',
    f'<figure>{graficos["02_funcoes_mais_frequentes.svg"]}<figcaption>Funções mais frequentes no ano mais recente.</figcaption></figure>',
    f'<figure>{graficos["03_faixas_salariais.svg"]}<figcaption>Top 3 de combinações entre nível e faixa salarial.</figcaption></figure>',
    f'<figure>{graficos["04_diversidade_genero.svg"]}<figcaption>Distribuição por gênero; o cruzamento por nível está na Gold.</figcaption></figure>',
    f'<figure>{graficos["05_modelo_trabalho.svg"]}<figcaption>Distribuição dos modelos de trabalho.</figcaption></figure>',
    f'<figure>{graficos["06_tecnologias_mais_citadas.svg"]}<figcaption>Tecnologias e ferramentas mais citadas.</figcaption></figure>',
    f'<figure>{graficos["07_indicadores_ia.svg"]}<figcaption>Prioridade para IA; uso e resultados estão nas tabelas GoldIA.</figcaption></figure>',
    f'<figure>{graficos["08_regioes_respondentes.svg"]}<figcaption>Regiões com maior número de respondentes.</figcaption></figure>',
    f'<figure>{graficos["09_motivos_nao_ia.svg"]}<figcaption>Top 3 motivos declarados para não utilizar IA.</figcaption></figure>',
])

html_relatorio = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Relatório Gold - State of Data Brasil</title>
  <style>
    :root {{ color-scheme: light; font-family: Arial, sans-serif; background: #eef2f7; color: #172033; }}
    body {{ margin: 0; }}
    header {{ background: linear-gradient(120deg, #132238, #2563eb); color: white; padding: 34px 5vw; }}
    header h1 {{ margin: 0 0 8px; font-size: 30px; }}
    header p {{ margin: 0; opacity: .9; max-width: 860px; line-height: 1.5; }}
    main {{ max-width: 1180px; margin: 24px auto; padding: 0 18px 40px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 14px; margin-bottom: 20px; }}
    .card, section {{ background: white; border-radius: 14px; box-shadow: 0 3px 14px rgba(15, 23, 42, .08); }}
    .card {{ padding: 18px; }}
    .card span, .card small {{ display: block; color: #64748b; }}
    .card strong {{ display: block; margin: 8px 0 3px; font-size: 25px; color: #0f3d8c; }}
    section {{ padding: 22px; margin-bottom: 18px; }}
    section h2 {{ margin: 0 0 8px; font-size: 21px; }}
    section p, li {{ line-height: 1.55; color: #475569; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 18px; }}
    figure {{ margin: 0; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 10px; }}
    figure svg {{ width: 100%; height: auto; display: block; }}
    figcaption {{ font-size: 13px; color: #64748b; padding: 8px 4px 2px; }}
    code {{ background: #e2e8f0; border-radius: 4px; padding: 2px 5px; }}
    footer {{ color: #64748b; font-size: 13px; margin-top: 18px; }}
  </style>
</head>
<body>
  <header>
    <h1>Relatório Gold — State of Data Brasil</h1>
    <p>Visão amigável dos principais indicadores gerados pelo AWS Glue a partir das pesquisas de 2023, 2024 e 2025. Os dados podem ser auditados pelas tabelas do Glue Data Catalog e pelas consultas salvas no Athena.</p>
  </header>
  <main>
    <div class="cards">{cartoes}</div>
    <section>
      <h2>Como ler este relatório</h2>
      <p>O job <code>GoldIndicadoresPTBR</code> lê a Silver corrigida, mantém uma resposta por combinação de ano e identificador e cria tabelas Gold agregadas. Este job visual lê essas tabelas pelo Glue Catalog e publica os gráficos no S3. As quantidades representam respondentes da pesquisa, não todo o mercado brasileiro.</p>
      <ul>
        <li><strong>quantidade:</strong> respondentes encontrados na categoria.</li>
        <li><strong>total_respondentes:</strong> total usado como base no agrupamento.</li>
        <li><strong>percentual:</strong> participação calculada pelo job Gold.</li>
        <li><strong>ano mais recente:</strong> {ano_recente}.</li>
      </ul>
    </section>
    <section>
      <h2>Gráficos principais</h2>
      <div class="grid">
        {cards_graficos}
      </div>
    </section>
    <section>
      <h2>Onde auditar</h2>
      <p>Banco Glue Catalog: <code>{DATABASE}</code>. Tabelas principais: <code>goldresumo</code>, <code>goldperfil</code>, <code>goldremuneracao</code>, <code>golddiversidade</code>, <code>goldtrabalho</code>, <code>goldtecnologias</code>, <code>goldia</code> e <code>goldmotivosia</code>. No Athena, use as consultas salvas <code>ValidacaoGoldResumo</code> e <code>ValidacaoGoldQualidade</code>.</p>
    </section>
    <footer>Gerado automaticamente pelo AWS Glue em s3://{BUCKET}/{PREFIXO}</footer>
  </main>
</body>
</html>"""

leia_me = f"""RELATORIO GOLD - STATE OF DATA BRASIL

Arquivo principal: s3://{BUCKET}/{PREFIXO}RelatorioGoldPTBR.html
Origem: tabelas Gold do Glue Catalog no banco {DATABASE}.
Job gerador: GoldGraficosPTBR.

O relatório é uma visão dos respondentes das pesquisas. As tabelas detalhadas
continuam disponíveis no Athena para auditoria e para a criação de novos
gráficos. Os arquivos SVG podem ser abertos diretamente no navegador.
"""

salvar_objeto(s3, "RelatorioGoldPTBR.html", html_relatorio, "text/html; charset=utf-8")
salvar_objeto(s3, "LEIA-ME.txt", leia_me, "text/plain; charset=utf-8")

job.commit()
