"""Funções visuais compartilhadas pelos gráficos locais.

Os scripts atuais usam ``spark_comum.py`` para leitura, limpeza, deduplicação
e agregação com PySpark. Este módulo concentra as funções de desenho com
Pillow e mantém funções antigas de apoio para compatibilidade; elas não são
chamadas pelo ``gerar_todos.py`` atual. A AWS não é alterada por estes arquivos.
"""

from __future__ import annotations

import math
import os
import re
import unicodedata
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
from PIL import Image, ImageDraw, ImageFont


PASTA_SCRIPTS = Path(__file__).resolve().parent
PASTA_PROJETO = PASTA_SCRIPTS.parent
INPUT_PADRAO = Path(
    os.environ.get(
        "STATE_DATA_CSV",
        r"C:\Users\joaop\Downloads\run-1788912744607-part-r-00000",
    )
)
OUTPUT_PADRAO = Path(
    os.environ.get("STATE_DATA_OUTPUT", str(PASTA_PROJETO / "saidas"))
)

FONTE_REGULAR = Path(r"C:\Windows\Fonts\segoeui.ttf")
FONTE_NEGRITO = Path(r"C:\Windows\Fonts\segoeuib.ttf")

FUNDO = (248, 250, 252)
NAVY = (24, 53, 87)
TEXTO = (40, 54, 70)
MUTED = (96, 111, 128)
GRID = (218, 225, 232)
PALETA = [
    (39, 112, 180),
    (35, 143, 145),
    (224, 132, 43),
    (113, 83, 157),
    (57, 143, 91),
    (194, 75, 75),
    (211, 164, 55),
    (109, 126, 145),
]


def fonte(tamanho: int, negrito: bool = False) -> ImageFont.FreeTypeFont:
    caminho = FONTE_NEGRITO if negrito else FONTE_REGULAR
    if caminho.exists():
        return ImageFont.truetype(str(caminho), tamanho)
    return ImageFont.load_default()


def texto(valor: object) -> str:
    if valor is None:
        return ""
    try:
        if pd.isna(valor):
            return ""
    except (TypeError, ValueError):
        pass
    valor_texto = str(valor).strip()
    return "" if valor_texto.lower() in {"nan", "none", "null"} else valor_texto


def normalizar(valor: object) -> str:
    return (
        unicodedata.normalize("NFKD", texto(valor))
        .encode("ascii", "ignore")
        .decode()
        .lower()
    )


def numero(valor: object) -> str:
    try:
        return f"{int(round(float(valor))):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def encurtar(valor: object, limite: int = 28) -> str:
    resultado = re.sub(r"\s+", " ", texto(valor)).strip()
    if len(resultado) <= limite:
        return resultado
    return resultado[: max(1, limite - 1)].rstrip() + "…"


def quebrar_texto(draw: ImageDraw.ImageDraw, valor: object, largura: int, fnt: ImageFont.FreeTypeFont, max_linhas: int = 2) -> list[str]:
    palavras = texto(valor).split()
    linhas: list[str] = []
    atual = ""
    for palavra in palavras:
        candidato = palavra if not atual else f"{atual} {palavra}"
        if not atual or draw.textbbox((0, 0), candidato, font=fnt)[2] <= largura:
            atual = candidato
        else:
            linhas.append(atual)
            atual = palavra
    if atual:
        linhas.append(atual)
    if len(linhas) > max_linhas:
        linhas = linhas[:max_linhas]
        linhas[-1] = encurtar(linhas[-1], max(8, int(largura / max(fnt.size, 1))))
    return linhas or [""]


def escrever_quebrado(draw: ImageDraw.ImageDraw, xy: tuple[int, int], valor: object, largura: int, fnt: ImageFont.FreeTypeFont, fill=TEXTO, max_linhas: int = 2, espacamento: int = 3) -> int:
    linhas = quebrar_texto(draw, valor, largura, fnt, max_linhas)
    x, y = xy
    altura = draw.textbbox((0, 0), "Ag", font=fnt)[3]
    for linha in linhas:
        draw.text((x, y), linha, font=fnt, fill=fill)
        y += altura + espacamento
    return y


def carregar_dados(caminho: Path | None = None) -> pd.DataFrame:
    """Carrega o CSV local e padroniza a chave e as colunas de tecnologia."""
    entrada = caminho or INPUT_PADRAO
    if not entrada.exists():
        raise FileNotFoundError(f"CSV não encontrado: {entrada}")
    df = pd.read_csv(entrada, encoding="utf-8", low_memory=False)
    if "ano_pesquisa" not in df.columns or "id_resposta" not in df.columns:
        raise ValueError("O CSV precisa conter ano_pesquisa e id_resposta.")
    df["ano_pesquisa"] = pd.to_numeric(df["ano_pesquisa"], errors="coerce")
    df["id_resposta"] = df["id_resposta"].map(texto)
    df = df.dropna(subset=["ano_pesquisa"]).copy()
    df["ano_pesquisa"] = df["ano_pesquisa"].astype(int)
    df = df.drop_duplicates(["ano_pesquisa", "id_resposta"], keep="first").copy()

    anos_anteriores = df["ano_pesquisa"].isin([2023, 2024])
    for coluna in [
        "linguagem_preferida",
        "cloud_preferida",
        "bi_preferida",
        "ferramenta_de_bi_preferida",
        "tecnologia_data_lake",
        "tecnologia_data_warehouse",
    ]:
        if coluna not in df.columns:
            df[coluna] = ""

    # Nas edições antigas, algumas perguntas ocupavam colunas com nomes que
    # mudaram. Mantemos uma coluna semântica única para comparar os três anos.
    df["linguagem_corrigida"] = df["linguagem_preferida"]
    df.loc[anos_anteriores, "linguagem_corrigida"] = df.loc[anos_anteriores, "cloud_preferida"]
    df["cloud_corrigida"] = df["cloud_preferida"]
    df.loc[anos_anteriores, "cloud_corrigida"] = df.loc[anos_anteriores, "ferramenta_de_bi_preferida"]
    df["bi_corrigida"] = df["ferramenta_de_bi_preferida"]
    df.loc[anos_anteriores, "bi_corrigida"] = df.loc[anos_anteriores, "bi_preferida"]
    return df


def anos(df: pd.DataFrame) -> list[int]:
    return sorted(int(ano) for ano in df["ano_pesquisa"].dropna().unique())


def serie(df: pd.DataFrame, coluna: str) -> pd.Series:
    if coluna not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df[coluna].map(texto)


def contagens(serie_valores: pd.Series, conversor=None, limite: int | None = None, excluir: Iterable[str] = ()) -> list[tuple[str, int]]:
    mapeada = serie_valores.map(conversor) if conversor else serie_valores.map(texto)
    mapeada = mapeada[mapeada != ""]
    excluidos = set(excluir)
    resultado = mapeada.value_counts()
    itens = [(str(chave), int(valor)) for chave, valor in resultado.items() if str(chave) not in excluidos]
    return itens[:limite] if limite else itens


def contagens_por_ano(df: pd.DataFrame, coluna: str, conversor=None, limite: int = 5, excluir: Iterable[str] = ()) -> dict[int, list[tuple[str, int]]]:
    return {
        ano: contagens(serie(df.loc[df["ano_pesquisa"] == ano], coluna), conversor, limite, excluir)
        for ano in anos(df)
    }


def padronizar_funcao(valor: object) -> str:
    original = texto(valor)
    s = normalizar(valor)
    if not s:
        return "Não informado"
    if "analise de dados" in s or "business intelligence" in s or re.search(r"\bbi\b", s):
        return "Análise de dados e BI"
    if "engenharia de dados" in s:
        return "Engenharia de dados"
    if "ciencia de dados" in s or "machine learning" in s or "cientista" in s:
        return "Ciência de dados e IA"
    if "desenvolvimento" in s or "software" in s:
        return "Desenvolvimento de software"
    if "engenharia de machine" in s:
        return "Engenharia de dados"
    if "gestao" in s or "lider" in s:
        return "Gestão e liderança"
    if "nao atuo" in s or "fora da area" in s:
        return "Fora da área de dados"
    return encurtar(original, 28)


def padronizar_nivel(valor: object) -> str:
    s = normalizar(valor)
    if not s:
        return "Não informado"
    if "junior" in s:
        return "Júnior"
    if "pleno" in s:
        return "Pleno"
    if "senior" in s:
        return "Sênior"
    if "especialista" in s or "staff" in s:
        return "Especialista/Staff"
    return "Outro nível"


def padronizar_genero(valor: object) -> str:
    s = normalizar(valor)
    if not s:
        return "Não informado"
    if "masculino" in s:
        return "Masculino"
    if "feminino" in s:
        return "Feminino"
    if "nao" in s or "prefiro" in s:
        return "Prefere não informar"
    return "Outro"


def padronizar_regiao(valor: object) -> str:
    s = normalizar(valor)
    mapa = {
        "sudeste": "Sudeste",
        "sul": "Sul",
        "nordeste": "Nordeste",
        "centro oeste": "Centro-Oeste",
        "norte": "Norte",
    }
    return mapa.get(s, texto(valor) or "Não informado")


def padronizar_salario(valor: object) -> str:
    s = normalizar(valor)
    if not s:
        return "Não informado"
    if "ate r$ 2.000" in s or "ate 2.000" in s:
        return "Até R$ 2 mil"
    if "2.001" in s and "4.000" in s:
        return "R$ 2 a 4 mil"
    if "4.001" in s and "6.000" in s:
        return "R$ 4 a 6 mil"
    if "6.001" in s and "8.000" in s:
        return "R$ 6 a 8 mil"
    if "8.001" in s and "12.000" in s:
        return "R$ 8 a 12 mil"
    if "12.001" in s and "16.000" in s:
        return "R$ 12 a 16 mil"
    if "16.001" in s and "20.000" in s:
        return "R$ 16 a 20 mil"
    if "acima de 20.000" in s:
        return "Acima de R$ 20 mil"
    return encurtar(valor, 22)


def padronizar_tecnologia(valor: object) -> str:
    s = normalizar(valor)
    if not s:
        return "Não informado"
    substituicoes = [
        ("amazon web services", "AWS"),
        ("amazon quicksight", "Amazon QuickSight"),
        ("microsoft powerbi", "Power BI"),
        ("google cloud", "Google Cloud"),
        ("cloud propria", "Cloud própria"),
        ("databriks", "Databricks"),
        ("nao tenho preferencia / nao sei opinar", "Sem preferência"),
        ("nao sei opinar / nao tenho preferencia", "Sem preferência"),
        ("servidores on premise/nao utilizamos cloud", "Sem cloud/on-premises"),
    ]
    for termo, substituto in substituicoes:
        if termo in s:
            return substituto
    return encurtar(valor, 22)


def padronizar_prioridade(valor: object) -> str:
    s = normalizar(valor)
    if not s:
        return "Não informado"
    if "principal prioridade" in s and "proximos" not in s:
        return "Principal prioridade"
    if "principais prioridades" in s or "proximos 2-4 anos" in s:
        return "Prioridade para os próximos anos"
    if "mais ou menos" in s:
        return "Iniciativa, sem prioridade"
    if "nao e uma iniciativa" in s or "nao tem sido uma prioridade" in s:
        return "Não é prioridade"
    if "nao sei" in s:
        return "Não sabe opinar"
    return "Outra resposta"


def padronizar_resultado(valor: object) -> str:
    s = normalizar(valor)
    if not s:
        return "Não informado"
    if "nao sei" in s:
        return "Não sabe opinar"
    if "parcial" in s or "alguns" in s:
        return "Parcialmente"
    if s.startswith("sim") or "bons resultados" in s:
        return "Sim"
    if s.startswith("nao"):
        return "Não"
    return encurtar(valor, 22)


def contagens_tecnologia(serie_valores: pd.Series, limite: int = 3) -> list[tuple[str, int]]:
    acumulado: dict[str, int] = {}
    for valor in serie_valores.map(texto):
        if not valor:
            continue
        partes = [parte.strip() for parte in re.split(r"\s*,\s*", valor) if parte.strip()]
        for parte in partes:
            chave = padronizar_tecnologia(parte)
            acumulado[chave] = acumulado.get(chave, 0) + 1
    return sorted(acumulado.items(), key=lambda item: item[1], reverse=True)[:limite]


def contagens_padrao(serie_valores: pd.Series, padroes: Sequence[tuple[str, Sequence[str]]], limite: int | None = None) -> list[tuple[str, int]]:
    limpos = serie_valores.map(normalizar)
    itens = []
    for rotulo, termos in padroes:
        total = int(limpos.map(lambda valor: any(termo in valor for termo in termos)).sum())
        itens.append((rotulo, total))
    itens.sort(key=lambda item: item[1], reverse=True)
    return itens[:limite] if limite else itens


def criar_canvas(titulo: str, subtitulo: str, altura: int = 1050) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    imagem = Image.new("RGB", (1600, altura), FUNDO)
    draw = ImageDraw.Draw(imagem)
    draw.text((70, 38), titulo, font=fonte(34, True), fill=NAVY)
    escrever_quebrado(draw, (70, 92), subtitulo, 1460, fonte(18), fill=MUTED, max_linhas=2)
    draw.line((70, 150, 1530, 150), fill=GRID, width=2)
    return imagem, draw


def rodape(draw: ImageDraw.ImageDraw, texto_rodape: str, altura: int = 1050) -> None:
    draw.line((70, altura - 72, 1530, altura - 72), fill=GRID, width=1)
    escrever_quebrado(draw, (70, altura - 58), texto_rodape, 1460, fonte(14), fill=MUTED, max_linhas=1)


def salvar(imagem: Image.Image, nome: str, pasta: Path | None = None) -> Path:
    destino = pasta or OUTPUT_PADRAO
    destino.mkdir(parents=True, exist_ok=True)
    caminho = destino / nome
    imagem.save(caminho, format="PNG", optimize=True)
    return caminho


def desenhar_barras_horizontais(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], itens: Sequence[tuple[str, int | float]], titulo: str, cor=PALETA[0], percentual: bool = False, maximo: float | None = None, tamanho_rotulo: int = 16) -> None:
    x0, y0, x1, y1 = caixa
    draw.text((x0, y0), titulo, font=fonte(21, True), fill=NAVY)
    itens = list(itens)
    if not itens:
        draw.text((x0, y0 + 60), "Sem dados suficientes", font=fonte(17), fill=MUTED)
        return
    topo, base = y0 + 50, y1 - 8
    largura_rotulo = min(270, max(170, int((x1 - x0) * 0.38)))
    inicio_barra, fim_barra, coluna_valor = x0 + largura_rotulo, x1 - 100, x1 - 8
    maior = maximo or max(float(valor) for _, valor in itens) or 1
    altura_linha = (base - topo) / len(itens)
    for indice, (rotulo, valor) in enumerate(itens):
        centro = topo + altura_linha * indice + altura_linha / 2
        fnt = fonte(tamanho_rotulo)
        escrever_quebrado(draw, (x0, int(centro - 12)), encurtar(rotulo, 30), largura_rotulo - 12, fnt, max_linhas=2)
        draw.line((inicio_barra, centro, fim_barra, centro), fill=GRID, width=22)
        comprimento = int((fim_barra - inicio_barra) * float(valor) / maior)
        draw.line((inicio_barra, centro, inicio_barra + comprimento, centro), fill=cor, width=22)
        rotulo_valor = f"{float(valor):.1f}%" if percentual else numero(valor)
        draw.text((coluna_valor, centro - 11), rotulo_valor, font=fonte(16, True), fill=TEXTO, anchor="ra")


def desenhar_colunas(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], itens: Sequence[tuple[str, int | float]], titulo: str, cor=PALETA[0], maximo: float | None = None) -> None:
    x0, y0, x1, y1 = caixa
    draw.text((x0, y0), titulo, font=fonte(21, True), fill=NAVY)
    itens = list(itens)
    if not itens:
        return
    topo, base = y0 + 52, y1 - 55
    esquerda, direita = x0 + 48, x1 - 24
    maior = maximo or max(float(valor) for _, valor in itens) or 1
    for nivel in range(5):
        valor_grade = maior * nivel / 4
        y = base - int((base - topo) * nivel / 4)
        draw.line((esquerda, y, direita, y), fill=GRID, width=1)
        draw.text((x0, y - 10), numero(valor_grade), font=fonte(13), fill=MUTED)
    passo = (direita - esquerda) / max(1, len(itens))
    largura = min(125, int(passo * 0.58))
    for indice, (rotulo, valor) in enumerate(itens):
        centro = esquerda + passo * (indice + 0.5)
        altura = int((base - topo) * float(valor) / maior)
        esquerda_barra = int(centro - largura / 2)
        draw.rectangle((esquerda_barra, base - altura, esquerda_barra + largura, base), fill=cor)
        draw.text((centro, max(topo - 4, base - altura - 30)), numero(valor), font=fonte(16, True), fill=TEXTO, anchor="mm")
        escrever_quebrado(draw, (int(centro - largura / 2), base + 14), rotulo, largura, fonte(15), fill=TEXTO, max_linhas=2)


def desenhar_stacked100(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], linhas: Sequence[tuple[str, dict[str, int]]], categorias: Sequence[str], titulo: str, cores: dict[str, tuple[int, int, int]], legenda: bool = True, rotulo_largura: int = 170) -> None:
    x0, y0, x1, y1 = caixa
    draw.text((x0, y0), titulo, font=fonte(21, True), fill=NAVY)
    topo, barra_altura = y0 + 52, 34
    inicio_barra, fim_barra = x0 + rotulo_largura, x1 - 18
    for indice, (rotulo, valores) in enumerate(linhas):
        y = topo + indice * 62
        total = sum(max(0, int(valor)) for valor in valores.values()) or 1
        draw.text((x0, y + 7), texto(rotulo), font=fonte(16), fill=TEXTO)
        atual = inicio_barra
        for categoria in categorias:
            valor = max(0, int(valores.get(categoria, 0)))
            if not valor:
                continue
            largura = int((fim_barra - inicio_barra) * valor / total)
            draw.rectangle((atual, y, atual + largura, y + barra_altura), fill=cores.get(categoria, PALETA[0]))
            if largura >= 62:
                draw.text((atual + largura / 2, y + barra_altura / 2), f"{valor / total:.0%}", font=fonte(13, True), fill=(255, 255, 255), anchor="mm")
            atual += largura
    if legenda:
        desenhar_legenda(draw, (x0, y1 - 24), categorias, cores, x1 - x0)


def desenhar_legenda(draw: ImageDraw.ImageDraw, xy: tuple[int, int], categorias: Sequence[str], cores: dict[str, tuple[int, int, int]], largura: int) -> None:
    x, y = xy
    inicio = x
    for categoria in categorias:
        largura_texto = draw.textbbox((0, 0), categoria, font=fonte(14))[2]
        if x + largura_texto + 46 > inicio + largura:
            x = inicio
            y += 25
        draw.rectangle((x, y + 3, x + 15, y + 18), fill=cores.get(categoria, PALETA[0]))
        draw.text((x + 22, y), categoria, font=fonte(14), fill=TEXTO)
        x += largura_texto + 47


def desenhar_tecnologia_painel(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], dados: Sequence[tuple[str, Sequence[tuple[str, int]]]], titulo: str, cor) -> None:
    x0, y0, x1, y1 = caixa
    draw.rounded_rectangle((x0, y0, x1, y1), radius=12, fill=(255, 255, 255), outline=GRID, width=2)
    draw.text((x0 + 18, y0 + 16), titulo, font=fonte(22, True), fill=NAVY)
    inicio = y0 + 62
    for indice, (tipo, itens) in enumerate(dados):
        y_tipo = inicio + indice * 112
        draw.text((x0 + 18, y_tipo), tipo, font=fonte(16, True), fill=TEXTO)
        maior = max([valor for _, valor in itens] or [1])
        for posicao, (rotulo, valor) in enumerate(itens[:3]):
            y = y_tipo + 25 + posicao * 25
            draw.text((x0 + 18, y - 8), encurtar(rotulo, 18), font=fonte(13), fill=TEXTO)
            inicio_barra, fim_barra = x0 + 155, x1 - 72
            draw.line((inicio_barra, y, fim_barra, y), fill=GRID, width=10)
            comprimento = int((fim_barra - inicio_barra) * valor / maior)
            draw.line((inicio_barra, y, inicio_barra + comprimento, y), fill=cor, width=10)
            draw.text((x1 - 12, y - 8), numero(valor), font=fonte(13, True), fill=TEXTO, anchor="ra")


def desenhar_aviso(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], titulo: str, texto_aviso: str) -> None:
    x0, y0, x1, y1 = caixa
    draw.rounded_rectangle((x0, y0, x1, y1), radius=12, fill=(255, 255, 255), outline=GRID, width=2)
    draw.text((x0 + 20, y0 + 20), titulo, font=fonte(22, True), fill=NAVY)
    escrever_quebrado(draw, (x0 + 20, y0 + 72), texto_aviso, x1 - x0 - 40, fonte(17), fill=MUTED, max_linhas=6)
