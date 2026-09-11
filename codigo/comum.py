"""Funções visuais compartilhadas pela reprodução dos gráficos Gold.

O processamento principal fica em ``spark_comum.py`` e usa um snapshot em
nível de respondente da base preparada na AWS. Aqui ficam as funções de
apresentação com Pillow e algumas funções antigas de apoio, mantidas por
compatibilidade.

Este repositório foi organizado para permitir a execução fora da AWS, no VS
Code, sem depender de credenciais ou de um bucket acessível. Em produção, o
job AWS Glue mantém as camadas e os caminhos S3 configurados no ambiente AWS.
Os arquivos deste diretório reproduzem localmente a etapa de análise e
visualização a partir do export da base preparada, sem alterar a infraestrutura
AWS.
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
PASTA_DADOS = PASTA_PROJETO / "dados"

# A fonte local é fixa e auditável: todos os gráficos devem ler o export
# disponibilizado nesta pasta, sem depender de variáveis de ambiente ou de um
# caminho externo à entrega.
INPUT_PADRAO = PASTA_DADOS / "state_of_data_gold_export.csv"
OUTPUT_PADRAO = Path(
    os.environ.get("STATE_DATA_OUTPUT", str(PASTA_PROJETO / "saidas"))
)

FONTE_REGULAR = Path(r"C:\Windows\Fonts\segoeui.ttf")
FONTE_NEGRITO = Path(r"C:\Windows\Fonts\segoeuib.ttf")

FUNDO = (247, 249, 252)  # #F7F9FC
CARTAO = (255, 255, 255)  # #FFFFFF
BORDA = (229, 234, 242)
SOMBRA = (232, 238, 247)
TRILHO = (232, 238, 246)
NAVY = (23, 43, 77)  # #172B4D
TEXTO = (34, 51, 76)
MUTED = (102, 112, 133)  # #667085
GRID = (231, 236, 244)
AZUL = (38, 132, 255)  # #2684FF
TEAL = (32, 191, 169)  # #20BFA9
ROXO = (117, 89, 232)  # #7559E8
MAGENTA = (232, 93, 158)  # #E85D9E
LARANJA = (245, 163, 64)  # #F5A340
VERDE = (55, 153, 102)
AMARELO = (213, 163, 45)
SLATE = (111, 129, 151)
PALETA = [AZUL, TEAL, LARANJA, ROXO, VERDE, MAGENTA, AMARELO, SLATE]

# A coluna ``ano_pesquisa`` registra 2025 para a edição que atravessa
# 2025–2026. Enquanto a coleta não fecha, o ano só pode ser lido como um
# acumulado parcial; ele não deve ser ranqueado contra as edições encerradas.
ANO_EDICAO_PARCIAL = 2025
ROTULO_EDICAO_PARCIAL = "2025\u20132026"
PERIODO_PADRAO = f"2023  \u00b7  2024  \u00b7  {ROTULO_EDICAO_PARCIAL}*"
AVISO_EDICAO_PARCIAL = (
    f"* {ROTULO_EDICAO_PARCIAL} = coleta parcial; o acumulado atual não é comparável "
    "ao volume final de 2023 e 2024."
)


def rotulo_ano(ano: int, detalhado: bool = False) -> str:
    """Retorna o rótulo editorial correto para cada edição da pesquisa."""
    if int(ano) == ANO_EDICAO_PARCIAL:
        return f"{ROTULO_EDICAO_PARCIAL} (parcial)" if detalhado else ROTULO_EDICAO_PARCIAL
    return str(int(ano))


def converter_para_percentuais(
    itens: Sequence[tuple[str, int | float]],
    total: int | float,
) -> list[tuple[str, float]]:
    """Converte contagens de uma edição em participação sobre o seu total."""
    if not total:
        return [(rotulo, 0.0) for rotulo, _ in itens]
    return [(rotulo, 100.0 * float(valor) / float(total)) for rotulo, valor in itens]


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
        "centro-oeste": "Centro-Oeste",
        "norte": "Norte",
    }
    return mapa.get(s, texto(valor) or "Não informado")


def padronizar_salario(valor: object) -> str:
    s = normalizar(valor)
    if not s:
        return "Não informado"
    valores = re.findall(r"\d{1,3}(?:\.\d{3})?", s)
    primeiro_valor = int(valores[0].replace(".", "")) if valores else None
    if primeiro_valor is None:
        return encurtar(valor, 22)
    if ("acima de" in s and primeiro_valor >= 20000) or primeiro_valor > 20000:
        return "Acima de R$ 20 mil"
    if primeiro_valor <= 2000:
        return "Até R$ 2 mil"
    if primeiro_valor <= 4000:
        return "R$ 2 a 4 mil"
    if primeiro_valor <= 6000:
        return "R$ 4 a 6 mil"
    if primeiro_valor <= 8000:
        return "R$ 6 a 8 mil"
    if primeiro_valor <= 12000:
        return "R$ 8 a 12 mil"
    if primeiro_valor <= 16000:
        return "R$ 12 a 16 mil"
    if primeiro_valor <= 20000:
        return "R$ 16 a 20 mil"
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
    if s == "nao informado":
        return "Não informado"
    if "nao sei" in s:
        return "Não sabe opinar"
    if "parcial" in s or "alguns" in s:
        return "Parcialmente"
    if "fase de investigacao" in s or "investigacao e planejamento" in s:
        return "Não — em investigação"
    if "ainda nao comecamos" in s or "nenhum projeto" in s:
        return "Não — não iniciado"
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
    draw.rounded_rectangle((45, 28, 1555, 150), radius=24, fill=CARTAO, outline=BORDA, width=2)
    draw.rounded_rectangle((70, 50, 78, 127), radius=4, fill=PALETA[0])
    draw.text((100, 47), "STATE OF DATA BRASIL  /  CAMADA GOLD", font=fonte(13, True), fill=PALETA[0])
    draw.text((100, 72), titulo, font=fonte(34, True), fill=NAVY)
    escrever_quebrado(draw, (100, 116), subtitulo, 1400, fonte(17), fill=MUTED, max_linhas=1)
    return imagem, draw


def rodape(draw: ImageDraw.ImageDraw, texto_rodape: str, altura: int = 1050) -> None:
    linha_y = altura - 82
    draw.line((70, linha_y, 1530, linha_y), fill=BORDA, width=1)
    draw.ellipse((70, linha_y + 20, 78, linha_y + 28), fill=PALETA[0])
    escrever_quebrado(draw, (92, linha_y + 11), texto_rodape, 1435, fonte(14), fill=MUTED, max_linhas=1)


def salvar(imagem: Image.Image, nome: str, pasta: Path | None = None) -> Path:
    destino = pasta or OUTPUT_PADRAO
    destino.mkdir(parents=True, exist_ok=True)
    caminho = destino / nome
    imagem.save(caminho, format="PNG", optimize=True)
    return caminho


def desenhar_cartao(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], raio: int = 18) -> None:
    """Desenha um painel branco com borda e sombra muito discreta."""
    x0, y0, x1, y1 = caixa
    draw.rounded_rectangle((x0 + 3, y0 + 4, x1 + 3, y1 + 4), radius=raio, fill=(231, 237, 244))
    draw.rounded_rectangle(caixa, radius=raio, fill=CARTAO, outline=BORDA, width=2)


def desenhar_trilho(draw: ImageDraw.ImageDraw, inicio: int, centro: float, fim: int, valor: float, maior: float, cor, altura: int = 18) -> None:
    """Desenha uma barra horizontal arredondada sobre um trilho neutro."""
    if fim <= inicio:
        return
    y0 = int(centro - altura / 2)
    y1 = y0 + altura
    raio = altura // 2
    draw.rounded_rectangle((inicio, y0, fim, y1), radius=raio, fill=TRILHO)
    comprimento = int((fim - inicio) * max(0.0, float(valor)) / max(float(maior), 1.0))
    if comprimento <= 0:
        return
    direita = min(fim, inicio + max(comprimento, altura))
    draw.rounded_rectangle((inicio, y0, direita, y1), radius=raio, fill=cor)


def desenhar_barras_horizontais(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], itens: Sequence[tuple[str, int | float]], titulo: str, cor=PALETA[0], percentual: bool = False, maximo: float | None = None, tamanho_rotulo: int = 16) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    draw.text((x0 + 20, y0 + 18), titulo, font=fonte(18, True), fill=NAVY)
    draw.text((x0 + 20, y0 + 45), "participação" if percentual else "respondentes", font=fonte(12), fill=MUTED)
    itens = list(itens)
    if not itens:
        draw.text((x0 + 20, y0 + 92), "Sem dados suficientes", font=fonte(15), fill=MUTED)
        return
    topo, base = y0 + 82, y1 - 24
    largura_rotulo = min(205, max(155, int((x1 - x0) * 0.40)))
    inicio_barra, fim_barra, coluna_valor = x0 + largura_rotulo + 24, x1 - 70, x1 - 18
    maior = maximo or max(float(valor) for _, valor in itens) or 1
    altura_linha = (base - topo) / len(itens)
    for indice, (rotulo, valor) in enumerate(itens):
        centro = topo + altura_linha * indice + altura_linha / 2
        fnt = fonte(min(tamanho_rotulo, 15))
        escrever_quebrado(draw, (x0 + 20, int(centro - 13)), texto(rotulo), largura_rotulo - 24, fnt, max_linhas=2, espacamento=1)
        desenhar_trilho(draw, inicio_barra, centro, fim_barra, float(valor), float(maior), cor, altura=17)
        rotulo_valor = f"{float(valor):.1f}%" if percentual else numero(valor)
        draw.text((coluna_valor, centro), rotulo_valor, font=fonte(15, True), fill=TEXTO, anchor="rm")


def desenhar_colunas(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], itens: Sequence[tuple[str, int | float]], titulo: str, cor=PALETA[0], maximo: float | None = None) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    draw.text((x0 + 20, y0 + 18), titulo, font=fonte(18, True), fill=NAVY)
    draw.text((x0 + 20, y0 + 45), "respostas válidas por edição", font=fonte(12), fill=MUTED)
    itens = list(itens)
    if not itens:
        return
    topo, base = y0 + 86, y1 - 72
    esquerda, direita = x0 + 70, x1 - 30
    maior = maximo or max(float(valor) for _, valor in itens) or 1
    for nivel in range(5):
        valor_grade = maior * nivel / 4
        y = base - int((base - topo) * nivel / 4)
        draw.line((esquerda, y, direita, y), fill=GRID, width=1)
        draw.text((x0 + 20, y), numero(valor_grade), font=fonte(12), fill=MUTED, anchor="lm")
    passo = (direita - esquerda) / max(1, len(itens))
    largura = min(118, int(passo * 0.58))
    for indice, (rotulo, valor) in enumerate(itens):
        centro = esquerda + passo * (indice + 0.5)
        altura = int((base - topo) * float(valor) / maior)
        esquerda_barra = int(centro - largura / 2)
        draw.rounded_rectangle((esquerda_barra, topo, esquerda_barra + largura, base), radius=10, fill=TRILHO)
        if altura > 0:
            draw.rounded_rectangle((esquerda_barra, base - altura, esquerda_barra + largura, base), radius=10, fill=cor)
        draw.text((centro, max(topo + 12, base - altura - 28)), numero(valor), font=fonte(16, True), fill=TEXTO, anchor="mm")
        escrever_quebrado(draw, (int(centro - largura / 2), base + 18), rotulo, largura, fonte(14), fill=TEXTO, max_linhas=2, espacamento=1)


def desenhar_stacked100(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], linhas: Sequence[tuple[str, dict[str, int]]], categorias: Sequence[str], titulo: str, cores: dict[str, tuple[int, int, int]], legenda: bool = True, rotulo_largura: int = 170) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    draw.text((x0 + 20, y0 + 18), titulo, font=fonte(18, True), fill=NAVY)
    draw.text((x0 + 20, y0 + 45), "composição percentual dentro de cada grupo", font=fonte(12), fill=MUTED)
    barra_altura = 30
    topo = y0 + 78
    base = y1 - 24
    passo = (base - topo - barra_altura) / max(len(linhas) - 1, 1) if linhas else 0
    inicio_barra, fim_barra = x0 + rotulo_largura + 18, x1 - 20
    for indice, (rotulo, valores) in enumerate(linhas):
        y = int(topo + indice * passo)
        total = sum(max(0, int(valor)) for valor in valores.values()) or 1
        draw.text((x0 + 20, y + barra_altura / 2), texto(rotulo), font=fonte(15, True), fill=TEXTO, anchor="lm")
        draw.rounded_rectangle((inicio_barra, y, fim_barra, y + barra_altura), radius=barra_altura // 2, fill=TRILHO)
        atual = inicio_barra
        for categoria in categorias:
            valor = max(0, int(valores.get(categoria, 0)))
            if not valor:
                continue
            largura = int((fim_barra - inicio_barra) * valor / total)
            draw.rectangle((atual, y, min(fim_barra, atual + largura), y + barra_altura), fill=cores.get(categoria, PALETA[0]))
            if largura >= 62:
                draw.text((atual + largura / 2, y + barra_altura / 2), f"{valor / total:.0%}", font=fonte(13, True), fill=(255, 255, 255), anchor="mm")
            atual += largura
    if legenda:
        desenhar_legenda(draw, (x0 + 20, y1 - 20), categorias, cores, x1 - x0 - 40)


def desenhar_legenda(draw: ImageDraw.ImageDraw, xy: tuple[int, int], categorias: Sequence[str], cores: dict[str, tuple[int, int, int]], largura: int) -> None:
    x, y = xy
    inicio = x
    for categoria in categorias:
        largura_texto = draw.textbbox((0, 0), categoria, font=fonte(13))[2]
        if x + largura_texto + 42 > inicio + largura:
            x = inicio
            y += 24
        draw.rounded_rectangle((x, y + 4, x + 13, y + 17), radius=4, fill=cores.get(categoria, PALETA[0]))
        draw.text((x + 20, y), categoria, font=fonte(13), fill=TEXTO)
        x += largura_texto + 42


def desenhar_tecnologia_painel(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], dados: Sequence[tuple[str, Sequence[tuple[str, int]]]], titulo: str, cor) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    draw.rounded_rectangle((x0 + 20, y0 + 19, x0 + 32, y0 + 49), radius=5, fill=cor)
    draw.text((x0 + 46, y0 + 17), titulo, font=fonte(21, True), fill=NAVY)
    inicio = y0 + 70
    for indice, (tipo, itens) in enumerate(dados):
        y_tipo = inicio + indice * 112
        draw.text((x0 + 18, y_tipo), tipo, font=fonte(16, True), fill=TEXTO)
        if indice:
            draw.line((x0 + 18, y_tipo - 16, x1 - 18, y_tipo - 16), fill=GRID, width=1)
        maior = max([valor for _, valor in itens] or [1])
        for posicao, (rotulo, valor) in enumerate(itens[:3]):
            y = y_tipo + 34 + posicao * 25
            escrever_quebrado(draw, (x0 + 18, y - 8), texto(rotulo), 138, fonte(12), fill=TEXTO, max_linhas=1)
            inicio_barra, fim_barra = x0 + 165, x1 - 72
            desenhar_trilho(draw, inicio_barra, y, fim_barra, float(valor), float(maior), cor, altura=12)
            draw.text((x1 - 12, y), numero(valor), font=fonte(13, True), fill=TEXTO, anchor="rm")


def desenhar_aviso(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], titulo: str, texto_aviso: str) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    draw.ellipse((x0 + 20, y0 + 22, x0 + 50, y0 + 52), fill=(229, 133, 47))
    draw.text((x0 + 35, y0 + 37), "i", font=fonte(17, True), fill=(255, 255, 255), anchor="mm")
    draw.text((x0 + 65, y0 + 22), titulo, font=fonte(18, True), fill=NAVY)
    escrever_quebrado(draw, (x0 + 20, y0 + 78), texto_aviso, x1 - x0 - 40, fonte(15), fill=MUTED, max_linhas=6)


# ---------------------------------------------------------------------------
# Camada visual premium usada pelos PNGs finais.
#
# As funções acima permanecem para compatibilidade com entregas antigas. As
# definições abaixo são as usadas pelos geradores atuais: mesma paleta em
# todos os painéis, cartões arejados, hierarquia de slide e áreas de texto
# calculadas antes do desenho para evitar cortes e sobreposições.
# ---------------------------------------------------------------------------


def _fonte_ajustada(valor: object, largura: int, maior: int, menor: int = 13, negrito: bool = True) -> ImageFont.FreeTypeFont:
    rotulo = texto(valor)
    for tamanho in range(maior, menor - 1, -1):
        candidata = fonte(tamanho, negrito)
        if candidata.getbbox(rotulo)[2] <= largura:
            return candidata
    return fonte(menor, negrito)


def _texto_largura(valor: object, fnt: ImageFont.FreeTypeFont) -> int:
    return int(fnt.getbbox(texto(valor))[2])


def percentual(valor: int | float, total: int | float, casas: int = 0) -> str:
    if not total:
        return "—"
    return f"{float(valor) / float(total):.{casas}%}"


def _desenhar_icone_kpi(draw: ImageDraw.ImageDraw, centro: tuple[int, int], rotulo: str, cor, raio: int = 17) -> None:
    """Cria o pequeno ícone circular inspirado nos cards de referência."""
    cx, cy = centro
    suave = tuple(255 - int((255 - canal) * 0.10) for canal in cor)
    draw.ellipse((cx - raio, cy - raio, cx + raio, cy + raio), fill=suave, outline=cor, width=2)
    rotulo_upper = texto(rotulo).upper()
    if "STATUS" in rotulo_upper:
        draw.rectangle((cx - 7, cy - 6, cx + 7, cy + 7), outline=cor, width=2)
        draw.line((cx - 3, cy - 10, cx - 3, cy - 5), fill=cor, width=2)
        draw.line((cx + 3, cy - 10, cx + 3, cy - 5), fill=cor, width=2)
        draw.line((cx - 7, cy - 1, cx + 7, cy - 1), fill=cor, width=2)
    elif "MAIOR" in rotulo_upper:
        draw.line((cx - 8, cy + 6, cx - 2, cy, cx + 2, cy + 3, cx + 8, cy - 7), fill=cor, width=2)
        draw.line((cx + 4, cy - 7, cx + 8, cy - 7, cx + 8, cy - 3), fill=cor, width=2)
    else:
        draw.line((cx - 8, cy - 5, cx - 1, cy - 5), fill=cor, width=2)
        draw.line((cx - 8, cy, cx + 3, cy), fill=cor, width=2)
        draw.line((cx - 8, cy + 5, cx + 8, cy + 5), fill=cor, width=2)


def _desenhar_badge_kpi(draw: ImageDraw.ImageDraw, centro: tuple[int, int], rotulo: str, cor) -> None:
    """Selo circular discreto para reforçar o estado do indicador sem inventar métricas."""
    cx, cy = centro
    suave = tuple(255 - int((255 - canal) * 0.08) for canal in cor)
    draw.ellipse((cx - 15, cy - 15, cx + 15, cy + 15), fill=suave, outline=cor, width=2)
    rotulo_upper = texto(rotulo).upper()
    if "MAIOR" in rotulo_upper:
        selo = "MAX"
    elif "STATUS" in rotulo_upper:
        selo = "ON"
    else:
        selo = "Σ"
    selo_fonte = _fonte_ajustada(selo, 23, 10, 8, True)
    draw.text((cx, cy), selo, font=selo_fonte, fill=cor, anchor="mm")


def desenhar_kpi(
    draw: ImageDraw.ImageDraw,
    caixa: tuple[int, int, int, int],
    rotulo: str,
    valor: object,
    detalhe: str = "",
    cor=AZUL,
) -> None:
    """Desenha um card compacto com ícone, selo e conteúdo alinhado."""
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa, raio=22)
    draw.rounded_rectangle((x0 + 20, y0 + 20, x0 + 28, y0 + 47), radius=4, fill=cor)
    _desenhar_icone_kpi(draw, (x0 + 57, y0 + 37), rotulo, cor)
    _desenhar_badge_kpi(draw, (x1 - 38, y0 + 37), rotulo, cor)

    coluna_texto = x0 + 92
    largura_texto = x1 - coluna_texto - 76
    fonte_rotulo = _fonte_ajustada(rotulo.upper(), largura_texto, 12, 10, True)
    draw.text((coluna_texto, y0 + 37), texto(rotulo).upper(), font=fonte_rotulo, fill=MUTED, anchor="lm")
    valor_fonte = _fonte_ajustada(valor, largura_texto, 34, 22, True)
    draw.text((coluna_texto, y0 + 86), texto(valor), font=valor_fonte, fill=NAVY, anchor="lm")
    if detalhe:
        detalhe_fonte = _fonte_ajustada(detalhe, x1 - coluna_texto - 32, 13, 11, False)
        draw.text((coluna_texto, y0 + 127), texto(detalhe), font=detalhe_fonte, fill=AZUL, anchor="lm")


def desenhar_insight(
    draw: ImageDraw.ImageDraw,
    caixa: tuple[int, int, int, int],
    rotulo: str,
    mensagem: str,
    cor=ROXO,
) -> None:
    """Faixa de insight que torna o painel interpretável sem outro slide."""
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa, raio=20)
    draw.rounded_rectangle((x0 + 20, y0 + 18, x0 + 28, y1 - 18), radius=4, fill=cor)
    compacto = y1 - y0 < 76
    draw.text((x0 + 46, y0 + (10 if compacto else 16)), texto(rotulo).upper(), font=fonte(12, True), fill=cor)
    escrever_quebrado(
        draw,
        (x0 + 46, y0 + (31 if compacto else 40)),
        mensagem,
        x1 - x0 - 70,
        fonte(14 if compacto else 15, True),
        fill=NAVY,
        max_linhas=1 if compacto else 2,
        espacamento=1 if compacto else 2,
    )


def criar_canvas(
    titulo: str,
    subtitulo: str,
    altura: int = 1000,
    indice: str | int | None = None,
    periodo: str | None = None,
) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    imagem = Image.new("RGB", (1600, altura), FUNDO)
    draw = ImageDraw.Draw(imagem)

    # Cabeçalho de slide: branco, arredondado e com uma única cor de chamada.
    desenhar_cartao(draw, (48, 30, 1552, 170), raio=26)
    draw.rounded_rectangle((76, 60, 84, 139), radius=4, fill=AZUL)
    draw.text((110, 54), "STATE OF DATA BRASIL  /  CAMADA GOLD", font=fonte(13, True), fill=AZUL)
    titulo_fonte = _fonte_ajustada(titulo, 1225, 36, 26, True)
    draw.text((110, 82), texto(titulo), font=titulo_fonte, fill=NAVY)
    subtitulo_fonte = _fonte_ajustada(subtitulo, 1235, 16, 13, False)
    draw.text((110, 130), texto(subtitulo), font=subtitulo_fonte, fill=MUTED)

    # Identificação útil para a apresentação, sem competir com o título.
    if indice is not None:
        draw.rounded_rectangle((1390, 54, 1526, 96), radius=21, fill=(240, 235, 255))
        draw.text((1458, 75), f"PAINEL {int(indice):02d} / 08", font=fonte(11, True), fill=ROXO, anchor="mm")
        periodo_texto = periodo or PERIODO_PADRAO
        periodo_fonte = _fonte_ajustada(periodo_texto, 156, 11, 9, True)
        draw.text((1458, 124), periodo_texto, font=periodo_fonte, fill=MUTED, anchor="mm")
    return imagem, draw


def rodape(draw: ImageDraw.ImageDraw, texto_rodape: str, altura: int = 1000) -> None:
    linha_y = altura - 70
    draw.line((70, linha_y, 1530, linha_y), fill=BORDA, width=1)
    draw.ellipse((70, linha_y + 18, 78, linha_y + 26), fill=AZUL)
    escrever_quebrado(draw, (92, linha_y + 9), texto_rodape, 1415, fonte(13), fill=MUTED, max_linhas=2, espacamento=1)


def desenhar_cartao(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], raio: int = 22) -> None:
    """Desenha um card branco com sombra mínima e sem borda pesada."""
    x0, y0, x1, y1 = caixa
    draw.rounded_rectangle((x0 + 4, y0 + 6, x1 + 4, y1 + 6), radius=raio, fill=SOMBRA)
    draw.rounded_rectangle(caixa, radius=raio, fill=CARTAO, outline=BORDA, width=1)


def desenhar_trilho(draw: ImageDraw.ImageDraw, inicio: int, centro: float, fim: int, valor: float, maior: float, cor, altura: int = 16) -> None:
    if fim <= inicio:
        return
    y0 = int(centro - altura / 2)
    y1 = y0 + altura
    draw.rounded_rectangle((inicio, y0, fim, y1), radius=altura // 2, fill=TRILHO)
    if valor <= 0 or maior <= 0:
        return
    comprimento = int((fim - inicio) * max(0.0, float(valor)) / float(maior))
    direita = min(fim, inicio + max(comprimento, altura))
    draw.rounded_rectangle((inicio, y0, direita, y1), radius=altura // 2, fill=cor)


def desenhar_barras_horizontais(
    draw: ImageDraw.ImageDraw,
    caixa: tuple[int, int, int, int],
    itens: Sequence[tuple[str, int | float]],
    titulo: str,
    cor=AZUL,
    percentual: bool = False,
    maximo: float | None = None,
    tamanho_rotulo: int = 16,
    subtitulo: str | None = None,
) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    draw.text((x0 + 20, y0 + 18), texto(titulo), font=_fonte_ajustada(titulo, x1 - x0 - 40, 18, 15, True), fill=NAVY)
    texto_subtitulo = subtitulo or ("participação" if percentual else "respondentes")
    draw.text((x0 + 20, y0 + 46), texto(texto_subtitulo), font=fonte(12), fill=MUTED)
    itens = list(itens)
    if not itens:
        draw.text((x0 + 20, y0 + 105), "Sem dados suficientes", font=fonte(15), fill=MUTED)
        return

    topo, base = y0 + 84, y1 - 24
    # Cartões compactos podem ter cinco categorias; nesse caso, reduzir o
    # espaçamento mínimo evita que a última linha ultrapasse o cartão.
    altura_linha = max(28.0, (base - topo) / len(itens))
    largura_rotulo = min(220, max(145, int((x1 - x0) * 0.41)))
    inicio_barra, fim_barra, coluna_valor = x0 + largura_rotulo + 30, x1 - 72, x1 - 18
    maior = float(maximo or max(float(valor) for _, valor in itens) or 1)
    tamanho = min(tamanho_rotulo, 15)
    for indice, (rotulo, valor) in enumerate(itens):
        centro = topo + altura_linha * indice + altura_linha / 2
        fonte_rotulo = fonte(tamanho, False)
        linhas = 2 if altura_linha >= 45 else 1
        escrever_quebrado(draw, (x0 + 20, int(centro - (15 if linhas == 2 else 8))), rotulo, largura_rotulo - 24, fonte_rotulo, fill=TEXTO, max_linhas=linhas, espacamento=1)
        desenhar_trilho(draw, inicio_barra, centro, fim_barra, float(valor), maior, cor, altura=16)
        rotulo_valor = f"{float(valor):.1f}%" if percentual else numero(valor)
        draw.text((coluna_valor, centro), rotulo_valor, font=fonte(15, True), fill=NAVY, anchor="rm")


def desenhar_colunas(
    draw: ImageDraw.ImageDraw,
    caixa: tuple[int, int, int, int],
    itens: Sequence[tuple[str, int | float]],
    titulo: str,
    cor=AZUL,
    maximo: float | None = None,
    cores: Sequence[tuple[int, int, int]] | None = None,
) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    draw.text((x0 + 22, y0 + 20), texto(titulo), font=fonte(18, True), fill=NAVY)
    draw.text((x0 + 22, y0 + 48), "respostas válidas por edição", font=fonte(12), fill=MUTED)
    itens = list(itens)
    if not itens:
        draw.text((x0 + 22, y0 + 112), "Sem dados suficientes", font=fonte(15), fill=MUTED)
        return
    topo, base = y0 + 104, y1 - 72
    esquerda, direita = x0 + 78, x1 - 32
    maior = float(maximo or max(float(valor) for _, valor in itens) or 1)
    for nivel in range(5):
        valor_grade = maior * nivel / 4
        y = base - int((base - topo) * nivel / 4)
        draw.line((esquerda, y, direita, y), fill=GRID, width=1)
        draw.text((x0 + 22, y), numero(valor_grade), font=fonte(12), fill=MUTED, anchor="lm")

    passo = (direita - esquerda) / max(1, len(itens))
    largura = min(112, max(60, int(passo * 0.48)))
    for indice, (rotulo, valor) in enumerate(itens):
        centro = esquerda + passo * (indice + 0.5)
        altura = int((base - topo) * float(valor) / maior)
        esquerda_barra = int(centro - largura / 2)
        cor_barra = cores[indice % len(cores)] if cores else cor
        draw.rounded_rectangle((esquerda_barra, topo, esquerda_barra + largura, base), radius=12, fill=TRILHO)
        if altura > 0:
            draw.rounded_rectangle((esquerda_barra, base - altura, esquerda_barra + largura, base), radius=12, fill=cor_barra)
        draw.text((centro, max(topo + 14, base - altura - 28)), numero(valor), font=fonte(16, True), fill=NAVY, anchor="mm")
        escrever_quebrado(draw, (int(centro - largura / 2), base + 18), rotulo, largura, fonte(14), fill=TEXTO, max_linhas=1, espacamento=1)


def desenhar_stacked100(
    draw: ImageDraw.ImageDraw,
    caixa: tuple[int, int, int, int],
    linhas: Sequence[tuple[str, dict[str, int]]],
    categorias: Sequence[str],
    titulo: str,
    cores: dict[str, tuple[int, int, int]],
    legenda: bool = True,
    rotulo_largura: int = 170,
    mostrar_rotulos_pequenos: bool = False,
) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    draw.text((x0 + 20, y0 + 18), texto(titulo), font=_fonte_ajustada(titulo, x1 - x0 - 40, 18, 15, True), fill=NAVY)
    draw.text((x0 + 20, y0 + 46), "composição percentual dentro de cada grupo", font=fonte(12), fill=MUTED)
    linhas = list(linhas)
    if not linhas:
        draw.text((x0 + 20, y0 + 105), "Sem dados suficientes", font=fonte(15), fill=MUTED)
        return
    barra_altura = 27
    topo = y0 + 84
    base_disponivel = y1 - (43 if legenda else 24)
    passo = (base_disponivel - topo - barra_altura) / max(len(linhas) - 1, 1)
    inicio_barra, fim_barra = x0 + rotulo_largura + 18, x1 - 20

    def rotulo_percentual(valor: int, total: int) -> str:
        proporcao = valor / total
        return "<1%" if proporcao < 0.01 else f"{proporcao:.0%}"

    def distribuir_rotulos(itens: list[tuple[float, str, tuple[int, int, int]]]) -> list[tuple[float, str, tuple[int, int, int], float]]:
        fnt = fonte(11, True)
        posicionados = []
        for centro, rotulo_percentual_, cor in itens:
            largura_texto = draw.textbbox((0, 0), rotulo_percentual_, font=fnt)[2]
            centro_limitado = max(
                inicio_barra + largura_texto / 2,
                min(centro, fim_barra - largura_texto / 2),
            )
            posicionados.append([centro_limitado, largura_texto, rotulo_percentual_, cor, centro])

        espacamento = 5
        for posicao in range(1, len(posicionados)):
            anterior = posicionados[posicao - 1]
            atual = posicionados[posicao]
            minimo = anterior[0] + (anterior[1] + atual[1]) / 2 + espacamento
            atual[0] = max(atual[0], minimo)

        if posicionados:
            excesso = posicionados[-1][0] + posicionados[-1][1] / 2 - fim_barra
            if excesso > 0:
                for item in posicionados:
                    item[0] -= excesso
            falta = inicio_barra - (posicionados[0][0] - posicionados[0][1] / 2)
            if falta > 0:
                for item in posicionados:
                    item[0] += falta

        return [(item[0], item[2], item[3], item[4]) for item in posicionados]

    for indice, (rotulo, valores) in enumerate(linhas):
        y = int(topo + indice * passo)
        total = sum(max(0, int(valor)) for valor in valores.values())
        draw.text((x0 + 20, y + barra_altura / 2), texto(rotulo), font=fonte(15, True), fill=TEXTO, anchor="lm")
        draw.rounded_rectangle((inicio_barra, y, fim_barra, y + barra_altura), radius=barra_altura // 2, fill=TRILHO)
        if total <= 0:
            draw.text((inicio_barra + 12, y + barra_altura / 2), "sem respostas", font=fonte(12), fill=MUTED, anchor="lm")
            continue
        atual = float(inicio_barra)
        rotulos_pequenos = []
        categorias_presentes = [categoria for categoria in categorias if max(0, int(valores.get(categoria, 0)))]
        for posicao, categoria in enumerate(categorias_presentes):
            valor = max(0, int(valores.get(categoria, 0)))
            largura_exata = (fim_barra - inicio_barra) * valor / total
            fim_segmento = fim_barra if posicao == len(categorias_presentes) - 1 else atual + largura_exata
            esquerda_segmento = int(round(atual))
            direita_segmento = int(round(fim_segmento))
            if valor > 0 and direita_segmento <= esquerda_segmento:
                direita_segmento = min(fim_barra, esquerda_segmento + 1)
            if direita_segmento > esquerda_segmento:
                draw.rectangle((esquerda_segmento, y, direita_segmento, y + barra_altura), fill=cores.get(categoria, AZUL))

            centro_segmento = atual + largura_exata / 2
            if largura_exata >= 56:
                draw.text(
                    (centro_segmento, y + barra_altura / 2),
                    rotulo_percentual(valor, total),
                    font=fonte(12, True),
                    fill=(255, 255, 255),
                    anchor="mm",
                )
            elif mostrar_rotulos_pequenos:
                rotulos_pequenos.append(
                    (posicao, centro_segmento, rotulo_percentual(valor, total), cores.get(categoria, AZUL))
                )
            atual = fim_segmento

        if mostrar_rotulos_pequenos and rotulos_pequenos:
            for lado in (0, 1):
                itens = [item[1:] for item in rotulos_pequenos if (item[0] + indice) % 2 == lado]
                for centro_rotulo, rotulo_pequeno, cor, centro_segmento in distribuir_rotulos(itens):
                    acima = lado == 0
                    y_rotulo = y - 14 if acima else y + barra_altura + 15
                    y_linha = y if acima else y + barra_altura
                    y_fim_linha = y_rotulo + 6 if acima else y_rotulo - 6
                    draw.line(
                        (int(centro_segmento), y_linha, int(centro_rotulo), y_fim_linha),
                        fill=cor,
                        width=1,
                    )
                    draw.text(
                        (centro_rotulo, y_rotulo),
                        rotulo_pequeno,
                        font=fonte(11, True),
                        fill=NAVY,
                        anchor="mm",
                    )
    if legenda:
        desenhar_legenda(draw, (x0 + 20, y1 - 28), categorias, cores, x1 - x0 - 40)


def desenhar_legenda(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    categorias: Sequence[str],
    cores: dict[str, tuple[int, int, int]],
    largura: int,
) -> int:
    x, y = xy
    inicio = x
    fnt = fonte(13)
    for categoria in categorias:
        largura_texto = _texto_largura(categoria, fnt)
        if x != inicio and x + largura_texto + 42 > inicio + largura:
            x = inicio
            y += 23
        draw.rounded_rectangle((x, y + 4, x + 13, y + 17), radius=4, fill=cores.get(categoria, AZUL))
        draw.text((x + 20, y), texto(categoria), font=fnt, fill=TEXTO)
        x += largura_texto + 42
    return y


def desenhar_tecnologia_painel(
    draw: ImageDraw.ImageDraw,
    caixa: tuple[int, int, int, int],
    dados: Sequence[tuple[str, Sequence[tuple[str, int | float]]]],
    titulo: str,
    cor,
    percentual: bool = False,
) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    draw.rounded_rectangle((x0 + 20, y0 + 20, x0 + 29, y0 + 52), radius=4, fill=cor)
    draw.text((x0 + 45, y0 + 17), texto(titulo), font=fonte(22, True), fill=NAVY)
    draw.text((x1 - 20, y0 + 23), "TOP 3", font=fonte(11, True), fill=cor, anchor="ra")
    draw.text(
        (x1 - 20, y0 + 44),
        "participação" if percentual else "menções",
        font=fonte(11),
        fill=MUTED,
        anchor="ra",
    )
    dados = list(dados)
    if not dados:
        draw.text((x0 + 20, y0 + 100), "Sem dados suficientes", font=fonte(15), fill=MUTED)
        return
    altura_secao = max(94.0, (y1 - y0 - 92) / len(dados))
    inicio = y0 + 70
    for indice, (tipo, itens) in enumerate(dados):
        y_tipo = int(inicio + indice * altura_secao)
        if indice:
            draw.line((x0 + 20, y_tipo - 15, x1 - 20, y_tipo - 15), fill=GRID, width=1)
        draw.text((x0 + 18, y_tipo), texto(tipo), font=fonte(15, True), fill=TEXTO)
        itens = list(itens)[:3]
        maior = max([valor for _, valor in itens] or [1])
        for posicao, (rotulo, valor) in enumerate(itens):
            y = y_tipo + 31 + posicao * 22
            escrever_quebrado(draw, (x0 + 18, y - 7), rotulo, 138, fonte(12), fill=TEXTO, max_linhas=1)
            inicio_barra, fim_barra = x0 + 158, x1 - 72
            desenhar_trilho(draw, inicio_barra, y, fim_barra, float(valor), float(maior), cor, altura=12)
            rotulo_valor = f"{float(valor):.1f}%" if percentual else numero(valor)
            draw.text((x1 - 12, y), rotulo_valor, font=fonte(12, True), fill=NAVY, anchor="rm")


def desenhar_aviso(draw: ImageDraw.ImageDraw, caixa: tuple[int, int, int, int], titulo: str, texto_aviso: str) -> None:
    x0, y0, x1, y1 = caixa
    desenhar_cartao(draw, caixa)
    cor = LARANJA
    draw.ellipse((x0 + 20, y0 + 22, x0 + 50, y0 + 52), fill=cor)
    draw.text((x0 + 35, y0 + 37), "i", font=fonte(17, True), fill=(255, 255, 255), anchor="mm")
    draw.text((x0 + 65, y0 + 22), texto(titulo), font=_fonte_ajustada(titulo, x1 - x0 - 90, 18, 14, True), fill=NAVY)
    escrever_quebrado(draw, (x0 + 20, y0 + 78), texto_aviso, x1 - x0 - 40, fonte(15), fill=MUTED, max_linhas=4, espacamento=3)
