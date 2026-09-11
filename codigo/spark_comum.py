"""Funções Spark compartilhadas pela reprodução local dos gráficos Gold.

O Spark lê uma base de respondentes preparada na AWS, limpa a chave, remove
duplicidades, padroniza categorias e calcula as agregações analíticas. O
módulo ``comum`` continua sendo usado apenas para desenhar os PNGs com Pillow.

## Por que este arquivo aceita dois tipos de caminho?

Os gráficos foram ajustados para serem demonstrados e reproduzidos fora da
AWS, a partir de um export em nível de respondente gerado durante o pipeline
AWS. Por isso, no uso local, o Spark lê sempre o arquivo:

    dados/state_of_data_gold_export.csv

Essa escolha permite que qualquer pessoa que clone o repositório consiga
executar o processamento com uma cópia autorizada do export, sem configurar
credenciais AWS.

Dentro da AWS, a mesma função também consegue receber uma URI S3 quando for
reutilizada em um job Glue. O caminho abaixo é apenas um exemplo ilustrativo;
o bucket e os prefixos reais continuam sendo os configurados no ambiente AWS:

    caminho_base_aws = "s3://<bucket-do-projeto>/silver/<base-corrigida>/"
    df = carregar_dados_spark(spark, caminho_base_aws)

Assim, ``Path`` representa o export quando a execução é local e ``s3://``
representa uma origem que pode ser usada em um ambiente Glue. Esta versão não
substitui os jobs AWS nem altera o bucket; ela apenas reproduz localmente as
análises para facilitar a auditoria e a execução pelo GitHub.
"""

from __future__ import annotations

import ctypes
import os
import re
import sys
from pathlib import Path
from typing import Callable, Iterable, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from codigo.comum import INPUT_PADRAO


NORMALIZAR_DE = "áàãâäéêëíîïóôõöúûüç"
NORMALIZAR_PARA = "aaaaaeeeiiioooouuuc"


def _caminho_curto_windows(caminho: str) -> str:
    """Evita falhas do spark-submit em caminhos Windows com acentos."""
    if os.name != "nt":
        return caminho
    buffer = ctypes.create_unicode_buffer(32768)
    tamanho = ctypes.windll.kernel32.GetShortPathNameW(caminho, buffer, len(buffer))
    return buffer.value if tamanho else caminho


def _configurar_ambiente_spark() -> None:
    """Configura Python, Java e SPARK_HOME para execução local no Windows."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if os.name != "nt":
        return

    if not os.environ.get("JAVA_HOME"):
        candidatos_java = [
            *Path(r"C:\Program Files\Java").glob("*/bin/java.exe"),
            *Path(r"C:\Program Files\Eclipse Adoptium").glob("*/bin/java.exe"),
        ]
        if candidatos_java:
            os.environ["JAVA_HOME"] = str(candidatos_java[0].parent.parent)

    if not os.environ.get("SPARK_HOME"):
        import pyspark

        caminho_spark = str(Path(pyspark.__file__).resolve().parent)
        os.environ["SPARK_HOME"] = _caminho_curto_windows(caminho_spark)


def iniciar_spark(nome: str = "GoldGraficosStateOfData") -> SparkSession:
    """Cria uma sessão Spark local para execução no VS Code."""
    _configurar_ambiente_spark()
    spark = (
        SparkSession.builder
        .appName(nome)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def obter_dados(df: DataFrame | None, nome: str) -> tuple[DataFrame, SparkSession | None]:
    """Usa um DataFrame compartilhado ou abre uma sessão para execução isolada."""
    if df is not None:
        return df, None
    spark = iniciar_spark(nome)
    return carregar_dados_spark(spark), spark


def _coluna(coluna: str | Column) -> Column:
    return F.col(coluna) if isinstance(coluna, str) else coluna


def texto_col(coluna: str | Column) -> Column:
    """Retorna uma coluna textual sem nulos e sem espaços nas extremidades."""
    return F.trim(F.coalesce(_coluna(coluna).cast("string"), F.lit("")))


def normalizar_col(coluna: str | Column) -> Column:
    """Normaliza caixa, acentos e espaços para comparações semânticas."""
    valor = F.lower(texto_col(coluna))
    valor = F.translate(valor, NORMALIZAR_DE, NORMALIZAR_PARA)
    return F.regexp_replace(valor, r"\s+", " ")


def _origem_spark(caminho: str | Path) -> str:
    """Valida uma origem local ou preserva uma URI remota para o Spark.

    A verificação ``Path.exists()`` é feita somente em arquivos locais. Uma
    URI ``s3://`` deve ser resolvida pelo conector do Spark disponível no AWS
    Glue, e não pelo sistema de arquivos do computador que executa este
    repositório.
    """
    origem = str(caminho)
    if origem.startswith(("s3://", "s3a://")):
        return origem

    caminho_local = Path(caminho)
    if not caminho_local.exists():
        raise FileNotFoundError(f"CSV não encontrado: {caminho_local}")
    return str(caminho_local)


def carregar_dados_spark(
    spark: SparkSession,
    caminho: str | Path | None = None,
) -> DataFrame:
    """Lê o export local ou uma base em S3 e prepara as três edições.

    Em execução local, ``caminho`` normalmente é um ``Path`` para o export
    CSV. Em um job Glue, pode ser uma URI como
    ``s3://<bucket>/silver/<base-corrigida>/``.
    """
    entrada = caminho or INPUT_PADRAO
    origem = _origem_spark(entrada)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .csv(origem)
    )
    obrigatorias = {"ano_pesquisa", "id_resposta"}
    ausentes = obrigatorias.difference(df.columns)
    if ausentes:
        raise ValueError(f"O CSV não contém as colunas obrigatórias: {sorted(ausentes)}")

    df = (
        df.withColumn("ano_pesquisa", F.col("ano_pesquisa").cast("int"))
        .withColumn("id_resposta", texto_col("id_resposta"))
        .filter(F.col("ano_pesquisa").isNotNull())
        .filter(F.col("id_resposta") != "")
        .dropDuplicates(["ano_pesquisa", "id_resposta"])
    )

    colunas_textuais = [
        "linguagem_preferida",
        "cloud_preferida",
        "bi_preferida",
        "ferramenta_de_bi_preferida",
        "tecnologia_data_lake",
        "tecnologia_data_warehouse",
    ]
    for coluna in colunas_textuais:
        if coluna not in df.columns:
            df = df.withColumn(coluna, F.lit(""))

    anos_anteriores = F.col("ano_pesquisa").isin(2023, 2024)
    df = (
        df.withColumn(
            "linguagem_corrigida",
            F.when(anos_anteriores, texto_col("cloud_preferida"))
            .otherwise(texto_col("linguagem_preferida")),
        )
        .withColumn(
            "cloud_corrigida",
            F.when(anos_anteriores, texto_col("ferramenta_de_bi_preferida"))
            .otherwise(texto_col("cloud_preferida")),
        )
        .withColumn(
            "bi_corrigida",
            F.when(anos_anteriores, texto_col("bi_preferida"))
            .otherwise(texto_col("ferramenta_de_bi_preferida")),
        )
    )
    return df.cache()


def anos(df: DataFrame) -> list[int]:
    return [
        int(linha["ano_pesquisa"])
        for linha in (
            df.select("ano_pesquisa")
            .distinct()
            .orderBy("ano_pesquisa")
            .collect()
        )
    ]


def quantidade_respostas(df: DataFrame) -> int:
    return df.count()


def respondentes_por_ano(df: DataFrame) -> dict[int, int]:
    """Retorna o volume acumulado de respostas únicas por edição."""
    return {
        int(linha["ano_pesquisa"]): int(linha["quantidade"])
        for linha in (
            df.groupBy("ano_pesquisa")
            .count()
            .withColumnRenamed("count", "quantidade")
            .orderBy("ano_pesquisa")
            .collect()
        )
    }


def totais_por_ano(
    df: DataFrame,
    coluna: str,
    normalizador: Callable[[str | Column], Column] | None = None,
    excluir: Iterable[str] = (),
) -> dict[int, int]:
    """Conta respostas válidas por edição para formar denominadores comparáveis."""
    resultado = {ano: 0 for ano in anos(df)}
    if coluna not in df.columns:
        return resultado

    categoria = normalizador(F.col(coluna)) if normalizador else texto_col(coluna)
    base = df.select("ano_pesquisa", categoria.alias("categoria")).filter(F.col("categoria") != "")
    excluir_lista = list(excluir)
    if excluir_lista:
        base = base.filter(~F.col("categoria").isin(*excluir_lista))

    for linha in (
        base.groupBy("ano_pesquisa")
        .count()
        .withColumnRenamed("count", "quantidade")
        .collect()
    ):
        resultado[int(linha["ano_pesquisa"])] = int(linha["quantidade"])
    return resultado


def padronizar_funcao_col(coluna: str | Column) -> Column:
    s = normalizar_col(coluna)
    return (
        F.when(s == "", "Não informado")
        .when(s.contains("analise de dados") | s.contains("business intelligence") | s.rlike(r"(^| )bi($| )"), "Análise de dados e BI")
        .when(s.contains("engenharia de dados") | s.contains("engenharia de machine"), "Engenharia de dados")
        .when(s.contains("ciencia de dados") | s.contains("machine learning") | s.contains("cientista"), "Ciência de dados e IA")
        .when(s.contains("desenvolvimento") | s.contains("software"), "Desenvolvimento de software")
        .when(s.contains("gestao") | s.contains("lider"), "Gestão e liderança")
        .when(s.contains("nao atuo") | s.contains("fora da area"), "Fora da área de dados")
        .otherwise(F.substring(texto_col(coluna), 1, 28))
    )


def padronizar_nivel_col(coluna: str | Column) -> Column:
    s = normalizar_col(coluna)
    return (
        F.when(s == "", "Não informado")
        .when(s.contains("junior"), "Júnior")
        .when(s.contains("pleno"), "Pleno")
        .when(s.contains("senior"), "Sênior")
        .when(s.contains("especialista") | s.contains("staff"), "Especialista/Staff")
        .otherwise("Outro nível")
    )


def padronizar_genero_col(coluna: str | Column) -> Column:
    s = normalizar_col(coluna)
    return (
        F.when(s == "", "Não informado")
        .when(s.contains("masculino"), "Masculino")
        .when(s.contains("feminino"), "Feminino")
        .when(s.contains("nao") | s.contains("prefiro"), "Prefere não informar")
        .otherwise("Outro")
    )


def padronizar_regiao_col(coluna: str | Column) -> Column:
    s = normalizar_col(coluna)
    return (
        F.when(s == "sudeste", "Sudeste")
        .when(s == "sul", "Sul")
        .when(s == "nordeste", "Nordeste")
        .when(s.isin("centro oeste", "centro-oeste"), "Centro-Oeste")
        .when(s == "norte", "Norte")
        .when(s == "", "Não informado")
        .otherwise(texto_col(coluna))
    )


def padronizar_salario_col(coluna: str | Column) -> Column:
    s = normalizar_col(coluna)
    primeiro_valor = F.regexp_replace(
        F.regexp_extract(s, r"(\d{1,3}(?:\.\d{3})?)", 1),
        r"\.",
        "",
    ).cast("int")
    acima_de_20_mil = s.contains("acima de") & (primeiro_valor >= 20000)
    return (
        F.when(s == "", "Não informado")
        # A base original divide algumas faixas em intervalos menores
        # (por exemplo, 2.001–3.000 e 3.001–4.000). O primeiro valor
        # numérico permite consolidar todos esses intervalos corretamente.
        .when(acima_de_20_mil | (primeiro_valor > 20000), "Acima de R$ 20 mil")
        .when(primeiro_valor <= 2000, "Até R$ 2 mil")
        .when(primeiro_valor <= 4000, "R$ 2 a 4 mil")
        .when(primeiro_valor <= 6000, "R$ 4 a 6 mil")
        .when(primeiro_valor <= 8000, "R$ 6 a 8 mil")
        .when(primeiro_valor <= 12000, "R$ 8 a 12 mil")
        .when(primeiro_valor <= 16000, "R$ 12 a 16 mil")
        .when(primeiro_valor <= 20000, "R$ 16 a 20 mil")
        .otherwise(F.substring(texto_col(coluna), 1, 22))
    )


def padronizar_tecnologia_col(coluna: str | Column) -> Column:
    s = normalizar_col(coluna)
    return (
        F.when(s == "", "Não informado")
        .when(s.contains("amazon web services"), "AWS")
        .when(s.contains("amazon quicksight"), "Amazon QuickSight")
        .when(s.contains("microsoft powerbi"), "Power BI")
        .when(s.contains("google cloud"), "Google Cloud")
        .when(s.contains("cloud propria"), "Cloud própria")
        .when(s.contains("databriks"), "Databricks")
        .when(s.contains("nao tenho preferencia / nao sei opinar") | s.contains("nao sei opinar / nao tenho preferencia"), "Sem preferência")
        .when(s.contains("servidores on premise/nao utilizamos cloud"), "Sem cloud/on-premises")
        .otherwise(F.substring(texto_col(coluna), 1, 22))
    )


def padronizar_prioridade_col(coluna: str | Column) -> Column:
    s = normalizar_col(coluna)
    return (
        F.when(s == "", "Não informado")
        .when(s.contains("principal prioridade") & ~s.contains("proximos"), "Principal prioridade")
        .when(s.contains("principais prioridades") | s.contains("proximos 2-4 anos"), "Prioridade para os próximos anos")
        .when(s.contains("mais ou menos"), "Iniciativa, sem prioridade")
        .when(s.contains("nao e uma iniciativa") | s.contains("nao tem sido uma prioridade"), "Não é prioridade")
        .when(s.contains("nao sei"), "Não sabe opinar")
        .otherwise("Outra resposta")
    )


def padronizar_resultado_col(coluna: str | Column) -> Column:
    s = normalizar_col(coluna)
    return (
        F.when(s == "", "Não informado")
        .when(s == "nao informado", "Não informado")
        .when(s.contains("nao sei"), "Não sabe opinar")
        .when(s.contains("parcial") | s.contains("alguns"), "Parcialmente")
        .when(s.contains("fase de investigacao") | s.contains("investigacao e planejamento"), "Não — em investigação")
        .when(s.contains("ainda nao comecamos") | s.contains("nenhum projeto"), "Não — não iniciado")
        .when(s.startswith("sim") | s.contains("bons resultados"), "Sim")
        .when(s.startswith("nao"), "Não")
        .otherwise(F.substring(texto_col(coluna), 1, 22))
    )


def contagens(
    df: DataFrame,
    coluna: str,
    normalizador: Callable[[str | Column], Column] | None = None,
    limite: int | None = None,
    excluir: Iterable[str] = (),
) -> list[tuple[str, int]]:
    """Agrupa uma categoria com ``groupBy`` e retorna poucos valores para o desenho."""
    if coluna not in df.columns:
        return []
    categoria = normalizador(F.col(coluna)) if normalizador else texto_col(coluna)
    agrupado = df.select(categoria.alias("categoria")).filter(F.col("categoria") != "")
    excluir_lista = list(excluir)
    if excluir_lista:
        agrupado = agrupado.filter(~F.col("categoria").isin(*excluir_lista))
    agrupado = (
        agrupado.groupBy("categoria")
        .count()
        .withColumnRenamed("count", "quantidade")
        .orderBy(F.desc("quantidade"), F.asc("categoria"))
    )
    if limite:
        agrupado = agrupado.limit(limite)
    return [(str(linha["categoria"]), int(linha["quantidade"])) for linha in agrupado.collect()]


def contagens_por_ano(
    df: DataFrame,
    coluna: str,
    normalizador: Callable[[str | Column], Column] | None = None,
    limite: int = 5,
    excluir: Iterable[str] = (),
) -> dict[int, list[tuple[str, int]]]:
    if coluna not in df.columns:
        return {ano: [] for ano in anos(df)}
    categoria = normalizador(F.col(coluna)) if normalizador else texto_col(coluna)
    base = df.select("ano_pesquisa", categoria.alias("categoria")).filter(F.col("categoria") != "")
    excluir_lista = list(excluir)
    if excluir_lista:
        base = base.filter(~F.col("categoria").isin(*excluir_lista))
    agrupado = base.groupBy("ano_pesquisa", "categoria").count().withColumnRenamed("count", "quantidade")
    janela = Window.partitionBy("ano_pesquisa").orderBy(F.desc("quantidade"), F.asc("categoria"))
    agrupado = agrupado.withColumn("posicao", F.row_number().over(janela)).filter(F.col("posicao") <= limite)
    resultado = {ano: [] for ano in anos(df)}
    for linha in agrupado.orderBy("ano_pesquisa", "posicao").collect():
        resultado.setdefault(int(linha["ano_pesquisa"]), []).append((str(linha["categoria"]), int(linha["quantidade"])))
    return resultado


def contagens_por_ano_e_grupo(
    df: DataFrame,
    grupo_coluna: str,
    categoria_coluna: str,
    normalizador_grupo: Callable[[str | Column], Column],
    normalizador_categoria: Callable[[str | Column], Column],
    grupos: Sequence[str],
    categorias: Sequence[str],
) -> dict[int, dict[str, dict[str, int]]]:
    if grupo_coluna not in df.columns or categoria_coluna not in df.columns:
        return {
            ano: {grupo_nome: {categoria_nome: 0 for categoria_nome in categorias} for grupo_nome in grupos}
            for ano in anos(df)
        }
    grupo = normalizador_grupo(F.col(grupo_coluna)).alias("grupo")
    categoria = normalizador_categoria(F.col(categoria_coluna)).alias("categoria")
    base = (
        df.select("ano_pesquisa", grupo, categoria)
        .filter(F.col("grupo").isin(*list(grupos)))
        .filter(F.col("categoria") != "")
    )
    agrupado = base.groupBy("ano_pesquisa", "grupo", "categoria").count().withColumnRenamed("count", "quantidade")
    resultado: dict[int, dict[str, dict[str, int]]] = {
        ano: {grupo_nome: {categoria_nome: 0 for categoria_nome in categorias} for grupo_nome in grupos}
        for ano in anos(df)
    }
    for linha in agrupado.collect():
        ano = int(linha["ano_pesquisa"])
        grupo_nome = str(linha["grupo"])
        categoria_nome = str(linha["categoria"])
        if ano in resultado and grupo_nome in resultado[ano] and categoria_nome in resultado[ano][grupo_nome]:
            resultado[ano][grupo_nome][categoria_nome] = int(linha["quantidade"])
    return resultado


def contagens_padrao(
    df: DataFrame,
    coluna: str,
    padroes: Sequence[tuple[str, Sequence[str]]],
    limite: int | None = None,
) -> list[tuple[str, int]]:
    """Conta respostas que contenham cada conjunto de termos, em Spark."""
    if coluna not in df.columns:
        return []
    texto = normalizar_col(F.col(coluna))
    expressoes = []
    for rotulo, termos in padroes:
        condicao = None
        for termo in termos:
            parcial = texto.contains(termo)
            condicao = parcial if condicao is None else (condicao | parcial)
        expressoes.append(F.sum(F.when(condicao, 1).otherwise(0)).alias(rotulo))
    linha = df.agg(*expressoes).collect()[0]
    itens = [(rotulo, int(linha[rotulo] or 0)) for rotulo, _ in padroes]
    itens = [(rotulo, valor) for rotulo, valor in itens if valor > 0]
    itens.sort(key=lambda item: (-item[1], item[0]))
    return itens[:limite] if limite else itens


def contagens_padrao_por_ano(
    df: DataFrame,
    coluna: str,
    padroes: Sequence[tuple[str, Sequence[str]]],
    limite: int | None = None,
) -> dict[int, list[tuple[str, int]]]:
    resultado = {}
    for ano in anos(df):
        resultado[ano] = contagens_padrao(df.filter(F.col("ano_pesquisa") == ano), coluna, padroes, limite)
    return resultado


def contagens_tecnologia(df: DataFrame, coluna: str, limite: int = 3) -> list[tuple[str, int]]:
    if coluna not in df.columns:
        return []
    valores = (
        df.select(F.explode(F.split(texto_col(coluna), r"\s*,\s*")).alias("valor"))
        .select(padronizar_tecnologia_col(F.col("valor")).alias("categoria"))
        .filter(~F.col("categoria").isin("", "Não informado"))
        .groupBy("categoria")
        .count()
        .withColumnRenamed("count", "quantidade")
        .orderBy(F.desc("quantidade"), F.asc("categoria"))
        .limit(limite)
    )
    return [(str(linha["categoria"]), int(linha["quantidade"])) for linha in valores.collect()]


def contagens_tecnologia_por_ano(df: DataFrame, coluna: str, limite: int = 3) -> dict[int, list[tuple[str, int]]]:
    resultado = {}
    for ano in anos(df):
        resultado[ano] = contagens_tecnologia(df.filter(F.col("ano_pesquisa") == ano), coluna, limite)
    return resultado
