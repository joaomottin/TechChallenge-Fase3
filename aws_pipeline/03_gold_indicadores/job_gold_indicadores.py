"""Job AWS Glue Spark - camada Gold do projeto State of Data Brasil.

Objetivo
--------
Ler a Silver corrigida pelo Glue Catalog, remover apenas chaves repetidas para
contagem, corrigir os nomes semânticos que mudaram entre as edições da pesquisa
e gerar tabelas Gold agregadas para as perguntas do PDF do projeto.

Saídas no S3
------------
- gold/GoldResumo/
- gold/GoldPerfil/
- gold/GoldRemuneracao/
- gold/GoldDiversidade/
- gold/GoldTecnologias/
- gold/GoldIA/
- gold/GoldTrabalho/
- gold/GoldMotivosIA/
- gold/GoldControleQualidade/

Cada pasta recebe um CSV com cabeçalho, quantidade, total de respondentes e
percentual. O Athena pode consultar diretamente essas pastas depois do crawler
ou da criação das tabelas no Glue Catalog.
"""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

BUCKET = "fiap-techchallenge3-2026"
GOLD_BASE = f"s3://{BUCKET}/gold"
DATABASE = "fiap_techchallenge3_2026"
SILVER_TABLE = "silver_silvercorrigido"

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


def texto(nome: str) -> F.Column:
    """Retorna uma coluna de texto mesmo se a coluna não existir."""
    if nome in silver.columns:
        return F.trim(F.col(nome).cast("string"))
    return F.lit(None).cast("string")


def preenchido(nome: str) -> F.Column:
    coluna = texto(nome)
    return coluna.isNotNull() & (coluna != "")


def rotulo_funcao() -> F.Column:
    coluna = F.lower(texto("funcao_atuacao"))
    return (
        F.when(coluna.rlike("an.lise de dados|analise de dados|bi:"), F.lit("Análise de dados e BI"))
        .when(coluna.rlike("engenharia de dados"), F.lit("Engenharia de dados"))
        .when(coluna.rlike("ci.ncia de dados|machine learning"), F.lit("Ciência de dados e IA"))
        .when(coluna.rlike("n.o atuo na .rea de dados|n.o atuo"), F.lit("Fora da área de dados"))
        .when(coluna.isNull() | (coluna == ""), F.lit("Não informado"))
        .otherwise(F.lit("Outra atuação"))
    )


def rotulo_nivel() -> F.Column:
    coluna = F.lower(texto("nivel"))
    return (
        F.when(coluna.contains("júnior") | coluna.contains("junior"), F.lit("Júnior"))
        .when(coluna.contains("pleno"), F.lit("Pleno"))
        .when(coluna.contains("sênior") | coluna.contains("senior"), F.lit("Sênior"))
        .when(coluna.contains("especialista") | coluna.contains("staff"), F.lit("Especialista/Staff"))
        .when(coluna.isNull() | (coluna == ""), F.lit("Não informado"))
        .otherwise(F.lit("Outro nível"))
    )


def rotulo_genero() -> F.Column:
    coluna = F.lower(texto("genero"))
    return (
        F.when(coluna.contains("masculino"), F.lit("Masculino"))
        .when(coluna.contains("feminino"), F.lit("Feminino"))
        .when(coluna.contains("prefiro") | coluna.contains("n.o informar"), F.lit("Prefere não informar"))
        .when(coluna.isNull() | (coluna == ""), F.lit("Não informado"))
        .otherwise(F.lit("Outro"))
    )


def rotulo_salario() -> F.Column:
    coluna = F.lower(texto("faixa_salarial"))
    return (
        F.when(coluna.contains("at. r$ 2.000") | coluna.contains("at. 2.000"), F.lit("Até R$ 2 mil"))
        .when(coluna.contains("2.001") & coluna.contains("4.000"), F.lit("R$ 2 a 4 mil"))
        .when(coluna.contains("4.001") & coluna.contains("6.000"), F.lit("R$ 4 a 6 mil"))
        .when(coluna.contains("6.001") & coluna.contains("8.000"), F.lit("R$ 6 a 8 mil"))
        .when(coluna.contains("8.001") & coluna.contains("12.000"), F.lit("R$ 8 a 12 mil"))
        .when(coluna.contains("12.001") & coluna.contains("16.000"), F.lit("R$ 12 a 16 mil"))
        .when(coluna.contains("16.001") & coluna.contains("20.000"), F.lit("R$ 16 a 20 mil"))
        .when(coluna.contains("acima de 20.000"), F.lit("Acima de R$ 20 mil"))
        .when(coluna.isNull() | (coluna == ""), F.lit("Não informado"))
        .otherwise(texto("faixa_salarial"))
    )


def rotulo_tecnologia(coluna: F.Column) -> F.Column:
    valor = F.lower(coluna)
    return (
        F.when(valor.contains("amazon web services"), F.lit("AWS"))
        .when(valor.contains("amazon quicksight"), F.lit("Amazon QuickSight"))
        .when(valor.contains("microsoft powerbi"), F.lit("Power BI"))
        .when(valor.contains("google cloud"), F.lit("Google Cloud"))
        .when(valor.contains("cloud pr.pria") | valor.contains("cloud própria"), F.lit("Cloud própria"))
        .when(valor.contains("databriks"), F.lit("Databricks"))
        .when(valor.contains("n.o tenho prefer.ncia") | valor.contains("n.o sei opinar"), F.lit("Sem preferência"))
        .when(valor.contains("servidores on premise") | valor.contains("n.o utilizamos cloud"), F.lit("Sem cloud/on-premises"))
        .otherwise(F.trim(coluna))
    )


def rotulo_prioridade_ia() -> F.Column:
    coluna = F.lower(texto("idade_q3_e"))
    return (
        F.when(coluna.contains("principal prioridade") & ~coluna.contains("próximos") & ~coluna.contains("proximos"), F.lit("Principal prioridade"))
        .when(coluna.contains("principais prioridades") | coluna.contains("próximos 2-4 anos") | coluna.contains("proximos 2-4 anos"), F.lit("Prioridade para os próximos anos"))
        .when(coluna.contains("mais ou menos"), F.lit("Iniciativa, sem prioridade"))
        .when(coluna.contains("n.o . uma iniciativa") | coluna.contains("n.o tem sido uma prioridade"), F.lit("Não é prioridade"))
        .when(coluna.contains("n.o sei"), F.lit("Não sabe opinar"))
        .when(coluna.isNull() | (coluna == ""), F.lit("Não informado"))
        .otherwise(F.lit("Outra resposta"))
    )


def rotulo_resultado_llm() -> F.Column:
    coluna = F.lower(texto("q3_h_empresa_esta_conseguindo_ter_bons_resultados_com_llms"))
    return (
        F.when(coluna.contains("em partes") | coluna.contains("alguns projetos"), F.lit("Parcialmente"))
        .when(coluna.contains("n.o sei"), F.lit("Não sabe opinar"))
        .when(coluna.startswith("sim"), F.lit("Sim"))
        .when(coluna.startswith("n.o") | coluna.startswith("não"), F.lit("Não"))
        .when(coluna.isNull() | (coluna == ""), F.lit("Não informado"))
        .otherwise(F.lit("Outra resposta"))
    )


def escrever_csv(frame: DataFrame, nome: str) -> None:
    """Grava uma saída Gold em CSV com cabeçalho e nome simples de pasta."""
    caminho = f"{GOLD_BASE}/{nome}/"
    (
        frame.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .csv(caminho)
    )


def agregar(frame: DataFrame, dimensoes: list[str], nome: str) -> None:
    """Conta respondentes distintos por ano e categoria e calcula percentual."""
    filtro = frame
    for dimensao in dimensoes:
        filtro = filtro.filter(F.col(dimensao).isNotNull() & (F.trim(F.col(dimensao)) != ""))

    agrupado = filtro.groupBy(*dimensoes).agg(
        F.countDistinct("_id_unico").alias("quantidade")
    )
    janela = Window.partitionBy("ano_pesquisa")
    resultado = (
        agrupado.withColumn("total_respondentes", F.sum("quantidade").over(janela))
        .withColumn(
            "percentual",
            F.round(F.col("quantidade") / F.col("total_respondentes") * F.lit(100), 2),
        )
        .orderBy(*dimensoes)
    )
    escrever_csv(resultado, nome)


# Leitura da Silver por meio do Glue Catalog.
silver = (
    glue_context.create_dynamic_frame.from_catalog(
        database=DATABASE,
        table_name=SILVER_TABLE,
        transformation_ctx="LeituraSilverCorrigido",
    )
    .toDF()
)

linhas_antes = silver.count()

silver = (
    silver.withColumn("ano_pesquisa", F.col("ano_pesquisa").cast("int"))
    .withColumn("id_resposta", texto("id_resposta"))
    .filter(F.col("ano_pesquisa").isNotNull() & preenchido("id_resposta"))
    .withColumn("_id_unico", F.concat_ws("::", F.col("ano_pesquisa"), F.col("id_resposta")))
)

linhas_chaves_repetidas = linhas_antes - silver.select("_id_unico").distinct().count()
silver = silver.dropDuplicates(["ano_pesquisa", "id_resposta"])

# Colunas amigáveis para consumo analítico e para os gráficos do PDF.
silver = (
    silver.withColumn("regiao_amigavel", F.coalesce(texto("regiao"), F.lit("Não informado")))
    .withColumn("setor_amigavel", F.coalesce(texto("setor"), F.lit("Não informado")))
    .withColumn("funcao_atuacao_amigavel", rotulo_funcao())
    .withColumn("nivel_amigavel", rotulo_nivel())
    .withColumn("genero_amigavel", rotulo_genero())
    .withColumn("faixa_salarial_amigavel", rotulo_salario())
    .withColumn("modelo_trabalho_amigavel", F.coalesce(texto("modelo_trabalho_ideal"), F.lit("Não informado")))
    .withColumn("prioridade_ia", rotulo_prioridade_ia())
    .withColumn("resultado_llm", rotulo_resultado_llm())
)

# O dicionário mostrou que os campos 4.c, 4.f, 4.h e 4.k mudaram de assunto.
# A correção mantém a Silver original e cria nomes semânticos estáveis no Gold.
silver = (
    silver.withColumn(
        "linguagem_preferida_corrigida",
        F.when(F.col("ano_pesquisa").isin(2023, 2024), texto("cloud_preferida"))
        .otherwise(texto("linguagem_preferida")),
    )
    .withColumn(
        "cloud_preferida_corrigida",
        F.when(F.col("ano_pesquisa").isin(2023, 2024), texto("ferramenta_de_bi_preferida"))
        .otherwise(texto("cloud_preferida")),
    )
    .withColumn(
        "bi_preferida_corrigida",
        F.when(F.col("ano_pesquisa").isin(2023, 2024), texto("bi_preferida"))
        .otherwise(texto("ferramenta_de_bi_preferida")),
    )
)

# 1. Total de respondentes por ano.
agregar(silver, ["ano_pesquisa"], "GoldResumo")

# 2. Perfil profissional e estrutura do mercado pesquisado.
agregar(
    silver,
    ["ano_pesquisa", "regiao_amigavel", "setor_amigavel", "funcao_atuacao_amigavel", "nivel_amigavel"],
    "GoldPerfil",
)

# 3. Faixa salarial por senioridade.
agregar(silver, ["ano_pesquisa", "nivel_amigavel", "faixa_salarial_amigavel"], "GoldRemuneracao")

# 4. Diversidade por gênero e nível.
agregar(silver, ["ano_pesquisa", "nivel_amigavel", "genero_amigavel"], "GoldDiversidade")

# 5. Região e modelo de trabalho.
agregar(silver, ["ano_pesquisa", "regiao_amigavel", "modelo_trabalho_amigavel"], "GoldTrabalho")

# 6. Tecnologias. Respostas múltiplas são abertas em categorias individuais.
tecnologias = []
for coluna, tipo in [
    ("linguagem_preferida_corrigida", "Linguagem"),
    ("cloud_preferida_corrigida", "Cloud"),
    ("bi_preferida_corrigida", "Ferramenta de BI"),
    ("tecnologia_data_lake", "Data Lake"),
    ("tecnologia_data_warehouse", "Data Warehouse"),
]:
    valores = (
        silver.select(
            "ano_pesquisa",
            "_id_unico",
            F.lit(tipo).alias("tipo_tecnologia"),
            F.explode(F.split(F.regexp_replace(texto(coluna), r"\s*,\s*", ","), ",")).alias("tecnologia"),
        )
        .withColumn("tecnologia", rotulo_tecnologia(F.col("tecnologia")))
        .filter(F.col("tecnologia").isNotNull() & (F.trim(F.col("tecnologia")) != ""))
    )
    tecnologias.append(valores)

tecnologias_long = tecnologias[0]
for valores in tecnologias[1:]:
    tecnologias_long = tecnologias_long.unionByName(valores)

tec_agrupadas = tecnologias_long.groupBy("ano_pesquisa", "tipo_tecnologia", "tecnologia").agg(
    F.countDistinct("_id_unico").alias("quantidade")
)
janela_tec = Window.partitionBy("ano_pesquisa", "tipo_tecnologia")
tec_resultado = (
    tec_agrupadas.withColumn("total_respondentes", F.sum("quantidade").over(janela_tec))
    .withColumn("percentual", F.round(F.col("quantidade") / F.col("total_respondentes") * 100, 2))
    .orderBy("ano_pesquisa", "tipo_tecnologia", F.desc("quantidade"))
)
escrever_csv(tec_resultado, "GoldTecnologias")

# 7. Prioridade, formas de uso e resultados de IA.
prioridade = silver.select(
    "ano_pesquisa", "_id_unico", F.lit("Prioridade de IA").alias("tipo_indicador"), F.col("prioridade_ia").alias("categoria")
).filter(F.col("categoria") != "Não informado")

uso_ia = [
    ("Uso independente", texto("ia_uso_empresa").rlike("(?i)colaboradores (utilizando|usando).*independente")),
    ("Uso em desenvolvimento", texto("ia_uso_empresa").rlike("(?i)desenvolvimento.*copilot")),
    ("Uso em processos internos", texto("ia_uso_empresa").rlike("(?i)processos internos|produtos internos")),
    ("Uso em produtos para clientes", texto("ia_uso_empresa").rlike("(?i)produtos externos|clientes finais")),
    ("Direcionamento centralizado", texto("ia_uso_empresa").rlike("(?i)direcionamento centralizado")),
    ("Ainda não é prioridade", texto("ia_uso_empresa").rlike("(?i)n.o tenho visto|n.o . prioridade")),
]
uso_array = F.array(*[F.when(condicao, F.lit(rotulo)) for rotulo, condicao in uso_ia])
uso = silver.select("ano_pesquisa", "_id_unico", F.explode(uso_array).alias("categoria")).withColumn(
    "tipo_indicador", F.lit("Forma de uso de IA")
).filter(F.col("categoria").isNotNull())

resultado_llm = silver.select(
    "ano_pesquisa", "_id_unico", F.lit("Resultado com LLM").alias("tipo_indicador"), F.col("resultado_llm").alias("categoria")
).filter(F.col("categoria") != "Não informado")

ia_long = prioridade.unionByName(uso.select(prioridade.columns)).unionByName(resultado_llm)
ia_agrupada = ia_long.groupBy("ano_pesquisa", "tipo_indicador", "categoria").agg(
    F.countDistinct("_id_unico").alias("quantidade")
)
janela_ia = Window.partitionBy("ano_pesquisa", "tipo_indicador")
ia_resultado = (
    ia_agrupada.withColumn("total_respondentes", F.sum("quantidade").over(janela_ia))
    .withColumn("percentual", F.round(F.col("quantidade") / F.col("total_respondentes") * 100, 2))
    .orderBy("ano_pesquisa", "tipo_indicador", F.desc("quantidade"))
)
escrever_csv(ia_resultado, "GoldIA")

# 8. Motivos para não utilizar IA, tratados como categorias independentes.
motivos = [
    ("Falta de expertise ou recursos", texto("q3_g_motivos_para_nao_usar_ai_generativa_e_llm").rlike("(?i)falta de expertise|falta de recursos")),
    ("Falta de compreensão dos casos de uso", texto("q3_g_motivos_para_nao_usar_ai_generativa_e_llm").rlike("(?i)falta de compreens.o")),
    ("Dados ainda não estão prontos", texto("q3_g_motivos_para_nao_usar_ai_generativa_e_llm").rlike("(?i)dados da empresa n.o est.o prontos")),
    ("ROI ainda não comprovado", texto("q3_g_motivos_para_nao_usar_ai_generativa_e_llm").rlike("(?i)retorno sobre investimento|roi n.o comprovado")),
    ("Segurança e privacidade", texto("q3_g_motivos_para_nao_usar_ai_generativa_e_llm").rlike("(?i)seguran.a e privacidade")),
    ("Baixa qualidade das respostas", texto("q3_g_motivos_para_nao_usar_ai_generativa_e_llm").rlike("(?i)baixa qualidade|alucina..o")),
    ("Propriedade intelectual", texto("q3_g_motivos_para_nao_usar_ai_generativa_e_llm").rlike("(?i)propriedade intelectual")),
]
motivos_array = F.array(*[F.when(condicao, F.lit(rotulo)) for rotulo, condicao in motivos])
motivos_long = silver.select("ano_pesquisa", "_id_unico", F.explode(motivos_array).alias("motivo")).filter(
    F.col("motivo").isNotNull()
)
motivos_agrupados = motivos_long.groupBy("ano_pesquisa", "motivo").agg(
    F.countDistinct("_id_unico").alias("quantidade")
)
janela_motivos = Window.partitionBy("ano_pesquisa")
motivos_resultado = (
    motivos_agrupados.withColumn("total_respondentes", F.sum("quantidade").over(janela_motivos))
    .withColumn("percentual", F.round(F.col("quantidade") / F.col("total_respondentes") * 100, 2))
    .orderBy("ano_pesquisa", F.desc("quantidade"))
)
escrever_csv(motivos_resultado, "GoldMotivosIA")

# Controle simples da execução para comprovar o tratamento no S3.
controle = spark.createDataFrame(
    [
        ("linhas_lidas", str(linhas_antes), "Linhas lidas da tabela Silver pelo Glue Catalog"),
        ("respostas_unicas", str(silver.count()), "Respostas distintas por ano e id_resposta usadas nos indicadores"),
        ("chaves_repetidas_excluidas", str(linhas_chaves_repetidas), "Chaves repetidas excluídas apenas das contagens Gold"),
        ("colunas_gold", "quantidade, total_respondentes, percentual", "Medidas presentes nas tabelas agregadas"),
        ("observacao", "entre os respondentes", "A pesquisa não representa todo o mercado brasileiro"),
    ],
    ["indicador", "valor", "descricao"],
)
escrever_csv(controle, "GoldControleQualidade")

job.commit()
