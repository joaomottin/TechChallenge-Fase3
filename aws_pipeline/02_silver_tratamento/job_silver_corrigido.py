"""Job Spark do AWS Glue para criar a camada Silver da pesquisa State of Data Brasil.

Objetivo do job
----------------
Ler os três CSVs da camada Bronze diretamente do Amazon S3, tratar as respostas
com PySpark e consolidar as edições 2023, 2024 e 2025-2026 em uma única base.
Cada edição possui respondentes diferentes; por isso, a consolidação correta é
UNION (empilhar linhas) e não JOIN (cruzar pessoas entre os anos).

Etapas documentadas no notebook
--------------------------------
1. Ler e validar o crosswalk manual recebido pelo grupo.
2. Selecionar somente as seções e perguntas relacionadas ao escopo do projeto.
3. Renomear as colunas para nomes canônicos, simples e consistentes.
4. Limpar textos, espaços e valores vazios.
5. Padronizar idade, data/hora e respostas binárias para tipos adequados.
6. Adicionar ano_pesquisa e arquivo_origem para rastreabilidade.
7. Remover somente linhas exatamente duplicadas e registrar a qualidade.
8. Unir os anos por nome de coluna e validar o resultado antes da gravação.

Saídas da camada Silver
------------------------
- SilverCorrigido: base tratada em CSV, fácil de abrir e consultar.
- SilverCorrigidoParquet: cópia da base em Parquet, particionada por ano_pesquisa
  para facilitar consultas analíticas e leitura por ferramentas de dados.
- DicionarioDados: relação entre código da pergunta, origem e coluna Silver.
- ControleQualidade: linhas, colunas, anos, nulos e possíveis chaves repetidas.

A camada Bronze original nunca é alterada. A camada Gold não é criada por este
job; ela deve ser produzida separadamente a partir da Silver já validada.
"""

# These are the sections that support the questions in the Tech Challenge PDF.
# A seção 0 é mantida para identificar a resposta e o horário do envio.from __future__ import annotations

import re
import sys
import unicodedata
from collections import defaultdict
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession, functions as F


REQUIRED_ARGS = [
    "JOB_NAME",
    "SOURCE_BUCKET",
    "BRONZE_PREFIX",
    "CROSSWALK_PATH",
    "SILVER_PATH",
    "DICTIONARY_PATH",
    "QUALITY_PATH",
]

# These are the sections that support the questions in the Tech Challenge PDF.
# Section 0 is kept only for answer identity and submission timestamp.
KEEP_SECTIONS = {"0", "1", "2", "3", "4", "5", "6", "7", "8"}

# Estas perguntas não são necessárias para as comparações do PDF. A Bronze permanece
# intacta; removê-las somente na Silver é reversível e mantém o foco da análise.
DROP_CODES = {
    "1.g",       # lives in Brazil (current UF/region are retained)
    "1.h",       # country of residence (the project focuses on Brazilian regions)
    "1.j",       # lives in the state of formation
    "1.k",       # state of origin parent question
    "1.k.1",     # UF of origin
    "1.k.2",     # region of origin
}

# Opções de seleção múltipla podem vir como 0/1, TRUE/FALSE ou Sim/Não, conforme
# a edição da pesquisa. Subperguntas geográficas e de idade são categóricas e
# não devem ser convertidas para binário mesmo quando possuem três componentes.
NON_BINARY_SUBCODES = {
    "1.a.1",
    "1.i.1",
    "1.i.2",
    "1.k.1",
    "1.k.2",
    "4.a.1",
}

TRUE_TOKENS = {
    "1",
    "1.0",
    "TRUE",
    "T",
    "VERDADEIRO",
    "SIM",
    "YES",
}
FALSE_TOKENS = {
    "0",
    "0.0",
    "FALSE",
    "F",
    "FALSO",
    "NAO",
    "NÃO",
    "NO",
}


def s3_parts(uri: str) -> Tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"Caminho S3 inválido: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def normalize_text(value: object) -> str:
    """Normaliza cabeçalhos e rótulos para comparação e criação de nomes."""
    if value is None:
        return ""
    text = str(value).replace("\ufeff", "").strip()
    text = text.replace('"', "'")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*([,._])\s*", r"\1", text)
    return text.lower()


def slugify(value: object, max_length: int = 58) -> str:
    text = str(value or "")
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = text.replace("&", " e ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:max_length].rstrip("_")


def code_section(code: str) -> str:
    return str(code).strip().split(".", 1)[0]


def code_is_kept(code: str) -> bool:
    clean = str(code or "").strip()
    return code_section(clean) in KEEP_SECTIONS and clean not in DROP_CODES


def canonical_label(row: Mapping[str, str]) -> str:
    for year in (2025, 2024, 2023):
        label = (row.get(f"rotulo_{year}") or "").strip()
        if label:
            return label
    return ""


def label_name(label: str, code: str) -> Optional[str]:
    """Retorna nomes simples para os principais campos usados na análise do PDF."""
    normalized = normalize_text(label)
    if str(code).strip() == "0" or normalized == "id":
        return "id_resposta"
    rules = (
        ("data/hora_envio", "data_envio"),
        ("hora_envio", "data_envio"),
        ("token", "id_resposta"),
        ("faixa idade", "faixa_idade"),
        ("idade", "idade"),
        ("genero", "genero"),
        ("cor/raca/etnia", "raca_etnia"),
        ("cor/raca", "raca_etnia"),
        ("pcd", "pcd"),
        ("uf onde mora", "uf"),
        ("uf_onde_mora", "uf"),
        ("regiao onde mora", "regiao"),
        ("regiao_onde_mora", "regiao"),
        ("estado onde mora", "estado"),
        ("estado_onde_mora", "estado"),
        ("nivel de ensino", "nivel_ensino"),
        ("nivel_de_ensino", "nivel_ensino"),
        ("area de formacao", "area_formacao"),
        ("area_de_formacao", "area_formacao"),
        ("situacao de trabalho", "situacao_trabalho"),
        ("situacao_de_trabalho", "situacao_trabalho"),
        ("setor", "setor"),
        ("numero de funcionarios", "num_funcionarios"),
        ("numero_de_funcionarios", "num_funcionarios"),
        ("atua como gestor", "atua_como_gestor"),
        ("atua_como_gestor", "atua_como_gestor"),
        ("cargo como gestor", "cargo_gestor"),
        ("cargo_como_gestor", "cargo_gestor"),
        ("cargo atual", "cargo_atual"),
        ("cargo_atual", "cargo_atual"),
        ("nivel", "nivel"),
        ("faixa salarial", "faixa_salarial"),
        ("faixa_salarial", "faixa_salarial"),
        ("tempo de experiencia em dados", "experiencia_dados"),
        ("tempo_de_experiencia_em_dados", "experiencia_dados"),
        ("tempo de experiencia em ti", "experiencia_ti"),
        ("tempo_de_experiencia_em_ti", "experiencia_ti"),
        ("satisfeito atualmente", "satisfeito_atual"),
        ("satisfeito_atualmente", "satisfeito_atual"),
        ("modelo de trabalho atual", "modelo_trabalho_atual"),
        ("modelo_de_trabalho_atual", "modelo_trabalho_atual"),
        ("modelo de trabalho ideal", "modelo_trabalho_ideal"),
        ("modelo_de_trabalho_ideal", "modelo_trabalho_ideal"),
        ("atitude em caso de retorno presencial", "atitude_retorno_presencial"),
        ("atitude_em_caso_de_retorno_presencial", "atitude_retorno_presencial"),
        ("empresa passou por layoff", "teve_layoff"),
        ("empresa_passou_por_layoff", "teve_layoff"),
        ("numero de pessoas em dados", "num_pessoas_dados"),
        ("numero_de_pessoas_em_dados", "num_pessoas_dados"),
        ("cargos no time de dados", "cargos_time_dados"),
        ("cargos_no_time_de_dados", "cargos_time_dados"),
        ("funcao de atuacao", "funcao_atuacao"),
        ("funcao_de_atuacao", "funcao_atuacao"),
        ("atuacao em dados", "atuacao_em_dados"),
        ("atuacao_em_dados", "atuacao_em_dados"),
        ("fontes de dados", "fontes_dados"),
        ("fontes_de_dados", "fontes_dados"),
        ("linguagem mais usada", "linguagem_principal"),
        ("linguagem_mais_usada", "linguagem_principal"),
        ("linguagem preferida", "linguagem_preferida"),
        ("linguagem_preferida", "linguagem_preferida"),
        ("banco de dados", "bancos_dados"),
        ("banco_de_dados", "bancos_dados"),
        ("cloud preferida", "cloud_preferida"),
        ("cloud_preferida", "cloud_preferida"),
        ("ferramenta de bi preferida", "bi_preferida"),
        ("ferramenta_de_bi_preferida", "bi_preferida"),
        ("ferramenta de qualidade de dados", "qualidade_dados"),
        ("ferramentas_de_qualidade_de_dados", "qualidade_dados"),
        ("ai generativa e llm e uma prioridade", "ia_prioridade"),
        ("ai_generativa_e_llm_e_uma_prioridade", "ia_prioridade"),
        ("empresa esta conseguindo ter bons resultados com llms", "ia_resultados_llm"),
        ("empresa_esta_conseguindo_ter_bons_resultados_com_llms", "ia_resultados_llm"),
        ("usa chatgpt ou copilot", "usa_chatgpt_copilot"),
        ("usa_chatgpt_ou_copilot", "usa_chatgpt_copilot"),
        ("objetivo na area de dados", "objetivo_area_dados"),
        ("objetivo_na_area_de_dados", "objetivo_area_dados"),
        ("oportunidade buscada", "oportunidade_buscada"),
        ("oportunidade_buscada", "oportunidade_buscada"),
        ("tempo em busca de oportunidade", "tempo_busca_oportunidade"),
        ("tempo_em_busca_de_oportunidade", "tempo_busca_oportunidade"),
        ("experiencia em processos seletivos", "experiencia_seletivos"),
        ("experiencia_em_processos_seletivos", "experiencia_seletivos"),
        ("maior tempo gasto como de", "tempo_de"),
        ("maior_tempo_gasto_como_de", "tempo_de"),
        ("maior tempo gasto como da", "tempo_da"),
        ("maior_tempo_gasto_como_da", "tempo_da"),
        ("maior tempo gasto como ds", "tempo_ds"),
        ("maior_tempo_gasto_como_ds", "tempo_ds"),
        ("possui data lake", "possui_data_lake"),
        ("possui_data_lake", "possui_data_lake"),
        ("tecnologia data lake", "tecnologia_data_lake"),
        ("tecnologia_data_lake", "tecnologia_data_lake"),
        ("possui data warehouse", "possui_data_warehouse"),
        ("possui_data_warehouse", "possui_data_warehouse"),
        ("tecnologia data warehouse", "tecnologia_data_warehouse"),
        ("tecnologia_data_warehouse", "tecnologia_data_warehouse"),
        ("motivo insatisfacao", "motivo_insatisfacao"),
        ("motivo_insatisfacao", "motivo_insatisfacao"),
        ("participou de entrevistas", "entrevistas_ultimos_6m"),
        ("participou_de_entrevistas", "entrevistas_ultimos_6m"),
        ("planos de mudar de emprego", "plano_mudar_emprego"),
        ("planos_de_mudar_de_emprego", "plano_mudar_emprego"),
        ("criterios para escolha de emprego", "criterios_escolha_emprego"),
        ("criterios_para_escolha_de_emprego", "criterios_escolha_emprego"),
        ("responsabilidades como gestor", "responsabilidades_gestor"),
        ("responsabilidades_como_gestor", "responsabilidades_gestor"),
        ("desafios como gestor", "desafios_gestor"),
        ("desafios_como_gestor", "desafios_gestor"),
        ("tipo de uso de ai generativa", "ia_uso_empresa"),
        ("tipo_de_uso_de_ai_generativa", "ia_uso_empresa"),
        ("tipo de uso de ai generativa e llm", "ia_uso_empresa"),
        ("tipo_de_uso_de_ai_generativa_e_llm", "ia_uso_empresa"),
        ("motivos para nao usar ai", "motivos_nao_ia"),
        ("motivos_para_nao_usar_ai", "motivos_nao_ia"),
    )
    for needle, name in rules:
        if needle in normalized:
            return name
    return None


def option_prefix(code: str, label: str) -> str:
    """Cria nomes legíveis para opções e caixas de seleção das seções da pesquisa."""
    normalized = normalize_text(label)
    code_parts = str(code).split(".")
    first = ".".join(code_parts[:2]) if len(code_parts) >= 2 else code

    # Para opções tecnológicas, um prefixo semântico é mais útil do que o
    # número bruto da pergunta na pesquisa.
    if any(word in normalized for word in ("python", "sql", "linguagem", "java", "scala", "javascript", "r ")):
        return "usa_"
    if any(word in normalized for word in ("mysql", "oracle", "postgres", "database", "banco", "dynamodb", "mongodb", "redshift", "athena", "snowflake", "databricks")):
        return "usa_banco_"
    if any(word in normalized for word in ("amazon web services", "aws", "google cloud", "gcp", "azure", "oracle cloud", "ibm", "on premise", "cloud propria")):
        return "usa_cloud_"
    if any(word in normalized for word in ("powerbi", "power bi", "tableau", "qlik", "looker", "metabase", "quicksight", "excel", "ferramenta de bi")):
        return "usa_bi_"
    if any(word in normalized for word in ("airflow", "glue", "etl", "talend", "pentaho", "fivetran", "databricks", "script python", "stored procedure")):
        return "usa_etl_"

    prefixes = {
        "1.e": "experiencia_prejudicada_",
        "1.f": "aspecto_prejudicado_",
        "2.l": "insatisfacao_",
        "2.o": "criterio_emprego_",
        "3.b": "tem_cargo_",
        "3.c": "responsabilidade_gestor_",
        "3.d": "desafio_gestor_",
        "3.f": "ia_uso_empresa_",
        "3.g": "motivo_nao_ia_",
        "3.h": "motivo_nao_ia_",
        "4.b": "fonte_dado_",
        "4.i": "ia_uso_empresa_",
        "4.j": "ia_produtividade_",
        "4.l": "ia_uso_empresa_",
        "4.m": "ia_produtividade_",
        "6.a": "rotina_de_",
        "6.b": "usa_etl_de_",
        "6.h": "tempo_de_",
        "7.a": "rotina_da_",
        "7.b": "usa_etl_da_",
        "7.c": "autonomia_da_",
        "7.d": "tempo_da_",
        "8.a": "rotina_ds_",
        "8.b": "tecnica_ds_",
        "8.c": "tecnologia_ds_",
        "8.d": "tempo_ds_",
    }
    if first in prefixes:
        return prefixes[first]
    return f"q{slugify(code)}_"


def target_name(row: Mapping[str, str]) -> str:
    manual = (row.get("coluna_unificada_final") or "").strip()
    if manual:
        return slugify(manual, max_length=70)

    code = (row.get("codigo") or "").strip()
    label = canonical_label(row)
    base = label_name(label, code)
    if base:
        return base

    if len(code.split(".")) >= 3:
        option = slugify(label, max_length=48)
        return f"{option_prefix(code, label)}{option or slugify(code)}"

    return f"q{slugify(code)}_{slugify(label) or 'resposta'}"


def binary_code(code: str) -> bool:
    clean = str(code or "").strip()
    return len(clean.split(".")) >= 3 and clean not in NON_BINARY_SUBCODES


def resolve_column(original: str, raw_columns: Sequence[str]) -> Optional[str]:
    if original in raw_columns:
        return original
    wanted = normalize_text(original)
    matches = [column for column in raw_columns if normalize_text(column) == wanted]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"Coluna ambígua após normalização: {original!r} -> {matches}")
    return None


def read_crosswalk(spark: SparkSession, path: str) -> List[Dict[str, str]]:
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )
    expected = {
        "codigo",
        "presente_em",
        "rotulo_2023",
        "coluna_original_2023",
        "rotulo_2024",
        "coluna_original_2024",
        "rotulo_2025",
        "coluna_original_2025",
        "coluna_unificada_final",
        "ok_ou_ajustar",
    }
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Crosswalk sem as colunas obrigatórias: {sorted(missing)}")
    rows = []
    for row in df.collect():
        values = row.asDict()
        rows.append({key: str(values.get(key) or "").strip() for key in expected})
    if not rows:
        raise ValueError("Crosswalk vazio")
    return rows


def list_bronze_keys(s3_client, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    continuation = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation
        response = s3_client.list_objects_v2(**kwargs)
        keys.extend(
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].lower().endswith(".csv")
        )
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")
    return keys


def find_year_keys(keys: Iterable[str]) -> Dict[int, str]:
    names = {key: key.rsplit("/", 1)[-1].lower() for key in keys}
    patterns = {
        2023: ("2023-2024",),
        2024: ("2024-2025",),
        2025: ("2025-2026",),
    }
    found: Dict[int, str] = {}
    for year, year_patterns in patterns.items():
        matches = [key for key, name in names.items() if any(pattern in name for pattern in year_patterns)]
        if len(matches) != 1:
            raise FileNotFoundError(
                f"Esperava exatamente um CSV Bronze para {year}; encontrados: {matches}"
            )
        found[year] = matches[0]
    return found


def clean_string(column):
    trimmed = F.trim(F.regexp_replace(column, r"\s+", " "))
    return F.when(trimmed.isNull() | (trimmed == ""), F.lit(None)).otherwise(trimmed)


def clean_binary(column):
    normalized = F.upper(F.trim(column))
    # Valores numéricos podem usar ponto ou vírgula como separador decimal.
    numeric = F.regexp_replace(normalized, ",", ".")
    return (
        F.when(normalized.isNull() | (normalized == ""), F.lit(None).cast("int"))
        .when(normalized.isin(*TRUE_TOKENS) | numeric.rlike(r"^1(?:\.0+)?$"), F.lit(1))
        .when(normalized.isin(*FALSE_TOKENS) | numeric.rlike(r"^0(?:\.0+)?$"), F.lit(0))
        .otherwise(F.lit(None).cast("int"))
    )


def clean_timestamp(column):
    text = clean_string(column)
    return F.coalesce(
        F.to_timestamp(text, "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(text, "dd/MM/yyyy HH:mm"),
        F.to_timestamp(text, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(text, "yyyy-MM-dd'T'HH:mm:ss"),
    )


def build_mapping(
    crosswalk: Sequence[Mapping[str, str]],
    raw_columns: Mapping[int, Sequence[str]],
) -> Tuple[Dict[int, Dict[str, str]], Dict[str, str], List[Dict[str, str]], List[str]]:
    """Monta os mapeamentos origem -> Silver e o dicionário de dados."""
    renames: Dict[int, Dict[str, str]] = {2023: {}, 2024: {}, 2025: {}}
    kinds: Dict[str, str] = {}
    dictionary_rows: List[Dict[str, str]] = []
    used_targets: Dict[str, str] = {}
    errors: List[str] = []

    for row in crosswalk:
        code = row.get("codigo", "").strip()
        if not code_is_kept(code):
            continue

        label = canonical_label(row)
        desired = target_name(row)
        if desired in used_targets and used_targets[desired] != code:
            # The survey changed the technical identifier from ``id`` in 2023
            # to ``token``/``0.a_token`` in 2024-2025. They are the same
            # business key and must land in one Silver column.
            identifier_codes = {"0", "0.a"}
            same_identifier = (
                desired == "id_resposta"
                and {used_targets[desired], code}.issubset(identifier_codes)
            )
            if not same_identifier:
                desired = f"{desired}_q{slugify(code)}"
        used_targets[desired] = code

        present_years: List[str] = []
        source_names: Dict[str, str] = {}
        for year in (2023, 2024, 2025):
            original = row.get(f"coluna_original_{year}", "").strip()
            if not original:
                continue
            resolved = resolve_column(original, raw_columns[year])
            if not resolved:
                errors.append(f"{year}: {original!r} (código {code})")
                continue
            renames[year][resolved] = desired
            present_years.append(str(year))
            source_names[str(year)] = resolved

        if not source_names:
            continue

        if desired in kinds and kinds[desired] != "binary" and binary_code(code):
            errors.append(f"coluna final com tipos conflitantes: {desired}")
        if binary_code(code):
            kinds[desired] = "binary"
        elif desired == "idade":
            kinds[desired] = "integer"
        elif desired == "data_envio":
            kinds[desired] = "timestamp"
        else:
            kinds.setdefault(desired, "string")

        dictionary_rows.append(
            {
                "codigo_pergunta": code,
                "coluna_silver": desired,
                "presente_em": ",".join(present_years),
                "rotulo_2023": row.get("rotulo_2023", ""),
                "rotulo_2024": row.get("rotulo_2024", ""),
                "rotulo_2025": row.get("rotulo_2025", ""),
                "origem_2023": source_names.get("2023", ""),
                "origem_2024": source_names.get("2024", ""),
                "origem_2025": source_names.get("2025", ""),
            }
        )

    for year in (2023, 2024, 2025):
        reverse: Dict[str, str] = {}
        for source, target in renames[year].items():
            if target in reverse and reverse[target] != source:
                # Duas colunas de origem para o mesmo destino poderiam
                # sobrescrever dados; por isso o job para antes de gravar a Silver.
                errors.append(
                    f"{year}: mais de uma coluna original para {target}: "
                    f"{reverse[target]!r} e {source!r}"
                )
            reverse[target] = source

    return renames, kinds, dictionary_rows, errors


def transform_year(
    spark: SparkSession,
    year: int,
    path: str,
    rename_map: Mapping[str, str],
    kinds: Mapping[str, str],
) -> Tuple[DataFrame, int, List[str]]:
    raw = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    raw_count = raw.count()
    if raw_count == 0:
        raise ValueError(f"CSV vazio: {path}")

    # A seleção é feita preservando os nomes originais, evitando que o Spark interprete
    # a pontuação dos cabeçalhos de 2023 como sintaxe de campo aninhado.
    selected_sources = [source for source in raw.columns if source in rename_map]
    if not selected_sources:
        raise ValueError(f"Nenhuma coluna do ano {year} foi mapeada pelo crosswalk")

    selected = raw.select(*[F.col(f"`{source}`").alias(rename_map[source]) for source in selected_sources])
    expressions = []
    for name in selected.columns:
        kind = kinds.get(name, "string")
        column = F.col(f"`{name}`")
        if kind == "binary":
            expression = clean_binary(column).alias(name)
        elif kind == "integer":
            expression = F.regexp_replace(clean_string(column), ",", ".").cast("double").cast("int").alias(name)
        elif kind == "timestamp":
            expression = clean_timestamp(column).alias(name)
        else:
            expression = clean_string(column).alias(name)
        expressions.append(expression)

    treated = selected.select(*expressions)
    treated = treated.withColumn("ano_pesquisa", F.lit(year).cast("int"))
    treated = treated.withColumn("arquivo_origem", F.lit(path.rsplit("/", 1)[-1]))

    ordered = ["ano_pesquisa", "arquivo_origem"] + [
        column for column in treated.columns if column not in {"ano_pesquisa", "arquivo_origem"}
    ]
    return treated.select(*ordered), raw_count, selected_sources


def quality_metrics(consolidated: DataFrame, raw_counts: Mapping[int, int], selected_columns: Sequence[str], linhas_duplicadas_exatas_removidas: int) -> List[Tuple[str, str, str]]:
    metrics: List[Tuple[str, str, str]] = []
    metrics.append(("linhas_duplicadas_exatas_removidas", str(linhas_duplicadas_exatas_removidas), "linhas exatamente iguais removidas antes da gravação"))
    final_count = consolidated.count()
    metrics.append(("linhas_bronze_total", str(sum(raw_counts.values())), "soma das linhas lidas dos três CSVs"))
    metrics.append(("linhas_silver_total", str(final_count), "linhas após tratamento e union"))
    metrics.append(("colunas_silver", str(len(selected_columns)), "inclui ano_pesquisa e arquivo_origem"))
    metrics.append(("anos_presentes", ",".join(str(row[0]) for row in consolidated.select("ano_pesquisa").distinct().orderBy("ano_pesquisa").collect()), "anos encontrados"))

    null_id = consolidated.where(F.col("id_resposta").isNull()).count() if "id_resposta" in consolidated.columns else final_count
    metrics.append(("id_resposta_nulo", str(null_id), "deve ser zero"))

    duplicate_ids = 0
    if "id_resposta" in consolidated.columns:
        duplicate_ids = (
            consolidated.where(F.col("id_resposta").isNotNull())
            .groupBy("ano_pesquisa", "id_resposta")
            .count()
            .where(F.col("count") > 1)
            .count()
        )
    metrics.append(("chaves_ano_id_duplicadas", str(duplicate_ids), "registradas; não removidas automaticamente"))

    for column in ("idade", "genero", "regiao", "cargo_atual", "nivel", "faixa_salarial"):
        if column in consolidated.columns:
            nulls = consolidated.where(F.col(column).isNull()).count()
            metrics.append((f"nulos_{column}", str(nulls), "contagem de nulos"))

    return metrics


def write_csv(df: DataFrame, path: str, coalesce_one: bool = True) -> None:
    writer = (
        df.coalesce(1) if coalesce_one else df
    ).write.mode("overwrite").option("header", True).option("sep", ",").option("quote", '"').option("escape", '"')
    writer.csv(path)


def write_parquet(df: DataFrame, path: str, partition_column: Optional[str] = None) -> None:
    """Grava um DataFrame em Parquet e pode particioná-lo por ano."""
    writer = df.write.mode("overwrite")
    if partition_column and partition_column in df.columns:
        writer = writer.partitionBy(partition_column)
    writer.parquet(path)


def main() -> None:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    source_bucket = args["SOURCE_BUCKET"]
    bronze_prefix = args["BRONZE_PREFIX"].lstrip("/")
    crosswalk_path = args["CROSSWALK_PATH"]

    s3_client = boto3.client("s3")
    bronze_keys = list_bronze_keys(s3_client, source_bucket, bronze_prefix)
    year_keys = find_year_keys(bronze_keys)
    year_paths = {year: f"s3://{source_bucket}/{key}" for year, key in year_keys.items()}

    crosswalk = read_crosswalk(spark, crosswalk_path)
    raw_columns: Dict[int, Sequence[str]] = {}
    # Lê apenas os cabeçalhos uma vez por ano antes de montar o mapeamento.
    for year, path in year_paths.items():
        raw_columns[year] = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .csv(path)
            .columns
        )

    renames, kinds, dictionary_rows, mapping_errors = build_mapping(crosswalk, raw_columns)
    if mapping_errors:
        preview = "\n".join(mapping_errors[:30])
        raise ValueError(f"Erros no mapeamento do crosswalk ({len(mapping_errors)}):\n{preview}")

    if not dictionary_rows:
        raise ValueError("Nenhuma coluna relevante foi selecionada para a Silver")

    treated_frames: List[DataFrame] = []
    raw_counts: Dict[int, int] = {}
    selected_by_year: Dict[int, List[str]] = {}
    for year in (2023, 2024, 2025):
        frame, raw_count, selected_sources = transform_year(
            spark, year, year_paths[year], renames[year], kinds
        )
        treated_frames.append(frame)
        raw_counts[year] = raw_count
        selected_by_year[year] = selected_sources

    consolidated = treated_frames[0]
    for frame in treated_frames[1:]:
        consolidated = consolidated.unionByName(frame, allowMissingColumns=True)

    # Remove somente registros completamente idênticos. Respostas diferentes
    # com a mesma chave continuam disponíveis para investigação no controle.
    linhas_antes_deduplicacao = consolidated.count()
    consolidated = consolidated.dropDuplicates()
    linhas_duplicadas_exatas_removidas = (
        linhas_antes_deduplicacao - consolidated.count()
    )

    ordered_first = [
        column
        for column in ("ano_pesquisa", "arquivo_origem", "id_resposta", "data_envio")
        if column in consolidated.columns
    ]
    remaining = [column for column in consolidated.columns if column not in ordered_first]
    consolidated = consolidated.select(*(ordered_first + sorted(remaining)))

    required_output = {"ano_pesquisa", "arquivo_origem", "id_resposta"}
    missing_output = required_output - set(consolidated.columns)
    if missing_output:
        raise ValueError(f"Campos obrigatórios ausentes na Silver: {sorted(missing_output)}")

    metrics = quality_metrics(consolidated, raw_counts, consolidated.columns, linhas_duplicadas_exatas_removidas)
    id_null_metric = next(value for name, value, _ in metrics if name == "id_resposta_nulo")
    if int(id_null_metric) > 0:
        raise ValueError(f"Há {id_null_metric} respostas sem id/token; Silver não será gravada")

    write_csv(consolidated, args["SILVER_PATH"], coalesce_one=True)
    silver_parquet_path = args["SILVER_PATH"].rstrip("/") + "Parquet/"
    write_parquet(
        consolidated,
        silver_parquet_path,
        partition_column="ano_pesquisa",
        )

    dictionary_schema = [
        "codigo_pergunta",
        "coluna_silver",
        "presente_em",
        "rotulo_2023",
        "rotulo_2024",
        "rotulo_2025",
        "origem_2023",
        "origem_2024",
        "origem_2025",
    ]
    dictionary_df = spark.createDataFrame(
        [tuple(row.get(column, "") for column in dictionary_schema) for row in dictionary_rows],
        dictionary_schema,
    )
    write_csv(dictionary_df.orderBy("codigo_pergunta"), args["DICTIONARY_PATH"], coalesce_one=True)

    quality_df = spark.createDataFrame(metrics, ["indicador", "valor", "descricao"])
    write_csv(quality_df, args["QUALITY_PATH"], coalesce_one=True)

    print("SilverCorrigido concluída")
    print(f"Entradas: {year_paths}")
    print(f"Colunas Silver: {len(consolidated.columns)}")
    print(f"Linhas Silver: {consolidated.count()}")
    print(f"Linhas duplicadas exatamente iguais removidas: {linhas_duplicadas_exatas_removidas}")
    print(f"Saída CSV: {args['SILVER_PATH']}")
    
    print(f"Saída Parquet: {silver_parquet_path}")
    job.commit()


if __name__ == "__main__":
    main()
