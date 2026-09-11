"""AWS Glue Job 01: CSV versionado no S3 Landing para Parquet Bronze.

O job preserva todas as linhas publicadas, renomeia as centenas de colunas para
identificadores posicionais estaveis e cataloga uma tabela por edicao. O nome
bruto, codigo e rotulo de cada pergunta ficam em ``bronze_question_dictionary``.
"""

from __future__ import annotations

import ast
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, functions as F, types as T


ARGS = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET", "GLUE_DATABASE", "MANIFEST_KEY"],
)
BUCKET = ARGS["S3_BUCKET"]
DATABASE = ARGS["GLUE_DATABASE"]
RUN_ID = os.environ.get("AWS_GLUE_JOB_RUN_ID", str(uuid.uuid4()))
S3 = boto3.client("s3")
GLUE = boto3.client("glue")


def read_json(key: str) -> dict:
    response = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def parse_question_header(raw_header: str) -> tuple[str, str]:
    header = raw_header.strip()
    if header.startswith("("):
        try:
            parsed = ast.literal_eval(header)
        except (SyntaxError, ValueError):
            parsed = None
        if isinstance(parsed, tuple) and len(parsed) >= 2:
            return str(parsed[0]).strip(), str(parsed[1]).strip()
    match = re.match(r"^(\d+(?:\.[A-Za-z0-9]+)+)[ _](.*)$", header)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    if "_" in header:
        return tuple(part.strip() for part in header.split("_", maxsplit=1))
    return header, header


def glue_type(data_type: T.DataType) -> str:
    if isinstance(data_type, T.StringType):
        return "string"
    if isinstance(data_type, T.IntegerType):
        return "int"
    if isinstance(data_type, T.LongType):
        return "bigint"
    if isinstance(data_type, T.DoubleType):
        return "double"
    if isinstance(data_type, T.BooleanType):
        return "boolean"
    if isinstance(data_type, T.TimestampType):
        return "timestamp"
    return data_type.simpleString()


def storage_descriptor(frame: DataFrame, location: str) -> dict:
    return {
        "Columns": [
            {"Name": field.name, "Type": glue_type(field.dataType)}
            for field in frame.schema.fields
        ],
        "Location": location,
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "Compressed": True,
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
    }


def upsert_table(name: str, frame: DataFrame, location: str, description: str) -> None:
    table_input = {
        "Name": name,
        "Description": description,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
        "StorageDescriptor": storage_descriptor(frame, location),
        "PartitionKeys": [],
    }
    try:
        GLUE.get_table(DatabaseName=DATABASE, Name=name)
    except GLUE.exceptions.EntityNotFoundException:
        GLUE.create_table(DatabaseName=DATABASE, TableInput=table_input)
    else:
        GLUE.update_table(DatabaseName=DATABASE, TableInput=table_input)


def write_metric(stage: str, payload: dict) -> None:
    key = f"metrics/{stage}/run_id={RUN_ID}.json"
    S3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=(json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)
manifest = read_json(ARGS["MANIFEST_KEY"])
dictionary_rows: list[dict] = []
metrics: list[dict] = []

for dataset in sorted(manifest["datasets"], key=lambda item: int(item["survey_year"])):
    year = int(dataset["survey_year"])
    filename = dataset["file"]["name"]
    input_uri = f"s3://{BUCKET}/landing/state_of_data/survey_year={year}/{filename}"
    output_uri = f"s3://{BUCKET}/bronze/state_of_data/survey_year={year}/"
    raw = (
        spark.read.option("header", "true")
        .option("encoding", "UTF-8")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(input_uri)
    )
    raw_headers = list(raw.columns)
    positional_names = [f"c{index:03d}" for index in range(len(raw_headers))]
    frame = raw.toDF(*positional_names)
    input_count = frame.count()
    expected_count = int(dataset["file"]["rows"])
    if input_count != expected_count:
        raise ValueError(f"{year}: {input_count} linhas lidas; esperado {expected_count}.")

    for index, raw_header in enumerate(raw_headers):
        question_code, question_label = parse_question_header(raw_header)
        dictionary_rows.append(
            {
                "survey_year": year,
                "ordinal": index,
                "bronze_column": positional_names[index],
                "raw_column_name": raw_header,
                "raw_question_code": question_code,
                "raw_question_label": question_label,
            }
        )

    enriched = (
        frame.withColumn("survey_year", F.lit(year).cast("int"))
        .withColumn("source_edition", F.lit(dataset["edition"]))
        .withColumn("source_file", F.lit(filename))
        .withColumn("source_sha256", F.lit(dataset["file"]["sha256"]))
        .withColumn("source_version", F.lit(int(dataset["version"])).cast("int"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(RUN_ID))
    )
    enriched.write.mode("overwrite").option("compression", "snappy").parquet(output_uri)
    upsert_table(
        f"bronze_state_of_data_{year}",
        enriched,
        output_uri,
        f"State of Data Brasil {year}: copia Bronze imutavel com colunas posicionais.",
    )
    metrics.append(
        {
            "survey_year": year,
            "input_uri": input_uri,
            "output_uri": output_uri,
            "input_rows": input_count,
            "output_rows": input_count,
            "columns": len(raw_headers),
        }
    )

dictionary_schema = T.StructType(
    [
        T.StructField("survey_year", T.IntegerType(), False),
        T.StructField("ordinal", T.IntegerType(), False),
        T.StructField("bronze_column", T.StringType(), False),
        T.StructField("raw_column_name", T.StringType(), False),
        T.StructField("raw_question_code", T.StringType(), True),
        T.StructField("raw_question_label", T.StringType(), True),
    ]
)
dictionary = spark.createDataFrame(dictionary_rows, schema=dictionary_schema)
dictionary_uri = f"s3://{BUCKET}/bronze/question_dictionary/"
dictionary.write.mode("overwrite").option("compression", "snappy").parquet(dictionary_uri)
upsert_table(
    "bronze_question_dictionary",
    dictionary,
    dictionary_uri,
    "Dicionario rastreavel das colunas brutas por edicao.",
)
write_metric(
    "bronze",
    {
        "job_name": ARGS["JOB_NAME"],
        "run_id": RUN_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "datasets": metrics,
    },
)
job.commit()
