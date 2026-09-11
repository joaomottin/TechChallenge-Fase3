import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node SilverCorrigido
SilverCorrigido_node1788911945788 = glueContext.create_dynamic_frame.from_catalog(database="fiap_techchallenge3_2026", table_name="silver_silvercorrigido", transformation_ctx="SilverCorrigido_node1788911945788")

# Script generated for node GoldIndicadores
GoldIndicadores_node1788912057092 = SelectFields.apply(frame=SilverCorrigido_node1788911945788, paths=["ano_pesquisa", "arquivo_origem", "id_resposta", "idade", "idade_q1_a_1", "genero", "raca_etnia", "pcd", "estado", "uf", "regiao", "nivel_ensino", "q1_m_area_de_formacao", "q2_a_situacao_de_trabalho", "setor", "atua_como_gestor", "cargo_atual", "nivel", "faixa_salarial", "experiencia_dados", "experiencia_ti", "modelo_trabalho_ideal", "funcao_atuacao", "fontes_dados", "idade_q3_e", "ia_uso_empresa", "q3_g_motivos_para_nao_usar_ai_generativa_e_llm", "q3_h_empresa_esta_conseguindo_ter_bons_resultados_com_llms", "linguagem_preferida", "cloud_preferida", "bi_preferida", "ferramenta_de_bi_preferida", "tecnologia_data_lake", "tecnologia_data_warehouse"], transformation_ctx="GoldIndicadores_node1788912057092")

# Script generated for node GoldIndicadoresS3
EvaluateDataQuality().process_rows(frame=GoldIndicadores_node1788912057092, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1788911813901", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
GoldIndicadoresS3_node1788912390481 = glueContext.write_dynamic_frame.from_options(frame=GoldIndicadores_node1788912057092, connection_type="s3", format="csv", connection_options={"path": "s3://fiap-techchallenge3-2026/gold/GoldIndicadores/", "partitionKeys": []}, transformation_ctx="GoldIndicadoresS3_node1788912390481")

job.commit()
