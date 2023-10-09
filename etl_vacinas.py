import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

crawler_name='NOME_CRAWLER'

# SQL para agrupar e contar a quantidade de registros
sqlGroupData = '''
select count(1) as quantidade, sexo, municipio, uf, data_aplicacao, dose, vacina, ano, mes
from myDataSource
group by sexo, municipio, uf, data_aplicacao, dose, vacina, ano, mes

'''

#função para agrupar e contar a quantidade de registros
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

#função para transformar o campo dose
def transformaColunas(rec):
    
    rec['dose'] = '1' if '1' in rec['vacina_descricao_dose'] else ('2' if '2' in rec['vacina_descricao_dose'] else 'Única')
    split_data = rec['vacina_dataAplicacao'].split('-')
    rec['ano'] = split_data[0]
    rec['mes'] = split_data[1]
    return rec
    
## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME', 's3'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leitura dos dados do Data Catalog
print('Reading data...')
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [f's3://{args["s3"]}/input']},
    format="csv",
    format_options={
        "withHeader": True,
        "separator":";"
    },
    transformation_ctx="datasource"
)

print("Data read", datasource.count())

if datasource.count() > 0:
    # Transformação da dose
    print('Transforming columns...')
    transformacao1 = Map.apply(
        frame=datasource, 
        f=transformaColunas, 
        transformation_ctx="transformacao1"
    )
    print("COLUNAS", transformacao1.toDF().columns)
    print("Data transformed", transformacao1.count())
    
    
    # Mapeamento das colunas para outros nomes, conversão de tipos e remover colunas desnecessárias
    print('Mapping columns')
    transformacao2 = ApplyMapping.apply(
        frame = transformacao1,
        mappings = [
            ("paciente_enumSexoBiologico", "string", "sexo", "string"), 
            ("estabelecimento_municipio_nome", "string", "municipio", "string"), 
            ("estabelecimento_uf", "string", "uf", "string"), 
            ("vacina_dataAplicacao", "string", "data_aplicacao", "date"), 
            ("dose", "string", "dose", "string"), 
            ("vacina_nome", "string", "vacina", "string"),
            ("ano", "string", "ano", "int"),
            ("mes", "string", "mes", "int")
        ], 
        transformation_ctx = "transformacao2"
    )
    
    print("Columns mapped", transformacao2.count())
    
    # Agrupamento e contagem dos registros por sexo, municipio, up, dose, vacina e data de aplicação
    print("Groupping data")
    transformacao3 = sparkSqlQuery(
        glueContext, 
        query = sqlGroupData, 
        mapping = {"myDataSource": transformacao2}, 
        transformation_ctx = "transformacao3"
    )
    
    print("Data group", transformacao3.count())
    

    print('Saving parquet')
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    transformacao3.toDF().write.mode("overwrite").format("parquet").partitionBy("ano","mes","uf").save(f's3://{args["s3"]}/lake')

import boto3
glue_client = boto3.client('glue')
glue_client.start_crawler(Name=crawler_name)

print("Finishing")
job.commit()