import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import date_format

"""
    Este código lee desde el AWS Glue Datacatalog una tabla en una base de datos, después realiza un join entre ambas tablas y calcula el total de la venta. 
    Finalmente, escribe el resultado en un archivo Parquet en S3.
    /
    This code reads a table from the AWS Glue Datacatalog, then performs a join between both tables and calculates the total sale. 
    Finally, it writes the result to a Parquet file in S3. 
"""

args = getResolvedOptions(sys.argv, ['JOB_NAME'])



sc=SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dyf_covid=glueContext.create_dynamic_frame.from_catalog(database="dbcovid", 
                                                        table_name="casos_positivos_de_covid_19_en_colombia__20240523_csv")

df_covid=dyf_covid.toDF()

#Apply the funtiom data_format to conver data format

df_covid= df_covid.withColumn("Fecha de notificación", date_format("Fecha de notificación", "yyyy-MM-dd")) \
    .withColumn("Fecha de diagnóstico", date_format("Fecha de diagnóstico", "yyyy-MM-dd")) \
    .withColumn("Fecha de inicio de síntomas", date_format("Fecha de inicio de síntomas", "yyyy-MM-dd")) \
    .withColumn("Fecha de recuperación", date_format("Fecha de recuperación", "yyyy-MM-dd")) \
    .withColumn("fecha reporte web", date_format("fecha reporte web", "yyyy-MM-dd")) \
    .withColumn("Fecha de muerte", date_format("Fecha de muerte", "yyyy-MM-dd"))

#Rename dataframe feature
df_covid = df_covid.withColumnRenamed('Código DIVIPOLA departamento', 'Codigo_departamento') \
          .withColumnRenamed('Código DIVIPOLA municipio', 'Código_municipio') \
          .withColumnRenamed("Nombre departamento", "Departamento") \
          .withColumnRenamed("Nombre municipio", "Ciudad") \
          .withColumnRenamed("Tipo de contagio", "Tipo_contagio") \
          .withColumnRenamed("Fecha de notificación", "Fecha_notificacion") \
          .withColumnRenamed("Fecha de diagnóstico", "Fecha_diagnostico") \
          .withColumnRenamed("Fecha de inicio de síntomas", "Fecha_inicio_sintomas") \
          .withColumnRenamed("Tipo de recuperación", "Tipo_recuperacion") \
          .withColumnRenamed("Fecha de muerte", "Fecha_muerte") \
          .withColumnRenamed("Fecha de recuperación", "Fecha_recuperacion") \
          .withColumnRenamed("fecha reporte web", "fecha_reporte_web")

dyf_transformed = DynamicFrame.fromDF(df_covid , glueContext, "dyf_transformed")

glueContext.write_dynamic_frame.from_options(
    frame = dyf_transformed,
    connection_type = "s3",
    connection_options = {"path": "s3://{your-rute}"},
    format = "parquet",
    transformation_ctx = "write_parquet"
)

job.commit()

