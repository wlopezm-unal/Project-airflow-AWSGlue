import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#1. Load data of the first transforms
dyf_covid=glueContext.create_dynamic_frame.from_catalog(database="db-targetcovid", 
                                                        table_name="target_data_wl")
#2. Convert data to Dataframe
df_covid=dyf_covid.toDF()

# 3. Elimitaned null values
df_covid = df_covid \
    .withColumn("Fecha_inicio_sintomas", when(col("Fecha_inicio_sintomas").isNull(), col("Fecha_diagnostico") - datediff("Fecha_diagnostico", "2023-01-01")).otherwise(col("Fecha_inicio_sintomas"))) \
    .withColumn("Tipo_recuperacion", when(col("Tipo_recuperacion").isNull(), "Muerto").otherwise(col("Tipo_recuperacion"))) \
    .withColumn("Fecha_muerte", when(col("Fecha_muerte").isNull(), "Recuperado").otherwise(col("Fecha_muerte"))) \
    .withColumn("Fecha_recuperacion", when(col("Fecha_recuperacion").isNull(), "Muerto").otherwise(col("Fecha_recuperacion"))) \
    .where(col("Fecha_diagnostico").isNotNull())

dyf_transformed = DynamicFrame.fromDF(df_covid , glueContext, "dyf_transformed")

#4. Chance to parquet format the final product
glueContext.write_dynamic_frame.from_options(
    frame = dyf_transformed,
    connection_type = "s3",
    connection_options = {"path": "s3://covid-target-silver-wl"},
    format = "parquet",
    transformation_ctx = "write_parquet"
)


job.commit()