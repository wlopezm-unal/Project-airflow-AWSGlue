from datetime import datetime, timedelta
from dags_helper import default_args
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator



dag=DAG(
    'aws_glue_etl_dag',
    default_args=default_args,
    description='AWS Glue ETL Pipeline triggered by Airflow. ETL on Covid-19 cases in Colombia',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl_awsGlue'],
)

#function aws_credentials
def get_aws_credentials():

    """
    Retrieves the AWS access key ID and secret access key from the Airflow connection.
    
    Returns:
        tuple: A tuple containing the access key ID and secret access key.
    """

    aws_hook = AwsBaseHook(aws_conn_id='awsConecction')
    credentials=aws_hook.get_credentials()
    return credentials.access_key, credentials.secret_key

start_task=PythonOperator(
    task_id='get_aws_credentials',
    python_callable=get_aws_credentials,        
    dag=dag
)

""" 
Read data from S3 bucket and transform it using a Glue job. After  save the data in a new S3 bucket that will be bronze layer in medallion architecture
"""
glue_task=GlueJobOperator(
    task_id='run_glue_job',
    job_name='etl-covid',
    script_location='s3://aws-glue-assets-841162671605-us-east-1/scripts/etl-covid.py',
    iam_role_name="AWSGlueServiceRole-glue",
    aws_conn_id='awsConecction',
    region_name='us-east-1',
    dag=dag
)

delete_null_values=GlueJobOperator(
    task_id='run_glue_job_null_values',
    job_name='tranform_data_covid_silver',
    script_location='s3://aws-glue-assets-841162671605-us-east-1/scripts/tranform_data_covid_silver.py',
    iam_role_name='AWSGlueServiceRole-glue',
    aws_conn_id='awsConecction',
    region_name='us-east-1',
    dag=dag
)

start_task >> glue_task >> delete_null_values