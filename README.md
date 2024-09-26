# Data Pipeline with Airflow, AWS Glue, Athena, S3
 ---------------------------------------------------------------------------------------------------------------------

 In this project, an architecture that integrates Redis, Apache Airflow and AWS Glue has been implemented for the orchestration of ETL (Extract, Transform and Load) processes. The configuration allows Airflow to manage and execute DAGs (Directed Acyclic Graphs) containing ETL scripts hosted on AWS Glue.
 
Redis is used as a messaging system to optimize communication between Airflow components, improving efficiency in task execution. In turn, Airflow orchestrates the Glue jobs, which are responsible for reading data from an S3 bucket, performing transformations such as changing date formatting and renaming columns in dataframes, and finally, saving the transformed results back to S3.

This integration enables seamless and scalable management of ETL processes, ensuring that data is processed efficiently and stored appropriately for further analysis. In addition, the use of Airflow provides visibility and control over the workflow, facilitating scheduling and monitoring of tasks.

The data for this project was obtained from Colombia's open data website. Link : https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr/about_data

---------------------------------------------------------------------------------------------------------------------

# Table of Contents

* Overview
* Architecture
* Prerequisites
* System Setup

---------------------------------------------------------------------------------------------------------------------

# Overview

1. Extract data from: datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr/about_data 
2. Store the raw data into an S3 bucket from Airflow.
3. Transform the data using AWS Glue and Amazon Athena.
4. Load the transformed data into Amazon Redshift for analytics and querying.

---------------------------------------------------------------------------------------------------------------------

# Architecture 
![image](https://github.com/user-attachments/assets/558c3405-667a-470d-8f51-7169fa73edf4)

* Datos.gov.co: Source of the data.
* Apache Airflow & Celery: Orchestrates the ETL process and manages task distribution.
* PostgreSQL: Temporary storage and metadata management.
* Amazon S3: Raw data storage.
* AWS Glue: Data cataloging and ETL jobs.
* Amazon Athena: SQL-based data transformation.
* Amazon Redshift: Data warehousing and analytics.

---------------------------------------------------------------------------------------------------------------------

# Prerequisites

* AWS Account with appropriate permissions for S3, Glue, Athena, and Redshift.
* Docker Installation
* Python 3.9 or higher

---------------------------------------------------------------------------------------------------------------------

# System Setup

1. Clone the repository
    git clone https://github.com/wlopezm-unal/Project-airflow-AWSGlue.git
2. Create a virtual environment
     python -m venv airflow-env
3. Activate the virtual environment.
    source ./airflow-env/Scripts/activate
4. Lauch docker compose
    docker compose up -d
5. Create bucket S3
6. Load dataset in S3
7. Define your rutes S3:// para the AWS_glue files
8. Configure Crawler of AWS GLue to read data what is in S3
9. Create ETL JOBs in AWS GLue, and copy the code whay you can found in aws_glue files of this repository
10. Create Airflow connection and insert Your credential AWS in Airflow
   ![image](https://github.com/user-attachments/assets/f30adc40-630b-4090-9034-4c41e499caed)
11. Run the DAG : aws_glue_etl_dag
12. Configure your Crawler in AWS Glue to you can see and work with the process data using AWS Athena
    ![image](https://github.com/user-attachments/assets/4344ce23-302a-46f7-a0f9-787e7cefa2b1)








