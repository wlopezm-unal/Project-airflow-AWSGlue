﻿# Data Pipeline with Airflow, AWS Glue, Athena, S3****
 ---------------------------------------------------------------------------------------------------------------------

 In this project, an architecture that integrates Redis, Apache Airflow and AWS Glue has been implemented for the orchestration of ETL (Extract, Transform and Load) processes. The configuration allows Airflow to manage and execute DAGs (Directed Acyclic Graphs) containing ETL scripts hosted on AWS Glue.
 
Redis is used as a messaging system to optimize communication between Airflow components, improving efficiency in task execution. In turn, Airflow orchestrates the Glue jobs, which are responsible for reading data from an S3 bucket, performing transformations such as changing date formatting and renaming columns in dataframes, and finally, saving the transformed results back to S3.

This integration enables seamless and scalable management of ETL processes, ensuring that data is processed efficiently and stored appropriately for further analysis. In addition, the use of Airflow provides visibility and control over the workflow, facilitating scheduling and monitoring of tasks.

---------------------------------------------------------------------------------------------------------------------

# Table of Contents****

* Overview
* Architecture
* Prerequisites
* System Setup

---------------------------------------------------------------------------------------------------------------------

# Overview****





