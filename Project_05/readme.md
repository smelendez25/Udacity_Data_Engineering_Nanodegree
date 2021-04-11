# Project 5: Data Pipelines with Airflow

<p align="center"><img src="images/logo.png" style="height: 100%; width: 100%; max-width: 200px" /></p>

## Introduction
As a data engineer, I was responsible for automating and monitoring the data warehouse ETL pipelines at Sparkify. Specifically, I delivered a dynamic and high grade data pipeline, which not only allowed for scheduled backfilling but also monitoring to ensure data quality. Data quality plays a big part in the analytics team at Sparkify, thus after the automated ETL processes, analyses were executed on top of the data warehouse with tests run to catch any discrepancies in the datasets.

The source data resides in S3 and needed to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Achievements
As their data engineer, I was responsible for automating the ETL pipelines through Airflow, extracting data from S3, loading data into staging tables and transforming the data into a star schema stored in Amazon Redshift. The data warehouse (automatically generated by the Airflow tasks) were then validated using custom analyses to detect any discrepancies in the databases.
Skills include:
* Using Airflow to automate ETL pipelines using Airflow, Python, Amazon Redshift.
* Transforming data from various sources into a star schema optimized for the analytics team's use cases.
* Writing custom operators to perform tasks such as staging data, filling the data warehouse, and validation through data quality checks.
* Setting up IAM Roles, Redshift Clusters, Airflow Connections.

## Work in Progress