Project Overview

The project focuses on building an ETL pipeline using Azure cloud services and Databricks to efficiently extract, transform, and store data in a medallion architecture (bronze, silver, and gold layers). The goal is to process structured and semi-structured data from various sources, ensure data quality and maintain historical records, and produce aggregated datasets for analysis. The pipeline is fully automated using Azure Data Factory (ADF).

Project Workflow

1. Data Extraction

Azure SQL Database:

Full Load: Extracts the complete table data for the initial load.

Incremental Load: Fetches only new or updated records since the last load using an audit table.

Azure Data Lake Storage (ADLS):

Extracts flat files (e.g., CSV, JSON) from the landing_zone.

The pipeline extracts data from 50+ tables across various hospital databases.

Audit Table: The audit table tracks the last load date to ensure incremental data extraction.

2. Data Storage - Bronze Layer

Data is saved in Parquet format in the bronze layer.

The bronze layer contains raw data as ingested from source systems without any transformations.

3. Data Transformation - Silver Layer

Data is loaded into Delta Tables in Databricks for transformation.

Key transformations performed:

Quality Checks: Ensuring data completeness, validity, and consistency.

Slowly Changing Dimensions (SCD) Type 2: Maintains historical changes by creating new records for updates.

Common Data Model (CDM) Implementation: Aligns data to a standardized schema for ease of integration and analysis.

4. Data Aggregation - Gold Layer

Stores aggregated and processed data for stakeholder analysis and reporting.

The gold layer provides optimized views for faster querying and dashboard integration.

Secrets Management

Azure Key Vault securely stores and manages secrets (e.g., database connection strings, and API keys).

Secrets are accessed programmatically within Databricks and ADF pipelines to ensure sensitive information is protected.

Automation

The entire ETL pipeline is orchestrated using Azure Data Factory (ADF).

ADF manages the scheduling, triggering, and monitoring of data movement and transformation workflows.

Technology Stack

Azure SQL Database: Source for structured data.

Azure Data Lake Storage (ADLS): Stores raw and transformed data.

Databricks: Used for data transformations and creating Delta tables.

Azure Data Factory (ADF): Orchestrates and automates the data pipeline.

Azure Key Vault: Manages and accesses secrets securely.

Parquet Format: Used for efficient data storage.

Delta Lake: Provides ACID transactions and versioning for the silver layer.

Key Features

Automated ETL Pipeline: Fully automated using ADF for hands-free data ingestion and processing.

Incremental Loading: Efficiently loads only new or modified data.

Audit Tracking: Tracks data ingestion timestamps to prevent duplicates.

Data Quality Assurance: Ensures data reliability through validation and checks.

Historical Data Management: Implements SCD Type 2 to track changes in dimensions.

Secure Secrets Management: Prevents exposure of sensitive credentials.

Optimize transformations for faster performance.

Conclusion

This project demonstrates using Azure cloud services, Databricks, and Delta Lake to build an end-to-end automated ETL pipeline that handles large datasets from multiple hospital databases, maintains data quality, and supports historical data analysis.
