# Project README

## Project Overview  
A robust data pipeline built with Azure and Databricks that processes data through bronze, silver, and gold layers. Data is stored in the Bronze layer as Parquet files, with transformations in the Silver layer ensuring data quality and supporting Slowly Changing Dimensions (SCD Type 2). The final aggregated data is placed in the Gold layer, making it ready for analytics and reporting. Audit tracking handles incremental loading, and Azure Key Vault manages secret storage, strengthening overall security and reliability.

![databricks_pipeline_1](https://github.com/user-attachments/assets/f7c32169-751a-4eaf-a799-c3d2111f6a7b)

![databrick_pipeline](https://github.com/user-attachments/assets/b37cbf93-1e0d-4b18-ac8f-0654f5aba81f)

![Azure_pipeline](https://github.com/user-attachments/assets/0425fdc5-b89c-4085-9176-6058d1f0b161)

![Azure_pipeline_2](https://github.com/user-attachments/assets/2e677e0e-4a83-4174-8bda-c6ddf9d82d45)


## Project Workflow  

### 1. **Data Extraction**  
- **Azure SQL Database**:  
  - **Full Load**: Extracts the complete table data for the initial load.  
  - **Incremental Load**: Fetches only new or updated records since the last load using an audit table.  
- **Azure Data Lake Storage (ADLS)**:  
  - Extracts flat files (e.g., CSV, JSON) from the `landing_zone`.  
- The pipeline extracts data from **50+ tables** across various hospital databases.  

**Audit Table**: The audit table tracks the last load date to ensure incremental data extraction.

### 2. **Data Storage - Bronze Layer**  
- Data is saved in **Parquet** format in the bronze layer.  
- The bronze layer contains raw data as ingested from source systems without any transformations.  

### 3. **Data Transformation - Silver Layer**  
- Data is loaded into **Delta Tables** in Databricks for transformation.  
- Key transformations performed:  
  - **Quality Checks**: Ensuring data completeness, validity, and consistency.  
  - **Slowly Changing Dimensions (SCD) Type 2**: Maintains historical changes by creating new records for updates.  
  - **Common Data Model (CDM) Implementation**: Aligns data to a standardized schema for ease of integration and analysis.  

### 4. **Data Aggregation - Gold Layer**  
- Stores aggregated and processed data for stakeholder analysis and reporting.  
- The gold layer provides optimized views for faster querying and dashboard integration.  

## Secrets Management  
- **Azure Key Vault** is used to securely store and manage secrets (e.g., database connection strings, API keys).  
- Secrets are accessed programmatically within Databricks and ADF pipelines to ensure sensitive information is protected.  

## Automation  
- The entire ETL pipeline is orchestrated using **Azure Data Factory (ADF)**.  
- ADF manages scheduling, triggering, and monitoring of data movement and transformation workflows.  

## Technology Stack  
- **Azure SQL Database**: Source for structured data.  
- **Azure Data Lake Storage (ADLS)**: Stores raw and transformed data.  
- **Databricks**: Used for data transformations and creating Delta tables.  
- **Azure Data Factory (ADF)**: Orchestrates and automates the data pipeline.  
- **Azure Key Vault**: Manages and accesses secrets securely.  
- **Parquet Format**: Used for efficient data storage.  
- **Delta Lake**: Provides ACID transactions and versioning for the silver layer.  

## Key Features  
- **Automated ETL Pipeline**: Fully automated using ADF for hands-free data ingestion and processing.  
- **Incremental Loading**: Efficiently loads only new or modified data.  
- **Audit Tracking**: Tracks data ingestion timestamps to prevent duplicates.  
- **Data Quality Assurance**: Ensures data reliability through validation and checks.  
- **Historical Data Management**: Implements SCD Type 2 to track changes in dimensions.  
- **Secure Secrets Management**: Prevents exposure of sensitive credentials.  

## Conclusion  
Thw project demonstrates the use of Azure cloud services, Databricks, and Delta Lake to build an end-to-end automated ETL pipeline that handles large datasets from multiple hospital databases, maintains data quality, and supports historical data analysis.
