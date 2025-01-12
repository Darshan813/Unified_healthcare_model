# Databricks notebook source
# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_icd (
# MAGIC     icd_code STRING,
# MAGIC     icd_code_type STRING,
# MAGIC     code_description STRING,
# MAGIC     refreshed_at TIMESTAMP
# MAGIC )
# MAGIC      

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC truncate table gold.dim_icd

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_icd
# MAGIC select distinct
# MAGIC   icd_code,
# MAGIC   icd_code_type,
# MAGIC   code_description,
# MAGIC   current_timestamp() refreshed_at
# MAGIC from
# MAGIC   silver.icd_codes
# MAGIC where
# MAGIC   is_current_flag = true  
