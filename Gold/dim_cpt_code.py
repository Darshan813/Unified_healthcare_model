# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold

# COMMAND ----------

# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_cpt_code
(
cpt_codes string,
procedure_code_category string,
procedure_code_descriptions string,
code_status string,
refreshed_at timestamp
)
""")

# COMMAND ----------

spark.sql("TRUNCATE TABLE gold.dim_cpt_code")

# COMMAND ----------

spark.sql("""
INSERT INTO gold.dim_cpt_code
SELECT 
cpt_codes,
procedure_code_category,
procedure_code_descriptions,
code_status,
current_timestamp() as refreshed_at
FROM silver.cptcodes
WHERE is_quarantined=false AND is_current=true
""")

# COMMAND ----------

df = spark.sql("SELECT * FROM gold.dim_cpt_code")
display(df)
