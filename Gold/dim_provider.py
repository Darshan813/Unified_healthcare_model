# Databricks notebook source
# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_provider
(
ProviderID string,
FirstName string,
LastName string,
DeptID string,
NPI long,
datasource string
)
""")

# COMMAND ----------

spark.sql("TRUNCATE TABLE gold.dim_provider")

# COMMAND ----------

spark.sql("""
INSERT INTO gold.dim_provider
SELECT 
Provider_ID,
FirstName,
LastName,
concat(DeptID,'-',datasource) as deptid,
NPI,
datasource
FROM silver.providers
WHERE is_quarantined = false
""")
