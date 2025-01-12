# Databricks notebook source
# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_department
(
Dept_Id string,
SRC_Dept_Id string,
Name string,
datasource string
)
""")

# COMMAND ----------

spark.sql("TRUNCATE TABLE gold.dim_department")

# COMMAND ----------

spark.sql("""
INSERT INTO gold.dim_department
SELECT 
DISTINCT
Dept_Id,
SRC_Dept_Id,
Name,
datasource
FROM silver.departments
WHERE is_quarantined = false
""")

# COMMAND ----------

df = spark.sql("SELECT * FROM gold.dim_department")
display(df)
