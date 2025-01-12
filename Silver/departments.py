# Databricks notebook source
print('Bheruji')

# COMMAND ----------

df_hosa = spark.read.parquet('/mnt/bronze/hosa/departments')
df_hosb = spark.read.parquet('/mnt/bronze/hosb/departments')

# COMMAND ----------

#Union two departments

df_merged = df_hosa.unionByName(df_hosb)
display(df_merged)

# COMMAND ----------

from pyspark.sql import functions as f
df_merged = df_merged.withColumn('SRC_Dept_id', f.col('DeptID'))\
                     .withColumn('Dept_ID', f.concat(f.col('DEptID'), f.lit('-'), f.col('datasource')))\
                     .drop('DeptID')


# COMMAND ----------

display(df_merged)

# COMMAND ----------

df_merged.createOrReplaceTempView('departments')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM departments;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.departments(
# MAGIC   dept_id INT,
# MAGIC   src_dept_id STRING,
# MAGIC   Name STRING, 
# MAGIC   datasource STRING,
# MAGIC   is_quarantined BOOLEAN
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.departments;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.departments

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver.departments
# MAGIC SELECT 
# MAGIC dept_id, src_dept_id, Name, datasource,
# MAGIC CASE
# MAGIC   WHEN src_dept_id IS NULL OR NAME IS NULL THEN TRUE
# MAGIC   ELSE FALSE
# MAGIC END as is_quarantined
# MAGIC FROM departments
