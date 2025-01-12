# Databricks notebook source
df_hosa = spark.read.parquet('/mnt/bronze/hosa/providers')
df_hosb = spark.read.parquet('/mnt/bronze/hosb/providers')
df_merge = df_hosa.unionByName(df_hosb)
df_merge.createOrReplaceTempView('providers')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.providers (
# MAGIC   Provider_id STRING,
# MAGIC   FirstName STRING, 
# MAGIC   LastName STRING, 
# MAGIC   Specialization STRING,
# MAGIC   DeptID STRING,
# MAGIC   NPI Long,
# MAGIC   datasource STRING,
# MAGIC   is_quarantined BOOLEAN
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.providers

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver.providers
# MAGIC SELECT 
# MAGIC DISTINCT providerID, 
# MAGIC FirstName as f_name,
# MAGIC LastName,
# MAGIC Specialization,
# MAGIC DeptID,
# MAGIC cast(NPI AS BIGINT) NPI,
# MAGIC datasource, 
# MAGIC   CASE 
# MAGIC     WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_q  
# MAGIC FROM providers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM providers;|

# COMMAND ----------


