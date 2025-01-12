# Databricks notebook source
print("Hello Bheruji") 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS audit;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS audit.load_logs (
# MAGIC     data_source STRING,
# MAGIC     tablename STRING,
# MAGIC     numberofrowscopied INT,
# MAGIC     watermarkcolumnname STRING,
# MAGIC     loaddate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM audit.load_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE Catalog Test;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED audit.load_logs

# COMMAND ----------


