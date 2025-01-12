# Databricks notebook source
display(dbutils.fs.mounts())


# COMMAND ----------

display(dbutils.fs.ls("/mnt/bronze/hosa"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------

claims_df = spark.read.format('csv').option('header', 'true').load('/mnt/landing-zone/claims_data/*.csv')

# COMMAND ----------

claims_df.count()

# COMMAND ----------

from pyspark.sql.functions import when, input_file_name, col
claims_df = claims_df.withColumn('datasource', when(input_file_name().contains('hospital1'), 'hosa').when(input_file_name().contains('hospital2'), 'hosb').otherwise(None))
                                 
    

# COMMAND ----------

display(claims_df)

# COMMAND ----------

claims_df.write.format('parquet').mode('overwrite').save('/mnt/bronze/claims')

# COMMAND ----------

claims_df.rdd.getNumPartitions()

# COMMAND ----------

cpt_code_df = spark.read.format('csv').option('header', 'true').load('/mnt/landing-zone/cpt_codes/*.csv')

# COMMAND ----------

print(cpt_code_df.columns)

# COMMAND ----------

from pyspark.sql.functions import col, replace
for col in cpt_code_df.columns:
    cpt_code_df = cpt_code_df.withColumnRenamed(col, col.replace(' ', '_'))


# COMMAND ----------

cpt_code_df.write.format('parquet').mode('overwrite').save('/mnt/bronze/cpt_codes/')

# COMMAND ----------


