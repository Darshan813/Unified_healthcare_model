# Databricks notebook source
# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_patient
(
    patient_key STRING,
    src_patientid STRING,
    firstname STRING,
    lastname STRING,
    middlename STRING,
    ssn STRING,
    phonenumber STRING,
    gender STRING,
    dob DATE,
    address STRING,
    datasource STRING
)
""")

# COMMAND ----------

spark.sql("TRUNCATE TABLE gold.dim_patient")

# COMMAND ----------

spark.sql("""
INSERT INTO gold.dim_patient
SELECT 
    patient_key,
    src_patientid,
    firstname,
    lastname,
    middlename,
    ssn,
    phonenumber,
    gender,
    dob,
    address,
    datasource
FROM silver.patients
WHERE is_current=true AND is_quarantined=false
""")
