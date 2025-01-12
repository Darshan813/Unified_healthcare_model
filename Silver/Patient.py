# Databricks notebook source
print('Bheruji')

# COMMAND ----------

df_hosa = spark.read.parquet('/mnt/bronze/hosa/patients')
df_hosb = spark.read.parquet('/mnt/bronze/hosb/patients')
df_hosa.createOrReplaceTempView('patient_hosa')
df_hosb.createOrReplaceTempView('patient_hosb')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM patient_hosa

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temp view cdm_patients AS
# MAGIC   SELECT concat(src_patient_id, '-', datasource) as patient_key, *
# MAGIC   FROM ( 
# MAGIC   SELECT 
# MAGIC   patientid as src_patient_id, 
# MAGIC   firstname,
# MAGIC   lastname,
# MAGIC   middlename,
# MAGIC   ssn,
# MAGIC   phonenumber,
# MAGIC   gender,
# MAGIC   dob,
# MAGIC   address,
# MAGIC   modifieddate,
# MAGIC   datasource
# MAGIC   FROM patient_hosa
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC   id, 
# MAGIC   f_name,
# MAGIC   l_name,
# MAGIC   m_name,
# MAGIC   ssn,
# MAGIC   phonenumber,
# MAGIC   gender,
# MAGIC   dob,
# MAGIC   address,
# MAGIC   updated_date,
# MAGIC   datasource
# MAGIC   FROM patient_hosb
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cdm_patients;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temp view quality_checks AS
# MAGIC SELECT 
# MAGIC patient_key,
# MAGIC src_patient_id, 
# MAGIC firstname,
# MAGIC lastname,
# MAGIC middlename,
# MAGIC ssn,
# MAGIC phonenumber,
# MAGIC gender,
# MAGIC dob,
# MAGIC address,
# MAGIC modifieddate,
# MAGIC datasource,
# MAGIC CASE 
# MAGIC   WHEN firstname IS NULL OR dob IS NULL OR firstname IS NULL OR lower(firstname) = 'null' THEN TRUE
# MAGIC   ELSE FALSE
# MAGIC END AS is_quarantined
# MAGIC FROM cdm_patients
# MAGIC

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.patients (
    Patient_Key STRING,
    SRC_PatientID STRING,
    FirstName STRING,
    LastName STRING,
    MiddleName STRING,
    SSN STRING,
    PhoneNumber STRING,
    Gender STRING,
    DOB DATE,
    Address STRING,
    SRC_ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.patients AS TARGET
# MAGIC USING quality_checks AS SOURCE
# MAGIC ON target.patient_key = source.patient_key 
# MAGIC AND target.is_current = true
# MAGIC WHEN MATCHED
# MAGIC AND
# MAGIC   (
# MAGIC target.SRC_PatientID <> source.SRC_Patient_ID OR
# MAGIC target.FirstName <> source.FirstName OR
# MAGIC target.LastName <> source.LastName OR
# MAGIC target.MiddleName <> source.MiddleName OR
# MAGIC target.SSN <> source.SSN OR
# MAGIC target.PhoneNumber <> source.PhoneNumber OR
# MAGIC target.Gender <> source.Gender OR
# MAGIC target.DOB <> source.DOB OR
# MAGIC target.Address <> source.Address OR
# MAGIC target.SRC_ModifiedDate <> source.modifieddate OR
# MAGIC target.datasource <> source.datasource OR
# MAGIC target.is_quarantined <> source.is_quarantineD
# MAGIC   )
# MAGIC THEN UPDATE SET 
# MAGIC   target.modified_date = current_timestamp(),
# MAGIC   target.is_current = false
# MAGIC
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (
# MAGIC      Patient_Key,
# MAGIC      src_PatientID,
# MAGIC      FirstName,
# MAGIC      LastName,
# MAGIC      MiddleName,
# MAGIC      SSN,
# MAGIC      PhoneNumber,
# MAGIC      Gender,
# MAGIC      DOB,
# MAGIC      Address,
# MAGIC      SRC_ModifiedDate,
# MAGIC      datasource,
# MAGIC      is_quarantined,
# MAGIC      inserted_date,
# MAGIC      modified_date,
# MAGIC      is_current
# MAGIC  )
# MAGIC  VALUES (
# MAGIC      source.Patient_Key,
# MAGIC      source.SRC_Patient_ID,
# MAGIC      source.FirstName,
# MAGIC      source.LastName,
# MAGIC      source.MiddleName,
# MAGIC      source.SSN,
# MAGIC      source.PhoneNumber,
# MAGIC      source.Gender,
# MAGIC      source.DOB,
# MAGIC      source.Address,
# MAGIC      source.ModifiedDate,
# MAGIC      source.datasource,
# MAGIC      source.is_quarantined,
# MAGIC      current_timestamp(), -- Set inserted_date to current timestamp
# MAGIC      current_timestamp(), -- Set modified_date to current timestamp
# MAGIC      true -- Mark as current
# MAGIC  );
# MAGIC
# MAGIC MERGE INTO silver.patients AS target
# MAGIC USING quality_checks AS Source
# MAGIC ON target.Patient_Key = Source.Patient_Key
# MAGIC AND target.is_current = true
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT(
# MAGIC      Patient_Key,
# MAGIC      SRC_PatientID,
# MAGIC      FirstName,
# MAGIC      LastName,
# MAGIC      MiddleName,
# MAGIC      SSN,
# MAGIC      PhoneNumber,
# MAGIC      Gender,
# MAGIC      DOB,
# MAGIC      Address,
# MAGIC      SRC_ModifiedDate,
# MAGIC      datasource,
# MAGIC      is_quarantined,
# MAGIC      inserted_date,
# MAGIC      modified_date,
# MAGIC      is_current
# MAGIC  )
# MAGIC  VALUES (
# MAGIC      source.Patient_Key,
# MAGIC      source.SRC_Patient_ID,
# MAGIC      source.FirstName,
# MAGIC      source.LastName,
# MAGIC      source.MiddleName,
# MAGIC      source.SSN,
# MAGIC      source.PhoneNumber,
# MAGIC      source.Gender,
# MAGIC      source.DOB,
# MAGIC      source.Address,
# MAGIC      source.ModifiedDate,
# MAGIC      source.datasource,
# MAGIC      source.is_quarantined,
# MAGIC      current_timestamp(), -- Set inserted_date to current timestamp
# MAGIC      current_timestamp(), -- Set modified_date to current timestamp
# MAGIC      true -- Mark as current
# MAGIC
# MAGIC  );
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  
# MAGIC FROM quality_checks 
# MAGIC WHERE is_quarantined = True
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.patients;

# COMMAND ----------



# COMMAND ----------


