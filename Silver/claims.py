# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as f

claims_df = spark.read.parquet("/mnt/bronze/claims")

claims_df = claims_df.withColumn(
    "datasource",
    f.when(f.input_file_name().contains("hospital1"), "hosa").when(f.input_file_name().contains("hospital2"), "hosb")
     .otherwise(None)
)

display(claims_df)


# COMMAND ----------
# COMMAND ----------


claims_df.createOrReplaceTempView("claims")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW quality_checks AS
SELECT 
 CONCAT(ClaimID,'-', datasource) AS ClaimID,
ClaimID AS  SRC_ClaimID,
TransactionID,
PatientID,
EncounterID,
ProviderID,
DeptID,
cast(ServiceDate as date) ServiceDate,
cast(ClaimDate as date) ClaimDate,
PayorID,
ClaimAmount,
PaidAmount,
ClaimStatus,
PayorType,
Deductible,
Coinsurance,
Copay,
cast(InsertDate as date) as SRC_InsertDate,
cast(ModifiedDate as date) as SRC_ModifiedDate,
datasource,
    CASE 
        WHEN ClaimID IS NULL OR TransactionID IS NULL OR PatientID IS NULL or ServiceDate IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM claims
""")

# COMMAND ----------

display(spark.sql("select * from quality_checks"))

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.claims (
ClaimID string,
SRC_ClaimID string,
TransactionID string,
PatientID string,
EncounterID string,
ProviderID string,
DeptID string,
ServiceDate date,
ClaimDate date,
PayorID string,
ClaimAmount string,
PaidAmount string,
ClaimStatus string,
PayorType string,
Deductible string,
Coinsurance string,
Copay string,
SRC_InsertDate date,
SRC_ModifiedDate date,
datasource string,
is_quarantined boolean,
audit_insertdate timestamp,
audit_modifieddate timestamp,
is_current boolean
)
USING DELTA
""")

# COMMAND ----------

spark.sql("""
MERGE INTO silver.claims AS target
USING quality_checks AS source
ON target.ClaimID = source.ClaimID AND target.is_current = true
WHEN MATCHED AND (
    target.SRC_ClaimID != source.SRC_ClaimID OR
    target.TransactionID != source.TransactionID OR
    target.PatientID != source.PatientID OR
    target.EncounterID != source.EncounterID OR
    target.ProviderID != source.ProviderID OR
    target.DeptID != source.DeptID OR
    target.ServiceDate != source.ServiceDate OR
    target.ClaimDate != source.ClaimDate OR
    target.PayorID != source.PayorID OR
    target.ClaimAmount != source.ClaimAmount OR
    target.PaidAmount != source.PaidAmount OR
    target.ClaimStatus != source.ClaimStatus OR
    target.PayorType != source.PayorType OR
    target.Deductible != source.Deductible OR
    target.Coinsurance != source.Coinsurance OR
    target.Copay != source.Copay OR
    target.SRC_InsertDate != source.SRC_InsertDate OR
    target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
    target.datasource != source.datasource OR
    target.is_quarantined != source.is_quarantined
) THEN
  UPDATE SET
    target.is_current = false,
    target.audit_modifieddate = current_timestamp()
""")

# COMMAND ----------

spark.sql("""
MERGE INTO silver.claims AS target
USING quality_checks AS source
ON target.ClaimID = source.ClaimID AND target.is_current = true
WHEN NOT MATCHED THEN
  INSERT (
    ClaimID,
    SRC_ClaimID,
    TransactionID,
    PatientID,
    EncounterID,
    ProviderID,
    DeptID,
    ServiceDate,
    ClaimDate,
    PayorID,
    ClaimAmount,
    PaidAmount,
    ClaimStatus,
    PayorType,
    Deductible,
    Coinsurance,
    Copay,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    audit_insertdate,
    audit_modifieddate,
    is_current
  )
  VALUES (
    source.ClaimID,
    source.SRC_ClaimID,
    source.TransactionID,
    source.PatientID,
    source.EncounterID,
    source.ProviderID,
    source.DeptID,
    source.ServiceDate,
    source.ClaimDate,
    source.PayorID,
    source.ClaimAmount,
    source.PaidAmount,
    source.ClaimStatus,
    source.PayorType,
    source.Deductible,
    source.Coinsurance,
    source.Copay,
    source.SRC_InsertDate,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    current_timestamp(),
    current_timestamp(),
    true
  )
""")

# COMMAND ----------

display(spark.sql("select * from silver.claims"))
