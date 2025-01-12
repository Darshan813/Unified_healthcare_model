# Databricks notebook source

spark.catalog.clearCache()

# Databricks notebook source
from pyspark.sql import functions as f

# Read the CSV file
cptcodes_df = spark.read.parquet("/mnt/bronze/cpt_codes/")

# Replace whitespaces in column names with underscores and convert to lowercase
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)
cptcodes_df.createOrReplaceTempView("cptcodes")

# SQL
display(spark.sql("select * from cptcodes"))

# COMMAND ----------

# SQL
spark.sql("""
CREATE OR REPLACE TEMP VIEW quality_checks AS
SELECT 
 cpt_codes,
 procedure_code_category,
 procedure_code_descriptions,
 code_status,
    CASE 
        WHEN cpt_codes IS NULL OR procedure_code_descriptions IS NULL  THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM cptcodes
""")

# COMMAND ----------

# SQL
display(spark.sql("select * from quality_checks"))

# COMMAND ----------

# SQL
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.cptcodes (
cpt_codes string,
procedure_code_category string,
procedure_code_descriptions string,
code_status string,
is_quarantined boolean,
audit_insertdate timestamp,
audit_modifieddate timestamp,
is_current boolean
)
USING DELTA
""")

# COMMAND ----------

# SQL
spark.sql("""
-- Update old record to implement SCD Type 2
MERGE INTO silver.cptcodes AS target
USING quality_checks AS source
ON target.cpt_codes = source.cpt_codes AND target.is_current = true
WHEN MATCHED AND (
    target.procedure_code_category != source.procedure_code_category OR
    target.procedure_code_descriptions != source.procedure_code_descriptions OR
    target.code_status != source.code_status OR
    target.is_quarantined != source.is_quarantined
) THEN
  UPDATE SET
    target.is_current = false,
    target.audit_modifieddate = current_timestamp()
""")

# COMMAND ----------

# SQL
spark.sql("""
-- Insert new record to implement SCD Type 2
MERGE INTO silver.cptcodes AS target
USING quality_checks AS source
ON target.cpt_codes = source.cpt_codes AND target.is_current = true
WHEN NOT MATCHED THEN
  INSERT (
    cpt_codes,
    procedure_code_category,
    procedure_code_descriptions,
    code_status,
    is_quarantined,
    audit_insertdate,
    audit_modifieddate,
    is_current
  )
  VALUES (
    source.cpt_codes,
    source.procedure_code_category,
    source.procedure_code_descriptions,
    source.code_status,
    source.is_quarantined,
    current_timestamp(),
    current_timestamp(),
    true
  )
""")

# COMMAND ----------

# SQL
spark.sql("select * from silver.cptcodes")
