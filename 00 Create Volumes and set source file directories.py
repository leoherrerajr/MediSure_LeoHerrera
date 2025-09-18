# Databricks notebook source
#set path
base_path = "/Volumes/medisure_llh"
bronze_path = f"{base_path}/bronze"
file_dir = f"{bronze_path}/source_files"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"
chk_path = f"{base_path}/checkpoint"

print(file_dir)
print(bronze_path)
print(silver_path)



# COMMAND ----------


#create member list table
from pyspark.sql.functions import col, current_date, date_format

spark.sql(f"""
CREATE TABLE IF NOT EXISTS medisure_llh.silver.members_lst (
         MemberID string
        , Name string
        , DOB date
        , Gender string
        , Region string
        , PlanType string
        , EffectiveDate date
        , Email string
        , IsActive double
        , LastUpdated date
        , RecordFlag STRING
        , InsertDate DATE
        , UpdateDate DATE        
        )
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC --create diagnosis dimension table
# MAGIC
# MAGIC create table if not exists medisure_llh.silver.diagnosis_dim (
# MAGIC   code string,
# MAGIC   description string,
# MAGIC   insertdate date,
# MAGIC   updatedate date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --table for claims' details
# MAGIC
# MAGIC create table if not exists medisure_llh.silver.claims_hdr (
# MAGIC   ClaimID string,
# MAGIC   MemberID string,
# MAGIC   ProviderID string,
# MAGIC   ClaimDate date,
# MAGIC   ServiceDate date,
# MAGIC   Amount double,
# MAGIC   Status string,
# MAGIC   ICD10Codes string,
# MAGIC   CPTCodes string,
# MAGIC   ClaimType string,
# MAGIC   SubmissionChannel string,
# MAGIC   Notes string,
# MAGIC   IngestTimestamp timestamp,
# MAGIC   AuditDtl  string,
# MAGIC   insertdate date,
# MAGIC   updatedate date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop objects
# MAGIC
# MAGIC --drop table if exists medisure_llh.silver.claims_icd10_dtl
# MAGIC --drop table if exists medisure_llh.silver.claims_cpt_dtl
# MAGIC --drop table if exists medisure_llh.silver.claims_hdr
# MAGIC drop table if exists medisure_llh.silver.providers_lst

# COMMAND ----------

# MAGIC %sql
# MAGIC --detail table for claims' ICD10 code array
# MAGIC create table if not exists medisure_llh.silver.claims_icd10_dtl (
# MAGIC   ClaimID string,
# MAGIC   MemberID string,
# MAGIC   ProviderID string,
# MAGIC   ClaimLineID int,
# MAGIC   ICD10Code string,
# MAGIC   auditflag string,
# MAGIC   insertdate date,
# MAGIC   updatedate date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --detail table for claims' cpt code array
# MAGIC
# MAGIC create table if not exists medisure_llh.silver.claims_cpt_dtl (
# MAGIC   ClaimID string,
# MAGIC   MemberID string,
# MAGIC   ProviderID string,
# MAGIC   ClaimLineID int,
# MAGIC   CPTCode string,
# MAGIC   auditflag string,
# MAGIC   insertdate date,
# MAGIC   updatedate date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from medisure_llh.bronze.providers

# COMMAND ----------

# MAGIC %sql
# MAGIC --create provider list table
# MAGIC CREATE TABLE IF NOT EXISTS medisure_llh.silver.providers_lst (
# MAGIC          ProviderID string
# MAGIC         , Name string
# MAGIC         , Specialties string
# MAGIC         , TIN string
# MAGIC         , LastVerified date
# MAGIC         , RecordFlag STRING
# MAGIC         , InsertDate DATE
# MAGIC         , UpdateDate DATE        
# MAGIC         )

# COMMAND ----------

# MAGIC %sql
# MAGIC --create provider address table
# MAGIC CREATE TABLE IF NOT EXISTS medisure_llh.silver.providers_addr (
# MAGIC          ProviderID string
# MAGIC         ,address_line_id int
# MAGIC         ,address string
# MAGIC         ,city string
# MAGIC         ,state string
# MAGIC         ,RecordFlag STRING 
# MAGIC         ,InsertDate DATE
# MAGIC         ,UpdateDate DATE        
# MAGIC         )

# COMMAND ----------

# MAGIC %sql
# MAGIC --create provider specialty list
# MAGIC CREATE TABLE IF NOT EXISTS medisure_llh.silver.providers_spec_dtl (
# MAGIC          ProviderID string
# MAGIC         , spec_line_id int
# MAGIC         ,Specialty string
# MAGIC         ,RecordFlag STRING
# MAGIC         ,InsertDate DATE       
# MAGIC         ,UpdateDate DATE        
# MAGIC         )

# COMMAND ----------

# MAGIC %sql
# MAGIC --create state lookup table for address validation
# MAGIC
# MAGIC CREATE TABLE medisure_llh.silver.state_lookup (
# MAGIC     StateCode STRING,
# MAGIC     StateName STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO medisure_llh.silver.state_lookup (StateCode, StateName) VALUES
# MAGIC     ('AL', 'Alabama'),
# MAGIC     ('AK', 'Alaska'),
# MAGIC     ('AZ', 'Arizona'),
# MAGIC     ('AR', 'Arkansas'),
# MAGIC     ('CA', 'California'),
# MAGIC     ('CO', 'Colorado'),
# MAGIC     ('CT', 'Connecticut'),
# MAGIC     ('DE', 'Delaware'),
# MAGIC     ('FL', 'Florida'),
# MAGIC     ('GA', 'Georgia'),
# MAGIC     ('HI', 'Hawaii'),
# MAGIC     ('ID', 'Idaho'),
# MAGIC     ('IL', 'Illinois'),
# MAGIC     ('IN', 'Indiana'),
# MAGIC     ('IA', 'Iowa'),
# MAGIC     ('KS', 'Kansas'),
# MAGIC     ('KY', 'Kentucky'),
# MAGIC     ('LA', 'Louisiana'),
# MAGIC     ('ME', 'Maine'),
# MAGIC     ('MD', 'Maryland'),
# MAGIC     ('MA', 'Massachusetts'),
# MAGIC     ('MI', 'Michigan'),
# MAGIC     ('MN', 'Minnesota'),
# MAGIC     ('MS', 'Mississippi'),
# MAGIC     ('MO', 'Missouri'),
# MAGIC     ('MT', 'Montana'),
# MAGIC     ('NE', 'Nebraska'),
# MAGIC     ('NV', 'Nevada'),
# MAGIC     ('NH', 'New Hampshire'),
# MAGIC     ('NJ', 'New Jersey'),
# MAGIC     ('NM', 'New Mexico'),
# MAGIC     ('NY', 'New York'),
# MAGIC     ('NC', 'North Carolina'),
# MAGIC     ('ND', 'North Dakota'),
# MAGIC     ('OH', 'Ohio'),
# MAGIC     ('OK', 'Oklahoma'),
# MAGIC     ('OR', 'Oregon'),
# MAGIC     ('PA', 'Pennsylvania'),
# MAGIC     ('RI', 'Rhode Island'),
# MAGIC     ('SC', 'South Carolina'),
# MAGIC     ('SD', 'South Dakota'),
# MAGIC     ('TN', 'Tennessee'),
# MAGIC     ('TX', 'Texas'),
# MAGIC     ('UT', 'Utah'),
# MAGIC     ('VT', 'Vermont'),
# MAGIC     ('VA', 'Virginia'),
# MAGIC     ('WA', 'Washington'),
# MAGIC     ('WV', 'West Virginia'),
# MAGIC     ('WI', 'Wisconsin'),
# MAGIC     ('WY', 'Wyoming');

# COMMAND ----------

# MAGIC %sql
# MAGIC --create fraudulent claims audit report
# MAGIC
# MAGIC create table if not exists medisure_llh.gold.fraudulent_claims (
# MAGIC   ClaimID string,
# MAGIC   MemberID string,
# MAGIC   ProviderID string,
# MAGIC   ClaimDate date,
# MAGIC   ServiceDate date,
# MAGIC   Amount double,
# MAGIC   AuditDtl  string,
# MAGIC   insertdate date
# MAGIC )