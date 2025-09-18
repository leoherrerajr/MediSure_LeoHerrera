-- Databricks notebook source
-- MAGIC %md
-- MAGIC Report as  Fraudulent claims for records found with invlid members, providers, and diagnosis 

-- COMMAND ----------

truncate table medisure_llh.gold.fraudulent_claims;

Insert into medisure_llh.gold.fraudulent_claims
(select ClaimID
, MemberID
, ProviderID
, ClaimDate
, ServiceDate
, Amount
, AuditDtl  
, current_date() as insertdate
from medisure_llh.silver.claims_hdr
where AuditDtl like '%INVALID%')

-- COMMAND ----------

