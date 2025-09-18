-- Databricks notebook source
-- MAGIC %md
-- MAGIC Cleanup duplicate claims and validate its details for audit and review purposes.

-- COMMAND ----------

--check and delete for duplicate claims

DELETE FROM medisure_llh.silver.claims_hdr
WHERE ClaimID IN (
  SELECT ClaimID
  FROM (
    SELECT
      ClaimID,
      MemberID,
      ProviderID,
      ROW_NUMBER() OVER (
        PARTITION BY ClaimID, MemberID, ProviderID
        ORDER BY ClaimID
      ) AS rn
    FROM medisure_llh.silver.claims_hdr
  ) t
  WHERE t.rn > 1
)

-- COMMAND ----------

--check and delete for incomplete key details
delete 
from medisure_llh.silver.claims_hdr
where ClaimID is null or MemberID is null or ProviderID is null;


-- COMMAND ----------

--audit claims for invalid providers

update medisure_llh.silver.claims_hdr
set AuditDtl = concat_ws(', ', AuditDtl, 'Invalid Provider')
where ProviderID not in (select ProviderID 
                          from medisure_llh.silver.providers_lst
                          where RecordFlag = 'OK')

-- COMMAND ----------

--validate claims' diagnosis

update medisure_llh.silver.claims_icd10_dtl
set auditflag = concat_ws(', ', auditflag, 'Invalid Diagnosis')
where ICD10Code not in (select ICD10Code 
                          from medisure_llh.silver.diagnosis_dim src);

update medisure_llh.silver.claims_hdr hdr
set AuditDtl = concat_ws(', ', AuditDtl, 'Invalid Diagnosis')
where ClaimID in (select ClaimID 
                    from medisure_llh.silver.claims_icd10_dtl dtl
                    where auditflag like '%Invalid Diagnosis%'
                      and dtl.claimid = hdr.claimid);
