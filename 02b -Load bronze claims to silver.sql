-- Databricks notebook source
-- MAGIC %md
-- MAGIC Loads new claims and their respective diagnosis and treatment details to their respective tables.

-- COMMAND ----------

--load claims from bronze to header table in silver

MERGE INTO medisure_llh.silver.claims_hdr AS tgt
USING medisure_llh.bronze.claims AS src
ON tgt.ClaimID = src.ClaimID
 and tgt.memberid = src.memberid
WHEN MATCHED THEN
  UPDATE SET
    tgt.ProviderID = src.ProviderID,
    tgt.ClaimDate = src.ClaimDate,
    tgt.ServiceDate = src.ServiceDate,
    tgt.Amount = src.Amount,
    tgt.Status = src.Status,
    tgt.ICD10Codes = src.ICD10Codes,
    tgt.CPTCodes = src.CPTCodes,
    tgt.ClaimType = src.ClaimType,
    tgt.SubmissionChannel = src.SubmissionChannel,
    tgt.Notes = src.Notes,
    tgt.IngestTimestamp = src.IngestTimestamp,
    tgt.updatedate = current_date()
WHEN NOT MATCHED THEN
  INSERT (
    ClaimID,
    MemberID,
    ProviderID,
    ClaimDate,
    ServiceDate,
    Amount,
    Status,
    ICD10Codes,
    CPTCodes,
    ClaimType,
    SubmissionChannel,
    Notes,
    IngestTimestamp,
    insertdate,
    updatedate
  )
  VALUES (
    src.ClaimID,
    src.MemberID,
    src.ProviderID,
    src.ClaimDate,
    src.ServiceDate,
    src.Amount,
    src.Status,
    src.ICD10Codes,
    src.CPTCodes,
    src.ClaimType,
    src.SubmissionChannel,
    src.Notes,
    src.IngestTimestamp,
    current_date(),
    current_date()
  )

-- COMMAND ----------

--explode ICD10 codes array to detail table for audit claim analysis

merge into medisure_llh.silver.claims_icd10_dtl tgt
using (select claimid
            , MemberID
            , ProviderID
            , row_number() over(partition by claimid order by claimid) as claimlineID
            , explode(split(ICD10Codes,';')) as ICD10Code            
        from medisure_llh.bronze.claims)src
on tgt.claimid = src.claimid
  and tgt.claimlineID = src.claimlineID
  and tgt.memberID = src.memberID
  and tgt.ProviderID = src.ProviderID
when matched then update 
set tgt.ICD10Code = src.ICD10Code
, tgt.auditflag = 'UPDATE'
, tgt.updatedate = current_date()
when not matched then insert (claimid
            , memberID
            , ProviderID
            , claimlineID
            , ICD10Code
            , auditflag
            , insertdate
            , updatedate) 
     values (src.claimid
            , src.memberID
            , src.ProviderID
            , src.claimlineID
            , src.ICD10Code
            , 'NEW'
            , current_date()
            , current_date())


-- COMMAND ----------

--load exploded CPT codes to detail table for audit claim analysis

merge into medisure_llh.silver.claims_cpt_dtl tgt
using (select claimid
              , MemberID
              , ProviderID
              , row_number() over(partition by claimid order by claimid) as claimlineID
              , explode(split(CPTCodes,';')) as CPTCode            
        from medisure_llh.bronze.claims)src
on tgt.claimid = src.claimid
and tgt.claimlineID = src.claimlineID
and tgt.memberid = src.memberid
and tgt.ProviderID = src.ProviderID
when matched then update 
set tgt.CPTCode = src.CPTCode
, tgt.auditflag = 'UPDATE'
, tgt.updatedate = current_date()
when not matched then insert (claimid
              , memberID
              , ProviderID
            , claimlineID 
            , CPTCode
            , auditflag
            , insertdate
            , updatedate) 
     values (src.claimid
              , src.memberID
              , src.ProviderID
            , src.claimlineID
            , src.CPTCode
            , 'NEW'
            , current_date() 
            , current_date())