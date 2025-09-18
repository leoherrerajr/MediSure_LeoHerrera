-- Databricks notebook source
--refresh Diagnosis (ICD10) data from bronze layer
begin
-- Delete old entries
DELETE FROM medisure_llh.silver.diagnosis_dim
WHERE EXISTS (
  SELECT 1
  FROM medisure_llh.bronze.diagnosis src
  WHERE medisure_llh.silver.diagnosis_dim.code = src.code
);

-- Insert new entries
INSERT INTO medisure_llh.silver.diagnosis_dim
SELECT code,
       description,
       current_date() AS insertdate,
       current_date() AS updatedate
FROM medisure_llh.bronze.diagnosis;
end

-- COMMAND ----------

--load new member data from file to list table:

merge into medisure_llh.silver.members_lst tgt
using (
  select 
    MemberID,
    Name,
    DOB,
    Gender,
    Region,
    PlanType,
    EffectiveDate,
    Email,
    IsActive,
    LastUpdated
  from medisure_llh.bronze.members
) src
on tgt.MemberID = src.MemberID
when matched then update set
  tgt.Name = src.Name,
  tgt.DOB = src.DOB,
  tgt.Gender = src.Gender,
  tgt.Region = src.Region,
  tgt.PlanType = src.PlanType,
  tgt.EffectiveDate = src.EffectiveDate,
  tgt.Email = src.Email,
  tgt.IsActive = src.IsActive,
  tgt.LastUpdated = src.LastUpdated,
  tgt.UpdateDate = current_date()
when not matched then insert (
  MemberID,
  Name,
  DOB,
  Gender,
  Region,
  PlanType,
  EffectiveDate,
  Email,
  IsActive,
  LastUpdated,
  RecordFlag,
  InsertDate,
  UpdateDate
) values (
  src.MemberID,
  src.Name,
  src.DOB,
  src.Gender,
  src.Region,
  src.PlanType,
  src.EffectiveDate,
  src.Email,
  src.IsActive,
  src.LastUpdated,
  '',
  current_date(),
  current_date()
)

-- COMMAND ----------

--refresh provider main data

merge into medisure_llh.silver.providers_lst tgt
using (select providerid
            , name
            , Specialties
            , tin
            , lastverified
            , 'NEW' as RecordFlag
            , current_date() as insertdate
            , current_date() as updatedate
        from medisure_llh.bronze.providers) src
on tgt.ProviderID = src.ProviderID
when matched then update 
set tgt.Name = src.Name
, tgt.Specialties = src.Specialties
, tgt.TIN = src.TIN
, tgt.LastVerified = src.LastVerified
, tgt.RecordFlag = 'UPDATED'
, tgt.updatedate = current_date()
when not matched then insert (ProviderID
            , Name
            , Specialties
            , TIN
            , LastVerified
            , RecordFlag
            , insertdate
            , updatedate) 
     values (src.ProviderID
            , src.Name
            , src.Specialties
            , src.TIN
            , src.LastVerified
            , src.RecordFlag
            , src.insertdate
            , src.updatedate)

-- COMMAND ----------

--refresh provider specialty details

begin
  --delete existing provider's specialty
  delete from medisure_llh.silver.providers_spec_dtl tgt
  where exists (select 1
                from medisure_llh.bronze.providers src
                where tgt.ProviderID = src.ProviderID);

  insert into medisure_llh.silver.providers_spec_dtl
  select ProviderID
  , row_number() over(partition by ProviderID order by ProviderID) as spc_line_id
  , explode(Specialties)
  , 'NEW' as RecordFlag
  , current_date() as insertdate
  , current_date() as updatedate
  from medisure_llh.bronze.providers;
end


-- COMMAND ----------

--refresh locations details

begin
  --delete existing provider's location
  delete from medisure_llh.silver.providers_addr tgt
  where exists (select 1
                from medisure_llh.bronze.providers src
                where tgt.ProviderID = src.ProviderID);

  insert into medisure_llh.silver.providers_addr
  select providerid
      , address_line_id
      , address
      , city
      , state
      , 'NEW' as RecordFlag
      , current_date() as insertdate
      , current_date() as updatedate
  from (select providerid
            , row_number() over(partition by ProviderID order by ProviderID) address_line_id
            , explode(locations) as location_raw
            , location_raw.address as address
            , location_raw.city as city
          , location_raw.state as state
      from medisure_llh.bronze.providers);
end