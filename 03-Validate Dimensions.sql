-- Databricks notebook source
-- MAGIC %md
-- MAGIC This notebook validates dimension tables (Members and Providers)

-- COMMAND ----------

--find members with duplicate names/entries

update medisure_llh.silver.members_lst
set RecordFlag = 'DUPLICATE NAMES FOUND'
where name in (
            select name from (select Name, count(1)
                              from medisure_llh.silver.members_lst
                              group by Name
                              having count(1) > 1));

--update providers with no issues found
update medisure_llh.silver.members_lst
set RecordFlag = 'OK'
where RecordFlag = '';

select RecordFlag, count(1) rowcount
from medisure_llh.silver.members_lst
group by RecordFlag;

-- COMMAND ----------

update medisure_llh.silver.providers_addr
set RecordFlag = 'INVALID STATE'
where state not in (select state from medisure_llh.silver.state_lookup);

update medisure_llh.silver.providers_addr
set RecordFlag = 'OK'
where RecordFlag = 'NEW';


select RecordFlag, count(1)
from medisure_llh.silver.providers_addr
group by RecordFlag;

-- COMMAND ----------

--update providers with duplicate entries

update medisure_llh.silver.providers_lst
set RecordFlag = 'DUPLICATE NAMES FOUND'
where name in (
            select name from (select Name, count(1)
                              from medisure_llh.silver.providers_lst
                              group by Name
                              having count(1) > 1)) ;

--flag providers with invalid address
update medisure_llh.silver.providers_lst tgt
set RecordFlag = concat_ws(', ', RecordFlag, 'INVALID ADDRESS')
where providerid in (
            select providerid from medisure_llh.silver.providers_addr src
            where src.ProviderID = tgt.ProviderID
              and src.RecordFlag = 'INVALID STATE');

--update providers with no issues found
update medisure_llh.silver.providers_lst
set RecordFlag = 'OK'
where RecordFlag = 'NEW';

select RecordFlag, count(1) rowcount
from medisure_llh.silver.providers_lst
group by RecordFlag;