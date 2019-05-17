--Code to load data to ref.apcd_claim_no_elig table
--A list of APCD member IDs for people with claims but no eligiblity information EVER
--Eli Kern (PHSKC-APDE)
--2019-5-14
--Run time: 2 min

------------------
--STEP 1: Select distinct IDs in member_month_detail table
-------------------
if object_id('tempdb..#temp1') is not null drop table #temp1;
select distinct internal_member_id
into #temp1
from PHClaims.stage.apcd_member_month_detail;

------------------
--STEP 2: Select distinct IDs in claim header table
-------------------
if object_id('tempdb..#temp2') is not null drop table #temp2;
select distinct internal_member_id
into #temp2
from PHClaims.stage.apcd_medical_claim_header;

------------------
--STEP 3: Select IDs in claim table but not in member_month table
-------------------
insert into PHClaims.ref.apcd_claim_no_elig with (tablock)
select internal_member_id as 'id_apcd' from #temp2
except
select internal_member_id from #temp1;