--Code to load data to ref.apcd_claim_no_elig table
--A list of APCD member IDs for people with claims but no eligiblity information EVER
--Eli Kern (PHSKC-APDE)
--2019-7-9
--Run time: 21 min

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
--STEP 3: Select distinct IDs in claim header table
-------------------
if object_id('tempdb..#temp3') is not null drop table #temp3;
select distinct internal_member_id
into #temp3
from PHClaims.stage.apcd_medical_claim;

------------------
--STEP 4: Union header and line-based lists of IDs
--This became necessary after we found 5 members in line table that are not in header table
-------------------
if object_id('tempdb..#temp4') is not null drop table #temp4;
select internal_member_id
into #temp4
from #temp2
union
select internal_member_id
from #temp3;

------------------
--STEP 5: Select IDs in claim table but not in member_month table
-------------------
insert into PHClaims.ref.apcd_claim_no_elig with (tablock)
select internal_member_id as 'id_apcd' from #temp4
except
select internal_member_id from #temp1;