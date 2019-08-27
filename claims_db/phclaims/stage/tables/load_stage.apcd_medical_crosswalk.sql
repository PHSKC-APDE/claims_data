--Code to load data to stage.apcd_medical_crosswalk
--Eli Kern (PHSKC-APDE)
--2019-6-27

------------------
--STEP 1: Insert data into table shell
-------------------
insert into PHClaims.stage.apcd_medical_crosswalk with (tablock)
--archived rows
select
[medical_claim_service_line_id]
,[extract_id]
,[inpatient_discharge_id]
,[medical_claim_header_id]
from PHclaims.archive.apcd_medical_crosswalk
--new rows from new extract
union
select
[medical_claim_service_line_id]
,[extract_id]
,[inpatient_discharge_id]
,[medical_claim_header_id]
from PHclaims.load_raw.apcd_medical_crosswalk;


------------------
--STEP 2: Temporary fix to get rid of duplicate rows except for extract ID [next time we should do complete overwrite for this table]
--Run time: 15 min
-------------------
--create table shell
if object_id('PHClaims.stage.apcd_medical_crosswalk_fix', 'U') is not null
	drop table PHClaims.stage.apcd_medical_crosswalk_fix;
create table PHClaims.stage.apcd_medical_crosswalk_fix (
	[medical_claim_service_line_id] [bigint] NULL,
	[extract_id] [int] NULL,
	[inpatient_discharge_id] [int] NULL,
	[medical_claim_header_id] [bigint] NULL
)
on [PRIMARY];

--flag claim lines with multiple extracts
if object_id('tempdb..#temp1') is not null drop table #temp1;
select a.medical_claim_service_line_id, a.extract_count
into #temp1
from (
select medical_claim_service_line_id, count(extract_id) as extract_count
from PHClaims.stage.apcd_medical_crosswalk
group by medical_claim_service_line_id
) as a
where a.extract_count > 1;

--insert data into table shell, keeping only latest extract for claim lines with multiple extract IDs
insert into PHClaims.stage.apcd_medical_crosswalk_fix with (tablock)
select a.medical_claim_service_line_id, a.extract_id, a.inpatient_discharge_id, a.medical_claim_header_id
from PHClaims.stage.apcd_medical_crosswalk as a
left join #temp1 as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id
where b.extract_count is null or
	(b.extract_count > 1 and a.extract_id = 159);

--drop old table
drop table PHClaims.stage.apcd_medical_crosswalk;
--rename new table

------------------
--STEP 3: Create clustered columnstore index (8 min)
-------------------
create clustered columnstore index idx_ccs_stage_apcd_medical_crosswalk on phclaims.stage.apcd_medical_crosswalk;

