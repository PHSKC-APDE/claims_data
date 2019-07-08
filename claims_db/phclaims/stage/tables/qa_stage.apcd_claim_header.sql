--QA of stage.apcd_claim_header table
--4/29/19
--Eli Kern


------------------
--STEP 1: Prepare temp table that is copy of medical_claim_header table with denied and orphaned claims
--re-written using min of line-level values; then use this table for comparisons below
--Note that this temp table also excludes members with no elig information
--Run time: 42 min
------------------
if object_id('tempdb..#temp1') is not null drop table #temp1;
select x.internal_member_id, x.medical_claim_header_id, x.internal_provider_id, x.icd_version_ind,
	x.emergency_room_flag, x.operating_room_flag, y.denied_line_min, y.orphaned_line_min
into #temp1
from (select * from PHClaims.stage.apcd_medical_claim_header where internal_member_id not in (select id_apcd from PHClaims.ref.apcd_claim_no_elig)) as x
left join (
select a.medical_claim_header_id, min(case when b.denied_claim_flag = 'Y' then 1 else 0 end) as denied_line_min,
	min(case when b.orphaned_adjustment_flag = 'Y' then 1 else 0 end) as orphaned_line_min
from PHClaims.stage.apcd_medical_crosswalk as a
left join PHClaims.stage.apcd_medical_claim as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id
group by a.medical_claim_header_id
) as y
on x.medical_claim_header_id = y.medical_claim_header_id;

---------------------
--STEP 2: Run QA
---------------------

--Confirm that claim header is distinct
select count(distinct claim_header_id) as header_cnt
from PHClaims.stage.apcd_claim_header;

select count(claim_header_id) as header_cnt
from PHClaims.stage.apcd_claim_header;

--Compare member counts
select count(distinct id_apcd) as member_cnt
from PHClaims.stage.apcd_claim_header;

select count(distinct internal_member_id) as member_cnt
from #temp1
where denied_line_min = 0 and orphaned_line_min = 0;

--All members should be in elig_demo and elig_timevar tables
select count(a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_header as a
left join PHClaims.final.apcd_elig_demo as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

select count(a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_header as a
left join PHClaims.final.apcd_elig_timevar as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

--Compare claim header counts
select count(distinct claim_header_id) as header_cnt
from PHClaims.stage.apcd_claim_header;

select count(distinct medical_claim_header_id) as header_cnt
from #temp1
where denied_line_min = 0 and orphaned_line_min = 0;

--Compare sum of provider ID
select sum(provider_id_apcd) as provider_id_sum
from PHClaims.stage.apcd_claim_header;

select sum(cast(internal_provider_id as bigint)) as provider_id_sum
from #temp1
where denied_line_min = 0 and orphaned_line_min = 0;

--Compare claim header counts by ICD-CM version
select icdcm_version, count(claim_header_id) as header_cnt
from PHClaims.stage.apcd_claim_header
group by icdcm_version;

select icd_version_ind, count(medical_claim_header_id) as header_cnt
from #temp1
where denied_line_min = 0 and orphaned_line_min = 0
group by icd_version_ind;

--Compare ED visit counts
select sum(ed_flag) as ed_cnt
from PHClaims.stage.apcd_claim_header;

select sum(case when emergency_room_flag = 'Y' then 1 else 0 end) as ed_cnt
from #temp1
where denied_line_min = 0 and orphaned_line_min = 0;

--Compare OR event counts
select sum(or_flag) as or_cnt
from PHClaims.stage.apcd_claim_header;

select sum(case when operating_room_flag = 'Y' then 1 else 0 end) as or_cnt
from #temp1
where denied_line_min = 0 and orphaned_line_min = 0;

--Compare acute inpatient stay counts
--Run time for this plus next block of code: 32 min
select sum(a.ipt_flag) as ipt_count
from (
select id_apcd, discharge_date, max(ipt_flag) as ipt_flag
from PHClaims.stage.apcd_claim_header
group by id_apcd, discharge_date
) as a;

--This code appears convoluted, but it's because it has to mimic process through which ipt claim lines are grouped by
--claim header (taking max of discharge dt), then grouped by id and discharge date, then summed
--Note that this code appropriately excludes members with no elig information
select sum(e.ipt_flag) as ipt_count
from (
	select d.id_apcd, d.discharge_dt, max(d.ipt_flag) as ipt_flag
	from (
		select c.medical_claim_header_id, max(a.internal_member_id) as id_apcd, max(a.discharge_dt) as discharge_dt,
			max(a.ipt_flag) as ipt_flag
		from (
		select medical_claim_service_line_id, internal_member_id, discharge_dt, 1 as ipt_flag,
			denied_claim_flag, orphaned_adjustment_flag
		from PHClaims.stage.apcd_medical_claim 
		where claim_type_id = '1' and type_of_setting_id = '1' and place_of_setting_id = '1'
			and (denied_claim_flag = 'N' AND orphaned_adjustment_flag = 'N')
			and claim_status_id in (-1, -2, 1, 5, 2, 6)
			and discharge_dt is not null
			and internal_member_id not in (select id_apcd from PHClaims.ref.apcd_claim_no_elig)
		) as a
		left join PHClaims.stage.apcd_medical_crosswalk as b
		on a.medical_claim_service_line_id = b.medical_claim_service_line_id
		left join PHClaims.stage.apcd_medical_claim_header as c
		on b.medical_claim_header_id = c.medical_claim_header_id
		group by c.medical_claim_header_id
	) as d
	group by d.id_apcd, d.discharge_dt
) as e;

--Check an individual inpatient stay to make sure discharge date has come through correctly
select id_apcd, claim_header_id, ipt_flag, discharge_date
from PHClaims.stage.apcd_claim_header
where claim_header_id = 629250577489368;

select a.*
from PHClaims.stage.apcd_medical_claim as a
left join PHClaims.stage.apcd_medical_crosswalk as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id
where b.medical_claim_header_id = 629250577489368;

--Verify that an example of a denied and orphaned claim header is excluded
select * 
from PHClaims.stage.apcd_claim_header
where claim_header_id = 629246595882276;

select * 
from PHClaims.stage.apcd_claim_header
where claim_header_id = 629250026243188;

--Query distinct APCD claim types by KC claim type
select distinct claim_type_id, claim_type_apcd_id
from PHClaims.stage.apcd_claim_header;

--Should be no claims where ipt_flag = 1 and discharge_date is null
select count(*)
from PHClaims.stage.apcd_claim_header
where ipt_flag = 1 and discharge_date is null;




-------------------------
--SAVING OLD CODE USED FOR EXPLORING DIFFERENCE IN HEADER VS LINE IPT COUNTS
-------------------------
--It appears due to >1 discharge dates being linkd to a single header
--Run time for these 3 temp tables: 10 min
if object_id('tempdb..#temp2') is not null drop table #temp2;
select distinct id_apcd, discharge_date
into #temp2
from PHClaims.stage.apcd_claim_header
where ipt_flag = 1;

if object_id('tempdb..#temp3') is not null drop table #temp3;
select distinct internal_member_id as id_apcd, discharge_dt
into #temp3
from PHClaims.stage.apcd_medical_claim 
where claim_type_id = '1' and type_of_setting_id = '1' and place_of_setting_id = '1'
	and (denied_claim_flag = 'N' AND orphaned_adjustment_flag = 'N')
	and claim_status_id in (-1, -2, 1, 5, 2, 6)
	and discharge_dt is not null;

if object_id('tempdb..#temp4') is not null drop table #temp4;
select id_apcd, discharge_dt into #temp4 from #temp3
except
select id_apcd, discharge_dt from #temp2;

select top 1000 * from #temp4
where id_apcd = 12168952259;

select * from #temp2
where id_apcd = 12168952259;

select *
from PHClaims.stage.apcd_medical_claim 
where internal_member_id = 12760940196 and discharge_dt >= '2018-02-01' and discharge_dt <= '2018-02-28';

select distinct medical_claim_header_id, inpatient_discharge_id
from PHClaims.stage.apcd_medical_crosswalk
where medical_claim_service_line_id in (629257854668948
,629257854668945
,629257854668951
,629257854668946
,629257854668953
,629257854668944
,629257854668943
,629257854668952
,629257854668941
,629257854668954
,629257854668950
,629257854668942
,629257854668955
,629257854668949
,629257854668947
,629257854369542
,629257854369537
,629257854369543
,629257854369539
,629257854369545
,629257854369544
,629257854369541
,629257854369540
,629257854369538);
