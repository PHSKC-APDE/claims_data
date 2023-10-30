-----------------------------------------------------
--Code to prep WAHBE smoking status data for UW Dugan team
--Eli Kern, May 2023


-----------------------
--Step 1: Normalize ACES ID variable in WAHBE data
-----------------------

IF OBJECT_ID(N'tempdb..#temp1') IS NOT NULL drop table #temp1;
select cast(case
	when len(aces_id) = 7 then '00' + cast(aces_id as varchar(9))
	when len(aces_id) = 8 then '0' + cast(aces_id as varchar(9))
end as varchar(9)) as aces_id_norm,
smoking_status,
eligibility_start_date,
eligibility_end_date
into #temp1
from claims.tmp_ek_wahbe_report_1602;

--QA
--select distinct len(aces_id_norm) from #temp1;


-----------------------
--Step 2: Join WAHBE data to raw Medicaid eligibility data on ACES ID
--Subset to those that join (inner join)
--2 min
-----------------------

IF OBJECT_ID(N'tempdb..#temp2') IS NOT NULL drop table #temp2;
select a.*, b.MEDICAID_RECIPIENT_ID, b.MBR_ACES_IDNTFR
into #temp2
from #temp1 as a
inner join (select distinct MBR_ACES_IDNTFR, MEDICAID_RECIPIENT_ID from claims.stage_mcaid_elig) as b
on a.aces_id_norm = b.MBR_ACES_IDNTFR;


-----------------------
--Step 3: Join to table holding person ID generated for UW project
-----------------------

IF OBJECT_ID(N'tempdb..#temp3') IS NOT NULL drop table #temp3;
select a.*, b.MEDICAID_RECIPIENT_ID
into #temp3
from claims.tmp_ek_dugan_person_id as a
left join (select distinct MEDICAID_RECIPIENT_ID from #temp2) as b
on a.id_mcaid = b.MEDICAID_RECIPIENT_ID;

-----------------------
--Step 3-Export: Select data for sharing with UW team
-----------------------

--These row counts should match: PASS
--select count(*) from #temp3;
--select count(*) from claims.tmp_ek_dugan_person_id;

IF OBJECT_ID(N'claims.tmp_ek_dugan_wahbe_data', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_dugan_wahbe_data;
select distinct a.id_uw,
	b.smoking_status as hbe_smoking_status,
	b.eligibility_start_date as hbe_eligibility_start_date,
	b.eligibility_end_date as hbe_eligibility_end_date
into claims.tmp_ek_dugan_wahbe_data
from #temp3 as a
left join #temp2 as b
on a.id_mcaid = b.MEDICAID_RECIPIENT_ID;

--These row counts should match: PASS
--select count(distinct id_uw) from claims.tmp_ek_dugan_wahbe_data;
--select count(*) from claims.tmp_ek_dugan_person_id;

--These row counts should match: PASS -- 1,287,408
--select count(*) from claims.tmp_ek_dugan_wahbe_data;
--select count(*) from (select distinct id_uw, hbe_smoking_status, hbe_eligigibility_start_date, hbe_eligibility_end_date from claims.tmp_ek_dugan_wahbe_data) as a;


-----------------------
--Step 4: Pull out ACES IDs from P1 data for people in study period not found in WAHBE data
-----------------------

--Pull out ACES IDs for people not found in WAHBE data, count records by P1 ID-ACES ID
IF OBJECT_ID(N'tempdb..#temp4') IS NOT NULL drop table #temp4;
select distinct a.id_mcaid, b.MBR_ACES_IDNTFR, b.row_count
into #temp4
from #temp3 as a
left join (
select MEDICAID_RECIPIENT_ID, MBR_ACES_IDNTFR, count(*) as row_count
from claims.stage_mcaid_elig
group by MEDICAID_RECIPIENT_ID, MBR_ACES_IDNTFR
) as b
on a.id_mcaid = b.MEDICAID_RECIPIENT_ID
where a.MEDICAID_RECIPIENT_ID is null;

--Rank ACES ID based on number of monthly records (also include ACES ID in rank order to avoid ties)
IF OBJECT_ID(N'tempdb..#temp5') IS NOT NULL drop table #temp5;
select id_mcaid, MBR_ACES_IDNTFR, row_count,
	rank() over (partition by id_mcaid order by row_count desc, MBR_ACES_IDNTFR) as aces_id_rank
into #temp5
from #temp4;

--For those with more than 1 ACES ID, choose the one with the largest number of monthly records
IF OBJECT_ID(N'tempdb..#temp6') IS NOT NULL drop table #temp6;
select distinct id_mcaid, MBR_ACES_IDNTFR
into #temp6
from #temp5
where aces_id_rank = 1;

--QA to make sure no more than 1 ACES ID per MEDICAID ID: PASS
select count(*) from #temp6;
select count(distinct id_mcaid) from #temp6;

select count(*) from #temp6; -- 222,809 people did not match to WAHBE data (24.5%)
select count(distinct id_mcaid) from #temp3 where MEDICAID_RECIPIENT_ID is not null; --687,153 people matched to WAHBE data (75.5%)
select count(*) from claims.tmp_ek_dugan_person_id; --909,962 people in UW study


-----------------------
--Step 5: Compare demographic and coverage characteristics of Medicaid members included and excluded from WAHBE data
-----------------------

select top 1 * from #temp6;
select top 1 * from #temp3 where medicaid_recipient_id is not null;
select top 1 * from claims.final_mcaid_elig_timevar;

--Folks not in WAHBE data
IF OBJECT_ID(N'tempdb..#temp7') IS NOT NULL drop table #temp7;
select a.id_mcaid, a.mbr_aces_idntfr, b.dual, b.bsp_group_cid, b.full_benefit, b.cov_type, sum(b.cov_time_day) as cov_time_day
into #temp7
from #temp6 as a
left join claims.final_mcaid_elig_timevar as b
on a.id_mcaid = b.id_mcaid
where b.from_date <= '2021-12-31' and b.to_date >= '2016-01-01'
group by a.id_mcaid, a.mbr_aces_idntfr, b.dual, b.bsp_group_cid, b.full_benefit, b.cov_type;

IF OBJECT_ID(N'tempdb..#temp8') IS NOT NULL drop table #temp8;
select *, 
	rank() over (partition by id_mcaid order by cov_time_day desc, dual, bsp_group_cid, full_benefit, cov_type) as cov_rank
into #temp8
from #temp7;

IF OBJECT_ID(N'tempdb..#temp9') IS NOT NULL drop table #temp9;
select distinct id_mcaid, mbr_aces_idntfr, dual, bsp_group_cid, full_benefit, cov_type
into #temp9
from #temp8
where cov_rank = 1;

--Tabulate by coverage characteristics
select 1 as sort_order, 'overall' as cov_group_cat, cast('1' as varchar(255)) as cov_group, count(distinct id_mcaid) as id_dcount from #temp9
union
select 2 as sort_order, 'dual' as cov_group_cat, cast(dual as varchar(255)) as dual, count(distinct id_mcaid) as id_dcount from #temp9 group by dual
union
select 3 as sort_order, 'full_benefit' as cov_group_cat, cast(full_benefit as varchar(255)) as full_benefit,
	count(distinct id_mcaid) as id_dcount from #temp9 group by full_benefit
union
select 4 as sort_order, 'cov_type' as cov_group_cat, cast(cov_type as varchar(255)) as cov_type,
	count(distinct id_mcaid) as id_dcount from #temp9 group by cov_type
union
select 5 as sort_order, 'bsp_group_name' as cov_group_cat, b.bsp_group_name, count(distinct id_mcaid) as id_dcount
	from #temp9 as a
	left join claims.ref_mcaid_rac_code as b
	on a.bsp_group_cid = b.bsp_group_cid
	group by b.bsp_group_name
order by sort_order, cov_group_cat, cov_group;

--Folks included in WAHBE data
select top 1 * from #temp3 where medicaid_recipient_id is not null;
select top 1 * from claims.final_mcaid_elig_timevar;

IF OBJECT_ID(N'tempdb..#temp11') IS NOT NULL drop table #temp11;
select a.id_mcaid, b.dual, b.bsp_group_cid, b.full_benefit, b.cov_type, sum(b.cov_time_day) as cov_time_day
into #temp11
from (select id_mcaid from #temp3 where medicaid_recipient_id is not null) as a
left join claims.final_mcaid_elig_timevar as b
on a.id_mcaid = b.id_mcaid
where b.from_date <= '2021-12-31' and b.to_date >= '2016-01-01'
group by a.id_mcaid, b.dual, b.bsp_group_cid, b.full_benefit, b.cov_type;

IF OBJECT_ID(N'tempdb..#temp12') IS NOT NULL drop table #temp12;
select *, 
	rank() over (partition by id_mcaid order by cov_time_day desc, dual, bsp_group_cid, full_benefit, cov_type) as cov_rank
into #temp12
from #temp11;

IF OBJECT_ID(N'tempdb..#temp13') IS NOT NULL drop table #temp13;
select distinct id_mcaid, dual, bsp_group_cid, full_benefit, cov_type
into #temp13
from #temp12
where cov_rank = 1;

--Tabulate by coverage characteristics
select 1 as sort_order, 'overall' as cov_group_cat, cast('1' as varchar(255)) as cov_group, count(distinct id_mcaid) as id_dcount from #temp13
union
select 2 as sort_order, 'dual' as cov_group_cat, cast(dual as varchar(255)) as dual, count(distinct id_mcaid) as id_dcount from #temp13 group by dual
union
select 3 as sort_order, 'full_benefit' as cov_group_cat, cast(full_benefit as varchar(255)) as full_benefit,
	count(distinct id_mcaid) as id_dcount from #temp13 group by full_benefit
union
select 4 as sort_order, 'cov_type' as cov_group_cat, cast(cov_type as varchar(255)) as cov_type,
	count(distinct id_mcaid) as id_dcount from #temp13 group by cov_type
union
select 5 as sort_order, 'bsp_group_name' as cov_group_cat, b.bsp_group_name, count(distinct id_mcaid) as id_dcount
	from #temp13 as a
	left join claims.ref_mcaid_rac_code as b
	on a.bsp_group_cid = b.bsp_group_cid
	group by b.bsp_group_name
order by sort_order, cov_group_cat, cov_group;



