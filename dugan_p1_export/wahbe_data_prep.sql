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

--NEXT STEP -- Ask Margaret/WAHBE if I can share 222,809 ACES IDs with WAHBE to see if they can find them in their data?