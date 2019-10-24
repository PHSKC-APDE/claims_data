--Code to load data to stage.apcd_elig_timevar table
--Member characteristics for which we are primarily interested in variance over time. 
--Includes program/eligibility coverage dates, residential address, and all geographic information (integer if possible). 
--Member ID is not distinct, but member ID-from_date is distinct.
--Eli Kern (PHSKC-APDE)
--2019-4-12

-- Code collapses data from 1+ rows per person per month to a single row of contiguous coverage for all time-varying variables (coverage type,
--dual flag, RAC code, ZIP code of residence
--Note for dental coverage: can't do same thing with dental coverage because there is no dental coverage information in member_month_detail
-- Takes 94 min to run

------------------
--Set extract max date which is used to convert future dates
--Add as function parameter to R function
------------------
declare @extract_date varchar(100);
set @extract_date = '2019-03-31';

-------------------
--STEP 1: Join distinct member IDs with year-month matrix
-------------------
if object_id('tempdb..#id') is not null drop table #id;
select distinct internal_member_id, 1 as flag
into #id
from phclaims.stage.apcd_member_month_detail;

if object_id('tempdb..#id_month') is not null drop table #id_month;
select a.internal_member_id, b.first_day_month, b.last_day_month
into #id_month
from #id as a
full join (select first_day_month, last_day_month, 1 as flag from phclaims.ref.date where first_day_month >= '2014-01-01' and last_day_month <= @extract_date) as b
on a.flag = b.flag;


-------------------
--STEP 2: Join with eligibility table by member ID
-------------------
if object_id('tempdb..#temp1') is not null drop table #temp1;
select a.internal_member_id, a.first_day_month, a.last_day_month, b.eligibility_start_dt, b.eligibility_end_dt, b.dual_flag, b.rac_code_id
into #temp1
from #id_month as a
left join (
  select internal_member_id, eligibility_start_dt,
  --set ongoing eligibility end dates to latest extract end date 
  case 
    when eligibility_end_dt > @extract_date then @extract_date
    else eligibility_end_dt
  end as eligibility_end_dt,
  --convert dual information to binary numeric flag
  case 
    when dual_eligibility_code_id in (-2,-1,5,6,7,15,16,29) then 0
    when dual_eligibility_code_id in (8,9,10,11,12,13,14,28) then 1
    else null
  end as dual_flag,
  cast(aid_category_id as int) as rac_code_id
  from phclaims.stage.apcd_eligibility
) as b
on a.internal_member_id = b.internal_member_id
where b.eligibility_start_dt <= a.last_day_month and b.eligibility_end_dt >= a.first_day_month;


-------------------
--STEP 3: Group by member month and select one dual flag, one RAC code
--Convert RAC codes to BSP codes, and add full benefit flag (based on RAC)
--Some Medicaid members have more than 1 RAC for an eligibility period (as we know), but as RACs
--are not desginated primary, we have to choose one
--Thus, take max of full_benefit flag, and then take max of BSP code (thus higher numbered BSPs chosen)
-------------------
if object_id('tempdb..#temp2') is not null drop table #temp2;
select x.internal_member_id, x.first_day_month, x.last_day_month, max(x.dual_flag) as dual_flag,
	max(x.bsp_group_cid) as bsp_group_cid, max(x.full_benefit) as full_benefit
into #temp2
from (
select a.internal_member_id, a.first_day_month, a.last_day_month, a.dual_flag, c.bsp_group_cid,
	case when c.full_benefit = 'Y' then 1 else 0 end as full_benefit
from #temp1 as a
left join PHClaims.ref.apcd_aid_category as b
on a.rac_code_id = b.aid_category_id
left join PHClaims.ref.mcaid_rac_code as c
on b.aid_category_code = c.rac_code
) as x
group by x.internal_member_id, x.first_day_month, x.last_day_month;


-------------------
--STEP 4: Join eligibility-based dual and RAC info to member_month_detail table and create coverage group variables
-------------------
if object_id('tempdb..#temp3') is not null drop table #temp3;
select a.internal_member_id, a.first_day_month as from_date,
  dateadd(day, -1, dateadd(month, 1, a.first_day_month)) as to_date,
  a.zip_code, b.dual_flag, b.bsp_group_cid, b.full_benefit,
  
  --create coverage categorical variable for medical coverage
  case
    when (a.med_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.med_commercial_eligibility_id is null and (a.med_medicare_eligibility_id is null and b.dual_flag = 0) then 1 --Medicaid only
    when (a.med_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.med_commercial_eligibility_id is null and (a.med_medicare_eligibility_id is not null and b.dual_flag = 0) then 2 --Medicare only
    when (a.med_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.med_commercial_eligibility_id is not null and a.med_medicare_eligibility_id is null then 3 --Commercial only
    when (a.med_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.med_commercial_eligibility_id is null and (a.med_medicare_eligibility_id is not null or b.dual_flag = 1) then 4 -- Medicaid-Medicare dual
    when (a.med_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.med_commercial_eligibility_id is not null and (a.med_medicare_eligibility_id is null and b.dual_flag = 0) then 5 --Medicaid-commercial dual
    when (a.med_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.med_commercial_eligibility_id is not null and (a.med_medicare_eligibility_id is not null and b.dual_flag = 0) then 6 --Medicare-commercial dual
    when (a.med_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.med_commercial_eligibility_id is not null and (a.med_medicare_eligibility_id is not null or b.dual_flag = 1) then 7 -- All three
    else 0 --no medical coverage
   end as med_covgrp,
  --create coverage categorical variable for medical coverage
  case
    when (a.rx_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.rx_commercial_eligibility_id is null and (a.rx_medicare_eligibility_id is null and b.dual_flag = 0) then 1 --Medicaid only
    when (a.rx_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.rx_commercial_eligibility_id is null and (a.rx_medicare_eligibility_id is not null and b.dual_flag = 0) then 2 --Medicare only
    when (a.rx_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.rx_commercial_eligibility_id is not null and a.rx_medicare_eligibility_id is null then 3 --Commercial only
    when (a.rx_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.rx_commercial_eligibility_id is null and (a.rx_medicare_eligibility_id is not null or b.dual_flag = 1) then 4 -- Medicaid-Medicare dual
    when (a.rx_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.rx_commercial_eligibility_id is not null and (a.rx_medicare_eligibility_id is null and b.dual_flag = 0) then 5 --Medicaid-commercial dual
    when (a.rx_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.rx_commercial_eligibility_id is not null and (a.rx_medicare_eligibility_id is not null and b.dual_flag = 0) then 6 --Medicare-commercial dual
    when (a.rx_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.rx_commercial_eligibility_id is not null and (a.rx_medicare_eligibility_id is not null or b.dual_flag = 1) then 7 -- All three
    else 0 --no pharm coverage
   end as pharm_covgrp
into #temp3
from (
select *, convert(date, cast(year_month as varchar(200)) + '01') as first_day_month
from phclaims.stage.apcd_member_month_detail
) as a
left join #temp2 as b
on a.internal_member_id = b.internal_member_id and a.first_day_month = b.first_day_month;


------------
--STEP 5: Assign a group number to each set of contiguous months by person, covgrp, dual_flag, BSP code, and full benefit flag, and ZIP code
----------------
if object_id('tempdb..#temp4') is not null drop table #temp4;
select distinct internal_member_id, from_date, to_date, zip_code, med_covgrp, pharm_covgrp, dual_flag, bsp_group_cid, full_benefit,
	datediff(month, '1900-01-01', from_date) - row_number() 
	over (partition by internal_member_id, zip_code, med_covgrp, pharm_covgrp, dual_flag, bsp_group_cid, full_benefit order by from_date) as group_num
into #temp4
from #temp3;


------------
--STEP 6: Taking the max and min of each contiguous period, collapse to one row
----------------
if object_id('tempdb..#temp5') is not null drop table #temp5;
select internal_member_id, zip_code, med_covgrp, pharm_covgrp, dual_flag, bsp_group_cid, full_benefit, min(from_date) as from_date, max(to_date) as to_date,
  datediff(day, min(from_date), max(to_date)) + 1 as cov_time_day
into #temp5
from #temp4
group by internal_member_id, zip_code, med_covgrp, pharm_covgrp, dual_flag, bsp_group_cid, full_benefit, group_num;


------------
--STEP 7: Add additional coverage flag and geographic variables and insert data into table shell
----------------
insert into PHClaims.stage.apcd_elig_timevar with (tablock)
select 
a.internal_member_id as id_apcd,
a.from_date,
a.to_date,
--Contiguous flag (contiguous with prior row)
case when datediff(day, lag(a.to_date, 1) over (partition by a.internal_member_id order by a.internal_member_id, a.from_date), a.from_date) = 1
then 1 else 0 end as contiguous,
a.med_covgrp,
a.pharm_covgrp, 
--Binary flags for medicaid and pharmacy coverage type
case when a.med_covgrp in (1,4,5,7) then 1 else 0 end as med_medicaid,
case when a.med_covgrp in (2,4,6,7) then 1 else 0 end as med_medicare,
case when a.med_covgrp in (3,5,6,7) then 1 else 0 end as med_commercial,
case when a.pharm_covgrp in (1,4,5,7) then 1 else 0 end as pharm_medicaid,
case when a.pharm_covgrp in (2,4,6,7) then 1 else 0 end as pharm_medicare,
case when a.pharm_covgrp in (3,5,6,7) then 1 else 0 end as pharm_commercial,
a.dual_flag as dual,
a.bsp_group_cid,
a.full_benefit,
a.zip_code as geo_zip,
d.countyfp as geo_county_code,
b.zip_group_desc as geo_county,
c.zip_group_code as geo_ach_code,
c.zip_group_desc as geo_ach,
case when b.zip_group_code is not null then 1 else 0 end as geo_wa,
a.cov_time_day,
getdate() as last_run
from #temp5 as a
left join (select zip_code, zip_group_code, zip_group_desc from phclaims.ref.apcd_zip_group where zip_group_type_desc = 'County') as b
on a.zip_code = b.zip_code
left join (select zip_code, zip_group_code, zip_group_desc from phclaims.ref.apcd_zip_group where left(zip_group_type_desc, 3) = 'Acc') as c
on a.zip_code = c.zip_code
left join PHClaims.ref.geo_county_code_wa as d
on b.zip_group_code = d.countyn;