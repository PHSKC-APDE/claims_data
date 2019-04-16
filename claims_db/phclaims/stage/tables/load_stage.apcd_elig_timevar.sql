--Code to load data to stage.apcd_elig_timevar table
--Member characteristics for which we are primarily interested in variance over time. 
--Includes program/eligibility coverage dates, residential address, and all geographic information (integer if possible). 
--Member ID is not distinct, but member ID-from_date is distinct.
--Eli Kern (PHSKC-APDE)
--2019-4-12

-- Code collapses data from 1+ rows per person per month to a single row of contiguous coverage for all time-varying variables (coverage type,
--dual flag, RAC code, ZIP code of residence
-- Takes 80 min to run

-------------------
--STEP 1: Join distinct member IDs with year-month matrix
-------------------
if object_id('tempdb..#id') is not null drop table #id;
select distinct internal_member_id, 1 as flag
into #id
from phclaims.stage.apcd_member_month_detail;

if object_id('tempdb..#id_month') is not null drop table #id_month;
select a.internal_member_id, b.beg_month, b.end_month
into #id_month
from #id as a
full join (select beg_month, end_month, 1 as flag from phclaims.ref.year_month where beg_month >= '2014-01-01' and end_month <= '2018-06-30') as b
on a.flag = b.flag;


-------------------
--STEP 2: Join with eligibility table by member ID
-------------------
if object_id('tempdb..#temp1') is not null drop table #temp1;
select a.internal_member_id, a.beg_month, a.end_month, b.eligibility_start_dt, b.eligibility_end_dt, b.dual_flag, b.rac_code
into #temp1
from #id_month as a
left join (
  select internal_member_id, eligibility_start_dt,
  --set ongoing eligibility end dates to latest extract end date 
  case 
    when eligibility_end_dt > '2018-06-30' then '2018-06-30'
    else eligibility_end_dt
  end as eligibility_end_dt,
  --convert dual information to binary numeric flag
  case 
    when dual_eligibility_code_id in (-2,-1,5,6,7,15,16,29) then 0
    when dual_eligibility_code_id in (8,9,10,11,12,13,14,28) then 1
    else null
  end as dual_flag,
  cast(aid_category_code as int) as rac_code
  from phclaims.stage.apcd_eligibility
) as b
on a.internal_member_id = b.internal_member_id
where b.eligibility_start_dt <= a.end_month and b.eligibility_end_dt >= a.beg_month;


-------------------
--STEP 3: Group by member month and select one dual flag, RAC code
-------------------
if object_id('tempdb..#temp2') is not null drop table #temp2;
select a.internal_member_id, a.beg_month, a.end_month, a.dual_flag, a.rac_code
into #temp2
from (
  select internal_member_id, beg_month, end_month, max(dual_flag) as dual_flag, max(rac_code) as rac_code
  from #temp1
  group by internal_member_id, beg_month, end_month
) as a;


-------------------
--STEP 4: Join eligibility-based dual and RAC info to member_month_detail table and create coverage group variables
-------------------
if object_id('tempdb..#temp3') is not null drop table #temp3;
select a.internal_member_id, a.beg_month as from_date,
  dateadd(day, -1, dateadd(month, 1, a.beg_month)) as to_date,
  a.zip_code, b.dual_flag, b.rac_code,
  
  --create coverage categorical variable for medical coverage
  case
    when (a.med_medicaid_eligibility_id is not null or b.rac_code > 0) and a.med_commercial_eligibility_id is null and (a.med_medicare_eligibility_id is null and b.dual_flag = 0) then 1 --Medicaid only
    when (a.med_medicaid_eligibility_id is null and b.rac_code < 0) and a.med_commercial_eligibility_id is null and (a.med_medicare_eligibility_id is not null and b.dual_flag = 0) then 2 --Medicare only
    when (a.med_medicaid_eligibility_id is null and b.rac_code < 0) and a.med_commercial_eligibility_id is not null and a.med_medicare_eligibility_id is null then 3 --Commercial only
    when (a.med_medicaid_eligibility_id is not null or b.rac_code > 0) and a.med_commercial_eligibility_id is null and (a.med_medicare_eligibility_id is not null or b.dual_flag = 1) then 4 -- Medicaid-Medicare dual
    when (a.med_medicaid_eligibility_id is not null or b.rac_code > 0) and a.med_commercial_eligibility_id is not null and (a.med_medicare_eligibility_id is null and b.dual_flag = 0) then 5 --Medicaid-commercial dual
    when (a.med_medicaid_eligibility_id is null and b.rac_code < 0) and a.med_commercial_eligibility_id is not null and (a.med_medicare_eligibility_id is not null and b.dual_flag = 0) then 6 --Medicare-commercial dual
    when (a.med_medicaid_eligibility_id is not null or b.rac_code > 0) and a.med_commercial_eligibility_id is not null and (a.med_medicare_eligibility_id is not null or b.dual_flag = 1) then 7 -- All three
    else 0 --no medical coverage
   end as med_covgrp,
  --create coverage categorical variable for medical coverage
  case
    when (a.rx_medicaid_eligibility_id is not null or b.rac_code > 0) and a.rx_commercial_eligibility_id is null and (a.rx_medicare_eligibility_id is null and b.dual_flag = 0) then 1 --Medicaid only
    when (a.rx_medicaid_eligibility_id is null and b.rac_code < 0) and a.rx_commercial_eligibility_id is null and (a.rx_medicare_eligibility_id is not null and b.dual_flag = 0) then 2 --Medicare only
    when (a.rx_medicaid_eligibility_id is null and b.rac_code < 0) and a.rx_commercial_eligibility_id is not null and a.rx_medicare_eligibility_id is null then 3 --Commercial only
    when (a.rx_medicaid_eligibility_id is not null or b.rac_code > 0) and a.rx_commercial_eligibility_id is null and (a.rx_medicare_eligibility_id is not null or b.dual_flag = 1) then 4 -- Medicaid-Medicare dual
    when (a.rx_medicaid_eligibility_id is not null or b.rac_code > 0) and a.rx_commercial_eligibility_id is not null and (a.rx_medicare_eligibility_id is null and b.dual_flag = 0) then 5 --Medicaid-commercial dual
    when (a.rx_medicaid_eligibility_id is null and b.rac_code < 0) and a.rx_commercial_eligibility_id is not null and (a.rx_medicare_eligibility_id is not null and b.dual_flag = 0) then 6 --Medicare-commercial dual
    when (a.rx_medicaid_eligibility_id is not null or b.rac_code > 0) and a.rx_commercial_eligibility_id is not null and (a.rx_medicare_eligibility_id is not null or b.dual_flag = 1) then 7 -- All three
    else 0 --no pharm coverage
   end as pharm_covgrp
into #temp3
from (
select *, convert(date, cast(year_month as varchar(200)) + '01') as beg_month
from phclaims.stage.apcd_member_month_detail
) as a
left join #temp2 as b
on a.internal_member_id = b.internal_member_id and a.beg_month = b.beg_month;


------------
--STEP 5: Assign a group number to each set of contiguous months by person, covgrp, dual_flag, RAC code, and ZIP code
----------------
if object_id('tempdb..#temp4') is not null drop table #temp4;
select distinct internal_member_id, from_date, to_date, zip_code, med_covgrp, pharm_covgrp, dual_flag, rac_code,
	datediff(month, '1900-01-01', from_date) - row_number() 
	over (partition by internal_member_id, zip_code, med_covgrp, pharm_covgrp, dual_flag, rac_code order by from_date) as group_num
into #temp4
from #temp3;


------------
--STEP 6: Taking the max and min of each contiguous period, collapse to one row
----------------
if object_id('tempdb..#temp5') is not null drop table #temp5;
select internal_member_id, zip_code, med_covgrp, pharm_covgrp, dual_flag, rac_code, min(from_date) as from_date, max(to_date) as to_date,
  datediff(day, min(from_date), max(to_date)) + 1 as cov_time_day
into #temp5
from #temp4
group by internal_member_id, zip_code, med_covgrp, pharm_covgrp, dual_flag, rac_code, group_num;


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
a.rac_code,
a.zip_code as geo_zip_code,
b.zip_group_code as geo_county_code,
b.zip_group_desc as geo_county,
c.zip_group_code as geo_ach_code,
c.zip_group_desc as geo_ach,
a.cov_time_day
from #temp5 as a
left join (select zip_code, zip_group_code, zip_group_desc from phclaims.ref.apcd_zip_group where zip_group_type_desc = 'County') as b
on a.zip_code = b.zip_code
left join (select zip_code, zip_group_code, zip_group_desc from phclaims.ref.apcd_zip_group where left(zip_group_type_desc, 3) = 'Acc') as c
on a.zip_code = c.zip_code;