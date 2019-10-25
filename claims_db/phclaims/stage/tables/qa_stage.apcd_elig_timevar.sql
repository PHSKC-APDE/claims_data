--QA of stage.apcd_elig_demo table
--4/11/19
--Eli Kern

----------------
--INTERNAL CONSISTENCY: Aggregate member counts across tables
----------------

--All people using elig_timevar
select count(distinct id_apcd) as id_dcount_timevar
from phclaims.stage.apcd_elig_timevar;
  
--All people using member_month_detail
select count(distinct internal_member_id) as id_dcount_mm
from phclaims.stage.apcd_member_month_detail;

--Member counts should match stage.apcd_elig_demo table
select count(distinct id_apcd) as id_dcount_demo
from PHClaims.final.apcd_elig_demo;
  
--All King people in 2016 using elig_covgrp
select count(distinct id_apcd) as id_dcount_timevar
from phclaims.stage.apcd_elig_timevar
where from_date <= '2016-12-31' and to_date >= '2016-01-01'
  and geo_ach = 'King';
  
--All King people in 2016 using member_month_detail
select count(distinct internal_member_id) as id_dcount_mm
from phclaims.stage.apcd_member_month_detail
where left(year_month,4) = '2016'
  and zip_code in (select zip_code from phclaims.ref.apcd_zip_group where zip_group_desc = 'King' and zip_group_type_desc = 'County');
  
--All King people in 2016 using eligibility
select count(distinct internal_member_id) as id_dcount_elig
from phclaims.stage.apcd_eligibility
where eligibility_start_dt <= '2016-12-31' and eligibility_end_dt >= '2016-01-01'
  and zip in (select zip_code from phclaims.ref.apcd_zip_group where zip_group_desc = 'King' and zip_group_type_desc = 'County');
  
--Investigate people who are in elig but not member_month
--ANSWER: All of these people actually show up in the member_month_detail table and thus the source of the difference is ZIP code. When
--a member has more than one ZIP code for a time period in eligibility table, OnPoint selects a single ZIP code. If this is not in King County then
--this results in an apparent undercount.
if object_id('tempdb..#temp1') is not null drop table #temp1;
select b.internal_member_id
into #temp1
from (
select distinct internal_member_id
from phclaims.stage.apcd_member_month_detail
where left(year_month,4) = '2016'
  and zip_code in (select zip_code from phclaims.ref.apcd_zip_group where zip_group_desc = 'King' and zip_group_type_desc = 'County')
) as a
right join (
select distinct internal_member_id
from phclaims.stage.apcd_eligibility
where eligibility_start_dt <= '2016-12-31' and eligibility_end_dt >= '2016-01-01'
  and zip in (select zip_code from phclaims.ref.apcd_zip_group where zip_group_desc = 'King' and zip_group_type_desc = 'County')
) as b
on a.internal_member_id = b.internal_member_id
where a.internal_member_id is null;

select a.eligibility_id, a.submitter_id, a.internal_member_id, a.coverage_class, a.eligibility_start_dt, a.eligibility_end_dt, a.product_code, a.dual_eligibility_code, a.aid_category_id
from phclaims.stage.apcd_eligibility as a
inner join #temp1 as b
on a.internal_member_id = b.internal_member_id
order by a.internal_member_id, eligibility_start_dt

select distinct a.internal_member_id, a.year_month, a.medical_eligibility_id, a.pharmacy_eligibility_id, a.zip_code
from phclaims.stage.apcd_member_month_detail as a
right join #temp1 as b
on a.internal_member_id = b.internal_member_id
where left(a.year_month,4) = '2016'
order by a.internal_member_id, a.year_month


----------------
--INTERNAL CONSISTENCY: Member by member QA
--Note that Medicare FFS eligibility segments appear to be added as monthly records to eligibility table
----------------
--member with med_covgrp = 0: 12381222126
--member with med_covgrp = 1: 11057531420
--member with med_covgrp = 2: 12597524324
--member with med_covgrp = 3: 11051242852
--member with med_covgrp = 4: 12009284101, note it appears that reportable RAC is not being pulled, but rather all RAC codes
--member with med_covgrp = 5: 11050747064
--member with med_covgrp = 6: 11050760757
--member with med_covgrp = 7: 11050747290
--member with multiple ZIP codes and contiguous and non-contiguous rows: 11269028924

select * from phclaims.stage.apcd_elig_timevar
where id_apcd = 11269028924
order by from_date;

select * from phclaims.stage.apcd_member_month_detail
where internal_member_id = 11269028924
order by year_month;

select eligibility_id, submitter_id, internal_member_id, coverage_class, eligibility_start_dt, eligibility_end_dt,
  product_code, dual_eligibility_code_id, aid_category_code 
from phclaims.stage.apcd_eligibility
where internal_member_id = 11269028924
--and coverage_class = 'MEDICAL'
order by eligibility_start_dt;