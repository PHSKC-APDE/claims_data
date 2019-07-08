--QA of stage.apcd_claim_ccw table
--5/13/19
--Eli Kern


------------------------
--STEP 1: Table-wide checks
------------------------

--Count number and percent of distinct people by condition
select ccw_code, ccw_desc, count(distinct id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_ccw
group by ccw_code, ccw_desc;

select count(distinct id_apcd) as id_dcount
from PHClaims.final.apcd_elig_demo;

--Count # of distinct conditions to make sure they've all run
select count(distinct ccw_code) as cond_count
from PHClaims.stage.apcd_claim_ccw;

--Check age distribution by condition for a given year
select c.ccw_code, c.ccw_desc, c.age_grp7, count(distinct id_apcd) as id_dcount
from (
	select a.id_apcd, a.ccw_code, a.ccw_desc, 
	case
		when b.age >= 0 and b.age < 5 then '0-4'
		when b.age >= 5 and b.age < 12 then '5-11'
		when b.age >= 12 and b.age < 18 then '12-17'
		when b.age >= 18 and b.age < 25 then '18-24'
		when b.age >= 25 and b.age < 45 then '25-44'
		when b.age >= 45 and b.age < 65 then '45-64'
		when b.age >= 65 or b.ninety_only = 1 then '65 and over'
	end as age_grp7
	from (
	select distinct id_apcd, ccw_code, ccw_desc
	from PHClaims.stage.apcd_claim_ccw
	where year(from_date) <= 2017 and year(to_date) >= 2017
	) as a
	left join (
	select id_apcd, ninety_only,
	case
		when (floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) >= 90) or (ninety_only = 1) then 90
		when floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) >=0 then floor((datediff(day, dob, '2017-12-31') + 1) / 365.25)
		when floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) = -1 then 0
	end as age
	from PHClaims.final.apcd_elig_demo
	) as b
	on a.id_apcd = b.id_apcd
	--where b.age is null
) as c
group by c.ccw_code, c.ccw_desc, c.age_grp7;

select c.age_grp7, count(distinct c.id_apcd) as id_dcount
from (
	select a.id_apcd,  
	case
		when b.age >= 0 and b.age < 5 then '0-4'
		when b.age >= 5 and b.age < 12 then '5-11'
		when b.age >= 12 and b.age < 18 then '12-17'
		when b.age >= 18 and b.age < 25 then '18-24'
		when b.age >= 25 and b.age < 45 then '25-44'
		when b.age >= 45 and b.age < 65 then '45-64'
		when b.age >= 65 or b.ninety_only = 1 then '65 and over'
	end as age_grp7
	from (
	select id_apcd
	from PHClaims.final.apcd_elig_timevar
	where year(from_date) <= 2017 and year(to_date) >= 2017
	) as a
	left join (
	select id_apcd, ninety_only,
	case
		when (floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) >= 90) or (ninety_only = 1) then 90
		when floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) >=0 then floor((datediff(day, dob, '2017-12-31') + 1) / 365.25)
		when floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) = -1 then 0
	end as age
	from PHClaims.final.apcd_elig_demo
	) as b
	on a.id_apcd = b.id_apcd
) as c
group by c.age_grp7;


------------------------
--STEP 2: Validate status of one person per condition with two or more time periods
------------------------

--Generic code to find people to QA
select top 1 a.*
from (
select id_apcd, count(from_date) as time_cnt
from PHClaims.stage.apcd_claim_ccw
where ccw_code = 1 -- change this to select different condition
group by id_apcd
) as a
where a.time_cnt > 1;

--Validate CCW status (1-2 min run time per condition)
declare @id bigint, @ccw_code tinyint, @clm_type varchar(100)
set @id = --retrieve from above code--
set @ccw_code = 1
set @clm_type = '1,2,3,4,5'

select *
from PHClaims.stage.apcd_claim_ccw
where ccw_code = @ccw_code and id_apcd = @id
order by from_date;

select a.id_apcd, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_hypothyroid
from PHClaims.final.apcd_claim_header as a
left join PHClaims.final.apcd_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join PHClaims.ref.dx_lookup as c
on b.icdcm_norm = c.dx and b.icdcm_version = c.dx_ver
where a.claim_type_id in (select * from PHClaims.dbo.Split(@clm_type, ','))
and a.id_apcd = @id
and c.ccw_hypothyroid = 1
order by a.first_service_date;


------------------------
--STEP 3: Validate diagnosis number conditions / diagnosis exclusions
------------------------

--Cataract - person excluded because of DX fields condition
--Claim header that should be excluded: 629257980861086
select *
from PHClaims.stage.apcd_claim_ccw
where ccw_code = 9 and id_apcd = 11050947020
order by from_date;

select a.id_apcd, a.first_service_date, a.claim_header_id, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_cataract
from PHClaims.final.apcd_claim_header as a
left join PHClaims.final.apcd_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join PHClaims.ref.dx_lookup as c
on b.icdcm_norm = c.dx and b.icdcm_version = c.dx_ver
where a.claim_type_id in (4,5)
and a.id_apcd = 11050947020
and c.ccw_cataract = 1
order by a.first_service_date;

--BPH - claim that should be excluded per diagnosis
--Claim header that should be excluded: 629250025757699
select *
from PHClaims.stage.apcd_claim_ccw
where ccw_code = 8 and id_apcd = 11278002499
order by from_date;

select a.id_apcd, a.claim_header_id, a.first_service_date, max(a.claim_type_id) as claim_type_id, max(c.ccw_bph) as ccw_bph, max(c.ccw_bph_exclude) as ccw_bph_exclude
from PHClaims.final.apcd_claim_header as a
left join PHClaims.final.apcd_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join PHClaims.ref.dx_lookup as c
on b.icdcm_norm = c.dx and b.icdcm_version = c.dx_ver
where a.claim_type_id in (1,2,3,4,5)
and a.id_apcd = 11278002499
and (c.ccw_bph = 1 or c.ccw_bph_exclude = 1)
group by a.id_apcd, a.claim_header_id, a.first_service_date
order by a.first_service_date;

--Stroke - claim that should be excluded per diagnosis
--Claim header that should be excluded: 629246622926380
select *
from PHClaims.stage.apcd_claim_ccw
where ccw_code = 27 and id_apcd = 11060407594
order by from_date;

select a.id_apcd, a.claim_header_id, a.first_service_date, max(a.claim_type_id) as claim_type_id, max(c.ccw_stroke) as ccw_stroke, 
	max(c.ccw_stroke_exclude1) as ccw_stroke_exclude1, max(c.ccw_stroke_exclude2) as ccw_stroke_exclude2
from PHClaims.final.apcd_claim_header as a
left join PHClaims.final.apcd_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join PHClaims.ref.dx_lookup as c
on b.icdcm_norm = c.dx and b.icdcm_version = c.dx_ver
where a.claim_type_id in (1,4,5)
and a.id_apcd = 11060407594
and (c.ccw_stroke = 1 or c.ccw_stroke_exclude1 = 1 or c.ccw_stroke_exclude2 = 1)
group by a.id_apcd, a.claim_header_id, a.first_service_date
order by a.first_service_date;