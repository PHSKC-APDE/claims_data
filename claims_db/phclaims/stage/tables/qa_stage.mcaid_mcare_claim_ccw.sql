--QA of stage.mcaid_mcare_claim_ccw table
--Eli Kern
--2019-10


------------------------
--STEP 1: Table-wide checks
------------------------

--Count number and percent of distinct people by condition
select ccw_code, ccw_desc, count(distinct id_apde) as id_dcount
from PHClaims.stage.mcaid_mcare_claim_ccw
group by ccw_code, ccw_desc
order by ccw_code;

select count(distinct id_apde) as id_dcount
from PHClaims.final.mcaid_mcare_elig_demo;

--Count # of distinct conditions to make sure they've all run
select count(distinct ccw_code) as cond_count
from PHClaims.stage.mcaid_mcare_claim_ccw;

--Check age distribution by condition for a given year
select c.ccw_code, c.ccw_desc, c.age_grp7, count(distinct id_apde) as id_dcount
from (
	select a.id_apde, a.ccw_code, a.ccw_desc, 
	case
		when b.age >= 0 and b.age < 5 then '0-4'
		when b.age >= 5 and b.age < 12 then '5-11'
		when b.age >= 12 and b.age < 18 then '12-17'
		when b.age >= 18 and b.age < 25 then '18-24'
		when b.age >= 25 and b.age < 45 then '25-44'
		when b.age >= 45 and b.age < 65 then '45-64'
		when b.age >= 65 then '65 and over'
	end as age_grp7
	from (
	select distinct id_apde, ccw_code, ccw_desc
	from PHClaims.stage.mcaid_mcare_claim_ccw
	where year(from_date) <= 2017 and year(to_date) >= 2017
	) as a
	left join (
	select id_apde,
	case
		when floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) >=0 then floor((datediff(day, dob, '2017-12-31') + 1) / 365.25)
		when floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) = -1 then 0
	end as age
	from PHClaims.final.mcaid_mcare_elig_demo
	) as b
	on a.id_apde = b.id_apde
	--where b.age is null
) as c
group by c.ccw_code, c.ccw_desc, c.age_grp7;

select c.age_grp7, count(distinct c.id_apde) as id_dcount
from (
	select a.id_apde,  
	case
		when b.age >= 0 and b.age < 5 then '0-4'
		when b.age >= 5 and b.age < 12 then '5-11'
		when b.age >= 12 and b.age < 18 then '12-17'
		when b.age >= 18 and b.age < 25 then '18-24'
		when b.age >= 25 and b.age < 45 then '25-44'
		when b.age >= 45 and b.age < 65 then '45-64'
		when b.age >= 65 then '65 and over'
	end as age_grp7
	from (
	select id_apde
	from PHClaims.final.mcaid_mcare_elig_timevar
	where year(from_date) <= 2017 and year(to_date) >= 2017
	) as a
	left join (
	select id_apde,
	case
		when floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) >=0 then floor((datediff(day, dob, '2017-12-31') + 1) / 365.25)
		when floor((datediff(day, dob, '2017-12-31') + 1) / 365.25) = -1 then 0
	end as age
	from PHClaims.final.mcaid_mcare_elig_demo
	) as b
	on a.id_apde = b.id_apde
) as c
group by c.age_grp7;


------------------------
--STEP 2: Validate status of one person per condition with two or more time periods
------------------------

--Generic code to find people to QA
select top 1 a.*
from (
select id_apde, count(from_date) as time_cnt
from PHClaims.stage.mcaid_mcare_claim_ccw
where ccw_code = 1 -- change this to select different condition
group by id_apde
) as a
where a.time_cnt > 1;

--Validate CCW status (1-2 min run time per condition)
declare @id bigint, @ccw_code tinyint, @clm_type varchar(100)
set @id = --retrieve from above code--
set @ccw_code = 1
set @clm_type = '1,2,3,4,5'

select *
from PHClaims.stage.mcaid_mcare_claim_ccw
where ccw_code = @ccw_code and id_apde = @id
order by from_date;

select a.id_apde, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_hypothyroid
from PHClaims.final.mcaid_mcare_claim_header as a
left join PHClaims.final.mcaid_mcare_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join PHClaims.ref.dx_lookup as c
on b.icdcm_norm = c.dx and b.icdcm_version = c.dx_ver
where a.claim_type_id in (select * from PHClaims.dbo.Split(@clm_type, ','))
and a.id_apde = @id
and c.ccw_hypothyroid = 1s
order by a.first_service_date;