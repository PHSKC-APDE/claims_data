--QA of stage.apcd_claim_ccw table
--5/13/19
--Eli Kern

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



--person who should not have cataract CCW because of DX fields condition
id_apcd = 11050947020
claim_header_id = 629257980861086

--claim that should not be included in BPH CCW definition because of DX exclusions
id_apcd = 11278002499
claim_header_id = 629250025757699

--a qualifying claim for BPH
id_apcd = 11051002955
claim_header_id = 629257853156008

--a claim that should be excluded from stroke definition by second criteria
id_apcd = 11058143040
claim_header_id = 629246622926380

--a person who should have stroke
id_apcd = 11050947017