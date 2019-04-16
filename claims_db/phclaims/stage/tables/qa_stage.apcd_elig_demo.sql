--QA of stage.apcd_elig_demo table
--4/11/19
--Eli Kern

----------------
--INTERNAL CONSISTENCY: Aggregate member counts across tables
----------------
--All people using elig_demo
select count(distinct id_apcd) as id_dcount_demo
from phclaims.stage.apcd_elig_demo;
 
--All people using elig_covgrp
--select count(distinct id_apcd) as id_dcount_timevar
--from phclaims.final.apcd_elig_timevar;

--All people using member_month_detail
select count(distinct internal_member_id) as id_dcount_member_month
from phclaims.stage.apcd_member_month_detail;
 
--Only one person per row in demoever table
select count(id_apcd) as id_count
from phclaims.stage.apcd_elig_demo;

----------------
--INTERNAL CONSISTENCY: Member by member QA
----------------

--Age < 1
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747392;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747392
order by internal_member_id, year_month;

--Age 1-5
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747025;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747025
order by internal_member_id, year_month;

--Age 6-17
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11268610588;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11268610588
order by internal_member_id, year_month;

--Age 18-44
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11268610582;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11268610582
order by internal_member_id, year_month;

--Age 45-89
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747030;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747030
order by internal_member_id, year_month;

--Someone with coverage gaps
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747044;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747044
order by internal_member_id, year_month;

--Someone with only a single age
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11238054810;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11238054810
order by internal_member_id, year_month;

--Age 90+ with multiple ages present in member_month
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747832;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747832
order by internal_member_id, year_month;

--Age 90+ with age of 90 only in member_month
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747327;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747327
order by internal_member_id, year_month;

--Male only
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747063;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747063
order by internal_member_id, year_month;

--Female only
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747032;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747032
order by internal_member_id, year_month;

--Multiple gender
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747626;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747626
order by internal_member_id, year_month;

--Gender unknown for just partial time (should NOT be gender unknown in final table)
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050762118;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050762118
order by internal_member_id, year_month;

--Gender unknown for partial time, unknown for last month
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11050807777;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050807777
order by internal_member_id, year_month;

--Gender fully unknown
select * from phclaims.stage.apcd_elig_demo
where id_apcd = 11057481990;

select internal_member_id, year_month,
       age, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11057481990
order by internal_member_id, year_month;
