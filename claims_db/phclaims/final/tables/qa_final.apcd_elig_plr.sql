--QA of final.apcd_elig_plr table
--4/24/19
--Eli Kern

--count rows
select count(*) as row_cnt
from [PHClaims].stage.apcd_elig_plr_2017;

select count(*) as row_cnt
from [PHClaims].final.apcd_elig_plr_2017;

--count columns
use PHClaims
go
select count(*) as col_cnt
from information_schema.columns
where table_catalog = 'PHClaims' -- the database
and table_schema = 'stage'
and table_name = 'apcd_' + 'elig_plr_2017';

select count(*) as col_cnt
from information_schema.columns
where table_catalog = 'PHClaims' -- the database
and table_schema = 'final'
and table_name = 'apcd_' + 'elig_plr_2017';

--drop stage table (eventually will be incorporated into R function)
--drop table PHClaims.stage.apcd_elig_plr_2017;