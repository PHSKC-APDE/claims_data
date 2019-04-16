--QA of final.apcd_elig_demo table
--4/11/19
--Eli Kern

--count rows
select count(*) as row_cnt
from [PHClaims].stage.apcd_elig_demo;

select count(*) as row_cnt
from [PHClaims].final.apcd_elig_demo;

--count columns
use PHClaims
go
select count(*) as col_cnt
from information_schema.columns
where table_catalog = 'PHClaims' -- the database
and table_schema = 'stage'
and table_name = 'apcd_' + 'elig_demo';

select count(*) as col_cnt
from information_schema.columns
where table_catalog = 'PHClaims' -- the database
and table_schema = 'final'
and table_name = 'apcd_' + 'elig_demo';

--drop stage table (eventually will be incorporated into R function)
--drop table PHClaims.stage.apcd_elig_demo;