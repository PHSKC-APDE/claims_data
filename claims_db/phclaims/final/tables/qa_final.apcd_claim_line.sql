--QA of final.apcd_claim_line table
--4/20/2019
--Eli Kern

--count rows
select count(*) as row_cnt
from [PHClaims].stage.apcd_claim_line;

select count(*) as row_cnt
from [PHClaims].final.apcd_claim_line;

--count columns
use PHClaims
go
select count(*) as col_cnt
from information_schema.columns
where table_catalog = 'PHClaims' -- the database
and table_schema = 'stage'
and table_name = 'apcd_' + 'claim_line';

select count(*) as col_cnt
from information_schema.columns
where table_catalog = 'PHClaims' -- the database
and table_schema = 'final'
and table_name = 'apcd_' + 'claim_line';

--drop stage table (eventually will be incorporated into R function)
--drop table PHClaims.stage.apcd_claim_line;