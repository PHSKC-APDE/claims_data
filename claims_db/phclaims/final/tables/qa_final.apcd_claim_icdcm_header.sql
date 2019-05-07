--QA of final.apcd_claim_icdcm_header table
--5/7/2019
--Eli Kern

--count rows
select count(*) as row_cnt
from [PHClaims].stage.apcd_claim_icdcm_header;

select count(*) as row_cnt
from [PHClaims].final.apcd_claim_icdcm_header;

--count columns
use PHClaims
go
select count(*) as col_cnt
from information_schema.columns
where table_catalog = 'PHClaims' -- the database
and table_schema = 'stage'
and table_name = 'apcd_' + 'claim_icdcm_header';

select count(*) as col_cnt
from information_schema.columns
where table_catalog = 'PHClaims' -- the database
and table_schema = 'final'
and table_name = 'apcd_' + 'claim_icdcm_header';

--drop stage table (eventually will be incorporated into R function)
--drop table PHClaims.stage.apcd_claim_icdcm_header;