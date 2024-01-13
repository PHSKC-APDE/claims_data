--Line-level QA of ref.apcd_provider_npi table
--Eli Kern
--2020-11
--2/7/23 edited by Susan Hernandez 

--Select APCD provider IDs that do not join to provider NPI reference table
--Run time: ~2 min
drop table #temp1;
select a.*
into #temp1
from PHClaims.final.apcd_claim_provider as a
left join PHClaims.ref.apcd_provider_npi as b
on a.provider_id_apcd = b.provider_id_apcd
where b.provider_id_apcd is null;
--(25239084 rows affected)

--Now verify that all of these providers have an invalid NPI:

--check provider master table first, should be ZERO, result: ZERO
select count(*) as row_count
from #temp1 as a
left join PHClaims.stage.apcd_provider_master as b
on a.provider_id_apcd = b.internal_provider_id
where b.internal_provider_id is not null;


--check provider table, all should have missing or invalid NPI, RESULT - all are invalid or missing
--valid NPI must be 10 digits and start with [1-9]
select distinct a.provider_id_apcd, b.orig_npi
into #temp2
from #temp1 as a
left join PHClaims.stage.apcd_provider as b
on a.provider_id_apcd = b.internal_provider_id
where b.orig_npi != '-1';

--len command counts the length of the original NPI
drop table #temp3;
select *, len(orig_npi) as npi_len 
into #temp3
from #temp2
order by npi_len, orig_npi;

--count the number of ids with each length
select count(npi_len) npi_len
, npi_len
from #temp3
group by npi_len;

select count(provider_id_apcd) count_provider_ids, count(distinct provider_id_apcd) d_count_provider_ids from #temp2;
--count_provider_ids	d_count_provider_ids ##exact 1009
--1623	1623

select count(provider_id_apcd) count_provider_ids
, count(distinct provider_id_apcd) d_count_provider_ids 
from PHClaims.ref.apcd_provider_npi ;
--count_provider_ids	d_count_provider_ids ##exact 10015
--3839870	3839870