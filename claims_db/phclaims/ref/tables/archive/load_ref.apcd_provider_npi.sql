--Code to load data to ref.apcd_provider_npi
--A crosswalk between APCD provider ID and most common NPI
--Eli Kern (PHSKC-APDE)
--2019-9-12

------------------
--STEP 1: Prepare provider data from provider_master table
-------------------
if object_id('tempdb..#provider_master') is not null drop table #provider_master;
select distinct internal_provider_id as provider_id_apcd, cast(npi as bigint) as npi, 1 as provider_master_flag
into #provider_master
from PHClaims.stage.apcd_provider_master;


------------------
--STEP 2: Prepare provider data from provider table
-------------------
if object_id('tempdb..#temp1') is not null drop table #temp1;
select a.provider_id_apcd, a.npi
into #temp1
from (
select internal_provider_id as provider_id_apcd,
case when orig_npi like '[1-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' then orig_npi else null end as npi
from PHClaims.stage.apcd_provider
) as a
where a.npi is not null;

--choose most common npi
if object_id('tempdb..#npi_rank') is not null drop table #npi_rank;
select b.provider_id_apcd, b.npi
into #npi_rank
from (
	select a.provider_id_apcd, a.npi, a.row_count,
	rank() over (partition by provider_id_apcd order by row_count desc, npi) as npi_rank
	from (
		select provider_id_apcd, npi, count(*) as row_count
		from #temp1
		where npi is not null
		group by provider_id_apcd, npi
	) as a
) as b
where npi_rank = 1;

--join all ranked information together
--subset to providers not in provider_master table above and to providers tha thave a non-null NPI
--for joining, i have to join on NPI and provider_id_apcd, and then take inner joing of that (as some NPIs in provider table have typos)
--check out provider_id_apcd 627423 as an example of this, where it would end up having two records in final table due to NPI typo
if object_id('tempdb..#provider') is not null drop table #provider;
select distinct c.provider_id_apcd, cast(d.npi as bigint) as npi, 0 as provider_master_flag
into #provider
from (
	--select distinct provider IDs in provider table where NPI is not in provider master table
	select distinct a.provider_id_apcd
	from (
		--join provider master and provider temp tables on NPI
		select distinct x.provider_id_apcd
		from (select distinct provider_id_apcd, npi from #temp1 where npi is not null) as x
		left join #provider_master as y
		on (x.npi = y.npi)
		where y.provider_master_flag is null
	) as a
	inner join (
		--join provider master and provider temp tables on provider ID
		select distinct x.provider_id_apcd
		from (select distinct provider_id_apcd, npi from #temp1 where npi is not null) as x
		left join #provider_master as y
		on (x.provider_id_apcd = y.provider_id_apcd)
		where y.provider_master_flag is null
	) as b
	on a.provider_id_apcd = b.provider_id_apcd
) as c
left join #npi_rank as d
on c.provider_id_apcd = d.provider_id_apcd;


------------------
--STEP 3: Join provider_master and provider table rows and insert into table shell
--Only allow rows from provider temp table where NPI is present
--QA checks done - NPI is ten digits, no NPI has more than one row in table
-------------------
insert into PHClaims.ref.apcd_provider_npi with (tablock)
select provider_id_apcd, npi, provider_master_flag
from #provider_master
union
select provider_id_apcd, npi, provider_master_flag
from #provider;


------------------
--STEP 4: Create clustered columnstore index (1 min)
-------------------
create clustered columnstore index idx_ccs_ref_apcd_provider_npi on phclaims.ref.apcd_provider_npi;