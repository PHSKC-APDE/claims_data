
---------------------------
--Step 1: Pull out and reshape provider taxonomy columns
--Run: 1 min
---------------------------
if object_id('tempdb..#tax_long') is not null drop table #tax_long;
select distinct npi, taxonomy, taxonomy_number
into #tax_long
from (
	select npi, healthcare_provider_taxonomy_code_1 as [01],
		healthcare_provider_taxonomy_code_2 as [02],
		healthcare_provider_taxonomy_code_3 as [03],
		healthcare_provider_taxonomy_code_4 as [04],
		healthcare_provider_taxonomy_code_5 as [05],
		healthcare_provider_taxonomy_code_6 as [06],
		healthcare_provider_taxonomy_code_7 as [07],
		healthcare_provider_taxonomy_code_8 as [08],
		healthcare_provider_taxonomy_code_9 as [09],
		healthcare_provider_taxonomy_code_10 as [10],
		healthcare_provider_taxonomy_code_11 as [11],
		healthcare_provider_taxonomy_code_12 as [12],
		healthcare_provider_taxonomy_code_13 as [13],
		healthcare_provider_taxonomy_code_14 as [14],
		healthcare_provider_taxonomy_code_15 as [15]
	from phclaims.ref.provider_nppes_load
	where address_practice_state = 'WA' or address_practice_state = 'WASHINGTON'
) as a
unpivot(taxonomy for taxonomy_number in ([01], [02], [03], [04], [05], [06], [07], [08], [09], [10], [11], [12], [13], [14], [15])) as taxonomy;


---------------------------
--Step 2: Pull out and reshape provider taxonomy primary flag columns
--Run: <1 min
---------------------------
if object_id('tempdb..#tax_primary_flag_long') is not null drop table #tax_primary_flag_long;
select distinct npi, primary_flag, taxonomy_number
into #tax_primary_flag_long
from (
	select npi, healthcare_provider_primary_taxonomy_switch_1 as [01],
		healthcare_provider_primary_taxonomy_switch_2 as [02],
		healthcare_provider_primary_taxonomy_switch_3 as [03],
		healthcare_provider_primary_taxonomy_switch_4 as [04],
		healthcare_provider_primary_taxonomy_switch_5 as [05],
		healthcare_provider_primary_taxonomy_switch_6 as [06],
		healthcare_provider_primary_taxonomy_switch_7 as [07],
		healthcare_provider_primary_taxonomy_switch_8 as [08],
		healthcare_provider_primary_taxonomy_switch_9 as [09],
		healthcare_provider_primary_taxonomy_switch_10 as [10],
		healthcare_provider_primary_taxonomy_switch_11 as [11],
		healthcare_provider_primary_taxonomy_switch_12 as [12],
		healthcare_provider_primary_taxonomy_switch_13 as [13],
		healthcare_provider_primary_taxonomy_switch_14 as [14],
		healthcare_provider_primary_taxonomy_switch_15 as [15]
	from phclaims.ref.provider_nppes_load
	where address_practice_state = 'WA' or address_practice_state = 'WASHINGTON'
) as a
unpivot(primary_flag for taxonomy_number in ([01], [02], [03], [04], [05], [06], [07], [08], [09], [10], [11], [12], [13], [14], [15])) as primary_flag;


---------------------------
--Step 3: Join taxonomy and primary flag columns into new table
--Run: <1 min
---------------------------
if object_id('tempdb..#tax_long_joined') is not null drop table #tax_long_joined;
select a.npi, a.taxonomy, a.taxonomy_number, b.primary_flag
into #tax_long_joined
from #tax_long as a
left join #tax_primary_flag_long as b
on (a.npi = b.npi) and (a.taxonomy_number = b.taxonomy_number);


---------------------------
--Step 4: Create primary and secondary taxonomy fields for each NPI
--Run: <1 min
---------------------------

--select primary taxonomy
if object_id('tempdb..#tax_primary') is not null drop table #tax_primary
select a.npi, a.taxonomy as primary_taxonomy, a.taxonomy_number, a.primary_flag, a.primary_flag_max
into #tax_primary
from (
	select npi, taxonomy, taxonomy_number, primary_flag,
		max(case when primary_flag = 'Y' then 1 else 0 end) over (partition by npi) as primary_flag_max
	from #tax_long_joined
) as a
where a.primary_flag = 'Y' or (a.primary_flag_max = 0 and a.taxonomy_number = '01');


--select secondary taxonomy
if object_id('tempdb..#tax_secondary') is not null drop table #tax_secondary
select a.npi, a.taxonomy as secondary_taxonomy, a.taxonomy_number, a.primary_flag, a.primary_tax_position
into #tax_secondary
from (
	select npi, taxonomy, taxonomy_number, primary_flag,
		max(case when primary_flag = 'Y' then taxonomy_number else 0 end) over (partition by npi) as primary_tax_position
	from #tax_long_joined
) as a
where (a.primary_tax_position = 1 and a.taxonomy_number = '02') or (a.primary_tax_position > 1 and a.taxonomy_number = '01')
	or (a.primary_tax_position = 0 and a.taxonomy_number = '02');


--join primary and secondary taxonomies
if object_id('tempdb..#tax_final') is not null drop table #tax_final
select a.npi, a.primary_taxonomy, b.secondary_taxonomy
into #tax_final
from #tax_primary as a
left join #tax_secondary as b
on a.npi = b.npi;

--QA: PASS
select *
from #tax_final
where npi in ('1003074410', '1003003153', '1003025081')
order by npi;

select npi, healthcare_provider_taxonomy_code_1, healthcare_provider_primary_taxonomy_switch_1, healthcare_provider_taxonomy_code_2, healthcare_provider_primary_taxonomy_switch_2,
	healthcare_provider_taxonomy_code_3, healthcare_provider_primary_taxonomy_switch_3
from PHClaims.ref.provider_nppes_load
where npi in ('1003074410', '1003003153', '1003025081')
order by npi;


---------------------------
--Step 5: Join taxonomy information to remainder of desired columns and insert into persistent table shell
--Run: 1 min
---------------------------
select a.npi,
	a.entity_type_code,
	a.name_org,
	a.name_last,
	a.name_first,
	a.name_middle,
	a.credential,
	a.name_org_other,
	a.name_org_other_type_code,
	a.address_practice_first,
	a.address_practice_second,
	a.address_practice_city,
	a.address_practice_state,
	a.address_practice_zip_code,
	cast(a.enumeration_date as date) as enumeration_date,
	cast(a.last_update as date) as last_update,
	a.gender_code,
	b.primary_taxonomy,
	b.secondary_taxonomy,
	a.is_sole_proprietor,
	a.is_organization_subpart,
	a.parent_organization_lbn,
	getdate() as last_run

into PHClaims.ref.provider_nppes_wa_load
from PHClaims.ref.provider_nppes_load as a
left join #tax_final as b
on a.npi = b.npi
where a.address_practice_state = 'WA' or a.address_practice_state = 'WASHINGTON';
