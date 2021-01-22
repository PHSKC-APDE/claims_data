---------------------------
--Step 1: Pull out and reshape provider taxonomy columns
---------------------------
if object_id('tempdb..#tax_long') is not null drop table #tax_long;
select distinct npi, taxonomy, taxonomy_number
into #tax_long
from (
	select cast(npi as bigint) as npi, healthcare_provider_taxonomy_code_1 as [01],
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
) as a
unpivot(taxonomy for taxonomy_number in ([01], [02], [03], [04], [05], [06], [07], [08], [09], [10], [11], [12], [13], [14], [15])) as taxonomy;


---------------------------
--Step 2: Pull out and reshape provider taxonomy primary flag columns
---------------------------
if object_id('tempdb..#tax_primary_flag_long') is not null drop table #tax_primary_flag_long;
select distinct npi, primary_flag, taxonomy_number
into #tax_primary_flag_long
from (
	select cast(npi as bigint) as npi, healthcare_provider_primary_taxonomy_switch_1 as [01],
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
) as a
unpivot(primary_flag for taxonomy_number in ([01], [02], [03], [04], [05], [06], [07], [08], [09], [10], [11], [12], [13], [14], [15])) as primary_flag;


---------------------------
--Step 3: Join taxonomy and primary flag columns into new table
---------------------------
if object_id('tempdb..#tax_long_joined') is not null drop table #tax_long_joined;
select a.npi, a.taxonomy, a.taxonomy_number, b.primary_flag
into #tax_long_joined
from #tax_long as a
left join #tax_primary_flag_long as b
on (a.npi = b.npi) and (a.taxonomy_number = b.taxonomy_number);


---------------------------
--Step 4: Collapse table to distinct taxonomy codes by NPI
---------------------------
if object_id('tempdb..#tax_distinct') is not null drop table #tax_distinct;
select npi, taxonomy, max(case when primary_flag = 'Y' then 1 else 0 end) as primary_flag
into #tax_distinct
from #tax_long_joined
group by npi, taxonomy;

--Add a new taxonomy # column based on order of taxonomies A-Z (after primary taxonomy)
if object_id('tempdb..#tax_ranked') is not null drop table #tax_ranked;
select *, rank() over (partition by npi order by primary_flag desc, taxonomy) as taxonomy_number
into #tax_ranked
from #tax_distinct;


---------------------------
--Step 5: Create three taxonomy fields for each NPI
---------------------------
if object_id('tempdb..#tax_final') is not null drop table #tax_final
select a.npi, a.taxonomy as taxonomy_1, b.taxonomy as taxonomy_2, c.taxonomy as taxonomy_3, a.primary_flag as taxonomy_primary_flag
into #tax_final
from (select * from #tax_ranked where primary_flag = 1 or taxonomy_number = 1) as a
left join (select * from #tax_ranked where taxonomy_number = 2) as b
on a.npi = b.npi
left join (select * from #tax_ranked where taxonomy_number = 3) as c
on a.npi = c.npi;


---------------------------
--Step 5: Join taxonomy information to remainder of desired columns and insert into persistent table shell
---------------------------
select cast(a.npi as bigint) as npi,
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
	case when a.address_practice_state = 'WA' or a.address_practice_state = 'WASHINGTON' then 1 else 0 end as geo_wa,
	cast(a.enumeration_date as date) as enumeration_date,
	cast(a.last_update as date) as last_update,
	case when a.entity_type_code is null and a.deactivation_date is not null then 1 else 0 end as deactivation_flag,
	cast(a.deactivation_date as date) as deactivation_date,
	a.gender_code,
	b.taxonomy_1,
	b.taxonomy_2,
	b.taxonomy_3,
	b.taxonomy_primary_flag,
	a.is_sole_proprietor,
	a.is_organization_subpart,
	a.parent_organization_lbn,
	getdate() as last_run

into PHClaims.ref.provider_nppes_apde_load
from PHClaims.ref.provider_nppes_load as a
left join #tax_final as b
on cast(a.npi as bigint) = b.npi;