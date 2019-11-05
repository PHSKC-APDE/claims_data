#### CODE TO LOAD & TABLE-LEVEL QA REF.KC_PROVIDER_MASTER
# Eli Kern, PHSKC (APDE)
#
# 2019-11

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_ref.kc_provider_master_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    ------------------
    --STEP 1: Prepare provider data from provider_master table
    -------------------
    if object_id('tempdb..#provider_master') is not null drop table #provider_master;
    select distinct cast(npi as bigint) as npi, entity_type,
    case when len(zip_physical) = 5 then zip_physical else null end as geo_zip_practice,
    case when primary_taxonomy in ('-1','-2') then null else primary_taxonomy end as primary_taxonomy, 
    case when secondary_taxonomy in ('-1','-2') then null else secondary_taxonomy end as secondary_taxonomy, 
    1 as apcd_provider_master_flag
    into #provider_master
    from PHClaims.stage.apcd_provider_master;
    
    
    ------------------
    --STEP 2: Prepare provider data from provider table
    -------------------
    if object_id('tempdb..#temp1') is not null drop table #temp1;
    select case when orig_npi like '[1-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' then orig_npi else null end as npi,
    entity_type,
    case when len(zip) = 5 then zip else null end as geo_zip_practice,
    case when len(primary_specialty_code) = 10 then primary_specialty_code else null end as taxonomy
    into #temp1
    from PHClaims.stage.apcd_provider;
    
    --choose most common entity type
    if object_id('tempdb..#entity_rank') is not null drop table #entity_rank;
    select b.npi, b.entity_type
    into #entity_rank
    from (
    	select a.npi, a.entity_type, a.row_count,
    	rank() over (partition by npi order by row_count desc, entity_type desc) as entity_type_rank
    	from (
    		select npi, entity_type, count(*) as row_count
    		from #temp1
    		where entity_type is not null
    		group by npi, entity_type
    	) as a
    ) as b
    where entity_type_rank = 1;
    
    --choose most common zip code type
    if object_id('tempdb..#geo_zip_practice_rank') is not null drop table #geo_zip_practice_rank;
    select b.npi, b.geo_zip_practice
    into #geo_zip_practice_rank
    from (
    	select a.npi, a.geo_zip_practice, a.row_count,
    	rank() over (partition by npi order by row_count desc, geo_zip_practice) as geo_zip_practice_rank
    	from (
    		select npi, geo_zip_practice, count(*) as row_count
    		from #temp1
    		where geo_zip_practice is not null
    		group by npi, geo_zip_practice
    	) as a
    ) as b
    where geo_zip_practice_rank = 1;
    
    --flag 1st and 2nd most common taxonomies
    if object_id('tempdb..#taxonomy_rank') is not null drop table #taxonomy_rank;
    select b.npi, b.taxonomy, b.taxonomy_rank
    into #taxonomy_rank
    from (
    	select a.npi, a.taxonomy, a.row_count,
    	rank() over (partition by npi order by row_count desc, taxonomy) as taxonomy_rank
    	from (
    		select npi, taxonomy, count(*) as row_count
    		from #temp1
    		where taxonomy is not null
    		group by npi, taxonomy
    	) as a
    ) as b
    where taxonomy_rank in (1,2);
    
    --choose most common taxonomy as primary
    if object_id('tempdb..#taxonomy1_rank') is not null drop table #taxonomy1_rank;
    select npi, taxonomy as primary_taxonomy
    into #taxonomy1_rank
    from #taxonomy_rank
    where taxonomy_rank = 1;
    
    --choose second common taxonomy as secondary
    if object_id('tempdb..#taxonomy2_rank') is not null drop table #taxonomy2_rank;
    select npi, taxonomy as secondary_taxonomy
    into #taxonomy2_rank
    from #taxonomy_rank
    where taxonomy_rank = 2;
    
    --join all ranked information together
    --subset to NPIs not in provider_master table above
    if object_id('tempdb..#provider') is not null drop table #provider;
    select cast(a.npi as bigint) as npi, b.entity_type, c.geo_zip_practice, d.primary_taxonomy, e.secondary_taxonomy,
    	0 as apcd_provider_master_flag
    into #provider
    --select distinct NPIs that are not in provider master table
    from (
    	select distinct x.npi
    	from #temp1 as x
    	left join #provider_master as y
    	on x.npi = y.npi
    	where y.apcd_provider_master_flag is null
    ) as a
    left join #entity_rank as b
    on a.npi = b.npi
    left join #geo_zip_practice_rank as c
    on a.npi = c.npi
    left join #taxonomy1_rank as d
    on a.npi = d.npi
    left join #taxonomy2_rank as e
    on a.npi = e.npi
    --remove row(s) for null NPIs
    where a.npi is not null;
    
    
    ------------------
    --STEP 3: Join provider_master and provider table rows and insert into table shell
    -------------------
    insert into PHClaims.ref.kc_provider_master with (tablock)
    select npi, entity_type, geo_zip_practice, primary_taxonomy, secondary_taxonomy, apcd_provider_master_flag
    from #provider_master
    union
    select npi, entity_type, geo_zip_practice, primary_taxonomy, secondary_taxonomy, apcd_provider_master_flag
    from #provider;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_ref.kc_provider_master_f <- function() {
  
    #no NPI should have more than 1 row
    res1 <- dbGetQuery(conn = db_claims, glue_sql(
      "select 'ref.kc_provider_master' as 'table', '# of NPIs with >1 row, expect 0' as qa_type,
      count(*) as qa
      from (
      	select npi, count(*) as row_count
      	FROM ref.kc_provider_master
      	group by npi
      ) as a
      where a.row_count >1;",
      .con = db_claims))
    
    #no NPI should be any length other than 10 digits
    res2 <- dbGetQuery(conn = db_claims, glue_sql(
      "select 'ref.kc_provider_master' as 'table', '# of NPIs with length != 10, expect 0' as qa_type,
      count(*) as qa
      from ref.kc_provider_master
      where len(npi) != 10;",
      .con = db_claims))
    
    #all NPIs should have an entity type
    res3 <- dbGetQuery(conn = db_claims, glue_sql(
      "select 'ref.kc_provider_master' as 'table', '# of NPIs with no entity type, expect 0' as qa_type,
      count(*) as qa
      from ref.kc_provider_master
      where entity_type is null;",
      .con = db_claims))
    
    #taxonomy should be 10 digits long
    res4 <- dbGetQuery(conn = db_claims, glue_sql(
      "select 'ref.kc_provider_master' as 'table', '# of taxonomies with length != 10, expect 0' as qa_type,
      count(*) as qa
      from ref.kc_provider_master
      where len(primary_taxonomy) != 10 or len(secondary_taxonomy) != 10;",
      .con = db_claims))
    
    #ZIP codes should be 5 digits long
    res5 <- dbGetQuery(conn = db_claims, glue_sql(
      "select 'ref.kc_provider_master' as 'table', '# of ZIP codes with length != 5, expect 0' as qa_type,
      count(*) as qa
      from ref.kc_provider_master
      where len(geo_zip_practice) != 5;",
      .con = db_claims))
  
    res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}