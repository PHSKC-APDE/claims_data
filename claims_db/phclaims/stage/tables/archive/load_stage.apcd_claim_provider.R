#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_PROVIDER
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_claim_provider_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    ------------------
    --STEP 1: Extract header-level provider variables, reshape, and insert into table shell
    -------------------
    insert into PHClaims.stage.apcd_claim_provider with (tablock)
    --reshape provider ID columns to single column
    select distinct id_apcd, medical_claim_header_id, first_service_date, last_service_date,
    cast(providers as bigint) as provider_id_apcd,
    cast(provider_type as varchar(255)) as provider_type,
    getdate() as last_run
    from (
    	select distinct internal_member_id as id_apcd, medical_claim_header_id,
    	min(first_service_dt) over(partition by medical_claim_header_id) as first_service_date,
    	max(last_service_dt) over(partition by medical_claim_header_id) as last_service_date,
    	billing_provider_internal_id as billing, rendering_internal_provider_id as rendering, 
    	attending_internal_provider_id as attending, referring_internal_provider_id as referring
      from PHClaims.stage.apcd_medical_claim as x
      --exclude denined/orphaned claims
      left join PHClaims.ref.apcd_denied_orphaned_header as y
      on x.medical_claim_header_id = y.claim_header_id
      where y.denied_header_min = 0 and y.orphaned_header_min = 0
    ) as a
    unpivot(providers for provider_type in(billing, rendering, attending, referring)) as providers
    where providers is not null;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_claim_provider_f <- function() {
  
  #compare min/max of provider ID variables with medical_claim table
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_provider' as 'table', 'qa1 = min/ qa2 = max of rendering provider ID' as qa_type,
    min(provider_id_apcd) as qa1, max(provider_id_apcd) as qa2
    from stage.apcd_claim_provider
    where provider_type = 'rendering';",
    .con = db_claims))
  
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_medical_claim' as 'table', 'qa1 = min/ qa2 = max of rendering provider ID' as qa_type,
    min(cast(rendering_internal_provider_id as bigint)) as qa1,
  	max(cast(rendering_internal_provider_id as bigint)) as qa2
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where b.denied_header_min = 0 and b.orphaned_header_min = 0;",
    .con = db_claims))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}