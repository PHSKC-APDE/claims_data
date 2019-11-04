#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_PROVIDER
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

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
    	from PHClaims.stage.apcd_medical_claim
    	where denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N'
    ) as a
    unpivot(providers for provider_type in(billing, rendering, attending, referring)) as providers
    where providers is not null;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_claim_provider_f <- function() {
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}