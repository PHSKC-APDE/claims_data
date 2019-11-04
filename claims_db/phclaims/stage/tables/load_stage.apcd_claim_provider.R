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
    --STEP 1: Grab NPIs for billing providers on Medicare FFS carrier claims using in-house data
    -------------------
    if object_id('tempdb..#temp1') is not null drop table #temp1;
    select a.medical_claim_header_id, b.carr_clm_blg_npi_num
    into #temp1
    from (
    	select submitter_clm_control_num collate SQL_Latin1_General_CP1_CS_AS as submitter_clm_control_num,
    		medical_claim_header_id
    	from PHClaims.stage.apcd_medical_claim
    	where submitted_claim_type_id in (24,25)
    ) as a
    left join PHClaims.stage.mcare_bcarrier_claims as b
    on a.submitter_clm_control_num = b.clm_id;
    
    
    ------------------
    --STEP 2: Extract header-level provider variables, adding billing NPI for Medicare FFS carrier claims
    -------------------
    if object_id('tempdb..#temp2') is not null drop table #temp2;
    select distinct a.internal_member_id as id_apcd, a.medical_claim_header_id,
    min(a.first_service_dt) over(partition by a.medical_claim_header_id) as first_service_date,
    max(a.last_service_dt) over(partition by a.medical_claim_header_id) as last_service_date,
    billing_provider_internal_id as billing, rendering_internal_provider_id as rendering, 
    attending_internal_provider_id as attending, referring_internal_provider_id as referring,
    --grab NPIs of billing providers for Medicare FFS carrier claims
    case
    	when a.submitted_claim_type_id in (24,25) then b.carr_clm_blg_npi_num
    	else null
    end as billing_npi_mcare_carrier
    into #temp2
    from PHClaims.stage.apcd_medical_claim as a
    left join #temp1 as b
    on a.medical_claim_header_id = b.medical_claim_header_id
    where denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N';
    
    
    ------------------
    --STEP 3: Reshaspe and insert into table shell
    -------------------
    insert into PHClaims.stage.apcd_claim_provider with (tablock)
    --reshape provider ID columns to single column
    select distinct id_apcd, medical_claim_header_id, first_service_date, last_service_date,
    cast(providers as bigint) as provider_id_apcd,
    cast(provider_type as varchar(255)) as provider_type,
    cast(billing_npi_mcare_carrier as bigint),
    getdate() as last_run
    from #temp2 as a
    unpivot(providers for provider_type in(billing, rendering, attending, referring)) as providers
    where providers is not null;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_claim_provider_f <- function() {
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}