#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_line
# Eli Kern, PHSKC (APDE)
#
# 2019-12
#
#2024-05-10 Eli update: Data from HCA, ETL in inthealth_edw

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_line_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(inthealth, glue::glue_sql(
    "--Code to load data to stage.mcare_claim_line table
    --Distinct line-level claim variables that do not have a dedicated table (e.g. revenue code). In other words elements for which there is only one distinct 
    --value per claim line.
    --Eli Kern (PHSKC-APDE)
    --2024-05
    
    ------------------
    --STEP 1: Select (distinct) desired columns from multi-year claim tables on stage schema
    --Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
    -------------------
    insert into stg_claims.stage_mcare_claim_line
        
    --bcarrier
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.line_num) as claim_line_id,
    cast(b.clm_from_dt as date) as first_service_date,
    cast(b.clm_thru_dt as date) as last_service_date,
    revenue_code = null,
    case
	    when len(trim(a.line_place_of_srvc_cd)) < 2 then right('0' + trim(a.line_place_of_srvc_cd), 2)
	    else a.line_place_of_srvc_cd
    end as place_of_service_code,
    a.line_cms_type_srvc_cd as type_of_service,
    'carrier' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_bcarrier_line as a
    left join stg_claims.mcare_bcarrier_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude denined claims using carrier/dme claim method
    where b.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
        
    --dme
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.line_num) as claim_line_id,
    cast(b.clm_from_dt as date) as first_service_date,
    cast(b.clm_thru_dt as date) as last_service_date,
    revenue_code = null,
    case
	    when len(trim(a.line_place_of_srvc_cd)) < 2 then right('0' + trim(a.line_place_of_srvc_cd), 2)
	    else a.line_place_of_srvc_cd
    end as place_of_service_code,
    a.line_cms_type_srvc_cd as type_of_service,
    'dme' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_dme_line as a
    left join stg_claims.mcare_dme_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude denined claims using carrier/dme claim method
    where b.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
        
    --hha
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_from_dt as date) as first_service_date,
    cast(b.clm_thru_dt as date) as last_service_date,
    case
	    when len(trim(a.rev_cntr)) < 4 then right('000' + trim(a.rev_cntr), 4)
	    else a.rev_cntr
    end as revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'hha' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_hha_revenue_center as a
    left join stg_claims.mcare_hha_base_claims  as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude denined claims using facility claim method
    where (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
        
    --hospice
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_from_dt as date) as first_service_date,
    cast(b.clm_thru_dt as date) as last_service_date,
    case
	    when len(trim(a.rev_cntr)) < 4 then right('000' + trim(a.rev_cntr), 4)
	    else a.rev_cntr
    end as revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'hospice' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_hospice_revenue_center as a
    left join stg_claims.mcare_hospice_base_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude denined claims using facility claim method
    where (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
        
    --inpatient
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_from_dt as date) as first_service_date,
    cast(b.clm_thru_dt as date) as last_service_date,
    case
	    when len(trim(a.rev_cntr)) < 4 then right('000' + trim(a.rev_cntr), 4)
	    else a.rev_cntr
    end as revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'inpatient' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_inpatient_revenue_center as a
    left join stg_claims.mcare_inpatient_base_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude denined claims using facility claim method
    where (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
    
    --inpatient data structure j
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_from_dt as date) as first_service_date,
    cast(b.clm_thru_dt as date) as last_service_date,
    case
	    when len(trim(a.rev_cntr)) < 4 then right('000' + trim(a.rev_cntr), 4)
	    else a.rev_cntr
    end as revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'inpatient' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_inpatient_revenue_center_j as a
    left join stg_claims.mcare_inpatient_base_claims_j as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude denined claims using facility claim method
    where (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
        
    --outpatient
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_from_dt as date) as first_service_date,
    cast(b.clm_thru_dt as date) as last_service_date,
    case
	    when len(trim(a.rev_cntr)) < 4 then right('000' + trim(a.rev_cntr), 4)
	    else a.rev_cntr
    end as revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'outpatient' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_outpatient_revenue_center as a
    left join stg_claims.mcare_outpatient_base_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude denined claims using facility claim method
    where (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
    
    --outpatient data structure j
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_from_dt as date) as first_service_date,
    cast(b.clm_thru_dt as date) as last_service_date,
    case
	    when len(trim(a.rev_cntr)) < 4 then right('000' + trim(a.rev_cntr), 4)
	    else a.rev_cntr
    end as revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'outpatient' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_outpatient_revenue_center_j as a
    left join stg_claims.mcare_outpatient_base_claims_j as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude denined claims using facility claim method
    where (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
        
    --snf
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_from_dt as date) as first_service_date,
    cast(b.clm_thru_dt as date) as last_service_date,
    case
	    when len(trim(a.rev_cntr)) < 4 then right('000' + trim(a.rev_cntr), 4)
	    else a.rev_cntr
    end as revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'snf' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_snf_revenue_center as a
    left join stg_claims.mcare_snf_base_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude denined claims using facility claim method
    where (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null;",
        .con = inthealth))
    }

#### Table-level QA script ####
qa_stage.mcare_claim_line_qa_f <- function() {
  
  #confirm that claim types with expected revenue codes have data for each year
  res1 <- dbGetQuery(conn = inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_line' as 'table',
  'rows with non-null revenue code' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_line
  where revenue_code is not null
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
  .con = inthealth))
  
  #confirm that claim types with expected revenue codes have data for each year
  res2 <- dbGetQuery(conn = inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_line' as 'table',
  'rows with non-null place of service and type of service code' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_line
  where place_of_service_code is not null and type_of_service is not null
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
  .con = inthealth))

  #make sure everyone is in bene_enrollment table
  res3 <- dbGetQuery(conn = inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_line' as 'table', '# members not in bene_enrollment, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_line as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.id_mcare = b.bene_id
    where b.bene_id is null;",
  .con = inthealth))
  
  #confirm all revenue codes are 4 digits
  res4 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_line' as 'table', '# of claims where length of revenue codes != 4, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_line
    where len(revenue_code) != 4;",
    .con = inthealth))
  
  #confirm all place of service codes are 2 digits
  res5 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_line' as 'table', '# of claims where length of pos codes != 2, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_line
    where len(place_of_service_code) != 2;",
    .con = inthealth))
  
  #confirm all type of service codes are 1 digit
  res6 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_line' as 'table', '# of claims where length of type of service codes != 1, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_line
    where len(type_of_service) != 1;",
    .con = inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}