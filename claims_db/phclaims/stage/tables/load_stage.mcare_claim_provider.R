#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_provider
# Eli Kern, PHSKC (APDE)
#
# 2019-12
#
#2024-05-15 Eli update: Data from HCA, ETL in inthealth_edw

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_provider_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "--Code to load data to stage.mcare_claim_provider table
    --Provider information as submitted reshaped to long
    --Eli Kern (PHSKC-APDE)
    --2024-05
        
    ------------------
    --STEP 1: Select and union desired columns from multi-year claim tables on stage schema
    --Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
    --Unpivot and insert into table shell
    -------------------
    insert into stg_claims.stage_mcare_claim_provider
    
    select z.id_mcare,
        claim_header_id,
        first_service_date,
        last_service_date,
        provider_npi,
        provider_type,
        provider_type_nch,
        provider_tin,
        case
        	when provider_type = 'rendering' then provider_zip_rendering
        	when provider_type = 'billing' then provider_zip_billing
        end as provider_zip,
        case
        	when provider_type = 'attending' then provider_specialty_attending
        	when provider_type = 'operating' then provider_specialty_operating
        	when provider_type = 'other' then provider_specialty_other
        	when provider_type = 'referring' then provider_specialty_referring
        	when provider_type = 'rendering' then provider_specialty_rendering
        end as provider_specialty,
        filetype_mcare,
        getdate() as last_run
        
    from (
        --bcarrier
        select id_mcare,
        	claim_header_id,
        	first_service_date,
        	last_service_date,
        	cast(providers as bigint) as 'provider_npi',
        	cast(provider_type as varchar(200)) as 'provider_type',
        	provider_type_nch,
        	provider_tin,
        	provider_zip_rendering,
        	provider_zip_billing,
        	provider_specialty_attending,
        	provider_specialty_operating,
        	provider_specialty_other,
        	provider_specialty_referring,
        	provider_specialty_rendering,
        	filetype_mcare,
        	getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'carrier' as filetype_mcare,
        	a.carr_clm_blg_npi_num as billing,
        	a.rfr_physn_npi as referring,
        	a.cpo_org_npi_num as care_plan_oversight,
        	a.carr_clm_sos_npi_num as site_of_service,
        	b.prf_physn_npi as rendering,
        	b.org_npi_num as organization,
        	b.carr_line_prvdr_type_cd as provider_type_nch,
        	b.tax_num as provider_tin,
        	b.prvdr_zip as provider_zip_rendering,
        	b.physn_zip_cd as provider_zip_billing,
        	provider_specialty_attending = null,
        	provider_specialty_operating = null,
        	provider_specialty_other = null,
        	provider_specialty_referring = null,
        	b.prvdr_spclty as provider_specialty_rendering
        	from stg_claims.mcare_bcarrier_claims as a
        	left join stg_claims.mcare_bcarrier_line as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using carrier/dme claim method
        	where a.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
        ) as x1
        
        --reshape from wide to long
        unpivot(providers for provider_type in (
        	billing,
        	referring,
        	care_plan_oversight,
        	site_of_service,
        	rendering,
        	organization)
        ) as providers
        where len(providers) = 10 and isnumeric(providers) = 1
        
        --dme
        union
        select id_mcare,
        	claim_header_id,
        	first_service_date,
        	last_service_date,
        	cast(providers as bigint) as 'provider_npi',
        	cast(provider_type as varchar(200)) as 'provider_type',
        	provider_type_nch,
        	provider_tin,
        	provider_zip_rendering,
        	provider_zip_billing,
        	provider_specialty_attending,
        	provider_specialty_operating,
        	provider_specialty_other,
        	provider_specialty_referring,
        	provider_specialty_rendering,
        	filetype_mcare,
        	getdate() as last_run
        
        from(
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'dme' as filetype_mcare,
        	b.prvdr_npi as billing,
        	a.rfr_physn_npi as referring,
        	provider_type_nch = null,
        	provider_tin = null,
        	provider_zip_rendering = null,
        	provider_zip_billing = null,
        	provider_specialty_attending = null,
        	provider_specialty_operating = null,
        	provider_specialty_other = null,
        	provider_specialty_referring = null,
        	provider_specialty_rendering = null
        	from stg_claims.mcare_dme_claims as a
        	left join stg_claims.mcare_dme_line as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using carrier/dme claim method
        	where a.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
        ) as x2
        
        --reshape from wide to long
        unpivot(providers for provider_type in (
        	billing,
        	referring)
        ) as providers
        where len(providers) = 10 and isnumeric(providers) = 1
        
        --hha
        union
        	select id_mcare,
        	claim_header_id,
        	first_service_date,
        	last_service_date,
        	--original diagnosis code
        	cast(providers as bigint) as 'provider_npi',
        	--procedure code number/type
        	cast(provider_type as varchar(200)) as 'provider_type',
        	--other provider information
        	provider_type_nch,
        	provider_tin,
        	--temporary provider zip and specialty columns for further processing
        	provider_zip_rendering,
        	provider_zip_billing,
        	provider_specialty_attending,
        	provider_specialty_operating,
        	provider_specialty_other,
        	provider_specialty_referring,
        	provider_specialty_rendering,
        	filetype_mcare,
        	getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'hha' as filetype_mcare,
        	a.org_npi_num as billing,
        	a.rfr_physn_npi as referring,
        	care_plan_oversight = null,
        	a.srvc_loc_npi_num as site_of_service,
    		case when a.rndrng_physn_npi is not null then a.rndrng_physn_npi else b.rndrng_physn_npi end as rendering,
        	organization = null,
        	a.at_physn_npi as attending,
        	a.op_physn_npi as operating,
        	a.ot_physn_npi as other,
        	provider_type_nch = null,
        	provider_tin = null,
        	a.clm_srvc_fac_zip_cd as provider_zip_rendering,
        	provider_zip_billing = null,
        	a.at_physn_spclty_cd as provider_specialty_attending,
        	a.op_physn_spclty_cd as provider_specialty_operating,
        	a.ot_physn_spclty_cd as provider_specialty_other,
        	a.rfr_physn_spclty_cd as provider_specialty_referring,
    		case when a.rndrng_physn_npi is not null then a.rndrng_physn_spclty_cd else b.rndrng_physn_spclty_cd end as provider_specialty_rendering
        	from stg_claims.mcare_hha_base_claims as a
        	left join stg_claims.mcare_hha_revenue_center as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using facility claim method
        	where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        ) as x3
        
        --reshape from wide to long
        unpivot(providers for provider_type in (
        	billing,
        	referring,
        	site_of_service,
        	rendering,
        	attending,
        	operating,
        	other)
        ) as providers
        where len(providers) = 10 and isnumeric(providers) = 1
        
        --hospice
        union
        select id_mcare,
        	claim_header_id,
        	first_service_date,
        	last_service_date,
        	cast(providers as bigint) as 'provider_npi',
        	cast(provider_type as varchar(200)) as 'provider_type',
        	provider_type_nch,
        	provider_tin,
        	provider_zip_rendering,
        	provider_zip_billing,
        	provider_specialty_attending,
        	provider_specialty_operating,
        	provider_specialty_other,
        	provider_specialty_referring,
        	provider_specialty_rendering,
        	filetype_mcare,
        	getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'hospice' as filetype_mcare,
        	a.org_npi_num as billing,
        	a.rfr_physn_npi as referring,
        	care_plan_oversight = null,
        	a.srvc_loc_npi_num as site_of_service,
    		case when a.rndrng_physn_npi is not null then a.rndrng_physn_npi else b.rndrng_physn_npi end as rendering,
        	organization = null,
        	a.at_physn_npi as attending,
        	a.op_physn_npi as operating,
        	a.ot_physn_npi as other,
        	provider_type_nch = null,
        	provider_tin = null,
        	provider_zip_rendering = null,
        	provider_zip_billing = null,
        	a.at_physn_spclty_cd as provider_specialty_attending,
        	a.op_physn_spclty_cd as provider_specialty_operating,
        	a.ot_physn_spclty_cd as provider_specialty_other,
        	a.rfr_physn_spclty_cd as provider_specialty_referring,
    		case when a.rndrng_physn_npi is not null then a.rndrng_physn_spclty_cd else b.rndrng_physn_spclty_cd end as provider_specialty_rendering
        	from stg_claims.mcare_hospice_base_claims as a
        	left join stg_claims.mcare_hospice_revenue_center as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using facility claim method
        	where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        ) as x4
        
        --reshape from wide to long
        unpivot(providers for provider_type in (
        	billing,
        	referring,
        	site_of_service,
        	rendering,
        	attending,
        	operating,
        	other)
        ) as providers
        where len(providers) = 10 and isnumeric(providers) = 1
        
        --inpatient
        union
        select id_mcare,
        	claim_header_id,
        	first_service_date,
        	last_service_date,
        	cast(providers as bigint) as 'provider_npi',
        	cast(provider_type as varchar(200)) as 'provider_type',
        	provider_type_nch,
        	provider_tin,
        	provider_zip_rendering,
        	provider_zip_billing,
        	provider_specialty_attending,
        	provider_specialty_operating,
        	provider_specialty_other,
        	provider_specialty_referring,
        	provider_specialty_rendering,
        	filetype_mcare,
        	getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'inpatient' as filetype_mcare,
        	a.org_npi_num as billing,
        	referring = null,
        	care_plan_oversight = null,
        	site_of_service = null,
    		case when a.rndrng_physn_npi is not null then a.rndrng_physn_npi else b.rndrng_physn_npi end as rendering,
        	organization = null,
        	a.at_physn_npi as attending,
        	a.op_physn_npi as operating,
        	a.ot_physn_npi as other,
        	provider_type_nch = null,
        	provider_tin = null,
        	provider_zip_rendering = null,
        	provider_zip_billing = null,
        	a.at_physn_spclty_cd as provider_specialty_attending,
        	a.op_physn_spclty_cd as provider_specialty_operating,
        	a.ot_physn_spclty_cd as provider_specialty_other,
        	provider_specialty_referring = null,
    		case when a.rndrng_physn_npi is not null then a.rndrng_physn_spclty_cd else b.rndrng_physn_spclty_cd end as provider_specialty_rendering
        	from stg_claims.mcare_inpatient_base_claims as a
        	left join stg_claims.mcare_inpatient_revenue_center as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using facility claim method
        	where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        ) as x5
        
        --reshape from wide to long
        unpivot(providers for provider_type in (
        	billing,
        	rendering,
        	attending,
        	operating,
        	other)
        ) as providers
        where len(providers) = 10 and isnumeric(providers) = 1
    
    	--inpatient data structure j
        union
        select id_mcare,
        	claim_header_id,
        	first_service_date,
        	last_service_date,
        	cast(providers as bigint) as 'provider_npi',
        	cast(provider_type as varchar(200)) as 'provider_type',
        	provider_type_nch,
        	provider_tin,
        	provider_zip_rendering,
        	provider_zip_billing,
        	provider_specialty_attending,
        	provider_specialty_operating,
        	provider_specialty_other,
        	provider_specialty_referring,
        	provider_specialty_rendering,
        	filetype_mcare,
        	getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'inpatient' as filetype_mcare,
        	a.org_npi_num as billing,
        	referring = null,
        	care_plan_oversight = null,
        	site_of_service = null,
    		b.rndrng_physn_npi as rendering,
        	organization = null,
        	a.at_physn_npi as attending,
        	a.op_physn_npi as operating,
        	a.ot_physn_npi as other,
        	provider_type_nch = null,
        	provider_tin = null,
        	provider_zip_rendering = null,
        	provider_zip_billing = null,
        	provider_specialty_attending = null,
        	provider_specialty_operating = null,
        	provider_specialty_other = null,
        	provider_specialty_referring = null,
    		provider_specialty_rendering = null
        	from stg_claims.mcare_inpatient_base_claims_j as a
        	left join stg_claims.mcare_inpatient_revenue_center_j as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using facility claim method
        	where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        ) as x6
        
        --reshape from wide to long
        unpivot(providers for provider_type in (
        	billing,
        	rendering,
        	attending,
        	operating,
        	other)
        ) as providers
        where len(providers) = 10 and isnumeric(providers) = 1
        
        --outpatient
        union
        select id_mcare,
        	claim_header_id,
        	first_service_date,
        	last_service_date,
        	cast(providers as bigint) as 'provider_npi',
        	cast(provider_type as varchar(200)) as 'provider_type',
        	provider_type_nch,
        	provider_tin,
        	provider_zip_rendering,
        	provider_zip_billing,
        	provider_specialty_attending,
        	provider_specialty_operating,
        	provider_specialty_other,
        	provider_specialty_referring,
        	provider_specialty_rendering,
        	filetype_mcare,
        	getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'outpatient' as filetype_mcare,
        	a.org_npi_num as billing,
        	a.rfr_physn_npi as referring,
        	care_plan_oversight = null,
        	a.srvc_loc_npi_num as site_of_service,
        	case
        		when a.rndrng_physn_npi is not null then a.rndrng_physn_npi
        		when len(b.rndrng_physn_npi) = 10 then b.rndrng_physn_npi
        		else null
        	end as rendering,
        	organization = null,
        	a.at_physn_npi as attending,
        	a.op_physn_npi as operating,
        	a.ot_physn_npi as other,
        	provider_type_nch = null,
        	provider_tin = null,
        	provider_zip_rendering = null,
        	provider_zip_billing = null,
        	a.at_physn_spclty_cd as provider_specialty_attending,
        	a.op_physn_spclty_cd as provider_specialty_operating,
        	a.ot_physn_spclty_cd as provider_specialty_other,
        	a.rfr_physn_spclty_cd as provider_specialty_referring,
        	case
        		when a.rndrng_physn_npi is not null then a.rndrng_physn_spclty_cd
        		when len(b.rndrng_physn_npi) = 10 then b.rndrng_physn_spclty_cd
        		else null
        	end as provider_specialty_rendering
        	from stg_claims.mcare_outpatient_base_claims as a
        	left join stg_claims.mcare_outpatient_revenue_center as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using facility claim method
        	where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        ) as x7
        
        --reshape from wide to long
        unpivot(providers for provider_type in (
        	billing,
        	referring,
        	site_of_service,
        	rendering,
        	attending,
        	operating,
        	other)
        ) as providers
        where len(providers) = 10 and isnumeric(providers) = 1
    
    	--outpatient data structure j
        union
        select id_mcare,
        	claim_header_id,
        	first_service_date,
        	last_service_date,
        	cast(providers as bigint) as 'provider_npi',
        	cast(provider_type as varchar(200)) as 'provider_type',
        	provider_type_nch,
        	provider_tin,
        	provider_zip_rendering,
        	provider_zip_billing,
        	provider_specialty_attending,
        	provider_specialty_operating,
        	provider_specialty_other,
        	provider_specialty_referring,
        	provider_specialty_rendering,
        	filetype_mcare,
        	getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'outpatient' as filetype_mcare,
        	a.org_npi_num as billing,
        	referring = null,
        	care_plan_oversight = null,
        	site_of_service = null,
        	b.rndrng_physn_npi as rendering,
        	organization = null,
        	a.at_physn_npi as attending,
        	a.op_physn_npi as operating,
        	a.ot_physn_npi as other,
        	provider_type_nch = null,
        	provider_tin = null,
        	provider_zip_rendering = null,
        	provider_zip_billing = null,
        	provider_specialty_attending = null,
        	provider_specialty_operating = null,
        	provider_specialty_other = null,
        	provider_specialty_referring = null,
    		provider_specialty_rendering = null
        	from stg_claims.mcare_outpatient_base_claims_j as a
        	left join stg_claims.mcare_outpatient_revenue_center_j as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using facility claim method
        	where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        ) as x8
        
        --reshape from wide to long
        unpivot(providers for provider_type in (
        	billing,
        	rendering,
        	attending,
        	operating,
        	other)
        ) as providers
        where len(providers) = 10 and isnumeric(providers) = 1
        
        --snf
        union
        select id_mcare,
        	claim_header_id,
        	first_service_date,
        	last_service_date,
        	cast(providers as bigint) as 'provider_npi',
        	cast(provider_type as varchar(200)) as 'provider_type',
        	provider_type_nch,
        	provider_tin,
        	provider_zip_rendering,
        	provider_zip_billing,
        	provider_specialty_attending,
        	provider_specialty_operating,
        	provider_specialty_other,
        	provider_specialty_referring,
        	provider_specialty_rendering,
        	filetype_mcare,
        	getdate() as last_run
        
        from (
        	select
    		--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'snf' as filetype_mcare,
        	a.org_npi_num as billing,
        	referring = null,
        	care_plan_oversight = null,
        	site_of_service = null,
        	a.rndrng_physn_npi as rendering,
        	organization = null,
        	a.at_physn_npi as attending,
        	a.op_physn_npi as operating,
        	a.ot_physn_npi as other,
        	provider_type_nch = null,
        	provider_tin = null,
        	provider_zip_rendering = null,
        	provider_zip_billing = null,
        	a.at_physn_spclty_cd as provider_specialty_attending,
        	a.op_physn_spclty_cd as provider_specialty_operating,
        	a.ot_physn_spclty_cd  as provider_specialty_other,
        	provider_specialty_referring = null,
        	a.rndrng_physn_spclty_cd as provider_specialty_rendering
        	from stg_claims.mcare_snf_base_claims as a
        	left join stg_claims.mcare_snf_revenue_center as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using facility claim method
        	where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        ) as x9
        	
        --reshape from wide to long
        unpivot(providers for provider_type in (
        	billing,
        	rendering,
        	attending,
        	operating,
        	other)
        ) as providers
        where len(providers) = 10 and isnumeric(providers) = 1
        
    ) as z
    --exclude claims among people who have no eligibility data
    left join stg_claims.mcare_bene_enrollment as w
    on z.id_mcare = w.bene_id
    where w.bene_id is not null;",
        .con = dw_inthealth))
    }

#### Table-level QA script ####
qa_stage.mcare_claim_provider_qa_f <- function() {
  
  #confirm that claim types have data for each year for expected provider types
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_provider' as 'table',
  'row count for specified provider type' as qa_type,
  filetype_mcare, provider_type, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_provider
  group by filetype_mcare, provider_type, year(last_service_date)
  order by filetype_mcare, provider_type, year(last_service_date);",
    .con = dw_inthealth))
  
  #make sure everyone is in bene_enrollment table
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_provider' as 'table', '# members not in bene_enrollment, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_provider as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.id_mcare = b.bene_id
    where b.bene_id is null;",
    .con = dw_inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}