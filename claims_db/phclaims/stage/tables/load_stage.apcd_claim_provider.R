#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_PROVIDER
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2024-04-29 update: Modified for HHSAW migration
# 2026-08-24 update: Modified under migration of ETL from Enclave -> KC
  # Added code to exclude i) non-WA residents, ii) people with no claims but no enrollment data, iii) condensed code using CROSS APPLY,
  # iv) add QA function
  
### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_provider_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
	insert into stg_claims.stage_apcd_claim_provider
	select distinct
		a.internal_member_id as id_apcd,
		b.medical_claim_header_id as claim_header_id,
		b.first_service_dt as first_service_date,
		b.last_service_dt as last_service_date,
		v.provider_id_apcd,
		v.provider_id_raw_apcd,
		v.provider_type,
		getdate() as last_run
	from stg_claims.apcd_medical_claim as a
	left join stg_claims.apcd_medical_claim_header as b
	on a.medical_claim_header_id = b.medical_claim_header_id
	left join stg_claims.apcd_ref_nonresident_id as y
	on a.internal_member_id = y.id_apcd
	left join stg_claims.apcd_ref_claim_no_elig as z
	on a.internal_member_id = z.id_apcd
	cross apply (
		select
			a.billing_internal_provider_id as provider_id_apcd,
			a.billing_provider_id as provider_id_raw_apcd,
			'billing' as provider_type
		where a.billing_internal_provider_id not in ('-1','-2')

		union all
		select
			a.attending_internal_provider_id,
			a.attending_provider_id,
			'attending'
		where a.attending_internal_provider_id not in ('-1','-2')

		union all
		select
			a.rendering_internal_provider_id,
			a.rendering_provider_id,
			'rendering'
		where a.rendering_internal_provider_id not in ('-1','-2')

		union all
		select
			a.referring_internal_provider_id,
			a.referring_provider_id,
			'referring'
		where a.referring_internal_provider_id not in ('-1','-2')
	) v
	--exclude denined/orphaned claims
	where (b.denied_header_flag = 'N' and b.orphaned_header_flag = 'N')
	--exclude members with no WA residency OR no elig data
	and (y.id_apcd is null and z.id_apcd is null);",
		.con = dw_inthealth))
}

#### Table-level QA script ####
qa_stage.apcd_claim_provider_f <- function() {
  
  #referring provider claim header count matches to raw data
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_provider' as 'table', '# referring provider claim headers, expect match to raw' as qa_type,
    count(distinct claim_header_id) as qa
    from stg_claims.stage_apcd_claim_provider
    where provider_type = 'referring';",
    .con = dw_inthealth))
  
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.apcd_medical_claim' as 'table', '# referring provider claim headers, expect match to apcd_claim_provider' as qa_type,
    count(distinct a.medical_claim_header_id) as qa
	from stg_claims.apcd_medical_claim as a
	left join stg_claims.apcd_medical_claim_header as b
	on a.medical_claim_header_id = b.medical_claim_header_id
	left join stg_claims.apcd_ref_nonresident_id as y
	on a.internal_member_id = y.id_apcd
	left join stg_claims.apcd_ref_claim_no_elig as z
	on a.internal_member_id = z.id_apcd
	where a.referring_internal_provider_id not in ('-1','-2')
	--exclude denined/orphaned claims
	and (b.denied_header_flag = 'N' and b.orphaned_header_flag = 'N')
	--exclude members with no WA residency OR no elig data
	and (y.id_apcd is null and z.id_apcd is null);",
    .con = dw_inthealth))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}