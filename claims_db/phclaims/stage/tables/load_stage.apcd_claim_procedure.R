#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_PROCEDURE
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2024-04-29 update: Modified for HHSAW migration
# 2025-04-02 update: Removed procedure_code_number column and consolidated modifier codes into single column
# 2026-08-24 update: Modified under migration of ETL from Enclave -> KC
  # Added code to exclude i) non-WA residents, ii) people with no claims but no enrollment data, iii) condensed code using CROSS APPLY,
  # iv) source ICD procedure codes from medical_claim_icd_procedure table
  
### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_procedure_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
	--Extract HCPCS and CPT procedure codes
	with line_procedure as (
		select
		distinct
		a.internal_member_id as id_apcd,
		a.medical_claim_header_id as claim_header_id,
		b.first_service_dt as first_service_date,
		b.last_service_dt as last_service_date,
		a.procedure_code,
		v.modifier_code
		from stg_claims.apcd_medical_claim as a
		left join stg_claims.apcd_medical_claim_header as b
		on a.medical_claim_header_id = b.medical_claim_header_id
		left join stg_claims.apcd_ref_nonresident_id as y
		on a.internal_member_id = y.id_apcd
		left join stg_claims.apcd_ref_claim_no_elig as z
		on a.internal_member_id = z.id_apcd
		cross apply (
			-- Valid modifiers
			select a.procedure_modifier_code_1
			where a.procedure_modifier_code_1 NOT IN ('-1','-2')

			union all
			select a.procedure_modifier_code_2
			where a.procedure_modifier_code_2 NOT IN ('-1','-2')

			union all
			select a.procedure_modifier_code_3
			where a.procedure_modifier_code_3 NOT IN ('-1','-2')

			union all
			select a.procedure_modifier_code_4
			where a.procedure_modifier_code_4 NOT IN ('-1','-2')

			-- No-modifier case: if ALL FOUR are invalid, produce a single NULL row
			union all
			select null
			where a.procedure_modifier_code_1 IN ('-1','-2')
			  and a.procedure_modifier_code_2 IN ('-1','-2')
			  and a.procedure_modifier_code_3 IN ('-1','-2')
			  and a.procedure_modifier_code_4 IN ('-1','-2')
		) v(modifier_code)
		where a.procedure_code not in ('-1','-2')
		--exclude denined/orphaned claims
		and (b.denied_header_flag = 'N' and b.orphaned_header_flag = 'N')
		--exclude members with no WA residency OR no elig data
		and (y.id_apcd is null and z.id_apcd is null)
	),
	--Extract ICD procedure codes
	icd_procedure as (
		select
		distinct
		a.internal_member_id as id_apcd,
		c.medical_claim_header_id as claim_header_id,
		c.first_service_dt as first_service_date,
		c.last_service_dt as last_service_date,
		a.icd_procedure_code as procedure_code,
		null as modifier_code
		from stg_claims.apcd_medical_claim_icd_procedure as a
		left join stg_claims.apcd_medical_claim as b
		on a.medical_claim_service_line_id = b.medical_claim_service_line_id
		left join stg_claims.apcd_medical_claim_header as c
		on b.medical_claim_header_id = c.medical_claim_header_id
		left join stg_claims.apcd_ref_nonresident_id as y
		on a.internal_member_id = y.id_apcd
		left join stg_claims.apcd_ref_claim_no_elig as z
		on a.internal_member_id = z.id_apcd
		where a.icd_procedure_code not in ('-1','-2')
		--exclude denined/orphaned claims
		and (c.denied_header_flag = 'N' and c.orphaned_header_flag = 'N')
		--exclude members with no WA residency OR no elig data
		and (y.id_apcd is null and z.id_apcd is null)
	)
	--Union and insert into table shell
	insert into stg_claims.stage_apcd_claim_procedure
	select *, getdate() as last_run from line_procedure
	union select *, getdate() as last_run from icd_procedure;",
    .con = dw_inthealth))
}


#### Table-level QA script ####
qa_stage.apcd_claim_procedure_f <- function() {
  
  #all members should be in elig_demo and elig_month tables
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_procedure' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_procedure as a
    left join stg_claims.stage_apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_procedure' as 'table', '# members not in elig_month, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_procedure as a
    left join stg_claims.stage_apcd_elig_month as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  #no null procedure codes
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_procedure' as 'table', '# of null procedure codes, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_procedure
    where procedure_code is null;",
    .con = dw_inthealth))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}