--Code to load data to stage.mcare_claim_procedure table
--Procedure codes reshaped to long
--Eli Kern (PHSKC-APDE)
--2020-01
--Run time: XX min

------------------
--STEP 1: Select and union desired columns from multi-year claim tables on stage schema
--Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
--Unpivot and insert into table shell
-------------------
insert into PHClaims.stage.mcare_claim_procedure with (tablock)

select distinct
	id_mcare,
	claim_header_id,
	first_service_date,
	last_service_date,

	--original diagnosis code
	procedure_codes as 'procedure_code',

	--procedure code number/type
	cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',

	--modifier codes
    procedure_code_hcps_modifier_1 as modifier_1,
	procedure_code_hcps_modifier_2 as modifier_2,
	procedure_code_hcps_modifier_3 as modifier_3,
	procedure_code_hcps_modifier_4 as modifier_4,

	filetype_mcare,
	getdate() as last_run

from (
	--bcarrier
	select
	top 100
	rtrim(a.id_mcare) as id_mcare,
	rtrim(a.claim_header_id) as claim_header_id,
	a.first_service_date,
	a.last_service_date,
	'carrier' as filetype_mcare,
	b.procedure_code_hcpcs as pchcpcs,
	b.procedure_code_hcps_modifier_1,
	b.procedure_code_hcps_modifier_2,
	procedure_code_hcps_modifier_3 = null,
	procedure_code_hcps_modifier_4 = null,
	b.procedure_code_betos as pcbetos,
	pc01 = null,
	pc02 = null,
	pc03 = null,
	pc04 = null,
	pc05 = null,
	pc06 = null,
	pc07 = null,
	pc08 = null,
	pc09 = null,
	pc10 = null,
	pc11 = null,
	pc12 = null,
	pc13 = null,
	pc14 = null,
	pc15 = null,
	pc16 = null,
	pc17 = null,
	pc18 = null,
	pc19 = null,
	pc20 = null,
	pc21 = null,
	pc22 = null,
	pc23 = null,
	pc24 = null,
	pc25 = null
	from PHClaims.stage.mcare_bcarrier_claims as a
	left join PHClaims.stage.mcare_bcarrier_line as b
	on a.claim_header_id = b.claim_header_id
	left join PHClaims.final.mcare_elig_demo as c
	on a.id_mcare = c.id_mcare
	--exclude denined claims using carrier/dme claim method
	where a.denial_code in ('1','2','3','4','5','6','7','8','9')
	--exclude claims among people who have no eligibility data
	and c.id_mcare is not null

	--dme
	union
	select
	top 100
	rtrim(a.id_mcare) as id_mcare,
	rtrim(a.claim_header_id) as claim_header_id,
	a.first_service_date,
	a.last_service_date,
	'dme' as filetype_mcare,
	b.procedure_code_hcpcs as pchcpcs,
	b.procedure_code_hcps_modifier_1,
	b.procedure_code_hcps_modifier_2,
	b.procedure_code_hcps_modifier_3,
	b.procedure_code_hcps_modifier_4,
	b.procedure_code_betos as pcbetos,
	pc01 = null,
	pc02 = null,
	pc03 = null,
	pc04 = null,
	pc05 = null,
	pc06 = null,
	pc07 = null,
	pc08 = null,
	pc09 = null,
	pc10 = null,
	pc11 = null,
	pc12 = null,
	pc13 = null,
	pc14 = null,
	pc15 = null,
	pc16 = null,
	pc17 = null,
	pc18 = null,
	pc19 = null,
	pc20 = null,
	pc21 = null,
	pc22 = null,
	pc23 = null,
	pc24 = null,
	pc25 = null
	from PHClaims.stage.mcare_dme_claims as a
	left join PHClaims.stage.mcare_dme_line as b
	on a.claim_header_id = b.claim_header_id
	left join PHClaims.final.mcare_elig_demo as c
	on a.id_mcare = c.id_mcare
	--exclude denined claims using carrier/dme claim method
	where a.denial_code in ('1','2','3','4','5','6','7','8','9')
	--exclude claims among people who have no eligibility data
	and c.id_mcare is not null

	--hha
	--placeholder once we receive HHA revenue center tables

	--hospice
	union
	select
	top 100
	rtrim(a.id_mcare) as id_mcare,
	rtrim(a.claim_header_id) as claim_header_id,
	a.first_service_date,
	a.last_service_date,
	'hospice' as filetype_mcare,
	b.procedure_code_hcpcs as pchcpcs,
	b.procedure_code_hcps_modifier_1,
	b.procedure_code_hcps_modifier_2,
	b.procedure_code_hcps_modifier_3,
	procedure_code_hcps_modifier_4 = null,
	pcbetos = null,
	pc01 = null,
	pc02 = null,
	pc03 = null,
	pc04 = null,
	pc05 = null,
	pc06 = null,
	pc07 = null,
	pc08 = null,
	pc09 = null,
	pc10 = null,
	pc11 = null,
	pc12 = null,
	pc13 = null,
	pc14 = null,
	pc15 = null,
	pc16 = null,
	pc17 = null,
	pc18 = null,
	pc19 = null,
	pc20 = null,
	pc21 = null,
	pc22 = null,
	pc23 = null,
	pc24 = null,
	pc25 = null
	from PHClaims.stage.mcare_hospice_base_claims as a
	left join PHClaims.stage.mcare_hospice_revenue_center as b
	on a.claim_header_id = b.claim_header_id
	left join PHClaims.final.mcare_elig_demo as c
	on a.id_mcare = c.id_mcare
	--exclude denined claims using carrier/dme claim method
	where (a.denial_code_facility = '' or a.denial_code_facility is null)
	--exclude claims among people who have no eligibility data
	and c.id_mcare is not null

	--inpatient
	union
	select
	top 100
	rtrim(a.id_mcare) as id_mcare,
	rtrim(a.claim_header_id) as claim_header_id,
	a.first_service_date,
	a.last_service_date,
	'inpatient' as filetype_mcare,
	b.procedure_code_hcpcs as pchcpcs,
	b.procedure_code_hcps_modifier_1,
	b.procedure_code_hcps_modifier_2,
	procedure_code_hcps_modifier_3 = null,
	procedure_code_hcps_modifier_4 = null,
	pcbetos = null,
	a.pc01,
	a.pc02,
	a.pc03,
	a.pc04,
	a.pc05,
	a.pc06,
	a.pc07,
	a.pc08,
	a.pc09,
	a.pc10,
	a.pc11,
	a.pc12,
	a.pc13,
	a.pc14,
	a.pc15,
	a.pc16,
	a.pc17,
	a.pc18,
	a.pc19,
	a.pc20,
	a.pc21,
	a.pc22,
	a.pc23,
	a.pc24,
	a.pc25
	from PHClaims.stage.mcare_inpatient_base_claims as a
	left join PHClaims.stage.mcare_inpatient_revenue_center as b
	on a.claim_header_id = b.claim_header_id
	left join PHClaims.final.mcare_elig_demo as c
	on a.id_mcare = c.id_mcare
	--exclude denined claims using carrier/dme claim method
	where (a.denial_code_facility = '' or a.denial_code_facility is null)
	--exclude claims among people who have no eligibility data
	and c.id_mcare is not null

	--outpatient
	union
	select
	top 100
	rtrim(a.id_mcare) as id_mcare,
	rtrim(a.claim_header_id) as claim_header_id,
	a.first_service_date,
	a.last_service_date,
	'outpatient' as filetype_mcare,
	b.procedure_code_hcpcs as pchcpcs,
	b.procedure_code_hcps_modifier_1,
	b.procedure_code_hcps_modifier_2,
	procedure_code_hcps_modifier_3 = null,
	procedure_code_hcps_modifier_4 = null,
	pcbetos = null,
	a.pc01,
	a.pc02,
	a.pc03,
	a.pc04,
	a.pc05,
	a.pc06,
	a.pc07,
	a.pc08,
	a.pc09,
	a.pc10,
	a.pc11,
	a.pc12,
	a.pc13,
	a.pc14,
	a.pc15,
	a.pc16,
	a.pc17,
	a.pc18,
	a.pc19,
	a.pc20,
	a.pc21,
	a.pc22,
	a.pc23,
	a.pc24,
	a.pc25
	from PHClaims.stage.mcare_outpatient_base_claims as a
	left join PHClaims.stage.mcare_outpatient_revenue_center as b
	on a.claim_header_id = b.claim_header_id
	left join PHClaims.final.mcare_elig_demo as c
	on a.id_mcare = c.id_mcare
	--exclude denined claims using carrier/dme claim method
	where (a.denial_code_facility = '' or a.denial_code_facility is null)
	--exclude claims among people who have no eligibility data
	and c.id_mcare is not null

	--snf
	union
	select
	top 100
	rtrim(a.id_mcare) as id_mcare,
	rtrim(a.claim_header_id) as claim_header_id,
	a.first_service_date,
	a.last_service_date,
	'snf' as filetype_mcare,
	b.procedure_code_hcpcs as pchcpcs,
	b.procedure_code_hcps_modifier_1,
	b.procedure_code_hcps_modifier_2,
	b.procedure_code_hcps_modifier_3,
	procedure_code_hcps_modifier_4 = null,
	pcbetos = null,
	a.pc01,
	a.pc02,
	a.pc03,
	a.pc04,
	a.pc05,
	a.pc06,
	a.pc07,
	a.pc08,
	a.pc09,
	a.pc10,
	a.pc11,
	a.pc12,
	a.pc13,
	a.pc14,
	a.pc15,
	a.pc16,
	a.pc17,
	a.pc18,
	a.pc19,
	a.pc20,
	a.pc21,
	a.pc22,
	a.pc23,
	a.pc24,
	a.pc25
	from PHClaims.stage.mcare_snf_base_claims as a
	left join PHClaims.stage.mcare_snf_revenue_center as b
	on a.claim_header_id = b.claim_header_id
	left join PHClaims.final.mcare_elig_demo as c
	on a.id_mcare = c.id_mcare
	--exclude denined claims using carrier/dme claim method
	where (a.denial_code_facility = '' or a.denial_code_facility is null)
	--exclude claims among people who have no eligibility data
	and c.id_mcare is not null
	
) as a
--reshape from wide to long
unpivot(procedure_codes for procedure_code_number in (
	pchcpcs,
	pcbetos,
	pc01,
	pc02,
	pc03,
	pc04,
	pc05,
	pc06,
	pc07,
	pc08,
	pc09,
	pc10,
	pc11,
	pc12,
	pc13,
	pc14,
	pc15,
	pc16,
	pc17,
	pc18,
	pc19,
	pc20,
	pc21,
	pc22,
	pc23,
	pc24,
	pc25)
) as procedure_codes
where procedure_codes is not null AND procedure_codes!=' ';