--Code to load data to stage.mcaid_mcare_claim_header
--Union of mcaid and mcare claim header tables
--Eli Kern (PHSKC-APDE)
--2019-10
--Run time: X min

-------------------
--STEP 1: Union mcaid and mcare claim header tables and insert into table shell
-------------------
insert into PHClaims.stage.mcaid_mcare_claim_header with (tablock)

--Medicaid claim header
select
b.id_apde
,a.claim_header_id
,a.clm_type_mcaid_id --note to normalize column name
,claim_type_mcare_id = null
,a.claim_type_id
,file_type_mcare = null
,a.first_service_date
,a.last_service_date
,a.patient_status
,a.admsn_source
,a.admsn_date
,a.admsn_time
,a.dschrg_date
,a.place_of_service_code
,a.type_of_bill_code
,a.clm_status_code
,a.billing_provider_npi
,a.drvd_drg_code
,a.insrnc_cvrg_code
,a.last_pymnt_date
,a.bill_date
,a.system_in_date
,a.claim_header_id_date
,a.primary_diagnosis
,a.icdcm_version
,a.primary_diagnosis_poa
,a.mental_dx1
,a.mental_dxany
,a.mental_dx_rda_any
,a.sud_dx_rda_any
,a.maternal_dx1
,a.maternal_broad_dx1
,a.newborn_dx1
,a.ed
,a.ed_nohosp
,a.ed_bh
,a.ed_avoid_ca
,a.ed_avoid_ca_nohosp
,a.ed_ne_nyu
,a.ed_pct_nyu
,a.ed_pa_nyu
,a.ed_npa_nyu
,a.ed_mh_nyu
,a.ed_sud_nyu
,a.ed_alc_nyu
,a.ed_injury_nyu
,a.ed_unclass_nyu
,a.ed_emergent_nyu
,a.ed_nonemergent_nyu
,a.ed_intermediate_nyu
,a.inpatient
,a.ipt_medsurg
,a.ipt_bh
,a.intent
,a.mechanism
,a.sdoh_any
,a.ed_sdoh
,a.ipt_sdoh
,a.ccs
,a.ccs_description
,a.ccs_description_plain_lang
,a.ccs_mult1
,a.ccs_mult1_description
,a.ccs_mult2
,a.ccs_mult2_description
,a.ccs_mult2_plain_lang
,a.ccs_final_description
,a.ccs_final_plain_lang
,getdate() as last_run
from PHClaims.final.mcaid_claim_header as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcaid = b.id_mcaid

union

--Medicare claim header
select
b.id_apde
,a.claim_header_id
,clm_type_mcaid_id = null --note to normalize column name
,a.claim_type_mcare_id
,a.claim_type_id
,a.filetype as file_type_mcare
,a.first_service_date
,a.last_service_date
,patient_status = null
,admsn_source = null
,admsn_date = null
,admsn_time = null
,dschrg_date = null
,place_of_service_code = null
,type_of_bill_code = null
,clm_status_code = null
,billing_provider_npi = null
,drvd_drg_code = null
,insrnc_cvrg_code = null
,last_pymnt_date = null
,bill_date = null
,system_in_date = null
,claim_header_id_date = null
,primary_diagnosis = null
,icdcm_version = null
,primary_diagnosis_poa = null
,mental_dx1 = null
,mental_dxany = null
,mental_dx_rda_any = null
,sud_dx_rda_any = null
,maternal_dx1 = null
,maternal_broad_dx1 = null
,newborn_dx1 = null
,ed = null
,ed_nohosp = null
,ed_bh = null
,ed_avoid_ca = null
,ed_avoid_ca_nohosp = null
,ed_ne_nyu = null
,ed_pct_nyu = null
,ed_pa_nyu = null
,ed_npa_nyu = null
,ed_mh_nyu = null
,ed_sud_nyu = null
,ed_alc_nyu = null
,ed_injury_nyu = null
,ed_unclass_nyu = null
,ed_emergent_nyu = null
,ed_nonemergent_nyu = null
,ed_intermediate_nyu = null
,inpatient = null
,ipt_medsurg = null
,ipt_bh = null
,intent = null
,mechanism = null
,sdoh_any = null
,ed_sdoh = null
,ipt_sdoh = null
,ccs = null
,ccs_description = null
,ccs_description_plain_lang = null
,ccs_mult1 = null
,ccs_mult1_description = null
,ccs_mult2 = null
,ccs_mult2_description = null
,ccs_mult2_plain_lang = null
,ccs_final_description = null
,ccs_final_plain_lang = null
,getdate() as last_run
from PHClaims.final.mcare_claim_header as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcare = b.id_mcare;