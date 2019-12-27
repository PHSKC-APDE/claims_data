--Code to load data to stage.mcare_snf_revenue_center
--Union of single-year files
--Eli Kern (PHSKC-APDE)
--2019-12
--Run time: xx min


insert into PHClaims.stage.mcare_snf_revenue_center_load with (tablock)

--2014 data
select
bene_id as id_mcare
,clm_id as claim_header_id
,clm_line_num as claim_line_id
,rev_cntr as revenue_code
,hcpcs_cd as procedure_code_hcpcs
,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
,hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3
,rev_cntr_ide_ndc_upc_num as ndc_code
,rev_cntr_ndc_qty as drug_quantity
,rev_cntr_ndc_qty_qlfr_cd as drug_uom
,rndrng_physn_npi as provider_rendering_npi
,rndrng_physn_spclty_cd as provider_rendering_specialty
,getdate() as last_run
from PHClaims.load_raw.mcare_snf_revenue_center_k_14

--2015 data
union
select
bene_id as id_mcare
,clm_id as claim_header_id
,clm_line_num as claim_line_id
,rev_cntr as revenue_code
,hcpcs_cd as procedure_code_hcpcs
,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
,hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3
,rev_cntr_ide_ndc_upc_num as ndc_code
,rev_cntr_ndc_qty as drug_quantity
,rev_cntr_ndc_qty_qlfr_cd as drug_uom
,rndrng_physn_npi as provider_rendering_npi
,rndrng_physn_spclty_cd as provider_rendering_specialty
,getdate() as last_run
from PHClaims.load_raw.mcare_snf_revenue_center_k_15

--2016 data
union
select
bene_id as id_mcare
,clm_id as claim_header_id
,clm_line_num as claim_line_id
,rev_cntr as revenue_code
,hcpcs_cd as procedure_code_hcpcs
,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
,hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3
,rev_cntr_ide_ndc_upc_num as ndc_code
,rev_cntr_ndc_qty as drug_quantity
,rev_cntr_ndc_qty_qlfr_cd as drug_uom
,rndrng_physn_npi as provider_rendering_npi
,rndrng_physn_spclty_cd as provider_rendering_specialty
,getdate() as last_run
from PHClaims.load_raw.mcare_snf_revenue_center_k_16;
