--Code to load data to stage.mcare_dme_line
--Union of single-year files
--Eli Kern (PHSKC-APDE)
--2019-12
--Run time: 1 min


insert into PHClaims.stage.mcare_dme_line_load with (tablock)

--2015 data
select
bene_id as id_mcare
,clm_id as claim_header_id
,line_num as claim_line_id
,line_cms_type_srvc_cd as type_of_service
,line_place_of_srvc_cd as place_of_service_code
,hcpcs_cd as procedure_code_hcpcs
,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
,hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3
,hcpcs_4th_mdfr_cd as procedure_code_hcps_modifier_4
,betos_cd as procedure_code_betos
,prvdr_npi as provider_supplier_npi
,getdate() as last_run
from PHClaims.load_raw.mcare_dme_line_k_15

--2016 data
union
select
bene_id as id_mcare
,clm_id as claim_header_id
,line_num as claim_line_id
,line_cms_type_srvc_cd as type_of_service
,line_place_of_srvc_cd as place_of_service_code
,hcpcs_cd as procedure_code_hcpcs
,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
,hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3
,hcpcs_4th_mdfr_cd as procedure_code_hcps_modifier_4
,betos_cd as procedure_code_betos
,prvdr_npi as provider_supplier_npi
,getdate() as last_run
from PHClaims.load_raw.mcare_dme_line_k_16;
