--Code to load data to stage.mcare_bcarrier_line
--Union of single-year files
--Eli Kern (PHSKC-APDE)
--2019-12
--Run time: 12 min


insert into PHClaims.stage.mcare_bcarrier_line_load with (tablock)

--2014 data
select top 100
encrypted723beneficiaryid as id_mcare
,encryptedclaimid as claim_header_id
,claimlinenumber as claim_line_id
,carrierlineperformingnpinumber as provider_rendering_npi
,carrierlineperforminggroupnpinum as provider_org_npi
,carrierlineprovidertypecode as provider_rendering_type
,lineprovidertaxnumber as provider_rendering_tin
,carrierlineperformingproviderzip as provider_rendering_zip
,linehcfaproviderspecialtycode as provider_rendering_specialty
,linehcfatypeservicecode as type_of_service
,lineplaceofservicecode as place_of_service_code
,linehealthcarecommonprocedurecod as procedure_code_hcpcs
,linehcpcsinitialmodifiercode as procedure_code_hcps_modifier_1
,linehcpcssecondmodifiercode as procedure_code_hcps_modifier_2
,linenchbetoscode as procedure_code_betos
,null as provider_billing_zip
,getdate() as last_run
from PHClaims.load_raw.mcare_bcarrier_line_j_14

--2015 data
union
select top 100
bene_id collate SQL_Latin1_General_Cp1_CS_AS as id_mcare
,clm_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id
,line_num as claim_line_id
,prf_physn_npi as provider_rendering_npi
,org_npi_num as provider_org_npi
,carr_line_prvdr_type_cd as provider_rendering_type
,tax_num as provider_rendering_tin
,prvdr_zip as provider_rendering_zip
,prvdr_spclty as provider_rendering_specialty
,line_cms_type_srvc_cd as type_of_service
,line_place_of_srvc_cd as place_of_service_code
,hcpcs_cd as procedure_code_hcpcs
,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
,betos_cd as procedure_code_betos
,physn_zip_cd as provider_billing_zip
,getdate() as last_run
from PHClaims.load_raw.mcare_bcarrier_line_k

--2016 data
union
select top 100
bene_id as id_mcare
,clm_id as claim_header_id
,line_num as claim_line_id
,prf_physn_npi as provider_rendering_npi
,org_npi_num as provider_org_npi
,carr_line_prvdr_type_cd as provider_rendering_type
,tax_num as provider_rendering_tin
,prvdr_zip as provider_rendering_zip
,prvdr_spclty as provider_rendering_specialty
,line_cms_type_srvc_cd as type_of_service
,line_place_of_srvc_cd as place_of_service_code
,hcpcs_cd as procedure_code_hcpcs
,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
,betos_cd as procedure_code_betos
,physn_zip_cd as provider_billing_zip
,getdate() as last_run
from PHClaims.load_raw.mcare_bcarrier_line_k_16;
