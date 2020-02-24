--Code to load data to stage.mcare_inpatient_revenue_center
--Union of single-year files
--Eli Kern (PHSKC-APDE)
--2019-12
--Run time: 1 min
--------------------
-------------------
--Shuva Dawadi
--2/12/2020
--for 2014-2016 data, altered var type from insensitive to sensitive and chagned all var types to varchar 
--added 2017 codeblock 

ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_j  alter column bene_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL
ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_j  alter column clm_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL
ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_j  alter column clm_thru_dt varchar(255)
ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_j  alter column clm_line_num varchar(255)
ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_j  alter column rev_cntr_ndc_qty varchar(255)



ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_k  alter column bene_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL
ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_k  alter column clm_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL
ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_k  alter column clm_thru_dt varchar(255)
ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_k  alter column clm_line_num varchar(255)
ALTER Table PHClaims.load_raw.mcare_inpatient_revenue_center_k  alter column rev_cntr_ndc_qty varchar(255)




insert into PHClaims.stage.mcare_inpatient_revenue_center_load with (tablock)

--2014 data
select
top 100
bene_id as id_mcare
,clm_id as claim_header_id
,clm_line_num as claim_line_id
,rev_cntr as revenue_code
,hcpcs_cd as procedure_code_hcpcs
,null as procedure_code_hcps_modifier_1
,null as procedure_code_hcps_modifier_2
,null as ndc_code
,rev_cntr_ndc_qty as drug_quantity
,rev_cntr_ndc_qty_qlfr_cd as drug_uom
,rndrng_physn_npi as provider_rendering_npi
,getdate() as last_run
from PHClaims.load_raw.mcare_inpatient_revenue_center_j

--2015 and 2016 data
union
select
top 100
bene_id as id_mcare
,clm_id as claim_header_id
,clm_line_num as claim_line_id
,rev_cntr as revenue_code
,hcpcs_cd as procedure_code_hcpcs
,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
,rev_cntr_ide_ndc_upc_num as ndc_code
,rev_cntr_ndc_qty as drug_quantity
,rev_cntr_ndc_qty_qlfr_cd as drug_uom
,null as provider_rendering_npi
,getdate() as last_run
from PHClaims.load_raw.mcare_inpatient_revenue_center_k

--2017 data
union
select
top 100
bene_id as id_mcare
,clm_id as claim_header_id
,clm_line_num as claim_line_id
,rev_cntr as revenue_code
,hcpcs_cd as procedure_code_hcpcs
,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
,rev_cntr_ide_ndc_upc_num as ndc_code
,rev_cntr_ndc_qty as drug_quantity
,rev_cntr_ndc_qty_qlfr_cd as drug_uom
,null as provider_rendering_npi
,getdate() as last_run
from PHClaims.load_raw.mcare_inpatient_revenue_center_k_17;

