--Code to load data to stage.mcare_outpatient_revenue_center
--Union of single-year files
--Eli Kern (PHSKC-APDE)
--2019-12
--Run time: 21 min
-----------------------------
----------------------------
--Shuva Dawadi
--2/12/2020
--adding 2017 codeblock 



insert into PHClaims.stage.mcare_outpatient_revenue_center_load with (tablock)

--2014 data
select
top 100
encrypted723beneficiaryid as id_mcare
,encryptedclaimid as claim_header_id
,claimlinenumber as claim_line_id
,revenuecentercode as revenue_code
,revenuecenterhealthcarecommonpro as procedure_code_hcpcs
,revenuecenterhcpcsinitialmodifie as procedure_code_hcps_modifier_1
,revenuecenterhcpcssecondmodifier as procedure_code_hcps_modifier_2
,revenuecenteridendcupcnumber as ndc_code
,revenuecenterndcquantity as drug_quantity
,revenuecenterndcquantityqualifie as drug_uom
,revenuecenterrenderingphysiciann as provider_rendering_npi
,null as provider_rendering_specialty
,getdate() as last_run
from PHClaims.load_raw.mcare_outpatient_revenue_center_k14

--2015 data
union
select
top 100
encrypted723beneficiaryid as id_mcare
,encryptedclaimid as claim_header_id
,claimlinenumber as claim_line_id
,revenuecentercode as revenue_code
,revenuecenterhealthcarecommonpro as procedure_code_hcpcs
,revenuecenterhcpcsinitialmodifie as procedure_code_hcps_modifier_1
,revenuecenterhcpcssecondmodifier as procedure_code_hcps_modifier_2
,revenuecenteridendcupcnumber as ndc_code
,revenuecenterndcquantity as drug_quantity
,revenuecenterndcquantityqualifie as drug_uom
,revenuecenterrenderingphysiciann as provider_rendering_npi
,revenuecenterrenderingphysicians as provider_rendering_specialty
,getdate() as last_run
from PHClaims.load_raw.mcare_outpatient_revenue_center_k15

--2016 data
union
select
top 100
encrypted723beneficiaryid as id_mcare
,encryptedclaimid as claim_header_id
,claimlinenumber as claim_line_id
,revenuecentercode as revenue_code
,revenuecenterhealthcarecommonpro as procedure_code_hcpcs
,revenuecenterhcpcsinitialmodifie as procedure_code_hcps_modifier_1
,revenuecenterhcpcssecondmodifier as procedure_code_hcps_modifier_2
,revenuecenteridendcupcnumber as ndc_code
,revenuecenterndcquantity as drug_quantity
,revenuecenterndcquantityqualifie as drug_uom
,revenuecenterrenderingphysiciann as provider_rendering_npi
,revenuecenterrenderingphysicians as provider_rendering_specialty
,getdate() as last_run
from PHClaims.load_raw.mcare_outpatient_revenue_center_k16

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
,rndrng_physn_npi as provider_rendering_npi
,rndrng_physn_spclty_cd as provider_rendering_specialty
,getdate() as last_run
from PHClaims.load_raw.mcare_outpatient_revenue_center_k_17;