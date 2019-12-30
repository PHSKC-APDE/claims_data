--Code to load data to stage.mcare_outpatient_revenue_center
--Union of single-year files
--Eli Kern (PHSKC-APDE)
--2019-12
--Run time: 21 min


insert into PHClaims.stage.mcare_outpatient_revenue_center_load with (tablock)

--2014 data
select
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
from PHClaims.load_raw.mcare_outpatient_revenue_center_k16;
