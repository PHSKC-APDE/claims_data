--Code to load data to stage.mcare_dme_claims
--Union of single-year files
--Eli Kern (PHSKC-APDE)
--2019-12
--Run time: xx min


insert into PHClaims.stage.mcare_dme_claims_load with (tablock)

--2015 data
select
bene_id as id_mcare
,clm_id as claim_header_id
,clm_from_dt as first_service_date
,clm_thru_dt as last_service_date
,nch_clm_type_cd as claim_type
,carr_clm_pmt_dnl_cd as denial_code
,rfr_physn_npi as provider_referring_npi
,prncpal_dgns_cd as dx01
,prncpal_dgns_vrsn_cd as dx01_ver
,icd_dgns_cd1 as dx02
,icd_dgns_vrsn_cd1 as dx02_ver
,icd_dgns_cd2 as dx03
,icd_dgns_vrsn_cd2 as dx03_ver
,icd_dgns_cd3 as dx04
,icd_dgns_vrsn_cd3 as dx04_ver
,icd_dgns_cd4 as dx05
,icd_dgns_vrsn_cd4 as dx05_ver
,icd_dgns_cd5 as dx06
,icd_dgns_vrsn_cd5 as dx06_ver
,icd_dgns_cd6 as dx07
,icd_dgns_vrsn_cd6 as dx07_ver
,icd_dgns_cd7 as dx08
,icd_dgns_vrsn_cd7 as dx08_ver
,icd_dgns_cd8 as dx09
,icd_dgns_vrsn_cd8 as dx09_ver
,icd_dgns_cd9 as dx10
,icd_dgns_vrsn_cd9 as dx10_ver
,icd_dgns_cd10 as dx11
,icd_dgns_vrsn_cd10 as dx11_ver
,icd_dgns_cd11 as dx12
,icd_dgns_vrsn_cd11 as dx12_ver
,icd_dgns_cd12 as dx13
,icd_dgns_vrsn_cd12 as dx13_ver
,getdate() as last_run
from PHClaims.load_raw.mcare_dme_claims_k_15

--2016 data
union
select
bene_id as id_mcare
,clm_id as claim_header_id
,clm_from_dt as first_service_date
,clm_thru_dt as last_service_date
,nch_clm_type_cd as claim_type
,carr_clm_pmt_dnl_cd as denial_code
,rfr_physn_npi as provider_referring_npi
,prncpal_dgns_cd as dx01
,prncpal_dgns_vrsn_cd as dx01_ver
,icd_dgns_cd1 as dx02
,icd_dgns_vrsn_cd1 as dx02_ver
,icd_dgns_cd2 as dx03
,icd_dgns_vrsn_cd2 as dx03_ver
,icd_dgns_cd3 as dx04
,icd_dgns_vrsn_cd3 as dx04_ver
,icd_dgns_cd4 as dx05
,icd_dgns_vrsn_cd4 as dx05_ver
,icd_dgns_cd5 as dx06
,icd_dgns_vrsn_cd5 as dx06_ver
,icd_dgns_cd6 as dx07
,icd_dgns_vrsn_cd6 as dx07_ver
,icd_dgns_cd7 as dx08
,icd_dgns_vrsn_cd7 as dx08_ver
,icd_dgns_cd8 as dx09
,icd_dgns_vrsn_cd8 as dx09_ver
,icd_dgns_cd9 as dx10
,icd_dgns_vrsn_cd9 as dx10_ver
,icd_dgns_cd10 as dx11
,icd_dgns_vrsn_cd10 as dx11_ver
,icd_dgns_cd11 as dx12
,icd_dgns_vrsn_cd11 as dx12_ver
,icd_dgns_cd12 as dx13
,icd_dgns_vrsn_cd12 as dx13_ver
,getdate() as last_run
from PHClaims.load_raw.mcare_dme_claims_k_16;
