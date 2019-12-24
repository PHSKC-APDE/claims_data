--Code to load data to stage.mcare_bcarrier_claims
--Union of single-year files
--Eli Kern (PHSKC-APDE)
--2019-12
--Run time: X


insert into PHClaims.stage.mcare_bcarrier_claims_load with (tablock)

--2014 data
select
encrypted723beneficiaryid as id_mcare
,encryptedclaimid as claim_header_id
,claimfromdate as first_service_date
,claimthroughdatedeterminesyearof as last_service_date
,nchclaimtypecode as claim_type
,carrierclaimpaymentdenialcode as denial_code
,provider_billing_npi = null
,carrierclaimreferingphysiciannpi as provider_referring_npi
,provider_cpo_npi = null
,provider_sos_npi = null
,primaryclaimdiagnosiscode as dx01
,primaryclaimdiagnosiscodediagnos as dx01_ver

--INSERT OTHER DIAGNOSIS FIELDS AND THEN MOVE ONTO 2015 DATAs


from PHClaims.load_raw.mcare_bcarrier_claims_k_14


--2015 data
union
select

--2015 data
union
select
