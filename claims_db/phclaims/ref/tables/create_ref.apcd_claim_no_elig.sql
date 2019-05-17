--Code to create ref.apcd_claim_no_elig table
--A list of APCD member IDs for people with claims but no eligiblity information EVER
--Eli Kern (PHSKC-APDE)
--2019-5-14

IF object_id('PHClaims.ref.apcd_claim_no_elig', 'U') is not null DROP TABLE PHClaims.ref.apcd_claim_no_elig;
CREATE TABLE PHClaims.ref.apcd_claim_no_elig (
	id_apcd bigint
);



