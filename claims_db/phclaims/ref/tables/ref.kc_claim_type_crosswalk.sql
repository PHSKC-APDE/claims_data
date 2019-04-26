--Code to create and load data to ref.kc_claim_type_crosswalk
--Crosswalk between King County claim type variable and ProviderOne, Medicare and WA-APCD
--Eli Kern (PHSKC-APDE)
--2019-4-26

---------------------
--STEP 1: Create table
---------------------

IF OBJECT_ID('[PHClaims].[ref].[kc_claim_type_crosswalk]', 'U') IS NOT NULL
DROP TABLE [PHClaims].[ref].kc_claim_type_crosswalk;
CREATE TABLE [PHClaims].[ref].kc_claim_type_crosswalk
([source_clm_type_id] VARCHAR(20)
,[source_clm_type_desc] VARCHAR(255)
,[source_desc] VARCHAR(255)
,[kc_clm_type_id] TINYINT
,[kc_clm_type_desc] VARCHAR(255)
);

TRUNCATE TABLE [PHClaims].[ref].kc_claim_type_crosswalk;

---------------------
--STEP 2: Load data from Excel file
---------------------
insert into PHClaims.ref.kc_claim_type_crosswalk
(source_clm_type_id
,source_clm_type_desc
,source_desc
,kc_clm_type_id
,kc_clm_type_desc)
select 
 cast('source_clm_type_id' as varchar(20)) as source_clm_type_id
,cast('source_clm_type_desc' as varchar(255)) as source_clm_type_desc
,cast('source_desc' as varchar(255)) as source_desc
,cast('kc_clm_type_id' as tinyint) as kc_clm_type_id
,cast('kc_clm_type_desc' as varchar(255)) as kc_clm_type_desc
from openrowset('Microsoft.ACE.OLEDB.12.0',
    'Excel 12.0; Database=C:\Users\kerneli\SharePoint\King County Cross-Sector Data - Doc\References\Claim type\kc_claim_type_crosswalk.xlsx',
	[kc_claim_type_crosswalk$]);

--FROM [KC\psylling].[tmp_MH-Dx-value-set-ICD9-10.xlsx];