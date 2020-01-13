--Shuva Dawadi
--10/10/2019


--Build claim header table using all claim types in Medicare files
-- Stage files used for the following: inpatient, outpaitnet, bcarrier files, snf, home health, dme
--Union all files together
--for the case sensitivity to carry through, the empty table has to specify case sensitive columns . 

--NOTES:
--use 'date' everywehere 'dt' was specificed 
--include : id, claim header id, date of service, to and from date, claim type, claim type standardized
--must be named in the same way inorder to use the medicaid package
--alter [claim_header_id] &  [id_mcare] so 
-- for some of these files, revenue_center is line level
--id_mcare varchar(100), claim_header_id  varchar(100) not bigintgers

--10/25/2019 update by Eli Kern:
--Adding standardized claim type to table, which is needed to create CCW table
--Modifying to write table to stage schema first, QA, then move to final schema



--inpatient
ALTER TABLE [PHClaims].[stage].[mcare_inpatient_base_claims]
ALTER COLUMN bene_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;

ALTER TABLE [PHClaims].[stage].[mcare_inpatient_base_claims]
ALTER COLUMN clm_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;


--outpatient

ALTER TABLE [PHClaims].[stage].[mcare_outpatient_base_claims]
ALTER COLUMN bene_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;

ALTER TABLE [PHClaims].[stage].[mcare_outpatient_base_claims]
ALTER COLUMN clm_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;


--bcarrier
ALTER TABLE [PHClaims].[stage].[mcare_bcarrier_claims]
ALTER COLUMN bene_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;

ALTER TABLE [PHClaims].[stage].[mcare_bcarrier_claims]
ALTER COLUMN clm_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;


--snf
ALTER TABLE [PHClaims].[stage].[mcare_snf_base_claims]
ALTER COLUMN bene_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;

ALTER TABLE [PHClaims].[stage].[mcare_snf_base_claims]
ALTER COLUMN clm_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;

--hh
ALTER TABLE [PHClaims].[stage].[mcare_hha_base_claims]
ALTER COLUMN bene_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;

ALTER TABLE [PHClaims].[stage].[mcare_hha_base_claims]
ALTER COLUMN clm_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;


--dme
ALTER TABLE [PHClaims].[stage].[mcare_dme_claims]
ALTER COLUMN bene_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;

ALTER TABLE [PHClaims].[stage].[mcare_dme_claims]
ALTER COLUMN clm_id varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;


--Create table shell;
if object_id('PHClaims.stage.mcare_claim_header', 'U') is not null drop table PHClaims.stage.mcare_claim_header;
create table PHClaims.stage.mcare_claim_header (
	id_mcare varchar(255) collate SQL_Latin1_General_CP1_CS_AS ,
	claim_header_id  varchar(255) collate SQL_Latin1_General_CP1_CS_AS ,
	first_service_date date,
	last_service_date date,
	claim_type_mcare_id varchar(255),
	claim_type_id tinyint,
	file_type_mcare tinyint
);

--extract data elements from each table
if object_id('tempdb..#temp1') is not null drop table #temp1;
SELECT bene_id as id_mcare, 
clm_id as claim_header_id, 
clm_from_dt as first_service_date, 
clm_thru_dt as last_service_date, 
nch_clm_type_cd as claim_type_mcare_id, 
filetype as file_type_mcare
into #temp1
FROM [PHClaims].[stage].[mcare_inpatient_base_claims]
union
SELECT bene_id as id_mcare, 
clm_id as claim_header_id, 
clm_from_dt as first_service_date, 
clm_thru_dt as last_service_date, 
nch_clm_type_cd as claim_type_mcare_id, 
filetype as file_type_mcare
from [PHClaims].[stage].[mcare_outpatient_base_claims]
union
SELECT bene_id as id_mcare, 
clm_id  as claim_header_id, 
from_dt as first_service_date, 
thru_dt as last_service_date, 
clm_type as claim_type_mcare_id, 
filetype as file_type_mcare
from [PHClaims].[stage].[mcare_bcarrier_claims]
union
SELECT bene_id as id_mcare, 
clm_id as claim_header_id, 
clm_from_dt as first_service_date, 
clm_thru_dt as last_service_date, 
nch_clm_type_cd as claim_type_mcare_id, 
filetype as file_type_mcare
from [PHClaims].[stage].[mcare_snf_base_claims]
union 
SELECT bene_id as id_mcare, 
clm_id as claim_header_id, 
clm_from_dt as first_service_date, 
clm_thru_dt as last_service_date, 
nch_clm_type_cd as claim_type_mcare_id, 
filetype as file_type_mcare
FROM [PHClaims].[stage].[mcare_hha_base_claims]
union 
SELECT bene_id as id_mcare, 
clm_id as claim_header_id, 
clm_from_dt as first_service_date, 
clm_thru_dt as last_service_date, 
nch_clm_type_cd as claim_type_mcare_id, 
filetype as file_type_mcare
FROM [PHClaims].[stage].[mcare_dme_claims]
;

--insert into table shell, adding standardized claim type
insert into PHClaims.stage.mcare_claim_header with (tablock)
select a.id_mcare,
	a.claim_header_id,
	a.first_service_date,
	a.last_service_date,
	a.claim_type_mcare_id,
	cast(b.kc_clm_type_id as tinyint) as claim_type_id,
	a.file_type_mcare
from #temp1 as a
left join (select * from PHClaims.ref.kc_claim_type_crosswalk where source_desc = 'mcare') as b
on a.claim_type_mcare_id = b.source_clm_type_id;

--QA
SELECT Top (1000) * FROM PHClaims.stage.mcare_claim_header;

SELECT COUNT (claim_type_mcare_id) distinct_claimtype, claim_type_mcare_id 
FROM PHClaims.stage.mcare_claim_header
GROUP BY claim_type_mcare_id
;
 
-- distinct_claimtype	claim_type_mcare_id
--	218,745				10
--	257,348				20
--  6,924				30
--  12,180,232			40
--  563,389				60
--  48,284,493			71
--  92,829				72
--  81,780				81
--  2,439,532			82


SELECT COUNT (file_type_mcare) distinct_filetype, file_type_mcare
FROM PHClaims.stage.mcare_claim_header
GROUP BY file_type_mcare;

--  distinct_filetype	filetype
--  396,611				1
--  166,778				2
--  3,731,289			3
--  4,122,489			4
--  4,326,454			5
--  15,294,707			6
--  16,372,195			7
--  16,710,420			8
--  1,252,045			9
--  1,269,267			10
--  86,185				11
--  89,228				12
--  88,859				13
--  76,465				14
--  72,784				15
--  69,496				16

--switch schema to final
drop table PHClaims.final.mcare_claim_header;
use PHClaims
go
alter schema final transfer stage.mcare_claim_header;


