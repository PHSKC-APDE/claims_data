--Shuva Dawadi 
--510/14.2019
--Build icdcm header table using all claim types in Medicare files
-- Stage files used for the following: inpatient, outpaitnet, bcarrier files, snf, home health, dme
--Union all files together
--For the case sensitivity to carry through, the empty table has to specify case sensitive columns . 


-------------------------------------------------------
-------------------------------------------------------
-------------------------------------------------------

if object_id('PHClaims.tmp.mcare_icdcm_header', 'U') is not null drop table PHClaims.final.mcare_claim_header;
create table PHClaims.tmp.mcare_icdcm_header (
	id_mcare varchar(255) collate SQL_Latin1_General_CP1_CS_AS ,
	claim_header_id  varchar(255) collate SQL_Latin1_General_CP1_CS_AS ,
	first_service_date date,
	last_service_date date,
	claim_type_mcare_id varchar(100),
	filetype tinyint
);



--Step 1-Build diagnosis table
If object_id('PHClaims.final.mcare_claim_icdcm_header', 'U') is not null DROP TABLE PHClaims.final.mcare_claim_icdcm_header;
create table PHClaims.final.mcare_claim_icdm_header (
	id_mcare varchar(255) collate SQL_Latin1_General_CP1_CS_AS ,
	claim_header_id  varchar(255) collate SQL_Latin1_General_CP1_CS_AS, 
	icdcm_raw varchar(100), 
    icdcm_norm varchar(100),
    icdcm_version tinyint,
    icdcm_number tinyint,
    filetype tinyint
	);


--Step 2 :extract data elements from each table
	
insert into PHClaims.final.mcare_claim_header with (tablock)
select 

       --original diagnosis code=
       diagnoses as 'icdcm_raw',
       case
                     when (diagnoses like '[0-9]%' and len(diagnoses) = 3) then diagnoses + '00'
                     when (diagnoses like '[0-9]%' and len(diagnoses) = 4) then diagnoses + '0'
                     when (diagnoses like 'V%' and clm_thru_dt < '2015-10-01' and len(diagnoses) = 3) then diagnoses + '00'
                     when (diagnoses like 'V%' and clm_thru_dt < '2015-10-01' and len(diagnoses) = 4) then diagnoses + '0'
                     when (diagnoses like 'E%' and clm_thru_dt < '2015-10-01' and len(diagnoses) = 3) then diagnoses + '00'
                     when (diagnoses like 'E%' and clm_thru_dt < '2015-10-01' and len(diagnoses) = 4) then diagnoses + '0'
                     else diagnoses 
       end as 'icdcm_norm',

       cast(
              case
                     when (diagnoses like '[0-9]%') then 9
                     when (diagnoses like 'V%' and clm_thru_dt < '2015-10-01') then 9
                     when (diagnoses like 'E%' and clm_thru_dt < '2015-10-01') then 9
                     else 10 
              end 
       as tinyint) as 'icdcm_version',
	
	--which diagnosis field the value came from
	  cast(substring(icdcm_number, 3,10) as varchar(200)) as 'icdcm_number',
	  --cast(filetype as varchar(255)) as 'filetype'

 (
       select top 1 bene_id AS 'id_mcare', clm_id as 'claim_header_id', clm_thru_dt, filetype=1,
       --admtg_dgns_cd] AS dxadmit,
       [icd_dgns_cd1] AS dx01, [icd_dgns_cd2] as dx02,
	   [icd_dgns_cd3] AS dx03, [icd_dgns_cd4] as dx04,
	   [icd_dgns_cd5] AS dx05, [icd_dgns_cd6] as dx06,
	   [icd_dgns_cd7] AS dx07, [icd_dgns_cd8] as dx08,
	   [icd_dgns_cd9] AS dx09, [icd_dgns_cd10] as dx10,
	   [icd_dgns_cd11] AS dx11, [icd_dgns_cd12] as dx12,
	   [icd_dgns_cd13] AS dx13, [icd_dgns_cd14] as dx14,
	   [icd_dgns_cd15] AS dx15, [icd_dgns_cd16] as dx16,
	   [icd_dgns_cd17] AS dx17, [icd_dgns_cd18] as dx18,
	   [icd_dgns_cd19] AS dx19, [icd_dgns_cd20] as dx20,
	   [icd_dgns_cd21] AS dx21, [icd_dgns_cd22] as dx22,
	   [icd_dgns_cd23] AS dx23, [icd_dgns_cd24] as dx24,
	   [icd_dgns_cd25] AS dx25,
	   [icd_dgns_e_cd1] AS dxe01,
	   filetype
 FROM [PHClaims].[stage].[mcare_inpatient_base_claims]
n
) a
unpivot(diagnoses for icdcm_number IN(dx01,dx02,dx03,dx04,dx05,dx06,dx07,dx08,dx09,dx10,dx11,dx12,dx13,dx14,dx15,dx16,dx17,dx18,dx19,dx20,dx21,dx22,dx23,dx24,dx25,dxe01)) as diagnoses
where diagnoses is not null AND  diagnoses!=' '; 


