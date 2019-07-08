
use PHClaims;
go

delete from [metadata].[qa_mcaid] where table_name = 'stage.mcaid_claim_procedure';

--All members should be in elig_demo and table
select count(a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_procedure] as a
where not exists
(
select 1 
from [stage].[mcaid_elig_demo] as b
where a.id_mcaid = b.id_mcaid
);

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_procedure]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'mcaid_elig_demo.id_mcaid foreign key check'
,'PASS'
,getdate()
,'All members in mcaid_claim_procedure are in mcaid_elig_demo';
go

--All members should be in elig_timevar table
select count(a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_procedure] as a
where not exists
(
select 1 
from [final].[mcaid_elig_timevar] as b
where a.id_mcaid = b.id_mcaid
);

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_procedure]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'mcaid_elig_time_var.id_mcaid foreign key check'
,'PASS'
,getdate()
,'All members in mcaid_claim_procedure are in mcaid_elig_time_var';

--Check that procedure codes are properly formed type
WITH CTE AS
(
SELECT
 CASE WHEN LEN([procedure_code]) = 5 AND ISNUMERIC([procedure_code]) = 1 THEN 'CPT Category I'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'F' THEN 'CPT Category II'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'T' THEN 'CPT Category III'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) IN ('M', 'U') THEN 'CPT Other'
      WHEN LEN([procedure_code]) = 5 AND SUBSTRING([procedure_code], 1, 1) LIKE '[A-Z]' AND ISNUMERIC(SUBSTRING([procedure_code], 2, 4)) = 1 THEN 'HCPCS'
      WHEN LEN([procedure_code]) IN (3, 4) AND ISNUMERIC([procedure_code]) = 1 THEN 'ICD-9-PCS'
      WHEN LEN([procedure_code]) = 7 THEN 'ICD-10-PCS'
	  ELSE 'UNKNOWN' END AS [code_system]
,*
FROM [PHClaims].[stage].[mcaid_claim_procedure]
)

SELECT 
DISTINCT [procedure_code]
FROM CTE
WHERE [code_system] = 'UNKNOWN'
ORDER BY [procedure_code];

/*
SELECT 
 [code_system]
,COUNT(*)
FROM CTE
GROUP BY [code_system];
*/
GO

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_procedure]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'Check procedure code digit structure'
,'PASS'
,getdate()
,'There are 4 improper 2-digit codes (13, 14, 60, 72)';

--Check that icdcm_number within ('01','02','03','04','05','06','07','08','09','10','11','12','line')
select count([procedure_code_number])
from [stage].[mcaid_claim_procedure]
where [procedure_code_number] not in 
('01'
,'02'
,'03'
,'04'
,'05'
,'06'
,'07'
,'08'
,'09'
,'10'
,'11'
,'12'
,'line');
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_procedure]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'Check values of [procedure_code_number]'
,'PASS'
,getdate()
,'All values in (01,02,03,04,05,06,07,08,09,10,11,12,line)';


--Count procedure codes that do not join to reference table
select distinct
 [procedure_code]
from [stage].[mcaid_claim_procedure] as a
where not exists
(
select 1
from [ref].[pcode] as b
where a.[procedure_code] = b.[pcode]
);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_procedure]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'procedure_code foreign key check'
,'FAIL'
,getdate()
,'513 procedure codes not in [ref].[pcode]';

/*
--procedure codes that do not join to reference table
select
 CASE WHEN LEN([procedure_code]) = 5 AND ISNUMERIC([procedure_code]) = 1 THEN 'CPT Category I'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'F' THEN 'CPT Category II'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'T' THEN 'CPT Category III'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) IN ('M', 'U') THEN 'CPT Other'
      WHEN LEN([procedure_code]) = 5 AND SUBSTRING([procedure_code], 1, 1) LIKE '[A-Z]' AND ISNUMERIC(SUBSTRING([procedure_code], 2, 4)) = 1 THEN 'HCPCS'
      WHEN LEN([procedure_code]) IN (3, 4) AND ISNUMERIC([procedure_code]) = 1 THEN 'ICD-9-PCS'
      WHEN LEN([procedure_code]) = 7 THEN 'ICD-10-PCS'
	  ELSE 'UNKNOWN' END AS [code_system]
,[procedure_code]
,count(*)
from [stage].[mcaid_claim_procedure] as a
where not exists
(
select 1
from [ref].[pcode] as b
where a.[procedure_code] = b.[pcode]
)
GROUP BY
 CASE WHEN LEN([procedure_code]) = 5 AND ISNUMERIC([procedure_code]) = 1 THEN 'CPT Category I'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'F' THEN 'CPT Category II'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'T' THEN 'CPT Category III'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) IN ('M', 'U') THEN 'CPT Other'
      WHEN LEN([procedure_code]) = 5 AND SUBSTRING([procedure_code], 1, 1) LIKE '[A-Z]' AND ISNUMERIC(SUBSTRING([procedure_code], 2, 4)) = 1 THEN 'HCPCS'
      WHEN LEN([procedure_code]) IN (3, 4) AND ISNUMERIC([procedure_code]) = 1 THEN 'ICD-9-PCS'
      WHEN LEN([procedure_code]) = 7 THEN 'ICD-10-PCS'
	  ELSE 'UNKNOWN' END
,[procedure_code]
ORDER BY
 CASE WHEN LEN([procedure_code]) = 5 AND ISNUMERIC([procedure_code]) = 1 THEN 'CPT Category I'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'F' THEN 'CPT Category II'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'T' THEN 'CPT Category III'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) IN ('M', 'U') THEN 'CPT Other'
      WHEN LEN([procedure_code]) = 5 AND SUBSTRING([procedure_code], 1, 1) LIKE '[A-Z]' AND ISNUMERIC(SUBSTRING([procedure_code], 2, 4)) = 1 THEN 'HCPCS'
      WHEN LEN([procedure_code]) IN (3, 4) AND ISNUMERIC([procedure_code]) = 1 THEN 'ICD-9-PCS'
      WHEN LEN([procedure_code]) = 7 THEN 'ICD-10-PCS'
	  ELSE 'UNKNOWN' END
,[procedure_code];
*/

--Compare number of people with claim_header table
select
 (select count(distinct id_mcaid) as id_dcount
  from [stage].[mcaid_claim_procedure])
,(select count(distinct id_mcaid) as id_dcount
  from [stage].[mcaid_claim_header])
,cast((select count(distinct id_mcaid) as id_dcount
  from [stage].[mcaid_claim_procedure]) as numeric) /
 (select count(distinct id_mcaid) as id_dcount
  from [stage].[mcaid_claim_header]);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_procedure]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'Compare procedure to header (number of people)'
,'PASS'
,getdate()
,'98.3% of people in procedure are in header';

-- Compare number of dx codes in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([from_date]) AS [claim_year]
,COUNT(*) AS [prior_num_procedure]
FROM [PHClaims].[dbo].[mcaid_claim_proc] AS a
INNER JOIN [dbo].[mcaid_claim_header] AS b
ON a.[tcn] = b.[tcn]
GROUP BY YEAR([from_date])
),

[stage] AS
(
SELECT
 YEAR(a.[first_service_date]) AS [claim_year]
,COUNT(*) AS [current_num_procedure]
FROM [stage].[mcaid_claim_procedure] AS a
INNER JOIN [stage].[mcaid_claim_header] AS b
ON a.[claim_header_id] = b.[claim_header_id]
GROUP BY YEAR(a.[first_service_date])
)

SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_num_procedure]
,[current_num_procedure]
,CAST([current_num_procedure] AS NUMERIC) / [prior_num_procedure] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
ORDER BY [claim_year];
GO

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_procedure]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'Compare new-to-prior procedure counts'
,'PASS'
,getdate()
,'Ratio ~1.02 from 2012-17';

SELECT [etl_batch_id]
      ,[last_run]
      ,[table_name]
      ,[qa_item]
      ,[qa_result]
      ,[qa_date]
      ,[note]
FROM [PHClaims].[metadata].[qa_mcaid]
WHERE [table_name] = 'stage.mcaid_claim_procedure';