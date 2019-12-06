
use [PHClaims];
go

delete from [metadata].[qa_mcaid] 
where [table_name] = 'stage.mcaid_claim_procedure';

declare @last_run as datetime;
declare @mcaid_elig_check as varchar(255);
declare @mcaid_elig_demo_check as varchar(255);
declare @procedure_code_len_check as varchar(255);
declare @procedure_code_number_check as varchar(255);
declare @pcode_lookup_check as varchar(255);




declare @pct_claim_header_id_with_dx as varchar(255);
declare @compare_current_prior_min as varchar(255);
declare @compare_current_prior_max as varchar(255);

set @last_run = (select max(last_run) from [stage].[mcaid_claim_procedure]);

--All members should be in [mcaid_elig] table
set @mcaid_elig_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_procedure] as a
where not exists
(
select 1 
from [stage].[mcaid_elig] as b
where a.id_mcaid = b.MEDICAID_RECIPIENT_ID
));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'All members should be in [mcaid_elig] table'
,CASE WHEN @mcaid_elig_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_check + ' members in mcaid_claim_procedure and are not in [mcaid_elig]';

--All members should be in [mcaid_elig_demo] table
set @mcaid_elig_demo_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_procedure] as a
where not exists
(
select 1 
from (SELECT [id_mcaid] FROM [final].[mcaid_elig_demo] UNION SELECT [id_mcaid] FROM [stage].[mcaid_elig_demo]) as b
where a.id_mcaid = b.id_mcaid
));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'All members should be in [mcaid_elig_demo] table'
,CASE WHEN @mcaid_elig_demo_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_demo_check + ' members in mcaid_claim_procedure and are not in [mcaid_elig_demo]';

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
FROM [stage].[mcaid_claim_procedure]
)

SELECT @procedure_code_len_check = COUNT(DISTINCT [procedure_code])
FROM CTE
WHERE [code_system] = 'UNKNOWN';

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'Check procedure code digit structure'
,CASE WHEN @procedure_code_len_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@procedure_code_len_check + ' procedure codes are not defined';

--Check that [procedure_code_number] in ('01','02','03','04','05','06','07','08','09','10','11','12','line')
set @procedure_code_number_check = 
(
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
,'line'));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'Check that procedure_code_number in 01-12 or admit'
,CASE WHEN @procedure_code_number_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@procedure_code_number_check + ' procedure codes have an incorrect procedure code number';

--Check if any procedure codes do not join to the reference table
set @pcode_lookup_check =
(
select count(distinct [procedure_code])
from [stage].[mcaid_claim_procedure] as a
where not exists
(
select 1
from [ref].[pcode] as b
where a.[procedure_code] = b.[pcode]
));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'Check if any procedure codes do not join to the reference table'
,CASE WHEN @pcode_lookup_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@pcode_lookup_check + ' procedure codes are not in [ref].[pcode]';

--Compare number of procedure codes in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [prior_num_pcode]
FROM [final].[mcaid_claim_procedure] AS a
GROUP BY YEAR([first_service_date])
),

[stage] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [current_num_pcode]
FROM [stage].[mcaid_claim_procedure] AS a
GROUP BY YEAR([first_service_date])
),

[compare] AS
(
SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_num_pcode]
,[current_num_pcode]
,CAST([current_num_pcode] AS NUMERIC) / [prior_num_pcode] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
)

SELECT 
 @compare_current_prior_min = MIN([pct_change])
,@compare_current_prior_max = MAX([pct_change])
FROM [compare]
WHERE [claim_year] >= YEAR(GETDATE()) - 3;

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_procedure'
,'Compare current vs. prior analytic tables'
,NULL
,getdate()
,'Min: ' + @compare_current_prior_min + ', Max: ' + @compare_current_prior_max + ' ratio of current to prior rows';

/*
SELECT
 [last_run]
,[table_name]
,[qa_item]
,[qa_result]
,[qa_date]
,[note]
FROM [PHClaims].[metadata].[qa_mcaid]
WHERE [table_name] IN
('stage.mcaid_claim_icdcm_header'
,'stage.mcaid_claim_line'
,'stage.mcaid_claim_pharm'
,'stage.mcaid_claim_procedure'
,'stage.mcaid_claim_header')
ORDER BY 
 [table_name]
,[qa_item];
*/