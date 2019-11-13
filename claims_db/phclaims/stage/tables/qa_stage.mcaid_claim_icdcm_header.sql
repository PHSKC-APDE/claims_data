
use [PHClaims];
go

delete from [metadata].[qa_mcaid] 
where [table_name] = 'stage.mcaid_claim_icdcm_header';

declare @last_run as datetime;
declare @mcaid_elig_check as varchar(255);
declare @mcaid_elig_demo_check as varchar(255);
declare @icd9cm_len_check as varchar(255);
declare @icd10cm_len_check as varchar(255);
declare @icdcm_number_check as varchar(255);
declare @dx_lookup_check as varchar(255);
declare @pct_claim_header_id_with_dx as varchar(255);
declare @compare_current_prior_min as varchar(255);
declare @compare_current_prior_max as varchar(255);

set @last_run = (select max(last_run) from [stage].[mcaid_claim_icdcm_header]);

--All members should be in [mcaid_elig] table
set @mcaid_elig_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_icdcm_header] as a
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
,'stage.mcaid_claim_icdcm_header'
,'All members should be in [mcaid_elig] table'
,CASE WHEN @mcaid_elig_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_check + ' members in mcaid_claim_icdcm_header and are not in [mcaid_elig]';

--All members should be in [mcaid_elig_demo] table
set @mcaid_elig_demo_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_icdcm_header] as a
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
,'stage.mcaid_claim_icdcm_header'
,'All members should be in [mcaid_elig_demo] table'
,CASE WHEN @mcaid_elig_demo_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_demo_check + ' members in mcaid_claim_icdcm_header and are not in [mcaid_elig_demo]';

--Check that ICD-9-CM length in (5)
set @icd9cm_len_check =
(
select count(*)
from [stage].[mcaid_claim_icdcm_header]
where icdcm_version = 9
and len([icdcm_norm]) not in (5)
);

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_icdcm_header'
,'Check that ICD-9-CM length in (5)'
,CASE WHEN @icd9cm_len_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@icd9cm_len_check + ' ICD-9-CM codes are not length 5';

--Check that ICD-10-CM length in (3,4,5,6,7)
set @icd10cm_len_check = 
(
select count(*)
from [stage].[mcaid_claim_icdcm_header]
where [icdcm_version] = 10
and len([icdcm_norm]) not in (3,4,5,6,7)
);

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_icdcm_header'
,'Check that ICD-10-CM length in (3,4,5,6,7)'
,CASE WHEN @icd10cm_len_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@icd10cm_len_check + ' ICD-10-CM codes are not in length (3,4,5,6,7)';

--Check that icdcm_number in ('01','02','03','04','05','06','07','08','09','10','11','12','admit')
set @icdcm_number_check = 
(
select count([icdcm_number])
from [stage].[mcaid_claim_icdcm_header]
where [icdcm_number] not in 
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
,'admit')
);

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_icdcm_header'
,'Check that icdcm_number in 01-12 or admit'
,CASE WHEN @icdcm_number_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@icdcm_number_check + ' ICD codes have an incorrect number';

--Check if any diagnosis codes do not join to ICD-CM reference table
set @dx_lookup_check =
(
select count(distinct 'ICD' + CAST([icdcm_version] AS VARCHAR(2)) + ' - ' + [icdcm_norm])
from [stage].[mcaid_claim_icdcm_header] as a
where not exists
(
select 1
from [ref].[dx_lookup] as b
where a.[icdcm_version] = b.[dx_ver] and a.[icdcm_norm] = b.[dx]
));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_icdcm_header'
,'Check if any diagnosis codes do not join to ICD-CM reference table'
,CASE WHEN @dx_lookup_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@dx_lookup_check + ' ICD codes are not in [ref].[dx_lookup]';

--Compare number of people with claim_header table
set @pct_claim_header_id_with_dx = 
(
select
 cast((select count(distinct id_mcaid) as id_dcount
 from [stage].[mcaid_claim_icdcm_header]) as numeric) /
 (select count(distinct id_mcaid) as id_dcount
  from [stage].[mcaid_claim_header])
);

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_icdcm_header'
,'Compare number of people with claim_header table'
,NULL
,getdate()
,@pct_claim_header_id_with_dx + ' proportion of members with a claim header have a dx';

--Compare number of dx codes in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [prior_num_dx]
FROM [final].[mcaid_claim_icdcm_header] AS a
GROUP BY YEAR([first_service_date])
),

[stage] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [current_num_dx]
FROM [stage].[mcaid_claim_icdcm_header] AS a
GROUP BY YEAR([first_service_date])
),

[compare] AS
(
SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_num_dx]
,[current_num_dx]
,CAST([current_num_dx] AS NUMERIC) / [prior_num_dx] AS [pct_change]
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
,'stage.mcaid_claim_icdcm_header'
,'Compare current vs. prior analytic tables'
,NULL
,getdate()
,'Min: ' + @compare_current_prior_min + ', Max: ' + @compare_current_prior_max + ' ratio of current to prior rows';

/*
WITH [final] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [prior_num_dx]
FROM [final].[mcaid_claim_icdcm_header] AS a
GROUP BY YEAR([first_service_date])
),

[stage] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [current_num_dx]
FROM [stage].[mcaid_claim_icdcm_header] AS a
GROUP BY YEAR([first_service_date])
),

[compare] AS
(
SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_num_dx]
,[current_num_dx]
,CAST([current_num_dx] AS NUMERIC) / [prior_num_dx] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
)

SELECT *
FROM [compare];
*/