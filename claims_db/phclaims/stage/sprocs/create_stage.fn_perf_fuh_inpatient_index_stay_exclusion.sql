
/*
This function joins the exclusions in
[stage].[fn_perf_fuh_inpatient_index_stay_readmit]
to the inpatient index stays in
[stage].[fn_perf_fuh_inpatient_index_stay]

This is done inside this function to allow indexes

LOGIC:
[flag] = 1 and [inpatient_within_30_day] = 0
denotes a valid inpatient index stay

Author: Philip Sylling
Created: 2019-04-25
Modified: 2019-08-09 | Point to new [final] analytic tables
Modified: 2019-09-20 | Use admit/discharge dates instead of first/last service dates

Returns:
 [id_mcaid]
,[age]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[flag] = 1
,[inpatient_within_30_day] = 0 if no subsequent readmission
*/

USE [PHClaims];
GO

IF OBJECT_ID('[stage].[fn_perf_fuh_inpatient_index_stay_exclusion]', 'TF') IS NOT NULL
DROP FUNCTION [stage].[fn_perf_fuh_inpatient_index_stay_exclusion];
GO
CREATE FUNCTION [stage].[fn_perf_fuh_inpatient_index_stay_exclusion]
(@measurement_start_date DATE
,@measurement_end_date DATE
,@age INT)

RETURNS @inpatient_index_stay_exclusion TABLE
([id_mcaid] VARCHAR(255) NULL 
,[age] INT NULL
,[claim_header_id] BIGINT NULL
,[admit_date] DATE NULL
,[discharge_date] DATE NULL
,[flag] INT NOT NULL
,[inpatient_within_30_day] INT NULL)
 AS
BEGIN

DECLARE @index_stay TABLE
([id_mcaid] VARCHAR(255) NULL 
,[age] INT NULL
,[claim_header_id] BIGINT NULL
,[admit_date] DATE NULL
,[discharge_date] DATE NULL
,[flag] INT NOT NULL
,INDEX idx_cl_index_stay_id_mcaid_discharge_date CLUSTERED([id_mcaid], [discharge_date]));

INSERT INTO @index_stay
([id_mcaid]
,[age]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[flag])

SELECT 
 a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[admit_date]
--,a.[discharge_date]
/*
Acute readmission or direct transfer:
If the discharge is followed by readmission or direct transfer to an acute 
inpatient care setting for a principal mental health diagnosis (Mental Health 
Diagnosis Value Set) within the 30-day follow-up period, count only the last 
discharge. Exclude both the initial discharge and the readmission/direct 
transfer discharge if the last discharge occurs after December 1 of the 
measurement year.
*/
,ISNULL(MAX(b.[discharge_date]), a.[discharge_date]) AS [discharge_date]
,a.[flag]
--,MAX(b.[admit_date]) AS [next_admit_date]
--,MAX(b.[discharge_date]) AS [next_discharge_date]

FROM [stage].[fn_perf_fuh_inpatient_index_stay]
(@measurement_start_date, @measurement_end_date, @age, 'Mental Illness') AS a
LEFT JOIN [stage].[fn_perf_fuh_inpatient_index_stay]
(@measurement_start_date, @measurement_end_date, @age, 'Mental Health Diagnosis') AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND b.[admit_date] BETWEEN DATEADD(DAY, 1, a.[discharge_date]) AND DATEADD(DAY, 30, a.[discharge_date])
GROUP BY
 a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[admit_date]
,a.[discharge_date]
,a.[flag]
ORDER BY [id_mcaid], [discharge_date];

DECLARE @readmit TABLE
([id_mcaid] VARCHAR(255) NULL 
,[claim_header_id] BIGINT NULL
,[admit_date] DATE NULL
,[discharge_date] DATE NULL
,[acuity] VARCHAR(255) NULL
,[flag] INT NOT NULL
,INDEX idx_cl_readmit_id_mcaid_admit_date CLUSTERED([id_mcaid], [admit_date]));

INSERT INTO @readmit
([id_mcaid]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[acuity]
,[flag])

SELECT 
 [id_mcaid]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[acuity]
,[flag]
FROM [stage].[fn_perf_fuh_inpatient_index_stay_readmit]
(@measurement_start_date, @measurement_end_date)
ORDER BY [id_mcaid], [admit_date];

INSERT INTO @inpatient_index_stay_exclusion
([id_mcaid]
,[age]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[flag]
,[inpatient_within_30_day])

SELECT 
 a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[admit_date]
,a.[discharge_date]
,a.[flag]
,MAX(ISNULL(b.[flag], 0)) AS [inpatient_within_30_day]
FROM @index_stay AS a
LEFT JOIN @readmit AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND b.[admit_date] BETWEEN DATEADD(DAY, 1, a.[discharge_date]) AND DATEADD(DAY, 30, a.[discharge_date])
GROUP BY 
 a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[admit_date]
,a.[discharge_date]
,a.[flag];

RETURN  
END;
GO

/*
SELECT * 
FROM [stage].[fn_perf_fuh_inpatient_index_stay_exclusion]
('2017-01-01', '2017-12-31', 6)
ORDER BY [inpatient_within_30_day];
*/
