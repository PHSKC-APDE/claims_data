
/*
This function:
(1)
combines discharges from 
[stage].[v_perf_fuh_inpatient_index_stay] WHERE [value_set_name] = 'Mental Illness'
and [stage].[v_perf_fuh_inpatient_index_stay] WHERE [value_set_name] = 'Mental Health Diagnosis'
and takes the LAST discharge date.
(2) Of the remaining discharges, those with a subsequent admission in
[stage].[v_perf_fuh_inpatient_index_stay_readmit] are identified.

This is done inside this MULTI-STATEMENT SQL function to allow indexes

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
,@measurement_end_date DATE)

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

/*
Acute readmission or direct transfer:
If the discharge is followed by readmission or direct transfer to an acute 
inpatient care setting for a principal mental health diagnosis (Mental Health 
Diagnosis Value Set) within the 30-day follow-up period, count only the last 
discharge. Exclude both the initial discharge and the readmission/direct 
transfer discharge if the last discharge occurs after December 1 of the 
measurement year.
*/
WITH CTE AS
(
SELECT
/*
If a discharge joins to a another discharge within 30 days,
retain claim details for the later discharge.
*/
 COALESCE(b.[value_set_name], a.[value_set_name]) AS [value_set_name]
,COALESCE(b.[id_mcaid], a.[id_mcaid]) AS [id_mcaid]
,COALESCE(b.[age], a.[age]) AS [age]
,COALESCE(b.[claim_header_id], a.[claim_header_id]) AS [claim_header_id]
,COALESCE(b.[admit_date], a.[admit_date]) AS [admit_date]
,COALESCE(b.[discharge_date], a.[discharge_date]) AS [discharge_date]
,COALESCE(b.[first_service_date], a.[first_service_date]) AS [first_service_date]
,COALESCE(b.[last_service_date], a.[last_service_date]) AS [last_service_date]
,COALESCE(b.[flag], a.[flag]) AS [flag]
/*
If a discharge joins to multiple discharges within 30 days,
retain the last claim, ORDER BY b.[discharge_date] DESC.
*/
,ROW_NUMBER() OVER(PARTITION BY a.[claim_header_id] ORDER BY b.[discharge_date] DESC) AS [row_num]

FROM [stage].[v_perf_fuh_inpatient_index_stay] AS a
LEFT JOIN [stage].[v_perf_fuh_inpatient_index_stay] AS b
ON b.[value_set_name] = 'Mental Health Diagnosis'
AND b.[discharge_date] BETWEEN @measurement_start_date AND @measurement_end_date
AND a.[id_mcaid] = b.[id_mcaid]
AND b.[discharge_date] BETWEEN DATEADD(DAY, 1, a.[discharge_date]) AND DATEADD(DAY, 30, a.[discharge_date])

WHERE 1 = 1
AND a.[value_set_name] = 'Mental Illness'
AND a.[discharge_date] BETWEEN @measurement_start_date AND @measurement_end_date

--ORDER BY a.[claim_header_id], b.[discharge_date]
)

INSERT INTO @index_stay
([id_mcaid]
,[age]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[flag])

SELECT
 [value_set_name]
,[id_mcaid]
,[age]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[first_service_date]
,[last_service_date]
,[flag]
FROM CTE 
WHERE [row_num] = 1
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
FROM [stage].[v_perf_fuh_inpatient_index_stay_readmit]
WHERE [admit_date] BETWEEN @measurement_start_date AND @measurement_end_date
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
