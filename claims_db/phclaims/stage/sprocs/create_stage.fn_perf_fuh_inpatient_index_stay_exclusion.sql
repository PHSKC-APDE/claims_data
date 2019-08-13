
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

Returns:
 [id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag], = 1
,[inpatient_within_30_day], = 0 if no subsequent readmission
*/

USE [PHClaims];
GO

IF OBJECT_ID('[stage].[fn_perf_fuh_inpatient_index_stay_exclusion]', 'TF') IS NOT NULL
DROP FUNCTION [stage].[fn_perf_fuh_inpatient_index_stay_exclusion];
GO
CREATE FUNCTION [stage].[fn_perf_fuh_inpatient_index_stay_exclusion]
(@measurement_start_date DATE
,@measurement_end_date DATE
,@age INT
,@dx_value_set_name VARCHAR(100)
,@exclusion_value_set_name VARCHAR(100))

RETURNS @inpatient_index_stay_exclusion TABLE
([id_mcaid] VARCHAR(255) NULL 
,[age] INT NULL
,[claim_header_id] BIGINT NULL
,[first_service_date] DATE NULL
,[last_service_date] DATE NULL
,[flag] INT NOT NULL
,[inpatient_within_30_day] INT NULL)
 AS
BEGIN

DECLARE @index_stay TABLE
([id_mcaid] VARCHAR(255) NULL 
,[age] INT NULL
,[claim_header_id] BIGINT NULL
,[first_service_date] DATE NULL
,[last_service_date] DATE NULL
,[flag] INT NOT NULL
,INDEX idx_cl_index_stay_id_mcaid_first_service_date CLUSTERED([id_mcaid], [first_service_date]));

INSERT INTO @index_stay
([id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag])

SELECT 
 [id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
FROM [stage].[fn_perf_fuh_inpatient_index_stay]
(@measurement_start_date, @measurement_end_date, @age, @dx_value_set_name);

DECLARE @readmit TABLE
([id_mcaid] VARCHAR(255) NULL 
,[claim_header_id] BIGINT NULL
,[first_service_date] DATE NULL
,[last_service_date] DATE NULL
,[flag] INT NOT NULL
,INDEX idx_cl_readmit_id_mcaid_first_service_date CLUSTERED([id_mcaid], [first_service_date]));

INSERT INTO @readmit
([id_mcaid]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag])

SELECT 
 [id_mcaid]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
FROM [stage].[fn_perf_fuh_inpatient_index_stay_readmit]
(@measurement_start_date, @measurement_end_date, @exclusion_value_set_name);

INSERT INTO @inpatient_index_stay_exclusion
([id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
,[inpatient_within_30_day])

SELECT 
 a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[first_service_date]
,a.[last_service_date]
,a.[flag]
,MAX(ISNULL(b.[flag], 0)) AS [inpatient_within_30_day]
FROM @index_stay AS a
LEFT JOIN @readmit AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND b.[first_service_date] BETWEEN a.[last_service_date] AND DATEADD(DAY, 30, a.[last_service_date])
GROUP BY 
 a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[first_service_date]
,a.[last_service_date]
,a.[flag];

RETURN  
END;
GO

/*
SELECT * 
FROM [stage].[fn_perf_fuh_inpatient_index_stay_exclusion]
('2017-01-01', '2017-12-31', 6, 'Mental Illness', 'Mental Health Diagnosis');
*/
