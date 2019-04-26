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
Last Modified: 2019-04-25

Returns:
 [id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag], = 1
,[inpatient_within_30_day], = 0 if no subsequent readmission
*/

USE PHClaims;
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
([id] VARCHAR(200) NULL 
,[age] INT NULL
,[tcn] VARCHAR(200) NULL
,[from_date] DATE NULL
,[to_date] DATE NULL
,[flag] INT NULL
,[inpatient_within_30_day] INT NULL)
 AS
BEGIN

DECLARE @index_stay TABLE
([id] VARCHAR(200) NULL 
,[age] INT NULL
,[tcn] VARCHAR(200) NULL
,[from_date] DATE NULL
,[to_date] DATE NULL
,[flag] INT NULL
,INDEX idx_cl_index_stay_id_from_date CLUSTERED([id], [from_date]));

INSERT INTO @index_stay
([id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag])

SELECT 
 [id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
FROM [stage].[fn_perf_fuh_inpatient_index_stay]
(@measurement_start_date, @measurement_end_date, @age, @dx_value_set_name);

DECLARE @readmit TABLE
([id] VARCHAR(200) NULL 
,[tcn] VARCHAR(200) NULL
,[from_date] DATE NULL
,[to_date] DATE NULL
,[flag] INT NULL
,INDEX idx_cl_readmit_id_from_date CLUSTERED([id], [from_date]));

INSERT INTO @readmit
([id]
,[tcn]
,[from_date]
,[to_date]
,[flag])

SELECT 
 [id]
,[tcn]
,[from_date]
,[to_date]
,[flag]
FROM [stage].[fn_perf_fuh_inpatient_index_stay_readmit]
(@measurement_start_date, @measurement_end_date, @exclusion_value_set_name);

INSERT INTO @inpatient_index_stay_exclusion
([id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
,[inpatient_within_30_day])

SELECT 
 a.[id]
,a.[age]
,a.[tcn]
,a.[from_date]
,a.[to_date]
,a.[flag]
,MAX(ISNULL(b.[flag], 0)) AS [inpatient_within_30_day]
FROM @index_stay AS a
LEFT JOIN @readmit AS b
ON a.[id] = b.[id]
AND b.[from_date] BETWEEN a.[to_date] AND DATEADD(DAY, 30, a.[to_date])
GROUP BY 
 a.[id]
,a.[age]
,a.[tcn]
,a.[from_date]
,a.[to_date]
,a.[flag];

RETURN  
END;
GO

/*
SELECT * 
FROM [stage].[fn_perf_fuh_inpatient_index_stay_exclusion]
('2017-01-01', '2017-12-31', 6, 'Mental Illness', 'Mental Health Diagnosis');
*/
