
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_perf_fuh_join_step]', 'P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_fuh_join_step];
GO
CREATE PROCEDURE [stage].[sp_perf_fuh_join_step]
 @measurement_start_date DATE
,@measurement_end_date DATE
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN
SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#index_stays'') IS NOT NULL
DROP TABLE #index_stays;

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
ON b.[value_set_name] = ''Mental Health Diagnosis''
AND b.[discharge_date] BETWEEN ''' 
+ CAST(@measurement_start_date AS VARCHAR(200)) + ''' AND ''' 
+ CAST(@measurement_end_date AS VARCHAR(200)) + '''
AND a.[id_mcaid] = b.[id_mcaid]
AND b.[discharge_date] BETWEEN DATEADD(DAY, 1, a.[discharge_date]) AND DATEADD(DAY, 30, a.[discharge_date])

WHERE 1 = 1
AND a.[value_set_name] = ''Mental Illness''
AND a.[discharge_date] BETWEEN ''' 
+ CAST(@measurement_start_date AS VARCHAR(200)) + ''' AND ''' 
+ CAST(@measurement_end_date AS VARCHAR(200)) + '''
--ORDER BY a.[claim_header_id], b.[discharge_date]
)

SELECT DISTINCT
 [id_mcaid]
,[age]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[first_service_date]
,[last_service_date]
,[flag]
INTO #index_stays
FROM CTE 
WHERE [row_num] = 1;

CREATE CLUSTERED INDEX idx_cl_#index_stays_id_mcaid_discharge_date ON #index_stays([id_mcaid], [discharge_date]);

IF OBJECT_ID(''tempdb..#readmit'') IS NOT NULL
DROP TABLE #readmit;

SELECT 
 [id_mcaid]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[acuity]
,[flag]

INTO #readmit
FROM [stage].[v_perf_fuh_inpatient_index_stay_readmit]
WHERE [admit_date] BETWEEN ''' 
+ CAST(@measurement_start_date AS VARCHAR(200)) + ''' AND ''' 
+ CAST(@measurement_end_date AS VARCHAR(200)) + ''';

CREATE CLUSTERED INDEX idx_cl_#readmit_id_mcaid_admit_date ON #readmit([id_mcaid], [admit_date]);

IF OBJECT_ID(''tempdb..#inpatient_index_stay_exclusion'') IS NOT NULL
DROP TABLE #inpatient_index_stay_exclusion;

WITH CTE AS
(
SELECT 
 c.[year_month]
,a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[admit_date]
,a.[discharge_date]
,a.[first_service_date]
,a.[last_service_date]
,a.[flag]
/* 
Discharges after the index stay are flagged if
EXCLUDE BELOW
Exclude discharges followed by readmission or direct transfer to a nonacute 
inpatient care setting within the 30-day follow-up period, regardless of 
principal diagnosis for the readmission.
Exclude discharges followed by readmission or direct transfer to an acute 
inpatient care setting within the 30-day follow-up period if the principal 
diagnosis was for non-mental health (any principal diagnosis code other than 
those included in the Mental Health Diagnosis Value Set).
*/
,MAX(ISNULL(b.[flag], 0)) AS [inpatient_within_30_day]

--If index stay occurs on 1st of month, then 31-day follow-up period contained within calendar month
,CASE WHEN DAY(a.[discharge_date]) = 1 AND MONTH(a.[discharge_date]) IN (1, 3, 5, 7, 8, 10, 12) 
      THEN 1 
	  ELSE 0 
 END AS [need_1_month_coverage]

FROM #index_stays AS a
LEFT JOIN #readmit AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND b.[admit_date] BETWEEN DATEADD(DAY, 1, a.[discharge_date]) AND DATEADD(DAY, 30, a.[discharge_date])
LEFT JOIN [ref].[date] AS c
ON a.[discharge_date] = c.[date]
GROUP BY
 c.[year_month]
,a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[admit_date]
,a.[discharge_date]
,a.[first_service_date]
,a.[last_service_date]
,a.[flag]
)

SELECT
 [year_month]
,[id_mcaid]
,[age]
,[claim_header_id]
,[admit_date]
,[discharge_date]
,[first_service_date]
,[last_service_date]
,[flag]
,[inpatient_within_30_day]
,[need_1_month_coverage]

INTO #inpatient_index_stay_exclusion
FROM CTE
WHERE 1 = 1
AND [inpatient_within_30_day] = 0;

IF OBJECT_ID(''tempdb..#follow_up_visits'') IS NOT NULL
DROP TABLE #follow_up_visits;

SELECT
 [id_mcaid]
,[claim_header_id]
,[service_date]
,[flag]
,[only_30_day_fu]
INTO #follow_up_visits
FROM [stage].[v_perf_fuh_follow_up_visit]
WHERE [service_date] BETWEEN ''' 
+ CAST(@measurement_start_date AS VARCHAR(200)) + ''' AND ''' 
+ CAST(@measurement_end_date AS VARCHAR(200)) + ''';

CREATE CLUSTERED INDEX [idx_cl_#follow_up_visits_id_mcaid_service_date] 
ON #follow_up_visits([id_mcaid], [service_date]);

/*
Join inpatient index stays with accompanying follow-up visits
*/
SELECT
 a.[year_month]
,a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[admit_date]
,a.[discharge_date]
,a.[first_service_date]
,a.[last_service_date]
,a.[flag] AS [inpatient_index_stay]
,a.[inpatient_within_30_day]
,a.[need_1_month_coverage]

/* Use aggregation function here because each index stay requires only one follow-up */
,MAX(ISNULL(b.[flag], 0)) AS [follow_up_7_day]
,MAX(ISNULL(c.[flag], 0)) AS [follow_up_30_day]

FROM #inpatient_index_stay_exclusion AS a

/*
THERE IS ONE IDIOSYNCRASY
Transitional care management services (TCM 14 Day Value Set), with or without a
telehealth modifier (Telehealth Modifier Value Set) meets criteria for only the
30-Day Follow-Up indicator.
*/
LEFT JOIN (SELECT * FROM #follow_up_visits WHERE [only_30_day_fu] = ''N'') AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND b.[service_date] BETWEEN DATEADD(DAY, 1, a.[discharge_date]) AND DATEADD(DAY, 7, a.[discharge_date])

LEFT JOIN #follow_up_visits AS c
ON a.[id_mcaid] = c.[id_mcaid]
AND c.[service_date] BETWEEN DATEADD(DAY, 1, a.[discharge_date]) AND DATEADD(DAY, 30, a.[discharge_date])

GROUP BY
 a.[year_month]
,a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[admit_date]
,a.[discharge_date]
,a.[first_service_date]
,a.[last_service_date]
,a.[flag]
,a.[inpatient_within_30_day]
,a.[need_1_month_coverage];'
PRINT @SQL;
END

EXEC sp_executeSQL 
 @statement=@SQL
,@params=N'@measurement_start_date DATE, @measurement_end_date DATE'
,@measurement_start_date=@measurement_start_date
,@measurement_end_date=@measurement_end_date;
GO

/*
IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
DROP TABLE #temp;
CREATE TABLE #temp
([year_month] INT
,[id_mcaid] VARCHAR(255)
,[age] INT
,[claim_header_id] BIGINT
,[admit_date] DATE
,[discharge_date] DATE
,[first_service_date] DATE
,[last_service_date] DATE
,[inpatient_index_stay] INT
,[inpatient_within_30_day] INT
,[need_1_month_coverage] INT
,[follow_up_7_day] INT
,[follow_up_30_day] INT);

INSERT INTO #temp
EXEC [stage].[sp_perf_fuh_join_step]
 @measurement_start_date='2017-01-01'
,@measurement_end_date='2017-12-31';

SELECT
 [inpatient_index_stay]
,[inpatient_within_30_day]
,[follow_up_7_day]
,[follow_up_30_day]
,COUNT(*)
FROM #temp
GROUP BY
 [inpatient_index_stay]
,[inpatient_within_30_day]
,[follow_up_7_day]
,[follow_up_30_day];
*/