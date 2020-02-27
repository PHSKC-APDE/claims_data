
USE [PHClaims];
GO

/*
Preliminary Stored Procedures
*/
EXEC [stage].[sp_mcaid_perf_elig_member_month];
GO

EXEC [stage].[sp_mcaid_perf_enroll_denom] @start_date_int = 201601, @end_date_int = 201912;
GO

EXEC [stage].[sp_mcaid_perf_distinct_member];
GO

--EXEC [stage].[sp_perf_enroll_provider] @start_date_int = 201702, @end_date_int = 201906;
--GO

/*
See summary of person-month-level table of enrollment and provider criteria
*/
SELECT 
 [year_month]
,[end_quarter]
,COUNT(*)
FROM [stage].[mcaid_perf_enroll_denom]
GROUP BY [year_month], [end_quarter]
ORDER BY [year_month], [end_quarter];

SELECT 
 [year_month]
,[end_quarter]
,COUNT(*)
FROM [stage].[perf_enroll_provider]
GROUP BY [year_month], [end_quarter]
ORDER BY [year_month], [end_quarter];

/*
See summary of person-month-level table of utilization-based numerators and 
denominators for aggregate-to-month-type measures
*/
SELECT 
 b.[measure_id]
,[measure_name]
,[num_denom]
,[load_date]
,COUNT(*)
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY b.[measure_id], [measure_name], [num_denom], [load_date]
ORDER BY b.[measure_id], [load_date], [num_denom];

SELECT 
 b.[measure_id]
,[measure_name]
,[load_date]
,COUNT(*)
FROM [stage].[perf_staging_event_date] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY b.[measure_id], [measure_name], [load_date]
ORDER BY b.[measure_id], [load_date];

SELECT 
 [year_month]
,b.[measure_id]
,[measure_name]
,[num_denom]
,[load_date]
,COUNT(*)
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
--WHERE [measure_name] = 'All-Cause ED Visits'
--WHERE [measure_name] = 'Acute Hospital Utilization'
--WHERE [measure_name] = 'Mental Health Treatment Penetration'
WHERE [measure_name] = 'SUD Treatment Penetration'
--WHERE [measure_name] = 'SUD Treatment Penetration (Opioid)'
--WHERE [measure_name] = 'Child and Adolescent Access to Primary Care'
GROUP BY [year_month], b.[measure_id], [measure_name], [num_denom], [load_date]
ORDER BY b.[measure_id], [load_date], [num_denom], [year_month];

/*
See summary of person-event-level table of utilization-based numerators and 
denominators for event-date-type measures. These are measures that cannot be 
aggregated to month until the measurement period is known.
*/
SELECT 
 [year_month]
,b.[measure_id]
,[measure_name]
,[load_date]
,COUNT(*)
--FROM [archive].[perf_staging_event_date] AS a
FROM [stage].[perf_staging_event_date] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
--WHERE [measure_name] LIKE 'Follow-up ED visit for Alcohol/Drug Abuse%'
--WHERE [measure_name] LIKE 'Follow-up ED visit for Mental Illness%'
--WHERE [measure_name] LIKE 'Follow-up Hospitalization for Mental Illness%'
--WHERE [measure_name] = 'Plan All-Cause Readmissions (30 days)'
GROUP BY [year_month], b.[measure_id], [measure_name], [load_date]
ORDER BY b.[measure_id], [load_date], [year_month];

EXEC [stage].[sp_perf_staging]
 @start_month_int = 201501
,@end_month_int = 201906
--,@measure_name = 'All-Cause ED Visits';
--,@measure_name = 'Acute Hospital Utilization';
--,@measure_name = 'Follow-up ED visit for Alcohol/Drug Abuse';
--,@measure_name = 'Follow-up ED visit for Mental Illness';
--,@measure_name = 'Follow-up Hospitalization for Mental Illness';
--,@measure_name = 'Mental Health Treatment Penetration';
--,@measure_name = 'SUD Treatment Penetration';
,@measure_name = 'SUD Treatment Penetration (Opioid)';
--,@measure_name = 'Plan All-Cause Readmissions (30 days)';
--,@measure_name = 'Child and Adolescent Access to Primary Care';

/*
See summary of person-measurement-year-level table of numerators and 
denominators.
*/

SELECT 
 b.[measure_id]
,[measure_name]
,[load_date]
,COUNT(*)
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY b.[measure_id], [measure_name], [load_date]
ORDER BY b.[measure_id], [load_date];

SELECT
 [beg_year_month]
,[end_year_month]
--,[age_grp]
,b.[measure_id]
,b.[measure_name]
,SUM([numerator]) AS [numerator]
,SUM([denominator]) AS [denominator]
,CAST(SUM([numerator]) AS NUMERIC) / SUM([denominator])
--,COUNT(*)
,[load_date]
--FROM [archive].[mcaid_perf_measure] AS a
FROM [stage].[mcaid_perf_measure] AS a
LEFT JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE 1 = 1
--AND [end_year_month] = 201806
--AND [measure_name] = 'All-Cause ED Visits'
--AND [measure_name] = 'Acute Hospital Utilization'
--AND [measure_name] LIKE 'Follow-up ED visit for Alcohol/Drug Abuse%'
--AND [measure_name] LIKE 'Follow-up ED visit for Mental Illness%'
--AND [measure_name] LIKE 'Follow-up Hospitalization for Mental Illness%'
--AND [measure_name] = 'Mental Health Treatment Penetration'
--AND [measure_name] = 'SUD Treatment Penetration'
--AND [measure_name] = 'SUD Treatment Penetration (Opioid)'
--AND [measure_name] = 'Plan All-Cause Readmissions (30 days)'
--AND [measure_name] = 'Child and Adolescent Access to Primary Care'
--AND [measure_name] = 'Asthma Medication Ratio'
GROUP BY /*[age_grp], */[beg_year_month], [end_year_month], b.[measure_id], [measure_name], [load_date]
ORDER BY b.[measure_id], [beg_year_month], [end_year_month], /*[age_grp], */[load_date];

-- THIS IS LOOP/CURSOR FOR MULTIPLE MEASUREMENT PERIODS
DECLARE 
 @end_month_int_input AS INT
,@measure_name_input AS VARCHAR(200);

DECLARE perf_measure_cursor CURSOR FAST_FORWARD FOR
SELECT 
 a.[year_month]
,b.[measure_name]
FROM [ref].[perf_year_month] AS a
--CROSS JOIN (VALUES('All-Cause ED Visits')) AS b([measure_name])
--CROSS JOIN (VALUES('Acute Hospital Utilization')) AS b([measure_name])
--CROSS JOIN (VALUES('Follow-up ED visit for Alcohol/Drug Abuse')) AS b([measure_name])
--CROSS JOIN (VALUES('Follow-up Hospitalization for Mental Illness')) AS b([measure_name])
--CROSS JOIN (VALUES('Follow-up ED visit for Mental Illness')) AS b([measure_name])
--CROSS JOIN (VALUES('Mental Health Treatment Penetration')) AS b([measure_name])
--CROSS JOIN (VALUES('SUD Treatment Penetration')) AS b([measure_name])
--CROSS JOIN (VALUES('SUD Treatment Penetration (Opioid)')) AS b([measure_name])
CROSS JOIN (VALUES('Plan All-Cause Readmissions (30 days)')) AS b([measure_name])
--CROSS JOIN (VALUES('Child and Adolescent Access to Primary Care')) AS b([measure_name])
WHERE 1 = 1
AND a.[year] BETWEEN 2018 AND 2018
AND a.[month] IN (3, 6, 9, 12)
ORDER BY [measure_name], [year_month];

SELECT [measure_name]
FROM (VALUES
 ('All-Cause ED Visits')
,('Acute Hospital Utilization')
,('Follow-up ED visit for Alcohol/Drug Abuse')
,('Follow-up Hospitalization for Mental Illness')
,('Follow-up ED visit for Mental Illness')
,('Mental Health Treatment Penetration')
,('SUD Treatment Penetration')
,('SUD Treatment Penetration (Opioid)')
,('Plan All-Cause Readmissions (30 days)')
,('Child and Adolescent Access to Primary Care')) AS a([measure_name]);

OPEN perf_measure_cursor;
FETCH NEXT FROM perf_measure_cursor INTO @end_month_int_input, @measure_name_input;

WHILE @@FETCH_STATUS = 0
BEGIN

EXEC [stage].[sp_perf_measures] 
 @end_month_int = @end_month_int_input
,@measure_name = @measure_name_input;

FETCH NEXT FROM perf_measure_cursor INTO @end_month_int_input, @measure_name_input;
END

CLOSE perf_measure_cursor;
DEALLOCATE perf_measure_cursor;

-- THIS IS FOR MANUAL DELETE
DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
--WHERE b.[measure_name] = 'All-Cause ED Visits'
--WHERE b.[measure_name] = 'Acute Hospital Utilization'
WHERE b.[measure_name] LIKE 'Follow-up ED visit for Alcohol/Drug Abuse' + '%'
AND [year_month] >= 201501
AND [year_month] <= 201712;

-- THIS IS FOR MANUAL CALCULATION (1 MEASUREMENT PERIOD)
EXEC [stage].[sp_perf_measures]
 @end_month_int = 201806
--,@measure_name = 'All-Cause ED Visits';
--,@measure_name = 'Acute Hospital Utilization';
,@measure_name = 'Follow-up ED visit for Alcohol/Drug Abuse';
--,@measure_name = 'Follow-up ED visit for Mental Illness';
--,@measure_name = 'Follow-up Hospitalization for Mental Illness';
--,@measure_name = 'Mental Health Treatment Penetration';
--,@measure_name = 'SUD Treatment Penetration';
--,@measure_name = 'SUD Treatment Penetration (Opioid)';
--,@measure_name = 'Plan All-Cause Readmissions (30 days)';
--,@measure_name = 'Child and Adolescent Access to Primary Care';

/*
There should be no duplicate rows at the measure-person-measurement period-level.
Check if NumRows = 1 below.
*/
SELECT [measure_name]
      ,[NumRows]
	  ,COUNT(*)
FROM
(
SELECT [end_year_month]
      ,[id_mcaid]
	  ,[measure_name]
	  ,COUNT(*) AS NumRows
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY [end_year_month], [id_mcaid], [measure_name]
) AS SubQuery
GROUP BY [measure_name], [NumRows]
ORDER BY [measure_name], [NumRows];

/*
See summary of person-measurement-year-level table of numerators and 
denominators.
*/
SELECT
 [beg_year_month]
,[end_year_month]
--,[age_grp]
,b.[measure_id]
,b.[measure_name]
,SUM([denominator]) AS [denominator]
,SUM([numerator]) AS [numerator]
,CAST(CAST(1000 AS NUMERIC) * SUM([numerator]) / SUM([denominator]) AS NUMERIC(10,1)) AS [rate]
,COUNT(*)
,[load_date]
,c.[mco_or_ffs]
--FROM [archive].[mcaid_perf_measure] AS a
FROM [stage].[mcaid_perf_measure] AS a
LEFT JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
LEFT JOIN [stage].[perf_enroll_provider] AS c
ON a.[id_mcaid] = c.[id_mcaid]
AND a.[end_year_month] = c.[year_month]
AND c.[coverage_months_t_12_m] >= 7
WHERE 1 = 1
AND [end_year_month] = 201806
--AND [measure_name] = 'All-Cause ED Visits'
AND [measure_name] = 'Acute Hospital Utilization'
--AND [measure_name] LIKE 'Follow-up ED visit for Alcohol/Drug Abuse%'
--AND [measure_name] LIKE 'Follow-up ED visit for Mental Illness%'
--AND [measure_name] LIKE 'Follow-up Hospitalization for Mental Illness%'
--AND [measure_name] = 'Mental Health Treatment Penetration'
--AND [measure_name] = 'SUD Treatment Penetration'
--AND [measure_name] = 'SUD Treatment Penetration (Opioid)'
--AND [measure_name] = 'Plan All-Cause Readmissions (30 days)'
--AND [measure_name] = 'Child and Adolescent Access to Primary Care'
--AND [measure_name] = 'Asthma Medication Ratio'
GROUP BY /*[age_grp], */[beg_year_month], [end_year_month], b.[measure_id], [measure_name], [load_date], c.[mco_or_ffs]
--ORDER BY b.[measure_id], [beg_year_month], [end_year_month], /*[age_grp], */[load_date], [mco_or_ffs];
ORDER BY [rate] DESC;

SELECT
 'HealthierHere' AS [ACH]
,'In-house' AS [Source]
,NULL AS [Measure Code Name]
,b.[measure_name] AS [Measure Friendly Name]
,CONVERT(CHAR(10), c.[last_day_month], 101) AS [Twelve Months Ending...]
,[age_grp] AS [Age Group]
,SUM([numerator]) AS [Numerator]
,SUM([denominator]) AS [Denominator]

--FROM [archive].[mcaid_perf_measure] AS a
FROM [stage].[mcaid_perf_measure] AS a
LEFT JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
LEFT JOIN (SELECT DISTINCT [year_month] ,[last_day_month] FROM [ref].[date]) AS c
ON a.[end_year_month] = c.[year_month]

WHERE 1 = 1
--AND [age_grp] IS NOT NULL
--AND [end_year_month] = 201712
--AND [measure_name] = 'All-Cause ED Visits'
--AND [measure_name] = 'Acute Hospital Utilization'
--AND [measure_name] LIKE 'Follow-up ED visit for Alcohol/Drug Abuse%'
--AND [measure_name] LIKE 'Follow-up ED visit for Mental Illness%'
--AND [measure_name] LIKE 'Follow-up Hospitalization for Mental Illness%'
--AND [measure_name] = 'Mental Health Treatment Penetration'
--AND [measure_name] = 'SUD Treatment Penetration'
--AND [measure_name] = 'SUD Treatment Penetration (Opioid)'
--AND [measure_name] = 'Plan All-Cause Readmissions (30 days)'
--AND [measure_name] = 'Child and Adolescent Access to Primary Care'
AND [measure_name] NOT IN ('Asthma Medication Ratio', 'Asthma Medication Ratio (1-year requirement)')
--GROUP BY [age_grp], [beg_year_month], [end_year_month], b.[measure_id], [measure_name], [load_date]
GROUP BY b.[measure_name], [age_grp], c.[last_day_month]
HAVING SUM([numerator]) >= 11 AND SUM([denominator]) >= 11
ORDER BY b.[measure_name], [age_grp], c.[last_day_month];



SELECT
 'HealthierHere' AS [ACH]
,'In-house' AS [Source]
,NULL AS [Measure Code Name]
,b.[measure_name] AS [Measure Friendly Name]
--,CONVERT(CHAR(10), c.[last_day_month], 101) AS [Twelve Months Ending...]
,c.[last_day_month]
,d.[age_grp_0] AS [Age Group]
,SUM([numerator]) AS [Numerator]
,SUM([denominator]) AS [Denominator]

FROM [stage].[mcaid_perf_measure] AS a
LEFT JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
LEFT JOIN (SELECT DISTINCT [year_month], [last_day_month] FROM [ref].[date]) AS c
ON a.[end_year_month] = c.[year_month]
LEFT JOIN [ref].[age_grp] AS d
ON a.[end_month_age] = d.[age]

WHERE 1 = 1
AND [measure_name] NOT IN ('Asthma Medication Ratio', 'Asthma Medication Ratio (1-year requirement)')
GROUP BY b.[measure_name], c.[last_day_month], d.[age_grp_0]
HAVING SUM([numerator]) >= 11 AND SUM([denominator]) >= 11
ORDER BY b.[measure_name], d.[age_grp_0], c.[last_day_month];


SELECT
 'HealthierHere' AS [ACH]
,'In-house' AS [Source]
,e.[hca_measure_name] AS [Measure_Code_Name]
,b.[measure_name] AS [Measure_Friendly_Name]
--,CONVERT(CHAR(10), c.[last_day_month], 101) AS [Twelve Months Ending...]
,c.[last_day_month] AS [Twelve_Months_Ending]
,[age_grp] AS [Age_Group]
,SUM([numerator]) AS [Numerator]
,SUM([denominator]) AS [Denominator]

--FROM [archive].[mcaid_perf_measure] AS a
FROM [stage].[mcaid_perf_measure] AS a
LEFT JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
LEFT JOIN (SELECT DISTINCT [year_month] ,[last_day_month] FROM [ref].[date]) AS c
ON a.[end_year_month] = c.[year_month]
LEFT JOIN (SELECT [measure_name], [hca_measure_name] FROM [ref].[perf_measure_name_xwalk] WHERE [age_group_desc] = 'Overall') AS e
ON b.[measure_name] = e.[measure_name]

WHERE 1 = 1
AND b.[measure_name] NOT IN ('Asthma Medication Ratio', 'Asthma Medication Ratio (1-year requirement)')
GROUP BY e.[hca_measure_name], b.[measure_name], c.[last_day_month], [age_grp]
HAVING SUM([numerator]) >= 11 AND SUM([denominator]) >= 11
ORDER BY b.[measure_name], c.[last_day_month], [age_grp];