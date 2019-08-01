
/*
This procedure join FUA ED visits (denominator events) with their follow-up 
visits (numerator events).

(1) Call [stage].[fn_perf_fua_ed_index_visit_exclusion] to get ED visits 
subject to:
[ed_within_30_day] = 0, no subsequent ED visit within 30 days
[inpatient_within_30_day] = 0, no subsequent inpatient stay within 30 days

(2) Call [stage].[fn_perf_fua_follow_up_visit] to get follow-up visits

(3) Join based on 
[follow-up].[first_service_date] BETWEEN [ED].[last_service_date] AND DATEADD(DAY, 7, [ED].[last_service_date])
or
[follow-up].[first_service_date] BETWEEN [ED].[last_service_date] AND DATEADD(DAY, 30, [ED].[last_service_date])

Author: Philip Sylling
Created: 2019-04-24
Modified: 2019-07-25 | Point to new [final] analytic tables

Returns:
 [year_month]
,[id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[ed_index_visit], flag for ED visit
,[ed_within_30_day]
,[inpatient_within_30_day]
,[need_1_month_coverage]
,[follow_up_7_day], flag for follow-up visit
,[follow_up_30_day], flag for follow-up visit
*/

USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_perf_fua_join_step]', 'P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_fua_join_step];
GO
CREATE PROCEDURE [stage].[sp_perf_fua_join_step]
 @measurement_start_date DATE
,@measurement_end_date DATE
,@age INT
,@dx_value_set_name VARCHAR(100)
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN
SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#index_visits'', ''U'') IS NOT NULL
DROP TABLE #index_visits;
SELECT 
--TOP(100)
 b.year_month
,a.*
--If index visit occurs on 1st of month, then 31-day follow-up period contained within calendar month
,CASE WHEN DAY([last_service_date]) = 1 AND MONTH([last_service_date]) IN (1, 3, 5, 7, 8, 10, 12) THEN 1 ELSE 0 END AS [need_1_month_coverage]

INTO #index_visits
FROM [stage].[fn_perf_fua_ed_index_visit_exclusion](''' 
+ CAST(@measurement_start_date AS VARCHAR(200)) + ''', ''' 
+ CAST(@measurement_end_date AS VARCHAR(200)) + ''', ' 
+ CAST(@age AS VARCHAR(200)) + ', ''' 
+ CAST(@dx_value_set_name AS VARCHAR(200)) + ''') AS a
INNER JOIN [ref].[perf_year_month] AS b
ON a.[first_service_date] BETWEEN b.[beg_month] AND b.[end_month]
WHERE 1 = 1
/* 
ED Visits and Inpatient Stays after the index visit are flagged by 
[stage].[fn_perf_fua_ed_index_visit_exclusion]
EXCLUDE BELOW
If a member has more than one ED visit in a 31-day period, include only the 
first eligible ED visit.
Exclude ED visits followed by admission to an acute or nonacute inpatient care 
setting on the date of the ED visit or within the 30 days after the ED visit 
(31 total days), regardless of principal diagnosis for the admission.
*/
AND [ed_within_30_day] = 0
AND [inpatient_within_30_day] = 0;

CREATE CLUSTERED INDEX [idx_cl_#index_visits_id_mcaid_first_service_date] ON #index_visits([id_mcaid], [first_service_date]);
--SELECT * FROM #index_visits;

IF OBJECT_ID(''tempdb..#AOD_Abuse_and_Dependence_icdcm_norm'') IS NOT NULL
DROP TABLE #AOD_Abuse_and_Dependence_icdcm_norm;
SELECT DISTINCT
--TOP(100)
 [claim_header_id]

INTO #AOD_Abuse_and_Dependence_icdcm_norm

--SELECT COUNT(*)
FROM [final].[mcaid_claim_icdcm_header] AS dx
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
(''AOD Abuse and Dependence'')
AND hed.[code_system] = ''ICD10CM''
AND dx.[icdcm_version] = 10 
-- Principal Diagnosis
AND dx.[icdcm_number] = ''01''
AND dx.[icdcm_norm] = hed.[code]
WHERE [first_service_date] BETWEEN ''' + CAST(@measurement_start_date AS VARCHAR(10)) + ''' AND ''' + CAST(@measurement_end_date AS VARCHAR(10)) + '''

CREATE UNIQUE CLUSTERED INDEX idx_cl_#AOD_Abuse_and_Dependence_icdcm_norm ON #AOD_Abuse_and_Dependence_icdcm_norm([claim_header_id]);

/*
Condition 1:
IET Stand Alone Visits Value Set with a principal diagnosis of AOD abuse or 
dependence (AOD Abuse and Dependence Value Set), with or without a telehealth 
modifier (Telehealth Modifier Value Set).
*/

IF OBJECT_ID(''tempdb..#IET_Stand_Alone_Visits_procedure_code'') IS NOT NULL
DROP TABLE #IET_Stand_Alone_Visits_procedure_code;
SELECT 
--TOP(100)
 [id_mcaid]
,pr.[claim_header_id]
,[first_service_date]
,[last_service_date]

INTO #IET_Stand_Alone_Visits_procedure_code

--SELECT COUNT(*)
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN #AOD_Abuse_and_Dependence_icdcm_norm AS dx
ON pr.[claim_header_id] = dx.[claim_header_id]
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
(''IET Stand Alone Visits'')
AND hed.[code_system] IN (''CPT'', ''HCPCS'')
AND pr.[procedure_code] = hed.[code]
WHERE [first_service_date] BETWEEN ''' + CAST(@measurement_start_date AS VARCHAR(10)) + ''' AND ''' + CAST(@measurement_end_date AS VARCHAR(10)) + '''

IF OBJECT_ID(''tempdb..#IET_Stand_Alone_Visits_rev_code'') IS NOT NULL
DROP TABLE #IET_Stand_Alone_Visits_rev_code;
SELECT 
--TOP(100)
 [id_mcaid]
,ln.[claim_header_id]
,[first_service_date]
,[last_service_date]

INTO #IET_Stand_Alone_Visits_rev_code

--SELECT COUNT(*)
FROM [final].[mcaid_claim_line] AS ln
INNER JOIN #AOD_Abuse_and_Dependence_icdcm_norm AS dx
ON ln.[claim_header_id] = dx.[claim_header_id]
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
(''IET Stand Alone Visits'')
AND hed.[code_system] = ''UBREV''
AND ln.[rev_code] = hed.[code]
WHERE [first_service_date] BETWEEN ''' + CAST(@measurement_start_date AS VARCHAR(10)) + ''' AND ''' + CAST(@measurement_end_date AS VARCHAR(10)) + '''

/*
Condition 2:
IET Visits Group 1 Value Set with IET POS Group 1 Value Set and a principal 
diagnosis of AOD abuse or dependence (AOD Abuse and Dependence Value Set), with
or without a telehealth modifier (Telehealth Modifier Value Set).
*/

IF OBJECT_ID(''tempdb..#IET_Visits_Group_1_procedure_code'') IS NOT NULL
DROP TABLE #IET_Visits_Group_1_procedure_code;
SELECT 
--TOP(100)
 [id_mcaid]
,pr.[claim_header_id]
,[first_service_date]
,[last_service_date]

INTO #IET_Visits_Group_1_procedure_code

--SELECT COUNT(*)
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN #AOD_Abuse_and_Dependence_icdcm_norm AS dx
ON pr.[claim_header_id] = dx.[claim_header_id]
INNER JOIN [archive].[hedis_code_system] AS hed_cpt
ON hed_cpt.[value_set_name] IN 
(''IET Visits Group 1'')
AND hed_cpt.[code_system] = ''CPT''
AND pr.[procedure_code] = hed_cpt.[code]
WHERE [first_service_date] BETWEEN ''' + CAST(@measurement_start_date AS VARCHAR(10)) + ''' AND ''' + CAST(@measurement_end_date AS VARCHAR(10)) + '''

IF OBJECT_ID(''tempdb..#IET_POS_Group_1_place_of_service_code'') IS NOT NULL
DROP TABLE #IET_POS_Group_1_place_of_service_code;
SELECT 
--TOP(100)
 [id_mcaid]
,hd.[claim_header_id]
,[first_service_date]
,[last_service_date]

INTO #IET_POS_Group_1_place_of_service_code

--SELECT COUNT(*)
FROM [final].[mcaid_claim_header] AS hd
INNER JOIN #AOD_Abuse_and_Dependence_icdcm_norm AS dx
ON hd.[claim_header_id] = dx.[claim_header_id]
INNER JOIN [archive].[hedis_code_system] AS hed_pos
ON hed_pos.[value_set_name] IN 
(''IET POS Group 1'')
AND hed_pos.[code_system] = ''POS'' 
AND hd.[place_of_service_code] = hed_pos.[code]
WHERE [first_service_date] BETWEEN ''' + CAST(@measurement_start_date AS VARCHAR(10)) + ''' AND ''' + CAST(@measurement_end_date AS VARCHAR(10)) + '''

/*
Condition 3:
IET Visits Group 2 Value Set with IET POS Group 2 Value Set and a principal 
diagnosis of AOD abuse or dependence (AOD Abuse and Dependence Value Set), with
or without a telehealth modifier (Telehealth Modifier Value Set).
*/

IF OBJECT_ID(''tempdb..#IET_Visits_Group_2_procedure_code'') IS NOT NULL
DROP TABLE #IET_Visits_Group_2_procedure_code;
SELECT 
--TOP(100)
 [id_mcaid]
,pr.[claim_header_id]
,[first_service_date]
,[last_service_date]

INTO #IET_Visits_Group_2_procedure_code

--SELECT COUNT(*)
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN #AOD_Abuse_and_Dependence_icdcm_norm AS dx
ON pr.[claim_header_id] = dx.[claim_header_id]
INNER JOIN [archive].[hedis_code_system] AS hed_cpt
ON hed_cpt.[value_set_name] IN 
(''IET Visits Group 2'')
AND hed_cpt.[code_system] = ''CPT''
AND pr.[procedure_code] = hed_cpt.[code]
WHERE [first_service_date] BETWEEN ''' + CAST(@measurement_start_date AS VARCHAR(10)) + ''' AND ''' + CAST(@measurement_end_date AS VARCHAR(10)) + '''

IF OBJECT_ID(''tempdb..#IET_POS_Group_2_place_of_service_code'') IS NOT NULL
DROP TABLE #IET_POS_Group_2_place_of_service_code;
SELECT 
--TOP(100)
 [id_mcaid]
,hd.[claim_header_id]
,[first_service_date]
,[last_service_date]

INTO #IET_POS_Group_2_place_of_service_code

--SELECT COUNT(*)
FROM [final].[mcaid_claim_header] AS hd
INNER JOIN #AOD_Abuse_and_Dependence_icdcm_norm AS dx
ON hd.[claim_header_id] = dx.[claim_header_id]
INNER JOIN [archive].[hedis_code_system] AS hed_pos
ON hed_pos.[value_set_name] IN 
(''IET POS Group 2'')
AND hed_pos.[code_system] = ''POS'' 
AND hd.[place_of_service_code] = hed_pos.[code]
WHERE [first_service_date] BETWEEN ''' + CAST(@measurement_start_date AS VARCHAR(10)) + ''' AND ''' + CAST(@measurement_end_date AS VARCHAR(10)) + '''

/*
Condition 4:
A telephone visit (Telephone Visits Value Set) with a principal diagnosis of 
AOD abuse or dependence (AOD Abuse and Dependence Value Set). 
*/

IF OBJECT_ID(''tempdb..#Telephone_Visits_procedure_code'') IS NOT NULL
DROP TABLE #Telephone_Visits_procedure_code;
SELECT 
--TOP(100)
 [id_mcaid]
,pr.[claim_header_id]
,[first_service_date]
,[last_service_date]

INTO #Telephone_Visits_procedure_code

--SELECT COUNT(*)
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN #AOD_Abuse_and_Dependence_icdcm_norm AS dx
ON pr.[claim_header_id] = dx.[claim_header_id]
INNER JOIN [archive].[hedis_code_system] AS hed_cpt
ON hed_cpt.[value_set_name] IN 
(''Telephone Visits'')
AND hed_cpt.[code_system] = ''CPT''
AND pr.[procedure_code] = hed_cpt.[code]
WHERE [first_service_date] BETWEEN ''' + CAST(@measurement_start_date AS VARCHAR(10)) + ''' AND ''' + CAST(@measurement_end_date AS VARCHAR(10)) + '''

/*
Condition 5:
An online assessment (Online Assessments Value Set) with a principal diagnosis 
of AOD abuse or dependence (AOD Abuse and Dependence Value Set).
*/

IF OBJECT_ID(''tempdb..#Online_Assessments_procedure_code'') IS NOT NULL
DROP TABLE #Online_Assessments_procedure_code;
SELECT 
--TOP(100)
 [id_mcaid]
,pr.[claim_header_id]
,[first_service_date]
,[last_service_date]

INTO #Online_Assessments_procedure_code

--SELECT COUNT(*)
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN #AOD_Abuse_and_Dependence_icdcm_norm AS dx
ON pr.[claim_header_id] = dx.[claim_header_id]
INNER JOIN [archive].[hedis_code_system] AS hed_cpt
ON hed_cpt.[value_set_name] IN 
(''Online Assessments'')
AND hed_cpt.[code_system] = ''CPT''
AND pr.[procedure_code] = hed_cpt.[code]
WHERE [first_service_date] BETWEEN ''' + CAST(@measurement_start_date AS VARCHAR(10)) + ''' AND ''' + CAST(@measurement_end_date AS VARCHAR(10)) + '''

IF OBJECT_ID(''tempdb..#follow_up_visits'', ''U'') IS NOT NULL
DROP TABLE #follow_up_visits;
CREATE TABLE #follow_up_visits
([id_mcaid] VARCHAR(255)
,[claim_header_id] BIGINT
,[first_service_date] DATE
,[last_service_date] DATE
,[flag] INT);

INSERT INTO #follow_up_visits
-- RETURN SET OF FOLLOW-UP VISITS
SELECT *, 1 AS [flag] FROM #IET_Stand_Alone_Visits_procedure_code

UNION

SELECT *, 1 AS [flag] FROM #IET_Stand_Alone_Visits_rev_code

UNION

(
SELECT *, 1 AS [flag] FROM #IET_Visits_Group_1_procedure_code

INTERSECT

SELECT *, 1 AS [flag] FROM #IET_POS_Group_1_place_of_service_code
)

UNION

(
SELECT *, 1 AS [flag] FROM #IET_Visits_Group_2_procedure_code

INTERSECT

SELECT *, 1 AS [flag] FROM #IET_POS_Group_2_place_of_service_code
)

UNION

SELECT *, 1 AS [flag] FROM #Telephone_Visits_procedure_code

UNION

SELECT *, 1 AS [flag] FROM #Online_Assessments_procedure_code;

CREATE CLUSTERED INDEX [idx_cl_#follow_up_visits_id_mcaid_first_service_date] ON #follow_up_visits([id_mcaid], [first_service_date]);
--SELECT * FROM #follow_up_visits;

/*
Join ED index visits with accompanying follow-up visits
*/
SELECT
 a.[year_month]
,a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[first_service_date]
,a.[last_service_date]
,a.[flag] AS [ed_index_visit]
,a.[ed_within_30_day]
,a.[inpatient_within_30_day]
,a.[need_1_month_coverage]

/* Use aggregation function here because each index visit requires only one follow-up */
,MAX(ISNULL(b.[flag], 0)) AS [follow_up_7_day]
,MAX(ISNULL(c.[flag], 0)) AS [follow_up_30_day]

FROM #index_visits AS a

LEFT JOIN #follow_up_visits AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND b.[first_service_date] BETWEEN a.[last_service_date] AND DATEADD(DAY, 7, a.[last_service_date])

LEFT JOIN #follow_up_visits AS c
ON a.[id_mcaid] = c.[id_mcaid]
AND c.[first_service_date] BETWEEN a.[last_service_date] AND DATEADD(DAY, 30, a.[last_service_date])

GROUP BY
 a.[year_month]
,a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[first_service_date]
,a.[last_service_date]
,a.[flag]
,a.[ed_within_30_day]
,a.[inpatient_within_30_day]
,a.[need_1_month_coverage];'
PRINT @SQL;
END

EXEC sp_executeSQL 
 @statement=@SQL
,@params=N'@measurement_start_date DATE, @measurement_end_date DATE, @age INT, @dx_value_set_name VARCHAR(100)'
,@measurement_start_date=@measurement_start_date, @measurement_end_date=@measurement_end_date, @age=@age, @dx_value_set_name=@dx_value_set_name;
GO

/*
IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
DROP TABLE #temp;
CREATE TABLE #temp
([year_month] INT
,[id_mcaid] VARCHAR(255)
,[age] INT
,[claim_header_id] BIGINT
,[first_service_date] DATE
,[last_service_date] DATE
,[ed_index_visit] INT
,[ed_within_30_day] INT
,[inpatient_within_30_day] INT
,[need_1_month_coverage] INT
,[follow_up_7_day] INT
,[follow_up_30_day] INT);

INSERT INTO #temp
EXEC [stage].[sp_perf_fua_join_step]
 @measurement_start_date='2017-01-01'
,@measurement_end_date='2017-12-31'
,@age=13
,@dx_value_set_name='AOD Abuse and Dependence';

SELECT * FROM #temp;
*/