
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[fn_perf_enroll_member_month]', 'IF') IS NOT NULL
DROP FUNCTION [stage].[fn_perf_enroll_member_month];
GO
CREATE FUNCTION [stage].[fn_perf_enroll_member_month]
(@start_date_int INT = 201701
,@end_date_int INT = 201712)
RETURNS TABLE 
AS
RETURN
/*
1. Create Age at beginning of month and end of month. This would correspond to age
at Beginning of Measurement Year or End of Measurement Year (typical)
2. Create enrollment gaps as ZERO rows by the following join
[dbo].[mcaid_elig_demoever] CROSS JOIN [ref].[perf_year_month] LEFT JOIN [stage].[perf_elig_member_month]
The ZERO rows are used to track changing enrollment threshold over time.
*/

SELECT 
 b.[year_month]
,b.[month]
,b.[beg_month]
,b.[end_month]
,a.[id]
,a.[dobnew] AS [dob]

,DATEDIFF(YEAR, a.[dobnew], b.[beg_month]) - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, a.[dobnew], b.[beg_month]), a.[dobnew]) > b.[beg_month] THEN 1 ELSE 0 END AS [beg_month_age]
,DATEDIFF(YEAR, a.[dobnew], b.[end_month]) - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, a.[dobnew], b.[end_month]), a.[dobnew]) > b.[end_month] THEN 1 ELSE 0 END AS [end_month_age]
,DATEDIFF(MONTH, a.[dobnew], b.[end_month]) - CASE WHEN DATEADD(MONTH, DATEDIFF(MONTH, a.[dobnew], b.[end_month]), a.[dobnew]) > b.[end_month] THEN 1 ELSE 0 END AS [age_in_months]

,CASE WHEN c.[MEDICAID_RECIPIENT_ID] IS NOT NULL THEN 1 ELSE 0 END AS [enrolled_any]
,CASE WHEN d.[full_benefit_flag] = 'Y' THEN 1 ELSE 0 END AS [full_benefit]
,CASE WHEN c.[DUAL_ELIG] = 'Y' THEN 1 ELSE 0 END AS [dual]
,ISNULL(e.[hospice_flag], 0) AS [hospice]
,CASE WHEN c.[MEDICAID_RECIPIENT_ID] IS NOT NULL AND d.[full_benefit_flag] = 'Y' AND c.[DUAL_ELIG] = 'N' THEN 1 ELSE 0 END AS [full_criteria]
,b.[row_num]

FROM [dbo].[mcaid_elig_demoever] AS a

CROSS JOIN 
(
SELECT *, ROW_NUMBER() OVER(ORDER BY [year_month]) AS [row_num]
FROM [ref].[perf_year_month]
WHERE [year_month] BETWEEN @start_date_int AND @end_date_int
--WHERE [year_month] BETWEEN 201701 AND 201712
) AS b

LEFT JOIN [stage].[perf_elig_member_month] AS c
ON a.[id] = c.[MEDICAID_RECIPIENT_ID]
AND b.[year_month] = c.[CLNDR_YEAR_MNTH]

LEFT JOIN [ref].[mcaid_rac_code] AS d
ON c.[RAC_CODE] = d.[rac_code]

LEFT JOIN [stage].[v_perf_hospice_member_month] AS e
ON a.[id] = e.[id]
AND b.[year_month] = e.[year_month];
GO

/*
IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
DROP TABLE #temp;
SELECT *
INTO #temp
FROM [stage].[fn_perf_enroll_member_month](201701, 201712);

SELECT TOP 100 *
FROM #temp;
*/