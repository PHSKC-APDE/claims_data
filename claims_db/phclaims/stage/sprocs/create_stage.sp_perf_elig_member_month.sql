
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_perf_elig_member_month]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_elig_member_month];
GO
CREATE PROCEDURE [stage].[sp_perf_elig_member_month]
AS
SET NOCOUNT ON;

BEGIN

-- Create slim table for temporary work
IF OBJECT_ID('tempdb..#temp') IS NOT NULL
DROP TABLE #temp;
SELECT
 [CLNDR_YEAR_MNTH]
,[MEDICAID_RECIPIENT_ID]
,[RPRTBL_RAC_CODE]
,[FROM_DATE]
,[TO_DATE]
,[COVERAGE_TYPE_IND]
,[MC_PRVDR_NAME]
,[DUAL_ELIG]
,[TPL_FULL_FLAG]
INTO #temp
FROM [stage].[mcaid_elig];

CREATE NONCLUSTERED INDEX [idx_nc_#temp] 
ON #temp([MEDICAID_RECIPIENT_ID], [CLNDR_YEAR_MNTH]);

IF OBJECT_ID('[stage].[perf_elig_member_month]', 'U') IS NOT NULL
DROP TABLE [stage].[perf_elig_member_month];

WITH CTE AS
(
SELECT
 [CLNDR_YEAR_MNTH]
,[MEDICAID_RECIPIENT_ID]
,[RPRTBL_RAC_CODE]
,[FROM_DATE]
,[TO_DATE]
,[COVERAGE_TYPE_IND]
,[MC_PRVDR_NAME]
,[DUAL_ELIG]
,[TPL_FULL_FLAG]
,ROW_NUMBER() OVER(PARTITION BY [MEDICAID_RECIPIENT_ID], [CLNDR_YEAR_MNTH] 
                   ORDER BY DATEDIFF(DAY, [FROM_DATE], [TO_DATE]) DESC) AS [row_num]
FROM #temp
)

SELECT
 [CLNDR_YEAR_MNTH]
,[MEDICAID_RECIPIENT_ID]
,[RPRTBL_RAC_CODE]
,[FROM_DATE]
,[TO_DATE]
,[COVERAGE_TYPE_IND]
,[MC_PRVDR_NAME]
,[DUAL_ELIG]
,[TPL_FULL_FLAG]

INTO [stage].[perf_elig_member_month]
FROM CTE
WHERE 1 = 1
AND [row_num] = 1;

ALTER TABLE [stage].[perf_elig_member_month] ALTER COLUMN [CLNDR_YEAR_MNTH] INT NOT NULL;
ALTER TABLE [stage].[perf_elig_member_month] ALTER COLUMN [MEDICAID_RECIPIENT_ID] VARCHAR(200) NOT NULL;
ALTER TABLE [stage].[perf_elig_member_month] ADD CONSTRAINT PK_stage_perf_elig_member_month PRIMARY KEY ([MEDICAID_RECIPIENT_ID], [CLNDR_YEAR_MNTH]);

END
GO