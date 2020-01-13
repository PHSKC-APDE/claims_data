
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_mcaid_perf_distinct_member]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_mcaid_perf_distinct_member];
GO
CREATE PROCEDURE [stage].[sp_mcaid_perf_distinct_member]
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN

SET @SQL = @SQL + N'
IF OBJECT_ID(''[stage].[mcaid_perf_distinct_member]'',''U'') IS NOT NULL
DROP TABLE [stage].[mcaid_perf_distinct_member];
SELECT DISTINCT
 [id_mcaid]
,CAST(GETDATE() AS DATE) AS [load_date]
INTO [stage].[mcaid_perf_distinct_member]
FROM [stage].[mcaid_perf_enroll_denom];

CREATE CLUSTERED INDEX [idx_cl_mcaid_perf_distinct_member_id_mcaid] ON [stage].[mcaid_perf_distinct_member]([id_mcaid]);'

PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL;
GO

/*
EXEC [stage].[sp_mcaid_perf_distinct_member];
*/