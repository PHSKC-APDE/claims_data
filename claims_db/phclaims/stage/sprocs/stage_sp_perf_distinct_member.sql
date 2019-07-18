
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_perf_distinct_member]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_distinct_member];
GO
CREATE PROCEDURE [stage].[sp_perf_distinct_member]
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN

SET @SQL = @SQL + N'
IF OBJECT_ID(''[stage].[perf_distinct_member]'',''U'') IS NOT NULL
DROP TABLE [stage].[perf_distinct_member];
SELECT DISTINCT [id_mcaid]
INTO [stage].[perf_distinct_member]
FROM [stage].[perf_enroll_denom];

CREATE CLUSTERED INDEX [idx_cl_perf_distinct_member_id_mcaid] ON [stage].[perf_distinct_member]([id_mcaid]);'

PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL;
GO

/*
EXEC [stage].[sp_perf_distinct_member];
*/