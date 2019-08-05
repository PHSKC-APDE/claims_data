
--USE [PHClaims];
USE [DCHS_Analytics];
GO

IF OBJECT_ID('[stage].[perf_staging_event_date]') IS NOT NULL
DROP TABLE [stage].[perf_staging_event_date];
CREATE TABLE [stage].[perf_staging_event_date]
([year_month] INT NOT NULL
,[event_date] DATE NOT NULL
,[id_mcaid] VARCHAR(255) NOT NULL
,[measure_id] SMALLINT NOT NULL
,[denominator] INT NOT NULL
,[numerator] INT NOT NULL
,[load_date] DATE NOT NULL
) ON [PRIMARY];
GO

/*
IF OBJECT_ID('[stage].[perf_bho_staging_event_date]') IS NOT NULL
DROP TABLE [stage].[perf_bho_staging_event_date];
CREATE TABLE [stage].[perf_bho_staging_event_date]
([year_month] INT NOT NULL
,[event_date] DATE NOT NULL
,[kcid] INT NOT NULL
,[p1_id] VARCHAR(255) NOT NULL
,[measure_id] SMALLINT NOT NULL
,[denominator] INT NOT NULL
,[numerator] INT NOT NULL
,[load_date] DATE NOT NULL
) ON [PRIMARY];
GO
*/