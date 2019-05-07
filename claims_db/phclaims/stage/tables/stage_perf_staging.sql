
USE [PHClaims]
GO

IF OBJECT_ID('[stage].[perf_staging]', 'U') IS NOT NULL
DROP TABLE [stage].[perf_staging];
CREATE TABLE [stage].[perf_staging]
([year_month] INT NOT NULL
,[id] VARCHAR(200) NOT NULL
,[measure_id] INT NOT NULL
,[num_denom] CHAR(1) NOT NULL
,[measure_value] INT NOT NULL
,[load_date] DATE NOT NULL);
GO

/*
-- DRAFT VERSION
IF OBJECT_ID('[dbo].[p4p_staging_event_date]', 'U') IS NOT NULL
DROP TABLE [dbo].[p4p_staging_event_date];
CREATE TABLE [dbo].[p4p_staging_event_date]
([year_month] INT NOT NULL
,[event_date] DATE NOT NULL
,[id] VARCHAR(200) NOT NULL
,[measure_id] INT NOT NULL
,[num_denom] CHAR(1) NOT NULL
,[measure_value] INT NOT NULL
,[load_date] DATE NOT NULL);
GO
*/
IF OBJECT_ID('[stage].[perf_staging_event_date]', 'U') IS NOT NULL
DROP TABLE [stage].[perf_staging_event_date];
CREATE TABLE [stage].[perf_staging_event_date]
([year_month] INT NOT NULL
,[event_date] DATE NOT NULL
,[id] VARCHAR(200) NOT NULL
,[measure_id] INT NOT NULL
,[denominator] INT NOT NULL
,[numerator] INT NOT NULL
,[load_date] DATE NOT NULL);
GO

/*
BHO Versions
*/

USE [DCHS_Analytics]
GO
IF OBJECT_ID('[stage].[perf_bho_staging]', 'U') IS NOT NULL
DROP TABLE [stage].[perf_bho_staging];
CREATE TABLE [stage].[perf_bho_staging]
([year_month] INT NOT NULL
,[kcid] INT NOT NULL
,[p1_id] VARCHAR(200) NULL
,[measure_id] INT NOT NULL
,[num_denom] CHAR(1) NOT NULL
,[measure_value] INT NOT NULL
,[load_date] DATE NOT NULL);
GO