
USE [PHClaims]
GO

IF OBJECT_ID('[stage].[mcaid_perf_enroll_provider]') IS NOT NULL
DROP TABLE [stage].[mcaid_perf_enroll_provider];
CREATE TABLE [stage].[mcaid_perf_enroll_provider]
([year_month] INT NOT NULL
,[end_quarter] INT NOT NULL
,[id_mcaid] VARCHAR(200) NOT NULL
,[mco_or_ffs] VARCHAR(3) NULL
,[coverage_months_t_12_m] INT NULL
,[load_date] DATE NOT NULL);
GO

CREATE CLUSTERED INDEX [idx_cl_mcaid_perf_enroll_provider_id_mcaid_year_month] ON [stage].[mcaid_perf_enroll_provider]([id_mcaid], [year_month]);
/*
DROP INDEX [idx_cl_mcaid_perf_enroll_provider_id_mcaid_year_month] ON [stage].[mcaid_perf_enroll_provider];
*/
