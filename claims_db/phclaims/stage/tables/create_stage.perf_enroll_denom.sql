
USE [PHClaims]
GO

IF OBJECT_ID('[stage].[perf_enroll_denom]') IS NOT NULL
DROP TABLE [stage].[perf_enroll_denom];
CREATE TABLE [stage].[perf_enroll_denom]
([year_month] INT NOT NULL
,[end_quarter] INT NOT NULL
,[id_mcaid] VARCHAR(255) NULL
,[dob] DATE NULL
,[end_month_age] INT NULL
,[age_in_months] INT NULL
,[last_zip_code] VARCHAR(255) NULL
,[enrolled_any] INT NOT NULL
,[enrolled_any_t_12_m] INT NULL
,[full_benefit] INT NOT NULL
,[full_benefit_t_12_m] INT NULL
,[dual] INT NOT NULL
,[dual_t_12_m] INT NULL
,[tpl] INT NOT NULL
,[tpl_t_12_m] INT NULL
,[hospice] INT NOT NULL
,[hospice_t_12_m] INT NULL
,[hospice_prior_t_12_m] INT NULL
,[hospice_p_2_m] INT NULL
,[full_criteria] INT NOT NULL
,[full_criteria_t_12_m] INT NULL
,[full_criteria_prior_t_12_m] INT NULL
,[full_criteria_p_2_m] INT NULL
,[load_date] DATE NOT NULL);
GO

CREATE CLUSTERED INDEX [idx_cl_perf_enroll_denom_id_mcaid_year_month] ON [stage].[perf_enroll_denom]([id_mcaid], [year_month]);
CREATE NONCLUSTERED INDEX [idx_nc_perf_enroll_denom_end_month_age] ON [stage].[perf_enroll_denom]([end_month_age]);
CREATE NONCLUSTERED INDEX [idx_nc_perf_enroll_denom_age_in_months] ON [stage].[perf_enroll_denom]([age_in_months]);
/*
DROP INDEX [idx_nc_perf_enroll_denom_age_in_months] ON [stage].[perf_enroll_denom];
DROP INDEX [idx_nc_perf_enroll_denom_end_month_age] ON [stage].[perf_enroll_denom];
DROP INDEX [idx_cl_perf_enroll_denom_id_mcaid_year_month] ON [stage].[perf_enroll_denom];
*/