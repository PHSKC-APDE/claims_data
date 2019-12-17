
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[mcaid_claim_periodic_snapshot]') IS NOT NULL
DROP TABLE [stage].[mcaid_claim_periodic_snapshot];
CREATE TABLE [stage].[mcaid_claim_periodic_snapshot]
([measure_period_id] SMALLINT NOT NULL
,[beg_year_month] INT NOT NULL
,[end_year_month] INT NOT NULL
,[id_mcaid] VARCHAR(255) NOT NULL
,[ed_pophealth_t_12_m] SMALLINT
,[elixhauser_t_12_m] SMALLINT
,[charlson_t_12_m] SMALLINT
,[gagne_t_12_m] SMALLINT
,CONSTRAINT [pk_mcaid_periodic_snapshot] PRIMARY KEY CLUSTERED ([id_mcaid], [end_year_month]));