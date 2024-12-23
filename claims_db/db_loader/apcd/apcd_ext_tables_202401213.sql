
CREATE EXTERNAL TABLE "claims"."stage_apcd_elig_plr_2023"
  ([id_apcd] BIGINT NULL, 
  [geo_wa] TINYINT NULL, 
  [overall_mcaid] TINYINT NULL, 
  [overall_mcaid_med] TINYINT NULL, 
  [overall_mcaid_pharm] TINYINT NULL, 
  [medical_coverage_6mo] TINYINT NULL, 
  [medical_coverage_7mo] TINYINT NULL, 
  [medical_coverage_11mo] TINYINT NULL, 
  [geo_zip] VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [geo_county] VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [geo_ach] VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [geo_ach_covd] BIGINT NULL, 
  [geo_ach_covper] NUMERIC(4,1) NULL, 
  [age] NUMERIC(25,0) NULL, 
  [age_grp7] VARCHAR(11) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [gender_me] VARCHAR(8) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [gender_recent] VARCHAR(8) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [gender_female] TINYINT NULL, 
  [gender_male] TINYINT NULL, 
  [race_eth_me] VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [race_me] VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [race_eth_recent] VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [race_recent] VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [race_aian] TINYINT NULL, 
  [race_asian] TINYINT NULL, 
  [race_black] TINYINT NULL, 
  [race_latino] TINYINT NULL, 
  [race_nhpi] TINYINT NULL, 
  [race_white] TINYINT NULL, 
  [race_unknown] TINYINT NULL, 
  [med_total_covd] BIGINT NULL, 
  [med_total_covper] NUMERIC(4,1) NULL, 
  [dual_covd] BIGINT NULL, 
  [dual_covper] NUMERIC(4,1) NULL, 
  [dual] TINYINT NULL, 
  [med_medicaid_covd] BIGINT NULL, 
  [med_medicare_covd] BIGINT NULL, 
  [med_commercial_covd] BIGINT NULL, 
  [med_medicaid_covper] NUMERIC(4,1) NULL, 
  [med_medicare_covper] NUMERIC(4,1) NULL, 
  [med_commercial_covper] NUMERIC(4,1) NULL, 
  [pharm_total_covd] BIGINT NULL, 
  [pharm_total_covper] NUMERIC(4,1) NULL, 
  [pharm_medicaid_covd] BIGINT NULL, 
  [pharm_medicare_covd] BIGINT NULL, 
  [pharm_commercial_covd] BIGINT NULL, 
  [pharm_medicaid_covper] NUMERIC(4,1) NULL, 
  [pharm_medicare_covper] NUMERIC(4,1) NULL, 
  [pharm_commercial_covper] NUMERIC(4,1) NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'stage_apcd_elig_plr_2023');
