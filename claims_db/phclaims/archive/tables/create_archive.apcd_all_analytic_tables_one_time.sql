--apcd_claim_ccw
CREATE TABLE phclaims.archive.apcd_claim_ccw(
	[id_apcd] [bigint] NULL,
	[from_date] [date] NULL,
	[to_date] [date] NULL,
	[ccw_code] [tinyint] NULL,
	[ccw_desc] [varchar](200) NULL
) ON [PHClaims_FG2];

insert into PHClaims.archive.apcd_claim_ccw with (tablock)
SELECT [id_apcd]
      ,[from_date]
      ,[to_date]
      ,[ccw_code]
      ,[ccw_desc]
FROM [final].[apcd_claim_ccw];

--apcd_claim_header
CREATE TABLE phclaims.archive.[apcd_claim_header](
	[id_apcd] [bigint] NULL,
	[extract_id] [int] NULL,
	[claim_header_id] [bigint] NULL,
	[submitter_id] [int] NULL,
	[provider_id_apcd] [bigint] NULL,
	[product_code_id] [int] NULL,
	[first_service_dt] [date] NULL,
	[last_service_dt] [date] NULL,
	[first_paid_dt] [date] NULL,
	[last_paid_dt] [date] NULL,
	[charge_amt] [numeric](38, 2) NULL,
	[primary_diagnosis] [varchar](20) NULL,
	[icdcm_version] [int] NULL,
	[header_status] [varchar](2) NULL,
	[claim_type_apcd_id] [varchar](100) NULL,
	[claim_type_id] [tinyint] NULL,
	[type_of_bill_code] [varchar](4) NULL,
	[ipt_flag] [tinyint] NULL,
	[discharge_dt] [date] NULL,
	[ed_flag] [tinyint] NULL,
	[or_flag] [tinyint] NULL
) ON [PHClaims_FG2];

insert into PHClaims.archive.[apcd_claim_header] with (tablock)
SELECT [id_apcd]
      ,[extract_id]
      ,[claim_header_id]
      ,[submitter_id]
      ,[provider_id_apcd]
      ,[product_code_id]
      ,[first_service_dt]
      ,[last_service_dt]
      ,[first_paid_dt]
      ,[last_paid_dt]
      ,[charge_amt]
      ,[primary_diagnosis]
      ,[icdcm_version]
      ,[header_status]
      ,[claim_type_apcd_id]
      ,[claim_type_id]
      ,[type_of_bill_code]
      ,[ipt_flag]
      ,[discharge_dt]
      ,[ed_flag]
      ,[or_flag]
FROM [final].[apcd_claim_header];

--apcd_claim_icdcm_header
CREATE TABLE phclaims.archive.[apcd_claim_icdcm_header](
	[id_apcd] [bigint] NULL,
	[extract_id] [int] NULL,
	[claim_header_id] [bigint] NULL,
	[icdcm_raw] [varchar](200) NULL,
	[icdcm_norm] [varchar](200) NULL,
	[icdcm_version] [tinyint] NULL,
	[icdcm_number] [varchar](200) NULL
) ON [PHClaims_FG2];

insert into PHClaims.archive.[apcd_claim_icdcm_header] with (tablock)
SELECT [id_apcd]
      ,[extract_id]
      ,[claim_header_id]
      ,[icdcm_raw]
      ,[icdcm_norm]
      ,[icdcm_version]
      ,[icdcm_number]
FROM [final].[apcd_claim_icdcm_header];

--apcd_elig_demo
CREATE TABLE phclaims.archive.[apcd_elig_demo](
	[id_apcd] [bigint] NULL,
	[dob] [date] NULL,
	[ninety_only] [tinyint] NULL,
	[gender_female] [tinyint] NULL,
	[gender_male] [tinyint] NULL,
	[gender_me] [varchar](8) NULL,
	[gender_recent] [varchar](8) NULL
) ON [PHClaims_FG2];

insert into PHClaims.archive.[apcd_elig_demo] with (tablock)
SELECT [id_apcd]
      ,[dob]
      ,[ninety_only]
      ,[gender_female]
      ,[gender_male]
      ,[gender_me]
      ,[gender_recent]
FROM [final].[apcd_elig_demo];

--apcd_elig_plr_2017
CREATE TABLE phclaims.archive.[apcd_elig_plr_2017](
	[id_apcd] [bigint] NULL,
	[geo_wa_resident] [tinyint] NULL,
	[overall_mcaid] [tinyint] NULL,
	[overall_mcaid_med] [tinyint] NULL,
	[overall_mcaid_pharm] [tinyint] NULL,
	[performance_11_wa] [tinyint] NULL,
	[performance_7_wa] [tinyint] NULL,
	[performance_11_ach] [tinyint] NULL,
	[performance_7_ach] [tinyint] NULL,
	[geo_zip_code] [varchar](10) NULL,
	[geo_county] [varchar](100) NULL,
	[geo_ach] [varchar](100) NULL,
	[geo_ach_covd] [bigint] NULL,
	[geo_ach_covper] [numeric](4, 1) NULL,
	[age] [numeric](25, 0) NULL,
	[age_grp7] [varchar](11) NULL,
	[gender_me] [varchar](8) NULL,
	[gender_recent] [varchar](8) NULL,
	[gender_female] [tinyint] NULL,
	[gender_male] [tinyint] NULL,
	[med_total_covd] [bigint] NULL,
	[med_total_covper] [numeric](4, 1) NULL,
	[dsrip_full_covd] [bigint] NULL,
	[dsrip_full_covper] [numeric](4, 1) NULL,
	[dual_covd] [bigint] NULL,
	[dual_covper] [numeric](4, 1) NULL,
	[dual] [tinyint] NULL,
	[med_medicaid_covd] [bigint] NULL,
	[rac_covd] [bigint] NULL,
	[med_medicare_covd] [bigint] NULL,
	[med_commercial_covd] [bigint] NULL,
	[med_medicaid_covper] [numeric](4, 1) NULL,
	[med_medicare_covper] [numeric](4, 1) NULL,
	[med_commercial_covper] [numeric](4, 1) NULL,
	[med_total_ccovd_max] [bigint] NULL,
	[med_medicaid_ccovd_max] [bigint] NULL,
	[med_medicare_ccovd_max] [bigint] NULL,
	[med_commercial_ccovd_max] [bigint] NULL,
	[med_total_covgap_max] [bigint] NULL,
	[med_medicaid_covgap_max] [bigint] NULL,
	[med_medicare_covgap_max] [bigint] NULL,
	[med_commercial_covgap_max] [bigint] NULL,
	[pharm_total_covd] [bigint] NULL,
	[pharm_total_covper] [numeric](4, 1) NULL,
	[pharm_medicaid_covd] [bigint] NULL,
	[pharm_medicare_covd] [bigint] NULL,
	[pharm_commercial_covd] [bigint] NULL,
	[pharm_medicaid_covper] [numeric](4, 1) NULL,
	[pharm_medicare_covper] [numeric](4, 1) NULL,
	[pharm_commercial_covper] [numeric](4, 1) NULL,
	[pharm_total_ccovd_max] [bigint] NULL,
	[pharm_medicaid_ccovd_max] [bigint] NULL,
	[pharm_medicare_ccovd_max] [bigint] NULL,
	[pharm_commercial_ccovd_max] [bigint] NULL,
	[pharm_total_covgap_max] [bigint] NULL,
	[pharm_medicaid_covgap_max] [bigint] NULL,
	[pharm_medicare_covgap_max] [bigint] NULL,
	[pharm_commercial_covgap_max] [bigint] NULL
) ON [PHClaims_FG2];

insert into PHClaims.archive.[apcd_elig_plr_2017] with (tablock)
SELECT [id_apcd]
      ,[geo_wa_resident]
      ,[overall_mcaid]
      ,[overall_mcaid_med]
      ,[overall_mcaid_pharm]
      ,[performance_11_wa]
      ,[performance_7_wa]
      ,[performance_11_ach]
      ,[performance_7_ach]
      ,[geo_zip_code]
      ,[geo_county]
      ,[geo_ach]
      ,[geo_ach_covd]
      ,[geo_ach_covper]
      ,[age]
      ,[age_grp7]
      ,[gender_me]
      ,[gender_recent]
      ,[gender_female]
      ,[gender_male]
      ,[med_total_covd]
      ,[med_total_covper]
      ,[dsrip_full_covd]
      ,[dsrip_full_covper]
      ,[dual_covd]
      ,[dual_covper]
      ,[dual]
      ,[med_medicaid_covd]
      ,[rac_covd]
      ,[med_medicare_covd]
      ,[med_commercial_covd]
      ,[med_medicaid_covper]
      ,[med_medicare_covper]
      ,[med_commercial_covper]
      ,[med_total_ccovd_max]
      ,[med_medicaid_ccovd_max]
      ,[med_medicare_ccovd_max]
      ,[med_commercial_ccovd_max]
      ,[med_total_covgap_max]
      ,[med_medicaid_covgap_max]
      ,[med_medicare_covgap_max]
      ,[med_commercial_covgap_max]
      ,[pharm_total_covd]
      ,[pharm_total_covper]
      ,[pharm_medicaid_covd]
      ,[pharm_medicare_covd]
      ,[pharm_commercial_covd]
      ,[pharm_medicaid_covper]
      ,[pharm_medicare_covper]
      ,[pharm_commercial_covper]
      ,[pharm_total_ccovd_max]
      ,[pharm_medicaid_ccovd_max]
      ,[pharm_medicare_ccovd_max]
      ,[pharm_commercial_ccovd_max]
      ,[pharm_total_covgap_max]
      ,[pharm_medicaid_covgap_max]
      ,[pharm_medicare_covgap_max]
      ,[pharm_commercial_covgap_max]
FROM [final].[apcd_elig_plr_2017];

--apcd_elig_timevar
CREATE TABLE phclaims.archive.[apcd_elig_timevar](
	[id_apcd] [bigint] NULL,
	[from_date] [date] NULL,
	[to_date] [date] NULL,
	[contiguous] [tinyint] NULL,
	[med_covgrp] [tinyint] NULL,
	[pharm_covgrp] [tinyint] NULL,
	[med_medicaid] [tinyint] NULL,
	[med_medicare] [tinyint] NULL,
	[med_commercial] [tinyint] NULL,
	[pharm_medicaid] [tinyint] NULL,
	[pharm_medicare] [tinyint] NULL,
	[pharm_commercial] [tinyint] NULL,
	[dual] [tinyint] NULL,
	[rac_code] [varchar](10) NULL,
	[geo_zip_code] [varchar](10) NULL,
	[geo_county_code] [varchar](20) NULL,
	[geo_county] [varchar](100) NULL,
	[geo_ach_code] [varchar](20) NULL,
	[geo_ach] [varchar](100) NULL,
	[cov_time_day] [bigint] NULL
) ON [PHClaims_FG2];

insert into PHClaims.archive.[apcd_elig_timevar] with (tablock)
SELECT [id_apcd]
      ,[from_date]
      ,[to_date]
      ,[contiguous]
      ,[med_covgrp]
      ,[pharm_covgrp]
      ,[med_medicaid]
      ,[med_medicare]
      ,[med_commercial]
      ,[pharm_medicaid]
      ,[pharm_medicare]
      ,[pharm_commercial]
      ,[dual]
      ,[rac_code]
      ,[geo_zip_code]
      ,[geo_county_code]
      ,[geo_county]
      ,[geo_ach_code]
      ,[geo_ach]
      ,[cov_time_day]
FROM [final].[apcd_elig_timevar];
