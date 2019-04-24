--Code to create final.apcd_elig_plr_DATE table
-- Creates a person level table with coverage stats, geographic information for member residence, and member demographics for requested date range
--Will become obsolete when claims package is extended to APCD data
--Eli Kern (PHSKC-APDE)
--2019-4-23

-------------------
--STEP 1: CHANGE YEAR/DATE suffix in table name as needed
-------------------

IF object_id('PHClaims.final.apcd_elig_plr_2017', 'U') is not null DROP TABLE PHClaims.final.apcd_elig_plr_2017;
CREATE TABLE PHClaims.final.apcd_elig_plr_2017 (
	id_apcd						 bigint,
	geo_wa_resident              tinyint,
	overall_mcaid                tinyint,
	overall_mcaid_med            tinyint,
	overall_mcaid_pharm          tinyint,
	performance_11_wa            tinyint,
	performance_7_wa             tinyint,
	performance_11_ach           tinyint,
	performance_7_ach            tinyint,
	geo_zip_code                 varchar(10),
	geo_county                   varchar(100),
	geo_ach                      varchar(100),
	geo_ach_covd                 bigint,
	geo_ach_covper               numeric(4,1),
	age                          numeric(25),
	age_grp7                     varchar(11),
	gender_me                    varchar(8),
	gender_recent			     varchar(8),
	gender_female                tinyint,
	gender_male                  tinyint,
	med_total_covd               bigint,
	med_total_covper             numeric(4,1),
	dsrip_full_covd              bigint,
	dsrip_full_covper            numeric(4,1),
	dual_covd                    bigint,
	dual_covper                  numeric(4,1),
	dual	                     tinyint,
	med_medicaid_covd            bigint,
	rac_covd                     bigint,
	med_medicare_covd            bigint,
	med_commercial_covd          bigint,
	med_medicaid_covper          numeric(4,1),
	med_medicare_covper          numeric(4,1),
	med_commercial_covper        numeric(4,1),
	med_total_ccovd_max          bigint,
	med_medicaid_ccovd_max       bigint,
	med_medicare_ccovd_max       bigint,
	med_commercial_ccovd_max     bigint,
	med_total_covgap_max         bigint,
	med_medicaid_covgap_max      bigint,
	med_medicare_covgap_max      bigint,
	med_commercial_covgap_max    bigint,
	pharm_total_covd             bigint,
	pharm_total_covper           numeric(4,1),
	pharm_medicaid_covd          bigint,
	pharm_medicare_covd          bigint,
	pharm_commercial_covd        bigint,
	pharm_medicaid_covper        numeric(4,1),
	pharm_medicare_covper        numeric(4,1),
	pharm_commercial_covper      numeric(4,1),
	pharm_total_ccovd_max        bigint,
	pharm_medicaid_ccovd_max     bigint,
	pharm_medicare_ccovd_max     bigint,
	pharm_commercial_ccovd_max   bigint,
	pharm_total_covgap_max       bigint,
	pharm_medicaid_covgap_max    bigint,
	pharm_medicare_covgap_max    bigint,
	pharm_commercial_covgap_max  bigint
);



