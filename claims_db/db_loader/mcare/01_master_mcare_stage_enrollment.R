#' @title Medicare Stage Table Creation
#' 
#' @description Code for creating stage enrollment tables for Medicare FFS data
#' 
#' @details Creates stg tables for medicare enrollment, names, and ssns from
#' other tables as part of the Medicare extraction process.
#' 

# ---- SETUP: Load packages ----
pacman::p_load(DBI, dplyr, ggplot2, glue, lubridate, odbc, stringr)
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")


# ---- STEP 1: Create state_mcare_bene_enrollment table from MBSF tables ----
# Columns selected based on review of Danny's original 2019 scripts used to create elig_timevar and elig_demo tables
# Reference: https://kc1.sharepoint.com/:x:/r/teams/DPH-KCCross-SectorData/Shared%20Documents/General/References/Medicare/ResDAC%20file%20layouts/apde_mbsf_columns_needed_2024.xlsx?d=w4d88d662b43a4097811423fc7813313b&csf=1&web=1&e=1CBks8


dw_inthealth <- create_db_connection("inthealth", interactive = FALSE, prod = TRUE)

bene_enrollment_sql <- glue::glue_sql(
  "if object_id(N'stg_claims.mcare_bene_enrollment', N'U') is not null drop table stg_claims.mcare_bene_enrollment;
  select
  [etl_batch_id]
  ,cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id
  ,[bene_enrollmt_ref_yr]
  ,[zip_cd]
  ,CAST([bene_birth_dt] AS DATE) AS bene_birth_dt
  ,CAST([bene_death_dt] AS DATE) AS bene_death_dt
  ,[sex_ident_cd]
  ,[bene_race_cd]
  ,[rti_race_cd]
  ,[mdcr_entlmt_buyin_ind_01]
  ,[mdcr_entlmt_buyin_ind_02]
  ,[mdcr_entlmt_buyin_ind_03]
  ,[mdcr_entlmt_buyin_ind_04]
  ,[mdcr_entlmt_buyin_ind_05]
  ,[mdcr_entlmt_buyin_ind_06]
  ,[mdcr_entlmt_buyin_ind_07]
  ,[mdcr_entlmt_buyin_ind_08]
  ,[mdcr_entlmt_buyin_ind_09]
  ,[mdcr_entlmt_buyin_ind_10]
  ,[mdcr_entlmt_buyin_ind_11]
  ,[mdcr_entlmt_buyin_ind_12]
  ,[hmo_ind_01]
  ,[hmo_ind_02]
  ,[hmo_ind_03]
  ,[hmo_ind_04]
  ,[hmo_ind_05]
  ,[hmo_ind_06]
  ,[hmo_ind_07]
  ,[hmo_ind_08]
  ,[hmo_ind_09]
  ,[hmo_ind_10]
  ,[hmo_ind_11]
  ,[hmo_ind_12]
  ,[ptd_cntrct_id_01]
  ,[ptd_cntrct_id_02]
  ,[ptd_cntrct_id_03]
  ,[ptd_cntrct_id_04]
  ,[ptd_cntrct_id_05]
  ,[ptd_cntrct_id_06]
  ,[ptd_cntrct_id_07]
  ,[ptd_cntrct_id_08]
  ,[ptd_cntrct_id_09]
  ,[ptd_cntrct_id_10]
  ,[ptd_cntrct_id_11]
  ,[ptd_cntrct_id_12]
  ,[dual_stus_cd_01]
  ,[dual_stus_cd_02]
  ,[dual_stus_cd_03]
  ,[dual_stus_cd_04]
  ,[dual_stus_cd_05]
  ,[dual_stus_cd_06]
  ,[dual_stus_cd_07]
  ,[dual_stus_cd_08]
  ,[dual_stus_cd_09]
  ,[dual_stus_cd_10]
  ,[dual_stus_cd_11]
  ,[dual_stus_cd_12]
  into stg_claims.mcare_bene_enrollment
  from stg_claims.mcare_mbsf_abcd_summary
-- Union to 2014 data
-- Join Part ABC and Part D data for 2014
-- QA below confirms left join is appropriate because no one has Part D but not Part AB
-- Union command will drop any duplicate rows within years
  UNION SELECT
  a.[etl_batch_id]
  ,cast(trim(a.bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id
  ,a.[bene_enrollmt_ref_yr]
  ,a.[bene_zip_cd]
  ,CAST(a.[bene_birth_dt] AS DATE) AS bene_birth_dtbene_birth_dt
  ,CAST(a.[bene_death_dt] AS DATE) AS bene_death_dt
  ,a.[bene_sex_ident_cd]
  ,a.[bene_race_cd]
  ,a.[rti_race_cd]
  ,a.[bene_mdcr_entlmt_buyin_ind_01]
  ,a.[bene_mdcr_entlmt_buyin_ind_02]
  ,a.[bene_mdcr_entlmt_buyin_ind_03]
  ,a.[bene_mdcr_entlmt_buyin_ind_04]
  ,a.[bene_mdcr_entlmt_buyin_ind_05]
  ,a.[bene_mdcr_entlmt_buyin_ind_06]
  ,a.[bene_mdcr_entlmt_buyin_ind_07]
  ,a.[bene_mdcr_entlmt_buyin_ind_08]
  ,a.[bene_mdcr_entlmt_buyin_ind_09]
  ,a.[bene_mdcr_entlmt_buyin_ind_10]
  ,a.[bene_mdcr_entlmt_buyin_ind_11]
  ,a.[bene_mdcr_entlmt_buyin_ind_12]
  ,a.[bene_hmo_ind_01]
  ,a.[bene_hmo_ind_02]
  ,a.[bene_hmo_ind_03]
  ,a.[bene_hmo_ind_04]
  ,a.[bene_hmo_ind_05]
  ,a.[bene_hmo_ind_06]
  ,a.[bene_hmo_ind_07]
  ,a.[bene_hmo_ind_08]
  ,a.[bene_hmo_ind_09]
  ,a.[bene_hmo_ind_10]
  ,a.[bene_hmo_ind_11]
  ,a.[bene_hmo_ind_12]
  ,b.[ptd_cntrct_id_01]
  ,b.[ptd_cntrct_id_02]
  ,b.[ptd_cntrct_id_03]
  ,b.[ptd_cntrct_id_04]
  ,b.[ptd_cntrct_id_05]
  ,b.[ptd_cntrct_id_06]
  ,b.[ptd_cntrct_id_07]
  ,b.[ptd_cntrct_id_08]
  ,b.[ptd_cntrct_id_09]
  ,b.[ptd_cntrct_id_10]
  ,b.[ptd_cntrct_id_11]
  ,b.[ptd_cntrct_id_12]
  ,b.[dual_stus_cd_01]
  ,b.[dual_stus_cd_02]
  ,b.[dual_stus_cd_03]
  ,b.[dual_stus_cd_04]
  ,b.[dual_stus_cd_05]
  ,b.[dual_stus_cd_06]
  ,b.[dual_stus_cd_07]
  ,b.[dual_stus_cd_08]
  ,b.[dual_stus_cd_09]
  ,b.[dual_stus_cd_10]
  ,b.[dual_stus_cd_11]
  ,b.[dual_stus_cd_12]
  from stg_claims.mcare_mbsf_ab_summary as a
  left join stg_claims.mcare_mbsf_d_cmpnts as b 
  on a.bene_id = b.bene_id;",
.con = dw_inthealth)
odbc::dbSendQuery(conn = dw_inthealth, bene_enrollment_sql)

# Confirm that no one has Part D coverage but not Part AB coverage in 2014
# Confirm no bene_id in mbsf_d table that doesn't exist in mbsf_ab table
# PASS condition: Expect 0
bene_check_sql <- glue::glue_sql("
  select count(a.bene_id) as row_count
  from stg_claims.mcare_mbsf_d_cmpnts as a
  left join stg_claims.mcare_mbsf_ab_summary as b
  on a.bene_id = b.bene_id
  where b.bene_id is null;",
.con = dw_inthealth)
odbc::dbGetQuery(conn = dw_inthealth, bene_check_sql)


# ---- STEP 2: Create NAMES and SSN tables for person linkage ---- 
# Note that HIC (alternate unique ID table is not used for linkage and thus
# not loaded here)

bene_names_sql <- glue::glue_sql("
  if object_id(N'stg_claims.mcare_bene_names', N'U') is not null drop table stg_claims.mcare_bene_names;
  select distinct
  cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id,
  bene_srnm_name,
  bene_gvn_name,
  bene_mdl_name
  into stg_claims.mcare_bene_names
  from stg_claims.mcare_edb_user_view;",
.con = dw_inthealth)
odbc::dbSendQuery(conn = dw_inthealth, bene_names_sql)

bene_ssn_sql <- glue::glue_sql("
  if object_id(N'stg_claims.mcare_bene_ssn', N'U') is not null drop table stg_claims.mcare_bene_ssn;
  select distinct
  cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id,
  ssn
  into stg_claims.mcare_bene_ssn
  from stg_claims.mcare_bene_ssn_xwalk;",
.con = dw_inthealth)
odbc::dbSendQuery(conn = dw_inthealth, bene_ssn_sql)


# --- QA Step 2: ---
# Ensure case-sensitivity is set for bene_id variable (expect CS trimmed varchar count > CI trimmed varchar)
bene_names_casetest_sql <- glue::glue_sql("
with temp1 as (select distinct
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id_cs,
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS as bene_id_ci,
lower(trim(bene_id)) as bene_id_lowercase
from stg_claims.mcare_bene_names
)

select 'case-sensitive trimmed varchar' as column_type, count(distinct bene_id_cs) as bene_id_dcount from temp1
union select 'case-insensitive trimmed varchar' as column_type, count(distinct bene_id_ci) as bene_id_dcount from temp1
union select 'lowercase trimmed varchar' as column_type,count(distinct bene_id_lowercase) as bene_id_dcount from temp1
order by bene_id_dcount desc;",
.con = dw_inthealth)
bene_names_casetest <- odbc::dbGetQuery(conn = dw_inthealth, bene_names_casetest_sql)
print(bene_names_casetest)

bene_ssn_casetest_sql <- glue::glue_sql("
with temp1 as (select distinct
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id_cs,
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS as bene_id_ci,
lower(trim(bene_id)) as bene_id_lowercase
from stg_claims.mcare_bene_ssn
)

select 'case-sensitive trimmed varchar' as column_type, count(distinct bene_id_cs) as bene_id_dcount from temp1
union select 'case-insensitive trimmed varchar' as column_type, count(distinct bene_id_ci) as bene_id_dcount from temp1
union select 'lowercase trimmed varchar' as column_type,count(distinct bene_id_lowercase) as bene_id_dcount from temp1
order by bene_id_dcount desc;",
.con = dw_inthealth)
bene_ssn_casetest <- odbc::dbGetQuery(conn = dw_inthealth, bene_ssn_casetest_sql)
print(bene_ssn_casetest)

# Ensure there are very few bene_id values in the [claims].[stage_mcare_bene_enrollment]
# table that do not match to the [claims].[stage_mcare_bene_names] or [claims].[stage_mcare_bene_ssn] tables
bene_id_test_names_sql <- glue::glue_sql("
SELECT  a.bene_id
FROM stg_claims.mcare_bene_enrollment a
LEFT JOIN stg_claims.mcare_bene_names b
  ON a.bene_id = b.bene_id
WHERE b.bene_id IS NULL   
",
.con = dw_inthealth)
bene_id_test_names <- odbc::dbGetQuery(conn = dw_inthealth, bene_id_test_names_sql)
nrow(bene_id_test_names)

bene_id_test_ssn_sql <- glue::glue_sql("
SELECT  a.bene_id
FROM stg_claims.mcare_bene_enrollment a
LEFT JOIN stg_claims.mcare_bene_ssn b
  ON a.bene_id = b.bene_id
WHERE b.bene_id IS NULL                                         
",
.con = dw_inthealth)
bene_id_test_ssn <- odbc::dbGetQuery(conn = dw_inthealth, bene_id_test_ssn_sql)
nrow(bene_id_test_ssn)

bene_id_test_both_sql <- glue::glue_sql("
SELECT a.bene_id
FROM stg_claims.mcare_bene_enrollment a
  LEFT JOIN (
    SELECT DISTINCT bene_id FROM stg_claims.mcare_bene_names
	UNION
	SELECT DISTINCT bene_id FROM stg_claims.mcare_bene_ssn
	) b ON a.bene_id = b.bene_id
	WHERE b.bene_id IS NULL                                   
",
.con = dw_inthealth)
bene_id_test_both <- odbc::dbGetQuery(conn = dw_inthealth, bene_id_test_both_sql)
nrow(bene_id_test_both)

# Ensure that the date of birth variable in the [claims].[stage_mcare_bene_enrollment] 
# is no longer garbled for 2020 and 2021 data (this could just be a one-time check,
# although it would be prudent for us to have QA here that aims to find issues
# with any date variables. For example, you would never want to see that dates 
# of birth are mostly clustered in a given year/month for a given enrollment_year,
# as this would indicate that something went wrong with how these dates were formatted/parsed along the way.
date_check_sql <- glue::glue_sql("
SELECT bene_enrollmt_ref_yr AS year, bene_id, CAST(bene_birth_dt AS DATE) AS bene_birth_dt, CAST(bene_death_dt AS DATE) AS bene_death_dt
FROM stg_claims.mcare_bene_enrollment
",
.con = dw_inthealth)
date_check <- odbc::dbGetQuery(conn = dw_inthealth, date_check_sql)

# Assert dates are length 10
invalid_date_length <- date_check[str_count(date_check$bene_birth_dt) != 10,]
nrow(invalid_date_length)

# convert birth dates that are length 10 to lubridates
date_distribution <- date_check[str_count(date_check$bene_birth_dt) == 10,]
date_distribution$bene_birth_dt <- as_date(date_distribution$bene_birth_dt)

# Check min and max years
min(date_distribution$year)
max(date_distribution$year)

# bar chart of birthdays by year
ggplot(date_distribution, aes(x=bene_birth_dt))+ 
  geom_histogram(binwidth=30, colour="purple") +
  scale_x_date(labels = scales::date_format("%Y")) +
  ylab("Frequency") + xlab("Year") +
  theme_bw()

# Facet year-month histogram
date_distribution <- date_distribution %>%
  # Get the year value to use it for the facetted plot
  mutate(year = year(bene_birth_dt),
         # Get the month-day dates and set all dates with a dummy year (2021 in this case)
         # This will get all your dates in a common x axis scale
         month_day = as_date(paste(3000,month(bene_birth_dt),day(bene_birth_dt), sep = "-")))
ggplot(date_distribution, aes(x = month_day)) +
  geom_histogram() +
  labs(x = "Month", 
       y = "Count", 
       title = "Birthdays per Month") +
  scale_x_date(labels = scales::date_format("%b"), 
               breaks = "1 month") +
  facet_wrap(~year) +
  theme(axis.text.x = element_text(angle = 90,
                                   vjust = 0.5,
                                   hjust = 1))
# Zoom in to main years of interest
ggplot(date_distribution[(date_distribution$year > 1924) & (date_distribution$year < 1970),], aes(x = month_day)) +
  geom_histogram() +
  labs(x = "Month", 
       y = "Count", 
       title = "Birthdays per Month") +
  scale_x_date(labels = scales::date_format("%b"), 
               breaks = "1 month") +
  facet_wrap(~year) +
  theme(axis.text.x = element_text(angle = 90,
                                   vjust = 0.5,
                                   hjust = 1))


# ---- Extra: ----
# Demonstrate need for case sensitivity
tables_sql <- glue::glue_sql("
--View collations settings of tables used in this demonstration
--Collation name must be set as SQL_Latin1_General_CP1_CS_AS for case sensitivity to be respected
SELECT t.name, c.name, c.collation_name
FROM SYS.COLUMNS c
JOIN SYS.TABLES t ON t.object_id = c.object_id
WHERE t.name in ('mcare_edb_user_view', 'mcare_bcarrier_claims')
and c.name in ('bene_id', 'clm_id')
order by t.name desc, c.name;",
.con = dw_inthealth)
test1 <- odbc::dbGetQuery(conn = dw_inthealth, tables_sql)

bene_ssn_sql <- glue::glue_sql("
--Demonstrate case-sensitive nature of bene_id variable
with temp1 as (select distinct
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id_cs,
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS as bene_id_ci,
lower(trim(bene_id)) as bene_id_lowercase
from stg_claims.mcare_edb_user_view
)

select 'case-sensitive trimmed varchar' as column_type, count(distinct bene_id_cs) as bene_id_dcount from temp1
union select 'case-insensitive trimmed varchar' as column_type, count(distinct bene_id_ci) as bene_id_dcount from temp1
union select 'lowercase trimmed varchar' as column_type,count(distinct bene_id_lowercase) as bene_id_dcount from temp1
order by bene_id_dcount desc;",
.con = dw_inthealth)
test2 <- odbc::dbGetQuery(conn = dw_inthealth, bene_ssn_sql)

clm_id_sql <- glue::glue_sql("
--Demonstrate case-sensitive nature of clm_id variable
with temp2 as (select distinct
cast(trim(clm_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as clm_id_cs,
cast(trim(clm_id) as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS as clm_id_ci,
lower(trim(clm_id)) as clm_id_lowercase
from stg_claims.mcare_bcarrier_claims
)

select 'case-sensitive trimmed varchar' as column_type, count(distinct clm_id_cs) as clm_id_dcount from temp2
union select 'case-insensitive trimmed varchar' as column_type, count(distinct clm_id_ci) as clm_id_dcount from temp2
union select 'lowercase trimmed varchar' as column_type,count(distinct clm_id_lowercase) as clm_id_dcount from temp2
order by clm_id_dcount desc;",
.con = dw_inthealth)
test3 <- odbc::dbGetQuery(conn = dw_inthealth, clm_id_sql)
