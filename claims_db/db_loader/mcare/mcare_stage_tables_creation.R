#' @title Medicare Stage Table Creation
#' 
#' @description Code for creating stage enrollment tables for Medicare FFS data
#' 
#' @details Creates stg tables for medicare enrollment, names, and ssns from
#' other tables as part of the Medicare extraction process.
#' 

# ---- Load packages ----
pacman::p_load(DBI, glue, odbc)
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")


# ---- STEP 1: ----
# Code to create state_mcare_bene_enrollment table from MBFS tables
# Columns selected based on review of Danny's original 2019 scripts used to
# create elig_timevar and elig_demo tables
# Reference: https://kc1.sharepoint.com/:x:/r/teams/DPH-KCCross-SectorData/Shared%20Documents/General/References/Medicare/ResDAC%20file%20layouts/apde_mbsf_columns_needed_2024.xlsx?d=w4d88d662b43a4097811423fc7813313b&csf=1&web=1&e=1CBks8


db_hhsaw <- create_db_connection("hhsaw", interactive = F, prod = T)

bene_enrollment_sql <- glue::glue_sql(
  "drop table if exists claims.stage_mcare_bene_enrollment
  select
  [etl_batch_id]
  ,cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id
  ,[bene_enrollmt_ref_yr]
  ,[zip_cd]
  ,[bene_birth_dt]
  ,[bene_death_dt]
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
  into claims.stage_mcare_bene_enrollment
  from [claims].[stage_mcare_mbsf_abcd_summary]
-- Union to 2014 data
-- Join Part ABC and Part D data for 2014
-- QA below confirms left join is appropriate because no one has Part D but not Part AB
-- Union command will drop any duplicate rows within years
  UNION SELECT
  a.[etl_batch_id]
  ,cast(trim(a.bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id
  ,a.[bene_enrollmt_ref_yr]
  ,a.[bene_zip_cd]
  ,a.[bene_birth_dt]
  ,a.[bene_death_dt]
  ,a.[bene_sex_id]
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
  from [claims].[stage_mcare_mbsf_ab_summary] as a
  left join [claims].[stage_mcare_mbsf_d_cmpnts] as b 
  on a.bene_id = b.bene_id",
.con = db_hhsaw)
odbc::dbSendQuery(conn = db_hhsaw, bene_enrollment_sql)

# Confirm that no one has Part D coverage but not Part AB coverage in 2014
# Confirm no bene_id in mbsf_d table that doesn't exist in mbsf_ab table
# PASS condition: Expect 0
bene_check_sql <- glue::glue_sql("
  select count(a.bene_id) as row_count
  from [claims].[stage_mcare_mbsf_d_cmpnts] as a
  left join [claims].[stage_mcare_mbsf_ab_summary] as b
  on a.bene_id = b.bene_id
  where b.bene_id is null",
.con = db_hhsaw)
odbc::dbGetQuery(conn = db_hhsaw, bene_check_sql)

# ---- STEP 2: ----
# Code to create NAMES and SSN tables for person linkage
# Note that HIC (alternate unique ID table is not used for linkage and thus
# not loaded here)

bene_names_sql <- glue::glue_sql("
  drop table if exists claims.stage_mcare_bene_names;
  select distinct
  cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id,
  bene_srnm_name,
  bene_gvn_name,
  bene_mdl_name
  into claims.stage_mcare_bene_names
  from [claims].[stage_mcare_edb_user_view]",
.con = db_hhsaw)
odbc::dbSendQuery(conn = db_hhsaw, bene_names_sql)

bene_ssn_sql <- glue::glue_sql("
  drop table if exists claims.stage_mcare_bene_ssn;
  select distinct
  cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id,
  ssn
  into claims.stage_mcare_bene_ssn
  from [claims].[stage_mcare_bene_ssn_xwalk]",
.con = db_hhsaw)
odbc::dbSendQuery(conn = db_hhsaw, bene_ssn_sql)


# ---- STEP 3: Header-level medical claim concepts ----
# Code to create stage_mcare_claim_header_prep table
# Columns selected based on concepts/columns needed to create analytic-ready claim tables
# Reference: https://kc1.sharepoint.com/:x:/r/teams/DPH-KCCross-SectorData/Shared%20Documents/General/References/Medicare/ResDAC%20file%20layouts/apde_claims_columns_needed_2024.xlsx?d=w157ab8c163714967847462a7271df79a&csf=1&web=1&e=uweatj
# Exclude denied claims per ResDAC guidance
# Exclude claims among people with no enrollment data
# Trim white space for id_mcare, claim_header_id

db_hhsaw <- create_db_connection("hhsaw", interactive = F, prod = T)

claim_header_prep_sql <- glue::glue_sql(
  "drop table if exists claims.stage_mcare_claim_header_prep;
  --BLANK--
  ",
  .con = db_hhsaw)
odbc::dbSendQuery(conn = db_hhsaw, claim_header_prep_sql)


# ---- Extra: ----
# Demonstrate need for case sensitivity
tables_sql <- glue::glue_sql("
--View collations settings of tables used in this demonstration
--Collation name must be set as SQL_Latin1_General_CP1_CS_AS for case sensitivity to be respected
SELECT t.name, c.name, c.collation_name
FROM SYS.COLUMNS c
JOIN SYS.TABLES t ON t.object_id = c.object_id
WHERE t.name in ('stage_mcare_edb_user_view', 'stage_mcare_bcarrier_claims')
and c.name in ('bene_id', 'clm_id')
order by t.name desc, c.name;",
.con = db_hhsaw)
test <- odbc::dbGetQuery(conn = db_hhsaw, tables_sql)

bene_ssn_sql <- glue::glue_sql("
--Demonstrate case-sensitive nature of bene_id variable
with temp1 as (select distinct
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id_cs,
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS as bene_id_ci,
lower(trim(bene_id)) as bene_id_lowercase
from claims.stage_mcare_edb_user_view
)

select 'case-sensitive trimmed varchar' as column_type, count(distinct bene_id_cs) as bene_id_dcount from temp1
union select 'case-insensitive trimmed varchar' as column_type, count(distinct bene_id_ci) as bene_id_dcount from temp1
union select 'lowercase trimmed varchar' as column_type,count(distinct bene_id_lowercase) as bene_id_dcount from temp1
order by bene_id_dcount desc;",
.con = db_hhsaw)
test2 <- odbc::dbGetQuery(conn = db_hhsaw, bene_ssn_sql)

clm_id_sql <- glue::glue_sql("
--Demonstrate case-sensitive nature of clm_id variable
with temp2 as (select distinct
cast(trim(clm_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as clm_id_cs,
cast(trim(clm_id) as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS as clm_id_ci,
lower(trim(clm_id)) as clm_id_lowercase
from claims.stage_mcare_bcarrier_claims
)

select 'case-sensitive trimmed varchar' as column_type, count(distinct clm_id_cs) as clm_id_dcount from temp2
union select 'case-insensitive trimmed varchar' as column_type, count(distinct clm_id_ci) as clm_id_dcount from temp2
union select 'lowercase trimmed varchar' as column_type,count(distinct clm_id_lowercase) as clm_id_dcount from temp2
order by clm_id_dcount desc;",
.con = db_hhsaw)
test3 <- odbc::dbGetQuery(conn = db_hhsaw, clm_id_sql)
