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

# ---- STEP 2: Create NAMES and SSN tables for person linkage ---- 
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


# --- QA Step 2: ---
# Ensure case-sensitivity is set for bene_id variable (expect CS trimmed varchar count > CI trimmed varchar)
bene_names_casetest_sql <- glue::glue_sql("
with temp1 as (select distinct
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id_cs,
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS as bene_id_ci,
lower(trim(bene_id)) as bene_id_lowercase
from claims.stage_mcare_bene_names
)

select 'case-sensitive trimmed varchar' as column_type, count(distinct bene_id_cs) as bene_id_dcount from temp1
union select 'case-insensitive trimmed varchar' as column_type, count(distinct bene_id_ci) as bene_id_dcount from temp1
union select 'lowercase trimmed varchar' as column_type,count(distinct bene_id_lowercase) as bene_id_dcount from temp1
order by bene_id_dcount desc;",
.con = db_hhsaw)
bene_names_casetest <- odbc::dbGetQuery(conn = db_hhsaw, bene_names_casetest_sql)
print(bene_names_casetest)

bene_ssn_casetest_sql <- glue::glue_sql("
with temp1 as (select distinct
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CS_AS as bene_id_cs,
cast(trim(bene_id) as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS as bene_id_ci,
lower(trim(bene_id)) as bene_id_lowercase
from claims.stage_mcare_bene_ssn
)

select 'case-sensitive trimmed varchar' as column_type, count(distinct bene_id_cs) as bene_id_dcount from temp1
union select 'case-insensitive trimmed varchar' as column_type, count(distinct bene_id_ci) as bene_id_dcount from temp1
union select 'lowercase trimmed varchar' as column_type,count(distinct bene_id_lowercase) as bene_id_dcount from temp1
order by bene_id_dcount desc;",
.con = db_hhsaw)
bene_ssn_casetest <- odbc::dbGetQuery(conn = db_hhsaw, bene_ssn_casetest_sql)
print(bene_ssn_casetest)

# Ensure there are very few bene_id values in the [claims].[stage_mcare_bene_enrollment]
# table that do not match to the [claims].[stage_mcare_bene_names] or [claims].[stage_mcare_bene_ssn] tables
bene_id_test_names_sql <- glue::glue_sql("
SELECT  a.bene_id
FROM claims.stage_mcare_bene_enrollment a
LEFT JOIN claims.stage_mcare_bene_names b
  ON a.bene_id = b.bene_id
WHERE b.bene_id IS NULL   
",
.con = db_hhsaw)
bene_id_test_names <- odbc::dbGetQuery(conn = db_hhsaw, bene_id_test_names_sql)
nrow(bene_id_test_names)

bene_id_test_ssn_sql <- glue::glue_sql("
SELECT  a.bene_id
FROM claims.stage_mcare_bene_enrollment a
LEFT JOIN claims.stage_mcare_bene_ssn b
  ON a.bene_id = b.bene_id
WHERE b.bene_id IS NULL                                         
",
.con = db_hhsaw)
bene_id_test_ssn <- odbc::dbGetQuery(conn = db_hhsaw, bene_id_test_ssn_sql)
nrow(bene_id_test_ssn)

bene_id_test_both_sql <- glue::glue_sql("
SELECT a.bene_id
FROM claims.stage_mcare_bene_enrollment a
  LEFT JOIN (
    SELECT DISTINCT bene_id FROM claims.stage_mcare_bene_names
	UNION
	SELECT DISTINCT bene_id FROM claims.stage_mcare_bene_ssn
	) b ON a.bene_id = b.bene_id
	WHERE b.bene_id IS NULL                                   
",
.con = db_hhsaw)
bene_id_test_both <- odbc::dbGetQuery(conn = db_hhsaw, bene_id_test_both_sql)
nrow(bene_id_test_both)

# Ensure that the date of birth variable in the [claims].[stage_mcare_bene_enrollment] 
# is no longer garbled for 2020 and 2021 data (this could just be a one-time check,
# although it would be prudent for us to have QA here that aims to find issues
# with any date variables. For example, you would never want to see that dates 
# of birth are mostly clustered in a given year/month for a given enrollment_year,
# as this would indicate that something went wrong with how these dates were formatted/parsed along the way.
date_check_sql <- glue::glue_sql("
SELECT bene_enrollmt_ref_yr, bene_id, bene_birth_dt, bene_death_dt
FROM claims.stage_mcare_bene_enrollment
",
.con = db_hhsaw)
date_check <- odbc::dbGetQuery(conn = db_hhsaw, date_check_sql)

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
  scale_x_date(labels = date_format("%Y")) +
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


# ---- STEP 3: Header-level medical claim concepts ----
# Code to create stage_mcare_claim_header_prep table
# Columns selected based on concepts/columns needed to create analytic-ready claim tables
# Reference: https://kc1.sharepoint.com/:x:/r/teams/DPH-KCCross-SectorData/Shared%20Documents/General/References/Medicare/ResDAC%20file%20layouts/apde_claims_columns_needed_2024.xlsx?d=w157ab8c163714967847462a7271df79a&csf=1&web=1&e=uweatj
# Exclude denied claims per ResDAC guidance
# Trim white space for id_mcare, claim_header_id
# Collation settings are inherited from raw data tables in Synapse
# Create table in Synapse (inthealth_edw)

dw_inthealth <- create_db_connection("inthealth", interactive = FALSE, prod = TRUE)

claim_header_prep_sql <- glue::glue_sql(
  "if object_id(N'stg_claims.mcare_claim_header_prep', N'U') is not null drop table stg_claims.mcare_claim_header_prep; 

--bcarrier claims

select --top 100--testing code

--core data elements
trim(bene_id) as id_mcare,
trim(clm_id) as claim_header_id,
cast(clm_from_dt as date) as first_service_date,
cast(clm_thru_dt as date) as last_service_date,
'carrier' as filetype_mcare,
nch_clm_type_cd as claim_type_mcare_id,
etl_batch_id,

--facility claim type data elements
facility_type_code = null,
service_type_code = null,
patient_status = null,
patient_status_code = null,
admission_date = null,
discharge_date = null,
ipt_admission_type = null,
ipt_admission_source = null,
drg_code = null,
hospice_from_date = null,

--ICD-CM codes
dxadmit = null,
icd_dgns_cd1 as dx01,
icd_dgns_cd2 as dx02,
icd_dgns_cd3 as dx03,
icd_dgns_cd4 as dx04,
icd_dgns_cd5 as dx05,
icd_dgns_cd6 as dx06,
icd_dgns_cd7 as dx07,
icd_dgns_cd8 as dx08,
icd_dgns_cd9 as dx09,
icd_dgns_cd10 as dx10,
icd_dgns_cd11 as dx11,
icd_dgns_cd12 as dx12,
dx13 = null,
dx14 = null,
dx15 = null,
dx16 = null,
dx17 = null,
dx18 = null,
dx19 = null,
dx20 = null,
dx21 = null,
dx22 = null,
dx23 = null,
dx24 = null,
dx25 = null,
dxecode_1 = null,
dxecode_2 = null,
dxecode_3 = null,
dxecode_4 = null,
dxecode_5 = null,
dxecode_6 = null,
dxecode_7 = null,
dxecode_8 = null,
dxecode_9 = null,
dxecode_10 = null,
dxecode_11 = null,
dxecode_12 = null,

--provider data elements
carr_clm_blg_npi_num as provider_billing_npi,
provider_rendering_npi = null,
provider_attending_npi = null,
provider_operating_npi = null,
rfr_physn_npi as provider_referring_npi,
cpo_org_npi_num as provider_cpo_npi,
carr_clm_sos_npi_num as provider_sos_npi,
provider_other_npi = null,

provider_rendering_zip = null,

provider_specialty_rendering = null,
provider_specialty_attending = null,
provider_specialty_operating = null,
provider_specialty_referring = null,
provider_specialty_other = null,

--cost data elements
clm_pmt_amt,
carr_clm_prmry_pyr_pd_amt as clm_prmry_pyr_pd_amt,
nch_clm_prvdr_pmt_amt as clm_prvdr_pmt_amt,
nch_clm_bene_pmt_amt as clm_bene_pmt_amt,
nch_carr_clm_sbmtd_chrg_amt as clm_sbmtd_chrg_amt,
nch_carr_clm_alowd_amt as clm_alowd_amt,
carr_clm_cash_ddctbl_apld_amt as clm_cash_ddctbl_apld_amt,
clm_bene_pd_amt,
clm_tot_chrg_amt = null,
clm_pass_thru_per_diem_amt = null,
nch_bene_ddctbl_amt = null,
nch_bene_coinsrnc_lblty_amt = null,
nch_bene_blood_ddctbl_lblty_am = null,
nch_ip_ncvrd_chrg_amt = null,
nch_ip_tot_ddctn_amt = null

into stg_claims.mcare_claim_header_prep
from stg_claims.mcare_bcarrier_claims
where carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')

--dme claims
union select --top 100--testing code

--core data elements
trim(bene_id) as id_mcare,
trim(clm_id) as claim_header_id,
cast(clm_from_dt as date) as first_service_date,
cast(clm_thru_dt as date) as last_service_date,
'dme' as filetype_mcare,
nch_clm_type_cd as claim_type_mcare_id,
etl_batch_id,

--facility claim type data elements
facility_type_code = null,
service_type_code = null,
patient_status = null,
patient_status_code = null,
admission_date = null,
discharge_date = null,
ipt_admission_type = null,
ipt_admission_source = null,
drg_code = null,
hospice_from_date = null,

--ICD-CM codes
dxadmit = null,
icd_dgns_cd1 as dx01,
icd_dgns_cd2 as dx02,
icd_dgns_cd3 as dx03,
icd_dgns_cd4 as dx04,
icd_dgns_cd5 as dx05,
icd_dgns_cd6 as dx06,
icd_dgns_cd7 as dx07,
icd_dgns_cd8 as dx08,
icd_dgns_cd9 as dx09,
icd_dgns_cd10 as dx10,
icd_dgns_cd11 as dx11,
icd_dgns_cd12 as dx12,
dx13 = null,
dx14 = null,
dx15 = null,
dx16 = null,
dx17 = null,
dx18 = null,
dx19 = null,
dx20 = null,
dx21 = null,
dx22 = null,
dx23 = null,
dx24 = null,
dx25 = null,
dxecode_1 = null,
dxecode_2 = null,
dxecode_3 = null,
dxecode_4 = null,
dxecode_5 = null,
dxecode_6 = null,
dxecode_7 = null,
dxecode_8 = null,
dxecode_9 = null,
dxecode_10 = null,
dxecode_11 = null,
dxecode_12 = null,

--provider data elements
provider_billing_npi = null,
provider_rendering_npi = null,
provider_attending_npi = null,
provider_operating_npi = null,
rfr_physn_npi as provider_referring_npi,
provider_cpo_npi = null,
provider_sos_npi = null,
provider_other_npi = null,

provider_rendering_zip = null,

provider_specialty_rendering = null,
provider_specialty_attending = null,
provider_specialty_operating = null,
provider_specialty_referring = null,
provider_specialty_other = null,

--cost data elements
clm_pmt_amt,
carr_clm_prmry_pyr_pd_amt as clm_prmry_pyr_pd_amt,
nch_clm_prvdr_pmt_amt as clm_prvdr_pmt_amt,
nch_clm_bene_pmt_amt as clm_bene_pmt_amt,
nch_carr_clm_sbmtd_chrg_amt as clm_sbmtd_chrg_amt,
nch_carr_clm_alowd_amt as clm_alowd_amt,
carr_clm_cash_ddctbl_apld_amt as clm_cash_ddctbl_apld_amt,
clm_bene_pd_amt,
clm_tot_chrg_amt = null,
clm_pass_thru_per_diem_amt = null,
nch_bene_ddctbl_amt = null,
nch_bene_coinsrnc_lblty_amt = null,
nch_bene_blood_ddctbl_lblty_am = null,
nch_ip_ncvrd_chrg_amt = null,
nch_ip_tot_ddctn_amt = null

from stg_claims.mcare_dme_claims
where carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')

--hha claims
union select --top 100--testing code

--core data elements
trim(bene_id) as id_mcare,
trim(clm_id) as claim_header_id,
cast(clm_from_dt as date) as first_service_date,
cast(clm_thru_dt as date) as last_service_date,
'hha' as filetype_mcare,
nch_clm_type_cd as claim_type_mcare_id,
etl_batch_id,

--facility claim type data elements
clm_fac_type_cd as facility_type_code,
clm_srvc_clsfctn_type_cd as service_type_code,
patient_status = null,
ptnt_dschrg_stus_cd  as patient_status_code,
clm_admsn_dt as admission_date,
nch_bene_dschrg_dt as discharge_date,
ipt_admission_type = null,
ipt_admission_source = null,
drg_code = null,
hospice_from_date = null,

--ICD-CM codes
dxadmit = null,
icd_dgns_cd1 as dx01,
icd_dgns_cd2 as dx02,
icd_dgns_cd3 as dx03,
icd_dgns_cd4 as dx04,
icd_dgns_cd5 as dx05,
icd_dgns_cd6 as dx06,
icd_dgns_cd7 as dx07,
icd_dgns_cd8 as dx08,
icd_dgns_cd9 as dx09,
icd_dgns_cd10 as dx10,
icd_dgns_cd11 as dx11,
icd_dgns_cd12 as dx12,
icd_dgns_cd13 as dx13,
icd_dgns_cd14 as dx14,
icd_dgns_cd15 as dx15,
icd_dgns_cd16 as dx16,
icd_dgns_cd17 as dx17,
icd_dgns_cd18 as dx18,
icd_dgns_cd19 as dx19,
icd_dgns_cd20 as dx20,
icd_dgns_cd21 as dx21,
icd_dgns_cd22 as dx22,
icd_dgns_cd23 as dx23,
icd_dgns_cd24 as dx24,
icd_dgns_cd25 as dx25,
icd_dgns_e_cd1 as dxecode_1,
icd_dgns_e_cd2 as dxecode_2,
icd_dgns_e_cd3 as dxecode_3,
icd_dgns_e_cd4 as dxecode_4,
icd_dgns_e_cd5 as dxecode_5,
icd_dgns_e_cd6 as dxecode_6,
icd_dgns_e_cd7 as dxecode_7,
icd_dgns_e_cd8 as dxecode_8,
icd_dgns_e_cd9 as dxecode_9,
icd_dgns_e_cd10 as dxecode_10,
icd_dgns_e_cd11 as dxecode_11,
icd_dgns_e_cd12 as dxecode_12,

--provider data elements
org_npi_num as provider_billing_npi,
rndrng_physn_npi as provider_rendering_npi,
at_physn_npi as provider_attending_npi,
op_physn_npi as provider_operating_npi,
rfr_physn_npi as provider_referring_npi,
provider_cpo_npi = null,
srvc_loc_npi_num as provider_sos_npi,
ot_physn_npi as provider_other_npi,

clm_srvc_fac_zip_cd as provider_rendering_zip,

rndrng_physn_spclty_cd as provider_specialty_rendering,
at_physn_spclty_cd as provider_specialty_attending,
op_physn_spclty_cd as provider_specialty_operating,
rfr_physn_spclty_cd as provider_specialty_referring,
ot_physn_spclty_cd as provider_specialty_other,

--cost data elements
clm_pmt_amt,
nch_prmry_pyr_clm_pd_amt as clm_prmry_pyr_pd_amt,
clm_prvdr_pmt_amt = null,
clm_bene_pmt_amt = null,
clm_sbmtd_chrg_amt = null,
clm_alowd_amt = null,
clm_cash_ddctbl_apld_amt = null,
clm_bene_pd_amt = null,
clm_tot_chrg_amt,
clm_pass_thru_per_diem_amt = null,
nch_bene_ddctbl_amt = null,
nch_bene_coinsrnc_lblty_amt = null,
nch_bene_blood_ddctbl_lblty_am = null,
nch_ip_ncvrd_chrg_amt = null,
nch_ip_tot_ddctn_amt = null

from stg_claims.mcare_hha_base_claims
where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)

--hospice claims
union select --top 100--testing code

--core data elements
trim(bene_id) as id_mcare,
trim(clm_id) as claim_header_id,
cast(clm_from_dt as date) as first_service_date,
cast(clm_thru_dt as date) as last_service_date,
'hospice' as filetype_mcare,
nch_clm_type_cd as claim_type_mcare_id,
etl_batch_id,

--facility claim type data elements
clm_fac_type_cd as facility_type_code,
clm_srvc_clsfctn_type_cd as service_type_code,
nch_ptnt_status_ind_cd as patient_status,
ptnt_dschrg_stus_cd  as patient_status_code,
admission_date = null,
nch_bene_dschrg_dt as discharge_date,
ipt_admission_type = null,
ipt_admission_source = null,
drg_code = null,
clm_hospc_start_dt_id as hospice_from_date,

--ICD-CM codes
dxadmit = null,
icd_dgns_cd1 as dx01,
icd_dgns_cd2 as dx02,
icd_dgns_cd3 as dx03,
icd_dgns_cd4 as dx04,
icd_dgns_cd5 as dx05,
icd_dgns_cd6 as dx06,
icd_dgns_cd7 as dx07,
icd_dgns_cd8 as dx08,
icd_dgns_cd9 as dx09,
icd_dgns_cd10 as dx10,
icd_dgns_cd11 as dx11,
icd_dgns_cd12 as dx12,
icd_dgns_cd13 as dx13,
icd_dgns_cd14 as dx14,
icd_dgns_cd15 as dx15,
icd_dgns_cd16 as dx16,
icd_dgns_cd17 as dx17,
icd_dgns_cd18 as dx18,
icd_dgns_cd19 as dx19,
icd_dgns_cd20 as dx20,
icd_dgns_cd21 as dx21,
icd_dgns_cd22 as dx22,
icd_dgns_cd23 as dx23,
icd_dgns_cd24 as dx24,
icd_dgns_cd25 as dx25,
icd_dgns_e_cd1 as dxecode_1,
icd_dgns_e_cd2 as dxecode_2,
icd_dgns_e_cd3 as dxecode_3,
icd_dgns_e_cd4 as dxecode_4,
icd_dgns_e_cd5 as dxecode_5,
icd_dgns_e_cd6 as dxecode_6,
icd_dgns_e_cd7 as dxecode_7,
icd_dgns_e_cd8 as dxecode_8,
icd_dgns_e_cd9 as dxecode_9,
icd_dgns_e_cd10 as dxecode_10,
icd_dgns_e_cd11 as dxecode_11,
icd_dgns_e_cd12 as dxecode_12,

--provider data elements
org_npi_num as provider_billing_npi,
rndrng_physn_npi as provider_rendering_npi,
at_physn_npi as provider_attending_npi,
op_physn_npi as provider_operating_npi,
rfr_physn_npi as provider_referring_npi,
provider_cpo_npi = null,
srvc_loc_npi_num as provider_sos_npi,
ot_physn_npi as provider_other_npi,

provider_rendering_zip = null,

rndrng_physn_spclty_cd as provider_specialty_rendering,
at_physn_spclty_cd as provider_specialty_attending,
op_physn_spclty_cd as provider_specialty_operating,
rfr_physn_spclty_cd as provider_specialty_referring,
ot_physn_spclty_cd as provider_specialty_other,

--cost data elements
clm_pmt_amt,
nch_prmry_pyr_clm_pd_amt as clm_prmry_pyr_pd_amt,
clm_prvdr_pmt_amt = null,
clm_bene_pmt_amt = null,
clm_sbmtd_chrg_amt = null,
clm_alowd_amt = null,
clm_cash_ddctbl_apld_amt = null,
clm_bene_pd_amt = null,
clm_tot_chrg_amt,
clm_pass_thru_per_diem_amt = null,
nch_bene_ddctbl_amt = null,
nch_bene_coinsrnc_lblty_amt = null,
nch_bene_blood_ddctbl_lblty_am = null,
nch_ip_ncvrd_chrg_amt = null,
nch_ip_tot_ddctn_amt = null

from stg_claims.mcare_hospice_base_claims
where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)

--inpatient claims
union select --top 100--testing code

--core data elements
trim(bene_id) as id_mcare,
trim(clm_id) as claim_header_id,
cast(clm_from_dt as date) as first_service_date,
cast(clm_thru_dt as date) as last_service_date,
'inpatient' as filetype_mcare,
nch_clm_type_cd as claim_type_mcare_id,
etl_batch_id,

--facility claim type data elements
clm_fac_type_cd as facility_type_code,
clm_srvc_clsfctn_type_cd as service_type_code,
nch_ptnt_status_ind_cd as patient_status,
ptnt_dschrg_stus_cd  as patient_status_code,
clm_admsn_dt as admission_date,
nch_bene_dschrg_dt as discharge_date,
clm_ip_admsn_type_cd as ipt_admission_type ,
clm_src_ip_admsn_cd as ipt_admission_source,
clm_drg_cd as drg_code,
hospice_from_date = null,

--ICD-CM codes
admtg_dgns_cd as dxadmit,
icd_dgns_cd1 as dx01,
icd_dgns_cd2 as dx02,
icd_dgns_cd3 as dx03,
icd_dgns_cd4 as dx04,
icd_dgns_cd5 as dx05,
icd_dgns_cd6 as dx06,
icd_dgns_cd7 as dx07,
icd_dgns_cd8 as dx08,
icd_dgns_cd9 as dx09,
icd_dgns_cd10 as dx10,
icd_dgns_cd11 as dx11,
icd_dgns_cd12 as dx12,
icd_dgns_cd13 as dx13,
icd_dgns_cd14 as dx14,
icd_dgns_cd15 as dx15,
icd_dgns_cd16 as dx16,
icd_dgns_cd17 as dx17,
icd_dgns_cd18 as dx18,
icd_dgns_cd19 as dx19,
icd_dgns_cd20 as dx20,
icd_dgns_cd21 as dx21,
icd_dgns_cd22 as dx22,
icd_dgns_cd23 as dx23,
icd_dgns_cd24 as dx24,
icd_dgns_cd25 as dx25,
icd_dgns_e_cd1 as dxecode_1,
icd_dgns_e_cd2 as dxecode_2,
icd_dgns_e_cd3 as dxecode_3,
icd_dgns_e_cd4 as dxecode_4,
icd_dgns_e_cd5 as dxecode_5,
icd_dgns_e_cd6 as dxecode_6,
icd_dgns_e_cd7 as dxecode_7,
icd_dgns_e_cd8 as dxecode_8,
icd_dgns_e_cd9 as dxecode_9,
icd_dgns_e_cd10 as dxecode_10,
icd_dgns_e_cd11 as dxecode_11,
icd_dgns_e_cd12 as dxecode_12,

--provider data elements
org_npi_num as provider_billing_npi,
rndrng_physn_npi as provider_rendering_npi,
at_physn_npi as provider_attending_npi,
op_physn_npi as provider_operating_npi,
provider_referring_npi = null,
provider_cpo_npi = null,
provider_sos_npi = null,
ot_physn_npi as provider_other_npi,

provider_rendering_zip = null,

rndrng_physn_spclty_cd as provider_specialty_rendering,
at_physn_spclty_cd as provider_specialty_attending,
op_physn_spclty_cd as provider_specialty_operating,
provider_specialty_referring = null,
ot_physn_spclty_cd as provider_specialty_other,

--cost data elements
clm_pmt_amt,
nch_prmry_pyr_clm_pd_amt as clm_prmry_pyr_pd_amt,
clm_prvdr_pmt_amt = null,
clm_bene_pmt_amt = null,
clm_sbmtd_chrg_amt = null,
clm_alowd_amt = null,
clm_cash_ddctbl_apld_amt = null,
clm_bene_pd_amt = null,
clm_tot_chrg_amt,
clm_pass_thru_per_diem_amt,
nch_bene_ip_ddctbl_amt as nch_bene_ddctbl_amt,
nch_bene_pta_coinsrnc_lblty_am as nch_bene_coinsrnc_lblty_amt,
nch_bene_blood_ddctbl_lblty_am,
nch_ip_ncvrd_chrg_amt,
nch_ip_tot_ddctn_amt

from stg_claims.mcare_inpatient_base_claims
where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)

--inpatient claims data structure J
union select --top 100--testing code

--core data elements
trim(bene_id) as id_mcare,
trim(clm_id) as claim_header_id,
cast(clm_from_dt as date) as first_service_date,
cast(clm_thru_dt as date) as last_service_date,
'inpatient' as filetype_mcare,
nch_clm_type_cd as claim_type_mcare_id,
etl_batch_id,

--facility claim type data elements
clm_fac_type_cd as facility_type_code,
clm_srvc_clsfctn_type_cd as service_type_code,
nch_ptnt_status_ind_cd as patient_status,
ptnt_dschrg_stus_cd  as patient_status_code,
clm_admsn_dt as admission_date,
nch_bene_dschrg_dt as discharge_date,
clm_ip_admsn_type_cd as ipt_admission_type ,
clm_src_ip_admsn_cd as ipt_admission_source,
clm_drg_cd as drg_code,
hospice_from_date = null,

--ICD-CM codes
admtg_dgns_cd as dxadmit,
icd_dgns_cd1 as dx01,
icd_dgns_cd2 as dx02,
icd_dgns_cd3 as dx03,
icd_dgns_cd4 as dx04,
icd_dgns_cd5 as dx05,
icd_dgns_cd6 as dx06,
icd_dgns_cd7 as dx07,
icd_dgns_cd8 as dx08,
icd_dgns_cd9 as dx09,
icd_dgns_cd10 as dx10,
icd_dgns_cd11 as dx11,
icd_dgns_cd12 as dx12,
icd_dgns_cd13 as dx13,
icd_dgns_cd14 as dx14,
icd_dgns_cd15 as dx15,
icd_dgns_cd16 as dx16,
icd_dgns_cd17 as dx17,
icd_dgns_cd18 as dx18,
icd_dgns_cd19 as dx19,
icd_dgns_cd20 as dx20,
icd_dgns_cd21 as dx21,
icd_dgns_cd22 as dx22,
icd_dgns_cd23 as dx23,
icd_dgns_cd24 as dx24,
icd_dgns_cd25 as dx25,
icd_dgns_e_cd1 as dxecode_1,
icd_dgns_e_cd2 as dxecode_2,
icd_dgns_e_cd3 as dxecode_3,
icd_dgns_e_cd4 as dxecode_4,
icd_dgns_e_cd5 as dxecode_5,
icd_dgns_e_cd6 as dxecode_6,
icd_dgns_e_cd7 as dxecode_7,
icd_dgns_e_cd8 as dxecode_8,
icd_dgns_e_cd9 as dxecode_9,
icd_dgns_e_cd10 as dxecode_10,
icd_dgns_e_cd11 as dxecode_11,
icd_dgns_e_cd12 as dxecode_12,

--provider data elements
org_npi_num as provider_billing_npi,
provider_rendering_npi = null,
at_physn_npi as provider_attending_npi,
op_physn_npi as provider_operating_npi,
provider_referring_npi = null,
provider_cpo_npi = null,
provider_sos_npi = null,
ot_physn_npi as provider_other_npi,

provider_rendering_zip = null,

provider_specialty_rendering = null,
provider_specialty_attending = null,
provider_specialty_operating = null,
provider_specialty_referring = null,
provider_specialty_other = null,

--cost data elements
clm_pmt_amt,
nch_prmry_pyr_clm_pd_amt as clm_prmry_pyr_pd_amt,
clm_prvdr_pmt_amt = null,
clm_bene_pmt_amt = null,
clm_sbmtd_chrg_amt = null,
clm_alowd_amt = null,
clm_cash_ddctbl_apld_amt = null,
clm_bene_pd_amt = null,
clm_tot_chrg_amt,
clm_pass_thru_per_diem_amt,
nch_bene_ip_ddctbl_amt as nch_bene_ddctbl_amt,
nch_bene_pta_coinsrnc_lblty_am as nch_bene_coinsrnc_lblty_amt,
nch_bene_blood_ddctbl_lblty_am,
nch_ip_ncvrd_chrg_amt,
nch_ip_tot_ddctn_amt

from stg_claims.mcare_inpatient_base_claims_j
where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)

--outpatient claims
union select --top 100--testing code

--core data elements
trim(bene_id) as id_mcare,
trim(clm_id) as claim_header_id,
cast(clm_from_dt as date) as first_service_date,
cast(clm_thru_dt as date) as last_service_date,
'outpatient' as filetype_mcare,
nch_clm_type_cd as claim_type_mcare_id,
etl_batch_id,

--facility claim type data elements
clm_fac_type_cd as facility_type_code,
clm_srvc_clsfctn_type_cd as service_type_code,
patient_status = null,
patient_status_code = null,
admission_date = null,
discharge_date = null,
ipt_admission_type = null,
ipt_admission_source = null,
drg_code = null,
hospice_from_date = null,

--ICD-CM codes
dxadmit = null,
icd_dgns_cd1 as dx01,
icd_dgns_cd2 as dx02,
icd_dgns_cd3 as dx03,
icd_dgns_cd4 as dx04,
icd_dgns_cd5 as dx05,
icd_dgns_cd6 as dx06,
icd_dgns_cd7 as dx07,
icd_dgns_cd8 as dx08,
icd_dgns_cd9 as dx09,
icd_dgns_cd10 as dx10,
icd_dgns_cd11 as dx11,
icd_dgns_cd12 as dx12,
icd_dgns_cd13 as dx13,
icd_dgns_cd14 as dx14,
icd_dgns_cd15 as dx15,
icd_dgns_cd16 as dx16,
icd_dgns_cd17 as dx17,
icd_dgns_cd18 as dx18,
icd_dgns_cd19 as dx19,
icd_dgns_cd20 as dx20,
icd_dgns_cd21 as dx21,
icd_dgns_cd22 as dx22,
icd_dgns_cd23 as dx23,
icd_dgns_cd24 as dx24,
icd_dgns_cd25 as dx25,
icd_dgns_e_cd1 as dxecode_1,
icd_dgns_e_cd2 as dxecode_2,
icd_dgns_e_cd3 as dxecode_3,
icd_dgns_e_cd4 as dxecode_4,
icd_dgns_e_cd5 as dxecode_5,
icd_dgns_e_cd6 as dxecode_6,
icd_dgns_e_cd7 as dxecode_7,
icd_dgns_e_cd8 as dxecode_8,
icd_dgns_e_cd9 as dxecode_9,
icd_dgns_e_cd10 as dxecode_10,
icd_dgns_e_cd11 as dxecode_11,
icd_dgns_e_cd12 as dxecode_12,

--provider data elements
org_npi_num as provider_billing_npi,
rndrng_physn_npi as provider_rendering_npi,
at_physn_npi as provider_attending_npi,
op_physn_npi as provider_operating_npi,
rfr_physn_npi as provider_referring_npi,
provider_cpo_npi = null,
srvc_loc_npi_num as provider_sos_npi,
ot_physn_npi as provider_other_npi,

provider_rendering_zip = null,

rndrng_physn_spclty_cd as provider_specialty_rendering,
at_physn_spclty_cd as provider_specialty_attending,
op_physn_spclty_cd as provider_specialty_operating,
rfr_physn_spclty_cd as provider_specialty_referring,
ot_physn_spclty_cd as provider_specialty_other,

--cost data elements
clm_pmt_amt,
nch_prmry_pyr_clm_pd_amt as clm_prmry_pyr_pd_amt,
clm_op_prvdr_pmt_amt as clm_prvdr_pmt_amt,
clm_op_bene_pmt_amt as clm_bene_pmt_amt,
clm_sbmtd_chrg_amt = null,
clm_alowd_amt = null,
clm_cash_ddctbl_apld_amt = null,
clm_bene_pd_amt = null,
clm_tot_chrg_amt,
clm_pass_thru_per_diem_amt = null,
nch_bene_ptb_ddctbl_amt as nch_bene_ddctbl_amt,
nch_bene_ptb_coinsrnc_amt as nch_bene_coinsrnc_lblty_amt,
nch_bene_blood_ddctbl_lblty_am,
nch_ip_ncvrd_chrg_amt = null,
nch_ip_tot_ddctn_amt = null

from stg_claims.mcare_outpatient_base_claims
where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)

--outpatient claims data structure J
union select --top 100--testing code

--core data elements
trim(bene_id) as id_mcare,
trim(clm_id) as claim_header_id,
cast(clm_from_dt as date) as first_service_date,
cast(clm_thru_dt as date) as last_service_date,
'outpatient' as filetype_mcare,
nch_clm_type_cd as claim_type_mcare_id,
etl_batch_id,

--facility claim type data elements
clm_fac_type_cd as facility_type_code,
clm_srvc_clsfctn_type_cd as service_type_code,
patient_status = null,
patient_status_code = null,
admission_date = null,
discharge_date = null,
ipt_admission_type = null,
ipt_admission_source = null,
drg_code = null,
hospice_from_date = null,

--ICD-CM codes
dxadmit = null,
icd_dgns_cd1 as dx01,
icd_dgns_cd2 as dx02,
icd_dgns_cd3 as dx03,
icd_dgns_cd4 as dx04,
icd_dgns_cd5 as dx05,
icd_dgns_cd6 as dx06,
icd_dgns_cd7 as dx07,
icd_dgns_cd8 as dx08,
icd_dgns_cd9 as dx09,
icd_dgns_cd10 as dx10,
icd_dgns_cd11 as dx11,
icd_dgns_cd12 as dx12,
icd_dgns_cd13 as dx13,
icd_dgns_cd14 as dx14,
icd_dgns_cd15 as dx15,
icd_dgns_cd16 as dx16,
icd_dgns_cd17 as dx17,
icd_dgns_cd18 as dx18,
icd_dgns_cd19 as dx19,
icd_dgns_cd20 as dx20,
icd_dgns_cd21 as dx21,
icd_dgns_cd22 as dx22,
icd_dgns_cd23 as dx23,
icd_dgns_cd24 as dx24,
icd_dgns_cd25 as dx25,
icd_dgns_e_cd1 as dxecode_1,
icd_dgns_e_cd2 as dxecode_2,
icd_dgns_e_cd3 as dxecode_3,
icd_dgns_e_cd4 as dxecode_4,
icd_dgns_e_cd5 as dxecode_5,
icd_dgns_e_cd6 as dxecode_6,
icd_dgns_e_cd7 as dxecode_7,
icd_dgns_e_cd8 as dxecode_8,
icd_dgns_e_cd9 as dxecode_9,
icd_dgns_e_cd10 as dxecode_10,
icd_dgns_e_cd11 as dxecode_11,
icd_dgns_e_cd12 as dxecode_12,

--provider data elements
org_npi_num as provider_billing_npi,
provider_rendering_npi = null,
at_physn_npi as provider_attending_npi,
op_physn_npi as provider_operating_npi,
provider_referring_npi = null,
provider_cpo_npi = null,
provider_sos_npi = null,
ot_physn_npi as provider_other_npi,

provider_rendering_zip = null,

provider_specialty_rendering = null,
provider_specialty_attending = null,
provider_specialty_operating = null,
provider_specialty_referring = null,
provider_specialty_other = null,

--cost data elements
clm_pmt_amt,
nch_prmry_pyr_clm_pd_amt as clm_prmry_pyr_pd_amt,
clm_op_prvdr_pmt_amt as clm_prvdr_pmt_amt,
clm_op_bene_pmt_amt as clm_bene_pmt_amt,
clm_sbmtd_chrg_amt = null,
clm_alowd_amt = null,
clm_cash_ddctbl_apld_amt = null,
clm_bene_pd_amt = null,
clm_tot_chrg_amt,
clm_pass_thru_per_diem_amt = null,
nch_bene_ptb_ddctbl_amt as nch_bene_ddctbl_amt,
nch_bene_ptb_coinsrnc_amt as nch_bene_coinsrnc_lblty_amt,
nch_bene_blood_ddctbl_lblty_am,
nch_ip_ncvrd_chrg_amt = null,
nch_ip_tot_ddctn_amt = null

from stg_claims.mcare_outpatient_base_claims_j
where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)

--snf claims
union select --top 100--testing code

--core data elements
trim(bene_id) as id_mcare,
trim(clm_id) as claim_header_id,
cast(clm_from_dt as date) as first_service_date,
cast(clm_thru_dt as date) as last_service_date,
'snf' as filetype_mcare,
nch_clm_type_cd as claim_type_mcare_id,
etl_batch_id,

--facility claim type data elements
clm_fac_type_cd as facility_type_code,
clm_srvc_clsfctn_type_cd as service_type_code,
nch_ptnt_status_ind_cd as patient_status,
ptnt_dschrg_stus_cd  as patient_status_code,
clm_admsn_dt as admission_date,
nch_bene_dschrg_dt as discharge_date,
clm_ip_admsn_type_cd as ipt_admission_type ,
clm_src_ip_admsn_cd as ipt_admission_source,
clm_drg_cd as drg_code,
hospice_from_date = null,

--ICD-CM codes
admtg_dgns_cd as dxadmit,
icd_dgns_cd1 as dx01,
icd_dgns_cd2 as dx02,
icd_dgns_cd3 as dx03,
icd_dgns_cd4 as dx04,
icd_dgns_cd5 as dx05,
icd_dgns_cd6 as dx06,
icd_dgns_cd7 as dx07,
icd_dgns_cd8 as dx08,
icd_dgns_cd9 as dx09,
icd_dgns_cd10 as dx10,
icd_dgns_cd11 as dx11,
icd_dgns_cd12 as dx12,
icd_dgns_cd13 as dx13,
icd_dgns_cd14 as dx14,
icd_dgns_cd15 as dx15,
icd_dgns_cd16 as dx16,
icd_dgns_cd17 as dx17,
icd_dgns_cd18 as dx18,
icd_dgns_cd19 as dx19,
icd_dgns_cd20 as dx20,
icd_dgns_cd21 as dx21,
icd_dgns_cd22 as dx22,
icd_dgns_cd23 as dx23,
icd_dgns_cd24 as dx24,
icd_dgns_cd25 as dx25,
icd_dgns_e_cd1 as dxecode_1,
icd_dgns_e_cd2 as dxecode_2,
icd_dgns_e_cd3 as dxecode_3,
icd_dgns_e_cd4 as dxecode_4,
icd_dgns_e_cd5 as dxecode_5,
icd_dgns_e_cd6 as dxecode_6,
icd_dgns_e_cd7 as dxecode_7,
icd_dgns_e_cd8 as dxecode_8,
icd_dgns_e_cd9 as dxecode_9,
icd_dgns_e_cd10 as dxecode_10,
icd_dgns_e_cd11 as dxecode_11,
icd_dgns_e_cd12 as dxecode_12,

--provider data elements
org_npi_num as provider_billing_npi,
rndrng_physn_npi as provider_rendering_npi,
at_physn_npi as provider_attending_npi,
op_physn_npi as provider_operating_npi,
provider_referring_npi = null,
provider_cpo_npi = null,
provider_sos_npi = null,
ot_physn_npi as provider_other_npi,

provider_rendering_zip = null,

rndrng_physn_spclty_cd as provider_specialty_rendering,
at_physn_spclty_cd as provider_specialty_attending,
op_physn_spclty_cd as provider_specialty_operating,
provider_specialty_referring = null,
ot_physn_spclty_cd as provider_specialty_other,

--cost data elements
clm_pmt_amt,
nch_prmry_pyr_clm_pd_amt as clm_prmry_pyr_pd_amt,
clm_prvdr_pmt_amt = null,
clm_bene_pmt_amt = null,
clm_sbmtd_chrg_amt = null,
clm_alowd_amt = null,
clm_cash_ddctbl_apld_amt = null,
clm_bene_pd_amt = null,
clm_tot_chrg_amt,
clm_pass_thru_per_diem_amt = null,
nch_bene_ip_ddctbl_amt as nch_bene_ddctbl_amt,
nch_bene_pta_coinsrnc_lblty_am as nch_bene_coinsrnc_lblty_amt,
nch_bene_blood_ddctbl_lblty_am,
nch_ip_ncvrd_chrg_amt,
nch_ip_tot_ddctn_amt

from stg_claims.mcare_snf_base_claims
where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null);",
  .con = dw_inthealth)

system.time(odbc::dbSendQuery(conn = dw_inthealth, claim_header_prep_sql))


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
test1 <- odbc::dbGetQuery(conn = db_hhsaw, tables_sql)

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
