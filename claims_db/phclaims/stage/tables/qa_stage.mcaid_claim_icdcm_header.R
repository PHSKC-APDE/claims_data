
# This code QAs table [stage].[mcaid_claim_icdcm_header]
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-12
# Alastair Matheson (building on SQL from Philip Sylling)
#
# QA checks:
# 1) IDs are all found in the elig tables
# 2) ICD-9-CM and ICD-10-CM codes are an appropriate length
# 3) icdcm_number falls in an acceptable range
# 4) (AlmosT) All dx codes are found in the ref table
# 5) Check there were as many or more diagnoses for each calendar year
# 6) [Not yet added - need a threshold for failure] Proprtion of IDs in claim header table with a dx


#### SET UP FUNCTIONS, ETC. ####
if (!exists("db_claims")) {
  db_claims <- dbConnect(odbc(), "PHClaims")  
}

last_run <- as.POSIXct(DBI::dbGetQuery(
  db_claims, "SELECT MAX (last_run) FROM stage.mcaid_claim_icdcm_header")[[1]])


#### Check all IDs are also found in the elig_demo and time_var tables ####
ids_demo_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM stage.mcaid_claim_icdcm_header AS a
  LEFT JOIN final.mcaid_elig_demo AS b
  ON a.id_mcaid = b.id_mcaid
  WHERE b.id_mcaid IS NULL"))

ids_timevar_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM stage.mcaid_claim_icdcm_header AS a
  LEFT JOIN final.mcaid_elig_timevar AS b
  ON a.id_mcaid = b.id_mcaid
  WHERE b.id_mcaid IS NULL"))

# Write findings to metadata
if (ids_demo_chk == 0 & ids_timevar_chk == 0) {
  ids_fail <- 0
  DBI::dbExecute(conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Distinct IDs compared to elig tables', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were the same number of IDs as in the final.mcaid_elig_demo ", 
                    "and final.mcaid_elig_timevar tables')",
                   .con = db_claims))
} else {
  ids_fail <- 1
  DBI::dbExecute(conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Distinct IDs compared to elig tables', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {ids_demo_chk} {DBI::SQL(ifelse(ids_demo_chk >= 0, 'more', 'fewer'))} ",
                      "IDs than in the final.mcaid_elig_demo table and ", 
                      "{ids_timevar_chk} {DBI::SQL(ifelse(ids_timevar_chk >= 0, 'more', 'fewer'))} ", 
                      "IDs than in the final.mcaid_elig_timevar table')",
                   .con = db_claims))
}


#### Check length of ICD codes ####
# ICD-9-CM should be 5
# ICD-10-CM should be 3-7

### ICD-9-CM
icd9_len_chk <- DBI::dbGetQuery(db_claims,
"SELECT MIN(LEN(icdcm_norm)) as min_len, MAX(LEN(icdcm_norm)) as max_len 
FROM stage.mcaid_claim_icdcm_header WHERE icdcm_version = 9")

# Write findings to metadata
if (icd9_len_chk$min_len == 5 & icd9_len_chk$max_len == 5) {
  icd9_len_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Length of ICD-9-CM codes', 
                   'PASS', 
                   {Sys.time()}, 
                   'The ICD-9-CM codes were all 5 characters in length')",
                                .con = db_claims))
} else {
  icd9_len_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Length of ICD-9-CM codes', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The ICD-9-CM codes ranged from {icd9_len_chk$min_len} to ",
                   "{icd9_len_chk$max_len} characters in length (should be all 5)')",
                                .con = db_claims))
}

### ICD-10-CM
icd10_len_chk <- DBI::dbGetQuery(db_claims,
"SELECT MIN(LEN(icdcm_norm)) as min_len, MAX(LEN(icdcm_norm)) as max_len 
FROM stage.mcaid_claim_icdcm_header WHERE icdcm_version = 10")

# Write findings to metadata
if (icd10_len_chk$min_len == 3 & icd10_len_chk$max_len == 7) {
  icd10_len_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Length of ICD-10-CM codes', 
                   'PASS', 
                   {Sys.time()}, 
                   'The ICD-10-CM codes ranged from {icd10_len_chk$min_len} to ",
                      "{icd10_len_chk$max_len} characters in length, as expected')",
                      .con = db_claims))
} else {
  icd10_len_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Length of ICD-10-CM codes', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The ICD-10-CM codes ranged from {icd10_len_chk$min_len} to ",
                      "{icd10_len_chk$max_len} characters in length (should be 3-7)')",
                      .con = db_claims))
}


#### Check that icdcm_number in ('01':'12','admit') ####
icdcm_num_chk <- as.integer(
  DBI::dbGetQuery(db_claims,
  "SELECT count([icdcm_number])
from [stage].[mcaid_claim_icdcm_header]
where [icdcm_number] not in 
('01','02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', 'admit')"))

# Write findings to metadata
if (icdcm_num_chk == 0) {
  icdcm_num_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'icdcm_number = 01-12 or admit', 
                   'PASS', 
                   {Sys.time()}, 
                   'All icdcm_number values were 01:12 or admit')",
                                .con = db_claims))
} else {
  icdcm_num_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'icdcm_number = 01-12 or admit', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {icdcm_num_chk} icdcm_number values not 01 through 12 or admit')",
                                .con = db_claims))
}



#### Check if any diagnosis codes do not join to ICD-CM reference table ####
dx_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT count(distinct 'ICD' + CAST([icdcm_version] AS VARCHAR(2)) + ' - ' + [icdcm_norm])
  FROM [stage].[mcaid_claim_icdcm_header] as a
  WHERE not exists
  (SELECT 1 FROM [ref].[dx_lookup] as b
    WHERE a.[icdcm_version] = b.[dx_ver] and a.[icdcm_norm] = b.[dx])"))

# Write findings to metadata
if (dx_chk < 100) {
  dx_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Almost all dx codes join to ICD-CM reference table ({dx_chk} did not)', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {dx_chk} dx values not in ref.dx_lookup (acceptable is < 100)')",
                                .con = db_claims))
} else {
  dx_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'All dx codes join to ICD-CM reference table', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {dx_chk} dx values not in ref.dx_lookup table (acceptable is < 100)')",
                                .con = db_claims))
}


#### Compare number of dx codes in current vs. prior analytic tables ####
num_dx_current <- DBI::dbGetQuery(db_claims,
 "SELECT YEAR([first_service_date]) AS [claim_year], COUNT(*) AS [prior_num_dx]
 FROM [final].[mcaid_claim_icdcm_header]
 GROUP BY YEAR([first_service_date]) ORDER BY YEAR([first_service_date])")

num_dx_new <- DBI::dbGetQuery(db_claims,
"SELECT YEAR([first_service_date]) AS [claim_year], COUNT(*) AS [current_num_dx]
 FROM [stage].[mcaid_claim_icdcm_header]
 GROUP BY YEAR([first_service_date]) ORDER by YEAR([first_service_date])")

num_dx_overall <- left_join(num_dx_new, num_dx_current, by = "claim_year") %>%
  mutate(pct_change = round((current_num_dx - prior_num_dx) / prior_num_dx * 100, 4))
               
# Write findings to metadata
if (max(num_dx_overall$pct_change, na.rm = T) > 0 & 
    min(num_dx_overall$pct_change, na.rm = T) >= 0) {
  num_dx_fail <- 0
  DBI::dbExecute(conn = db_claims, 
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Change in number of diagnoses', 
                   'PASS', 
                   {Sys.time()}, 
                   'The following years had more diagnoses than in the final schema table: ", 
                 "{DBI::SQL(glue::glue_collapse(num_dx_overall$claim_year[num_dx_overall$pct_change > 0], 
                        sep = ', ', last = ' and '))}')",
                 .con = db_claims))
} else if (min(num_dx_overall$pct_change, na.rm = T) + max(num_dx_overall$pct_change, na.rm = T) == 0) {
  num_dx_fail <- 1
  DBI::dbExecute(conn = db_claims, 
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Change in number of diagnoses', 
                   'FAIL', 
                   {Sys.time()}, 
                   'No change in the number of diagnoses compared to final schema table')",
                 .con = db_claims))
} else if (min(num_dx_overall$pct_change, na.rm = T) < 0) {
  num_dx_fail <- 1
  DBI::dbExecute(conn = db_claims, 
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_icdcm_header',
                   'Change in number of diagnoses', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The following years had fewer diagnoses than in the final schema table: ", 
                 "{DBI::SQL(glue::glue_collapse(num_dx_overall$claim_year[num_dx_overall$pct_change < 0], 
                        sep = ', ', last = ' and '))}')",
                 .con = db_claims))
}


#### Check the proportion of people in the claim_header table who have a dx ####
# Not yet implemented, need to adapt SQL code and adopt a threshold
# --Compare number of people with claim_header table
# set @pct_claim_header_id_with_dx = 
#   (
#     select
#     cast((select count(distinct id_mcaid) as id_dcount
#           from [stage].[mcaid_claim_icdcm_header]) as numeric) /
#       (select count(distinct id_mcaid) as id_dcount
#        from [stage].[mcaid_claim_header])
#   );
# 
# insert into [metadata].[qa_mcaid]
# select 
# NULL
# ,@last_run
# ,'stage.mcaid_claim_icdcm_header'
# ,'Compare number of people with claim_header table'
# ,NULL
# ,getdate()
# ,@pct_claim_header_id_with_dx + ' proportion of members with a claim header have a dx';


#### SUM UP FAILURES ####
fail_tot <- sum(ids_fail, icd9_len_fail, icd10_len_fail, icdcm_num_fail,
                dx_fail, num_dx_fail)



#### CLEAN UP ####
rm(last_run)
rm(ids_demo_chk, ids_timevar_chk)
rm(icd9_len_chk, icd10_len_chk)
rm(icdcm_num_chk)
rm(dx_chk)
rm(num_dx_current, num_dx_new, num_dx_overall)
rm(ids_fail, icd9_len_fail, icd10_len_fail, icdcm_num_fail, dx_fail, num_dx_fail)