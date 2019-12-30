# This code QAs table [stage].[mcaid_claim_header]
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-12
# Alastair Matheson (building on SQL from Philip Sylling)
#
# QA checks:
# 1) Claim header is distinct
# 2) IDs are all found in the elig tables
# 3) Check there were as many or more claims for each calendar year
# 4) Check there were as many or more ED visits for each calendar year


#### SET UP FUNCTIONS, ETC. ####
if (!exists("db_claims")) {
  db_claims <- dbConnect(odbc(), "PHClaims")  
}

last_run <- as.POSIXct(DBI::dbGetQuery(
  db_claims, "SELECT MAX (last_run) FROM stage.mcaid_claim_header")[[1]])


#### Check claim headers are distinct ####
cnt_claims <- DBI::dbGetQuery(db_claims,
  "SELECT COUNT(claim_header_id) as rows_tot, COUNT(DISTINCT claim_header_id) as rows_distinct
  FROM stage.mcaid_claim_header")

# Write findings to metadata
if (cnt_claims$rows_tot == cnt_claims$rows_distinct) {
  distinct_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_header',
                   'All claim headers are distinct', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {cnt_claims$rows_tot} claim headers and all were distinct')",
                                .con = db_claims))
} else {
  distinct_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_header',
                   'All claim headers are distinct', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {cnt_claims$rows_tot} claim headers but {cnt_claims$rows_distinct} were distinct')",
                                .con = db_claims))
}


#### Check all IDs are also found in the elig_demo and time_var tables ####
ids_demo_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM stage.mcaid_claim_header AS a
  LEFT JOIN final.mcaid_elig_demo AS b
  ON a.id_mcaid = b.id_mcaid
  WHERE b.id_mcaid IS NULL"))

ids_timevar_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM stage.mcaid_claim_header AS a
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
                   'stage.mcaid_claim_header',
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
                   'stage.mcaid_claim_header',
                   'Distinct IDs compared to elig tables', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {ids_demo_chk} {DBI::SQL(ifelse(ids_demo_chk >= 0, 'more', 'fewer'))} ",
                      "IDs than in the final.mcaid_elig_demo table and ", 
                      "{ids_timevar_chk} {DBI::SQL(ifelse(ids_timevar_chk >= 0, 'more', 'fewer'))} ", 
                      "IDs than in the final.mcaid_elig_timevar table')",
                   .con = db_claims))
}


#### Compare number of claim headers in current vs. prior analytic tables ####
num_header_current <- DBI::dbGetQuery(db_claims,
 "SELECT YEAR([first_service_date]) AS [claim_year], COUNT(*) AS [current_num_header]
 FROM [final].[mcaid_claim_header]
 GROUP BY YEAR([first_service_date]) ORDER BY YEAR([first_service_date])")

num_header_new <- DBI::dbGetQuery(db_claims,
"SELECT YEAR([first_service_date]) AS [claim_year], COUNT(*) AS [new_num_header]
 FROM [stage].[mcaid_claim_header]
 GROUP BY YEAR([first_service_date]) ORDER by YEAR([first_service_date])")

num_header_overall <- left_join(num_header_new, num_header_current, by = "claim_year") %>%
  mutate(pct_change = round((new_num_header - current_num_header) / current_num_header * 100, 4))
               
# Write findings to metadata
if (max(num_header_overall$pct_change, na.rm = T) > 0 & 
    min(num_header_overall$pct_change, na.rm = T) >= 0) {
  num_header_fail <- 0
  DBI::dbExecute(conn = db_claims, 
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_header',
                   'Change in number of claim headers', 
                   'PASS', 
                   {Sys.time()}, 
                   'The following years had more claim headers than in the final schema table: ", 
                 "{DBI::SQL(glue::glue_collapse(num_header_overall$claim_year[num_header_overall$pct_change > 0], 
                        sep = ', ', last = ' and '))}')",
                 .con = db_claims))
} else if (min(num_header_overall$pct_change, na.rm = T) + max(num_header_overall$pct_change, na.rm = T) == 0) {
  num_header_fail <- 1
  DBI::dbExecute(conn = db_claims, 
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_header',
                   'Change in number of claim headers', 
                   'FAIL', 
                   {Sys.time()}, 
                   'No change in the number of claim headers compared to final schema table')",
                 .con = db_claims))
} else if (min(num_header_overall$pct_change, na.rm = T) < 0) {
  num_header_fail <- 1
  DBI::dbExecute(conn = db_claims, 
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_header',
                   'Change in number of claim headers', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The following years had fewer claim headers than in the final schema table: ", 
                 "{DBI::SQL(glue::glue_collapse(num_header_overall$claim_year[num_header_overall$pct_change < 0], 
                        sep = ', ', last = ' and '))}')",
                 .con = db_claims))
}


#### Compare number of ED visits in current vs. prior analytic tables ####
num_ed_current <- DBI::dbGetQuery(db_claims,
  "SELECT YEAR([first_service_date]) AS [claim_year], SUM([ed]) AS [current_num_ed]
  FROM [final].[mcaid_claim_header]
  GROUP BY YEAR([first_service_date]) ORDER BY YEAR([first_service_date])")

num_ed_new <- DBI::dbGetQuery(db_claims,
  "SELECT YEAR([first_service_date]) AS [claim_year], SUM([ed]) AS [new_num_ed]
  FROM [stage].[mcaid_claim_header]
  GROUP BY YEAR([first_service_date]) ORDER by YEAR([first_service_date])")

num_ed_overall <- left_join(num_ed_new, num_ed_current, by = "claim_year") %>%
  mutate(pct_change = round((new_num_ed - current_num_ed) / current_num_ed * 100, 4))

# Write findings to metadata
if (max(num_ed_overall$pct_change, na.rm = T) > 0 & 
    min(num_ed_overall$pct_change, na.rm = T) >= 0) {
  num_ed_fail <- 0
  DBI::dbExecute(conn = db_claims, 
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_header',
                   'Change in number of ED visits', 
                   'PASS', 
                   {Sys.time()}, 
                   'The following years had more ED visits than in the final schema table: ", 
                                "{DBI::SQL(glue::glue_collapse(num_ed_overall$claim_year[num_ed_overall$pct_change > 0], 
                        sep = ', ', last = ' and '))}')",
                                .con = db_claims))
} else if (min(num_ed_overall$pct_change, na.rm = T) + max(num_ed_overall$pct_change, na.rm = T) == 0) {
  num_ed_fail <- 1
  DBI::dbExecute(conn = db_claims, 
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_header',
                   'Change in number of ED visits', 
                   'FAIL', 
                   {Sys.time()}, 
                   'No change in the number of ED visits compared to final schema table')",
                                .con = db_claims))
} else if (min(num_ed_overall$pct_change, na.rm = T) < 0) {
  num_ed_fail <- 1
  DBI::dbExecute(conn = db_claims, 
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_claim_header',
                   'Change in number of ED visits', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The following years had fewer ED visits than in the final schema table: ", 
                                "{DBI::SQL(glue::glue_collapse(num_ed_overall$claim_year[num_ed_overall$pct_change < 0], 
                        sep = ', ', last = ' and '))}')",
                                .con = db_claims))
}


#### Could add in other checked here ####
# Check against each temp table that goes into claim header, e.g., sum of mental_dx_rda_any


#### SUM UP FAILURES ####
fail_tot <- sum(distinct_fail, ids_fail, num_header_fail, num_ed_fail)


#### CLEAN UP ####
rm(last_run)
rm(cnt_claims)
rm(ids_demo_chk, ids_timevar_chk)
rm(num_header_current, num_header_new, num_header_overall)
rm(num_ed_current, num_ed_new, num_ed_overall)
rm(distinct_fail, ids_fail, num_header_fail, num_ed_fail)
