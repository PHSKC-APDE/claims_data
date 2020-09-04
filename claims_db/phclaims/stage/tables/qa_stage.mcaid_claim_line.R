
# This code QAs table claims.stage_mcaid_claim_line
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-12
# Alastair Matheson (building on SQL from Philip Sylling)
#
# QA checks:
# 1) IDs are all found in the elig tables
# 2) Same number of distinct claim lines as in stage.mcaid_claim table
# 3) Revenue code is formatted properly
# 4) (Almost) All RAC codes are found in the the ref table
# 5) Check there were as many or more claim lines for each calendar year




#### SET UP FUNCTIONS, ETC. ####
if (!exists("db_claims")) {
  db_claims <- dbConnect(odbc(), "PHClaims")  
}

last_run <- as.POSIXct(DBI::dbGetQuery(
  db_claims, "SELECT MAX (last_run) FROM claims.stage_mcaid_claim_line")[[1]])


#### Check all IDs are also found in the elig_demo and time_var tables ####
ids_demo_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM claims.stage_mcaid_claim_line AS a
  LEFT JOIN claims.final_mcaid_elig_demo AS b
  ON a.id_mcaid = b.id_mcaid
  WHERE b.id_mcaid IS NULL"))

ids_timevar_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM claims.stage_mcaid_claim_line AS a
  LEFT JOIN claims.final_mcaid_elig_timevar AS b
  ON a.id_mcaid = b.id_mcaid
  WHERE b.id_mcaid IS NULL"))

# Write findings to metadata
if (ids_demo_chk == 0 & ids_timevar_chk == 0) {
  ids_fail <- 0
  DBI::dbExecute(conn = db_claims,
    glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Distinct IDs compared to elig tables', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were the same number of IDs as in the claims.final_mcaid_elig_demo ", 
                    "and claims.final_mcaid_elig_timevar tables')",
                   .con = db_claims))
} else {
  ids_fail <- 1
  DBI::dbExecute(conn = db_claims,
    glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Distinct IDs compared to elig tables', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {ids_demo_chk} {DBI::SQL(ifelse(ids_demo_chk >= 0, 'more', 'fewer'))} ",
                      "IDs than in the claims.final_mcaid_elig_demo table and ", 
                      "{ids_timevar_chk} {DBI::SQL(ifelse(ids_timevar_chk >= 0, 'more', 'fewer'))} ", 
                      "IDs than in the claims.final_mcaid_elig_timevar table')",
                   .con = db_claims))
}


#### Check number of rows compared to raw ####
rows_line <- as.integer(odbc::dbGetQuery(
  conn = db_claims, "SELECT COUNT(DISTINCT [claim_line_id]) FROM claims.stage_mcaid_claim_line"))
rows_raw <- as.integer(odbc::dbGetQuery(
  conn = db_claims, "SELECT COUNT(DISTINCT [CLM_LINE_TCN]) FROM claims.stage_mcaid_claim"))

if (rows_line == rows_raw) {
  rows_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Number of distinct claim lines compared to raw data', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were the same number of distinct claim lines as in the raw data')",
                                .con = db_claims))
} else {
  rows_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Number of distinct claim lines compared to raw data', 
                   'FAIL', 
                   {Sys.time()}, 
                   'claims.stage_mcaid_claim_line had {rows_line} distinct claim lines ", 
                   "compared to {rows_raw} in claims.stage_mcaid_claim')",
                                .con = db_claims))
}


#### Check format of rev_code ####
rev_format <- as.integer(odbc::dbGetQuery(
  conn = db_claims,
  "SELECT count(*) FROM claims.stage_mcaid_claim_line
  WHERE [rev_code] IS NOT NULL AND 
    (len([rev_code]) <> 4 OR isnumeric([rev_code]) = 0)"))

if (rev_format == 0) {
  rev_code_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Format of rev_code field', 
                   'PASS', 
                   {Sys.time()}, 
                   'All rows of rev_code formatted properly')",
                                .con = db_claims))
} else {
  rev_code_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Format of rev_code field', 
                   'FAIL', 
                   {Sys.time()}, 
                   'rev_code field had some rows with length != 4 or characters')",
                                .con = db_claims))
}




#### Check if any RAC codes do not join to reference table ####
rac_chk <- as.integer(DBI::dbGetQuery(
  db_claims,
  "SELECT count(distinct 'RAC Code - ' + CAST([rac_code_line] AS VARCHAR(255)))
  FROM claims.stage_mcaid_claim_line as a
  WHERE NOT EXISTS
  (
    SELECT 1 FROM claims.ref_mcaid_rac_code as b
    WHERE a.[rac_code_line] = b.[rac_code]
  )"))



# Write findings to metadata
if (rac_chk < 50) {
  rac_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Almost all RAC codes join to reference table', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {rac_chk} RAC values not in ref.mcaid_rac_code (acceptable is < 50)')",
                                .con = db_claims))
} else {
  rac_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Almost all RAC codes join to reference table', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {rac_chk} RAC not in ref.mcaid_rac_code table (acceptable is < 50)')",
                                .con = db_claims))
}


#### Compare number of claim lines in current vs. prior analytic tables ####
if (DBI::dbExistsTable(db_claims, DBI::Id(schema = "claims", table = "final_mcaid_claim_line"))) {
  num_claim_current <- DBI::dbGetQuery(db_claims,
                                       "SELECT YEAR([first_service_date]) AS [claim_year], COUNT(*) AS [current_claim_line]
 FROM claims.final_mcaid_claim_line
 GROUP BY YEAR([first_service_date]) ORDER BY YEAR([first_service_date])")
  
  num_claim_new <- DBI::dbGetQuery(db_claims,
                                   "SELECT YEAR([first_service_date]) AS [claim_year], COUNT(*) AS [new_claim_line]
 FROM claims.stage_mcaid_claim_line
 GROUP BY YEAR([first_service_date]) ORDER by YEAR([first_service_date])")
  
  num_claim_overall <- left_join(num_claim_new, num_claim_current, by = "claim_year") %>%
    mutate(pct_change = round((new_claim_line - current_claim_line) / current_claim_line * 100, 4))
  
  # Write findings to metadata
  if (max(num_claim_overall$pct_change, na.rm = T) > 0 & 
      min(num_claim_overall$pct_change, na.rm = T) >= 0) {
    num_claim_fail <- 0
    DBI::dbExecute(conn = db_claims, 
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Change in number of claim lines', 
                   'PASS', 
                   {Sys.time()}, 
                   'The following years had more claim lines than in the final schema table: ", 
                                  "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_claim_overall$claim_year[num_claim_overall$pct_change > 0], 
                                            pct = round(abs(num_claim_overall$pct_change[num_claim_overall$pct_change > 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                  .con = db_claims))
  } else if (min(num_claim_overall$pct_change, na.rm = T) + max(num_claim_overall$pct_change, na.rm = T) == 0) {
    num_claim_fail <- 1
    DBI::dbExecute(conn = db_claims, 
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Change in number of claim lines', 
                   'FAIL', 
                   {Sys.time()}, 
                   'No change in the number of claim lines compared to final schema table')",
                                  .con = db_claims))
  } else if (min(num_claim_overall$pct_change, na.rm = T) < 0) {
    num_claim_fail <- 1
    DBI::dbExecute(conn = db_claims, 
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_line',
                   'Change in number of claim lines', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The following years had fewer claim lines than in the final schema table: ", 
                                  "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_claim_overall$claim_year[num_claim_overall$pct_change < 0], 
                                            pct = round(abs(num_claim_overall$pct_change[num_claim_overall$pct_change < 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                  .con = db_claims))
  }
} else {
  num_claim_fail <- 0
}



#### SUM UP FAILURES ####
fail_tot <- sum(ids_fail, rows_fail, rev_code_fail, rac_fail, num_claim_fail)


#### CLEAN UP ####
rm(last_run)
rm(ids_demo_chk, ids_timevar_chk)
rm(rows_line, rows_raw)
rm(rev_format)
rm(num_claim_current, num_claim_new, num_claim_overall)
rm(ids_fail, rows_fail, rev_code_fail, rac_fail, num_claim_fail)