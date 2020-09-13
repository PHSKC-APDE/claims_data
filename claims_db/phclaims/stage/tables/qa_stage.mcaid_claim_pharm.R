# This code QAs table claims.stage_mcaid_claim_pharm
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-12
# Alastair Matheson (building on SQL from Philip Sylling)
#
# QA checks:
# 1) IDs are all found in the elig tables
# 2) Check that NDCs are formatted properly
# 3) Check there were as many or more NDCs for each calendar year


#### SET UP FUNCTIONS, ETC. ####
if (!exists("db_claims")) {
  db_claims <- dbConnect(odbc(), "PHClaims")  
}

last_run <- as.POSIXct(DBI::dbGetQuery(
  db_claims, "SELECT MAX (last_run) FROM claims.stage_mcaid_claim_pharm")[[1]])


#### Check all IDs are also found in the elig_demo and time_var tables ####
ids_demo_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM claims.stage_mcaid_claim_pharm AS a
  LEFT JOIN claims.final_mcaid_elig_demo AS b
  ON a.id_mcaid = b.id_mcaid
  WHERE b.id_mcaid IS NULL"))

ids_timevar_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM claims.stage_mcaid_claim_pharm AS a
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
                   'claims.stage_mcaid_claim_pharm',
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
                   'claims.stage_mcaid_claim_pharm',
                   'Distinct IDs compared to elig tables', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {ids_demo_chk} {DBI::SQL(ifelse(ids_demo_chk >= 0, 'more', 'fewer'))} ",
                      "IDs than in the claims.final_mcaid_elig_demo table and ", 
                      "{ids_timevar_chk} {DBI::SQL(ifelse(ids_timevar_chk >= 0, 'more', 'fewer'))} ", 
                      "IDs than in the claims.final_mcaid_elig_timevar table')",
                   .con = db_claims))
}


#### Check format of ndc ####
ndc_format <- as.integer(odbc::dbGetQuery(
  conn = db_claims,
  "SELECT count(*) FROM claims.stage_mcaid_claim_pharm
  WHERE [ndc] IS NOT NULL AND 
    (len([ndc]) <> 11 OR isnumeric([ndc]) = 0)"))

if (ndc_format == 0) {
  ndc_format_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_pharm',
                   'Format of ndc field', 
                   'PASS', 
                   {Sys.time()}, 
                   'All rows of ndc formatted properly')",
                                .con = db_claims))
} else {
  ndc_format_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_pharm',
                   'Format of ndc field', 
                   'FAIL', 
                   {Sys.time()}, 
                   'ndc field had some rows with length != 11 or numeric')",
                                .con = db_claims))
}


#### Compare number of claim lines in current vs. prior analytic tables ####
if (DBI::dbExistsTable(db_claims,
                       DBI::Id(schema = "claims", table = "final_mcaid_claim_pharm"))) {
  num_rx_current <- DBI::dbGetQuery(db_claims,
                                    "SELECT YEAR([rx_fill_date]) AS [claim_year], COUNT(*) AS [current_num_rx]
 FROM claims.final_mcaid_claim_pharm
 GROUP BY YEAR([rx_fill_date]) ORDER BY YEAR([rx_fill_date])")
  
  num_rx_new <- DBI::dbGetQuery(db_claims,
                                "SELECT YEAR([rx_fill_date]) AS [claim_year], COUNT(*) AS [new_num_rx]
 FROM claims.stage_mcaid_claim_pharm
 GROUP BY YEAR([rx_fill_date]) ORDER by YEAR([rx_fill_date])")
  
  num_rx_overall <- left_join(num_rx_new, num_rx_current, by = "claim_year") %>%
    mutate(pct_change = round((new_num_rx - current_num_rx) / current_num_rx * 100, 4))
  
  # Write findings to metadata
  if (max(num_rx_overall$pct_change, na.rm = T) > 0 & 
      min(num_rx_overall$pct_change, na.rm = T) >= 0) {
    num_rx_fail <- 0
    DBI::dbExecute(conn = db_claims, 
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_pharm',
                   'Change in number of pharmacy claim rows', 
                   'PASS', 
                   {Sys.time()}, 
                   'The following years had more pharmacy claim rows than in the final schema table: ", 
                                  "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_rx_overall$claim_year[num_rx_overall$pct_change > 0], 
                                            pct = round(abs(num_rx_overall$pct_change[num_rx_overall$pct_change > 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                  .con = db_claims))
  } else if (min(num_rx_overall$pct_change, na.rm = T) + max(num_rx_overall$pct_change, na.rm = T) == 0) {
    num_rx_fail <- 1
    DBI::dbExecute(conn = db_claims, 
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_pharm',
                   'Change in number of pharmacy claim row', 
                   'FAIL', 
                   {Sys.time()}, 
                   'No change in the number of claim lines compared to final schema table')",
                                  .con = db_claims))
  } else if (min(num_rx_overall$pct_change, na.rm = T) < 0) {
    num_rx_fail <- 1
    DBI::dbExecute(conn = db_claims, 
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_pharm',
                   'Change in number of pharmacy claim row', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The following years had fewer pharmacy claim rows than in the final schema table: ", 
                                  "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_rx_overall$claim_year[num_rx_overall$pct_change < 0], 
                                            pct = round(abs(num_rx_overall$pct_change[num_rx_overall$pct_change < 0]), 2)),
                                 '{year} ({pct}% fewer)'), sep = ', ', last = ' and '))}')",
                                  .con = db_claims))
  }
} else {
  num_rx_fail <- 0
}



#### SUM UP FAILURES ####
fail_tot <- sum(ids_fail, ndc_format_fail, num_rx_fail)


#### CLEAN UP ####
rm(last_run)
rm(ids_demo_chk, ids_timevar_chk)
rm(ndc_format)
rm(num_rx_current, num_rx_new, num_rx_overall)
rm(ids_fail, ndc_format_fail, num_rx_fail)