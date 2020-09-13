
# This code QAs table claims.stage_mcaid_claim_procedure
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-12
# Alastair Matheson (building on SQL from Philip Sylling)
#
# QA checks:
# 1) IDs are all found in the elig tables
# 2) Procedure codes are formatted appropriately
# 3) procedure_code_number falls in an acceptable range
# 4) (Almost) All dx codes are found in the ref table
# 5) Check there were as many or more diagnoses for each calendar year


#### SET UP FUNCTIONS, ETC. ####
if (!exists("db_claims")) {
  db_claims <- dbConnect(odbc(), "PHClaims")  
}

last_run <- as.POSIXct(DBI::dbGetQuery(
  db_claims, "SELECT MAX (last_run) FROM claims.stage_mcaid_claim_procedure")[[1]])


#### Check all IDs are also found in the elig_demo and time_var tables ####
ids_demo_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM claims.stage_mcaid_claim_procedure AS a
  LEFT JOIN claims.final_mcaid_elig_demo AS b
  ON a.id_mcaid = b.id_mcaid
  WHERE b.id_mcaid IS NULL"))

ids_timevar_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM claims.stage_mcaid_claim_procedure AS a
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
                   'claims.stage_mcaid_claim_procedure',
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
                   'claims.stage_mcaid_claim_procedure',
                   'Distinct IDs compared to elig tables', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {ids_demo_chk} {DBI::SQL(ifelse(ids_demo_chk >= 0, 'more', 'fewer'))} ",
                      "IDs than in the claims.final_mcaid_elig_demo table and ", 
                      "{ids_timevar_chk} {DBI::SQL(ifelse(ids_timevar_chk >= 0, 'more', 'fewer'))} ", 
                      "IDs than in the claims.final_mcaid_elig_timevar table')",
                   .con = db_claims))
}


#### Check format of procedure codes ####
procedure_format_chk <- as.integer(DBI::dbGetQuery(db_claims,
"WITH CTE AS
(
SELECT
 CASE WHEN LEN([procedure_code]) = 5 AND ISNUMERIC([procedure_code]) = 1 THEN 'CPT Category I'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'F' THEN 'CPT Category II'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) = 'T' THEN 'CPT Category III'
      WHEN LEN([procedure_code]) = 5 AND ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND SUBSTRING([procedure_code], 5, 1) IN ('M', 'U') THEN 'CPT Other'
      WHEN LEN([procedure_code]) = 5 AND SUBSTRING([procedure_code], 1, 1) LIKE '[A-Z]' AND ISNUMERIC(SUBSTRING([procedure_code], 2, 4)) = 1 THEN 'HCPCS'
      WHEN LEN([procedure_code]) IN (3, 4) AND ISNUMERIC([procedure_code]) = 1 THEN 'ICD-9-PCS'
      WHEN LEN([procedure_code]) = 7 THEN 'ICD-10-PCS'
	  ELSE 'UNKNOWN' END AS [code_system]
,*
FROM claims.stage_mcaid_claim_procedure
)

SELECT COUNT(DISTINCT [procedure_code])
FROM CTE
WHERE [code_system] = 'UNKNOWN';"))


# Write findings to metadata
if (procedure_format_chk < 50) {
  procedure_format_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_procedure',
                   'Format of procedure codes', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {procedure_format_chk} distinct procedure codes with an unknown format (<50 ok)')",
                                .con = db_claims))
} else {
  procedure_format_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_procedure',
                   'Format of procedure codes', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {procedure_format_chk} distinct procedure codes with an unknown format')",
                                .con = db_claims))
}


#### Check that procedure_code_number in ('01':'12','line') ####
procedure_num_chk <- as.integer(
  DBI::dbGetQuery(db_claims,
  "select count([procedure_code_number])
  from claims.stage_mcaid_claim_procedure
  where [procedure_code_number] not in
  ('01','02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', 'line')"))

# Write findings to metadata
if (procedure_num_chk == 0) {
  procedure_num_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_procedure',
                   'procedure_code_number = 01-12 or line', 
                   'PASS', 
                   {Sys.time()}, 
                   'All procedure_code_number values were 01:12 or line')",
                                .con = db_claims))
} else {
  procedure_num_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_procedure',
                   'procedure_code_number = 01-12 or line', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {procedure_num_chk} procedure_code_number values not 01 through 12 or line')",
                                .con = db_claims))
}


#### Check if any diagnosis codes do not join to reference table ####
procedure_chk <- as.integer(DBI::dbGetQuery(db_claims,
  "select count(distinct [procedure_code]) 
  from claims.stage_mcaid_claim_procedure as a 
  where not exists
  (
    select 1
    from claims.ref_pcode as b
    where a.[procedure_code] = b.[pcode])"))

# Write findings to metadata
if (procedure_chk < 200) {
  procedure_fail <- 0
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_procedure',
                   'Almost all procedure codes join to reference table', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {procedure_chk} procedure codes not in ref.pcode (acceptable is < 200)')",
                                .con = db_claims))
} else {
  procedure_fail <- 1
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_procedure',
                   'Almost all procedure codes join to reference table', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {procedure_chk} procedure codes not in ref.pcode table (acceptable is < 200)')",
                                .con = db_claims))
}



#### Compare number of dx codes in current vs. prior analytic tables ####
if (DBI::dbExistsTable(db_claims,
                       DBI::Id(schema = "claims", table = "final_mcaid_claim_procedure"))) {
  
  
  num_procedure_current <- DBI::dbGetQuery(db_claims,
                                           "SELECT YEAR([first_service_date]) AS [claim_year], COUNT(*) AS [current_num_procedure]
 FROM claims.final_mcaid_claim_procedure
 GROUP BY YEAR([first_service_date]) ORDER BY YEAR([first_service_date])")
  
  num_procedure_new <- DBI::dbGetQuery(db_claims,
                                       "SELECT YEAR([first_service_date]) AS [claim_year], COUNT(*) AS [new_num_procedure]
 FROM claims.stage_mcaid_claim_procedure
 GROUP BY YEAR([first_service_date]) ORDER by YEAR([first_service_date])")
  
  num_procedure_overall <- left_join(num_procedure_new, num_procedure_current, by = "claim_year") %>%
    mutate(pct_change = round((new_num_procedure - current_num_procedure) / current_num_procedure * 100, 4))
  
  # Write findings to metadata
  if (max(num_procedure_overall$pct_change, na.rm = T) > 0 & 
      min(num_procedure_overall$pct_change, na.rm = T) >= 0) {
    num_procedure_fail <- 0
    DBI::dbExecute(conn = db_claims, 
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_procedure',
                   'Change in number of procedures', 
                   'PASS', 
                   {Sys.time()}, 
                   'The following years had more procedures than in the final schema table: ", 
                                  "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_procedure_overall$claim_year[num_procedure_overall$pct_change > 0], 
                                            pct = round(abs(num_procedure_overall$pct_change[num_procedure_overall$pct_change > 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                  .con = db_claims))
  } else if (min(num_procedure_overall$pct_change, na.rm = T) + max(num_procedure_overall$pct_change, na.rm = T) == 0) {
    num_procedure_fail <- 1
    DBI::dbExecute(conn = db_claims, 
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_procedure',
                   'Change in number of procedures', 
                   'FAIL', 
                   {Sys.time()}, 
                   'No change in the number of procedures compared to final schema table')",
                                  .con = db_claims))
  } else if (min(num_procedure_overall$pct_change, na.rm = T) < 0) {
    num_procedure_fail <- 1
    DBI::dbExecute(conn = db_claims, 
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'claims.stage_mcaid_claim_procedure',
                   'Change in number of procedures', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The following years had fewer procedures than in the final schema table: ", 
                                  "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_procedure_overall$claim_year[num_procedure_overall$pct_change < 0], 
                                            pct = round(abs(num_procedure_overall$pct_change[num_procedure_overall$pct_change < 0]), 2)),
                                 '{year} ({pct}% fewer)'), sep = ', ', last = ' and '))}')",
                                  .con = db_claims))
  }
} else {
  num_procedure_fail <- 0
}



#### SUM UP FAILURES ####
fail_tot <- sum(ids_fail, procedure_format_fail, procedure_num_fail,
                procedure_fail, num_procedure_fail)



#### CLEAN UP ####
rm(last_run)
rm(ids_demo_chk, ids_timevar_chk)
rm(procedure_format_chk)
rm(procedure_num_chk)
rm(procedure_chk)
rm(num_procedure_current, num_procedure_new, num_procedure_overall)
rm(ids_fail, procedure_format_fail, procedure_num_fail, procedure_fail, num_procedure_fail)
