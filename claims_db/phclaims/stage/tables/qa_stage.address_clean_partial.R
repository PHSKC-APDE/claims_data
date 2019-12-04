#### CODE TO QA STAGE.ADDRESS_CLEAN
# Alastair Matheson, PHSKC (APDE)
#
# 2019-12
#
### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


### Pull out run date of stage.mcaid_elig_timevar
last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.address_clean")[[1]])


### Check rows in stage vs ref
rows_stage <- as.integer(dbGetQuery(db_claims, "SELECT COUNT (*) AS row_cnt FROM stage.address_clean"))
rows_ref <- as.integer(dbGetQuery(db_claims, "SELECT COUNT (*) AS row_cnt FROM ref.address_clean"))

if (rows_stage < rows_ref) {
  dbGetQuery(db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'stage.address_clean',
                             'Row counts',
                             'FAIL',
                             {Sys.time()}, 
                             'Stage table has {rows_stage - rows_ref} fewer rows than ref table')",
                   .con = db_claims))
} else {
  dbGetQuery(db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'stage.address_clean',
                             'Row counts',
                             'PASS',
                             {Sys.time()}, 
                             'Stage table has {rows_stage - rows_ref} more rows than ref table')",
                   .con = db_claims))
}


### Check names of fields
names_stage <- names(odbc::dbGetQuery(conn = db_claims, "SELECT TOP (0) * FROM stage.address_clean"))
names_ref <- names(odbc::dbGetQuery(conn = db_claims, "SELECT TOP (0) * FROM ref.address_clean"))

if (min(names_stage == names_ref) == 0) {
  dbGetQuery(db_claims,
             glue::glue_sql("INSERT INTO metadata.qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'stage.address_clean',
                             'Field names',
                             'FAIL',
                             {Sys.time()}, 
                             'Stage table columns do not match ref table')",
                            .con = db_claims))
} else if (min(names_stage == names_ref) == 1) {
  dbGetQuery(db_claims,
             glue::glue_sql("INSERT INTO metadata.qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'stage.address_clean',
                             'Field names',
                             'FAIL',
                             {Sys.time()}, 
                             'Stage table columns match ref table')",
                            .con = db_claims))
} else {
  message("Something went wrong when checking columns in stage.address_clean")
}


### Clean up
rm(rows_stage, rows_ref)
rm(names_stage, names_ref)