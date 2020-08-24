#### CODE TO QA ref.stage_address_clean
# Alastair Matheson, PHSKC (APDE)
#
# 2019-12
#
### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


qa.address_clean_partial <- function(conn_db = NULL) {
  
  ### Pull out run date of stage.mcaid_elig_timevar
  last_run <- as.POSIXct(odbc::dbGetQuery(conn_db, "SELECT MAX (last_run) FROM ref.stage_address_clean")[[1]])
  
  
  ### Check rows in stage vs ref
  rows_stage <- as.integer(dbGetQuery(conn_db, "SELECT COUNT (*) AS row_cnt FROM ref.stage_address_clean"))
  rows_ref <- as.integer(dbGetQuery(conn_db, "SELECT COUNT (*) AS row_cnt FROM ref.address_clean"))
  
  if (rows_stage < rows_ref) {
    dbGetQuery(conn_db,
               glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'ref.stage_address_clean',
                             'Row counts',
                             'FAIL',
                             {Sys.time()}, 
                             'Stage table has {rows_stage - rows_ref} fewer rows than ref table')",
                              .con = conn_db))
    message(glue::glue("FAIL: Stage table has {rows_stage - rows_ref} fewer rows than ref table"))
  } else {
    dbGetQuery(conn_db,
               glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'ref.stage_address_clean',
                             'Row counts',
                             'PASS',
                             {Sys.time()}, 
                             'Stage table has {rows_stage - rows_ref} more rows than ref table')",
                              .con = conn_db))
    message(glue::glue("PASS: Stage table has {rows_stage - rows_ref} more rows than ref table"))
  }
  
  
  ### Check names of fields
  names_stage <- names(odbc::dbGetQuery(conn = conn_db, "SELECT TOP (0) * FROM ref.stage_address_clean"))
  names_ref <- names(odbc::dbGetQuery(conn = conn_db, "SELECT TOP (0) * FROM ref.address_clean"))
  
  if (min(names_stage == names_ref) == 0) {
    dbGetQuery(conn_db,
               glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'ref.stage_address_clean',
                             'Field names',
                             'FAIL',
                             {Sys.time()}, 
                             'Stage table columns do not match ref table')",
                              .con = conn_db))
    message("FAIL: Column order does not match between stage and ref.address_clean tables")
  } else if (min(names_stage == names_ref) == 1) {
    dbGetQuery(conn_db,
               glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'ref.stage_address_clean',
                             'Field names',
                             'PASS',
                             {Sys.time()}, 
                             'Stage table columns match ref table')",
                              .con = conn_db))
    message("PASS: Column order matches between stage and ref.address_clean tables")
  } else {
    message("FAIL: Something went wrong when checking columns in ref.stage_address_clean")
  }
  
  
  ### Clean up
  # rm(rows_stage, rows_ref) # Keep this to make sure the correct # rows are loaded to ref
  rm(names_stage, names_ref)
}
