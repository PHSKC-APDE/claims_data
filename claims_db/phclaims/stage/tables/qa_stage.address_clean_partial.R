#### CODE TO QA ref.stage_address_clean
# Alastair Matheson, PHSKC (APDE)
#
# 2019-12
#
### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

qa.address_clean_partial <- function(conn = NULL,
                                     server = c("hhsaw", "phclaims"),
                                     config = NULL,
                                     get_config = F) {
  
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- config[[server]][["ref_table"]]
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                     config[[server]][["qa_table"]])
  
  ### Pull out run date of stage.mcaid_elig_timevar
  last_run <- as.POSIXct(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT MAX (last_run) FROM {`ref_schema`}.{`ref_table`}",
                         .con = conn))[[1]])
  
  
  ### Check rows in stage vs ref
  rows_stage <- as.integer(dbGetQuery(conn, 
                                      glue::glue_sql("SELECT COUNT (*) AS row_cnt FROM {`to_schema`}.{`to_table`}",
                                                     .con = conn)))
  rows_ref <- as.integer(dbGetQuery(conn, 
                                    glue::glue_sql("SELECT COUNT (*) AS row_cnt FROM {`ref_schema`}.{`ref_table`}",
                                                   .con = conn)))
  
  if (rows_stage < rows_ref) {
    row_qa_fail <- 1
    dbGetQuery(conn,
               glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                             'Row counts',
                             'FAIL',
                             {Sys.time()}, 
                             'Stage table has {rows_stage - rows_ref} fewer rows than ref table')",
                              .con = conn))
    message(glue::glue("FAIL: Stage table has {rows_stage - rows_ref} fewer rows than ref table"))
  } else {
    row_qa_fail <- 0
    dbGetQuery(conn,
               glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                             'Row counts',
                             'PASS',
                             {Sys.time()}, 
                             'Stage table has {rows_stage - rows_ref} more rows than ref table')",
                              .con = conn))
    message(glue::glue("PASS: Stage table has {rows_stage - rows_ref} more rows than ref table"))
  }
  
  
  ### Check names of fields
  names_stage <- names(odbc::dbGetQuery(conn = conn, 
                                        glue::glue_sql("SELECT TOP (0) * FROM {`to_schema`}.{`to_table`}",
                                                       .con = conn)))
  names_ref <- names(odbc::dbGetQuery(conn = conn, 
                                      glue::glue_sql("SELECT TOP (0) * FROM {`ref_schema`}.{`ref_table`}",
                                                     .con = conn)))
  
  if (min(names_stage == names_ref) == 0) {
    col_qa_fail <- 1
    dbGetQuery(conn,
               glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                             'Field names',
                             'FAIL',
                             {Sys.time()}, 
                             'Stage table columns do not match ref table')",
                              .con = conn))
    message("FAIL: Column order does not match between stage and ref.address_clean tables")
  } else if (min(names_stage == names_ref) == 1) {
    col_qa_fail <- 0
    dbGetQuery(conn,
               glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                             'Field names',
                             'PASS',
                             {Sys.time()}, 
                             'Stage table columns match ref table')",
                              .con = conn))
    message("PASS: Column order matches between stage and ref.address_clean tables")
  } else {
    col_qa_fail <- 1
    message("FAIL: Something went wrong when checking columns in ref.stage_address_clean")
  }
  
  
  
  ### Summarize
  qa_total <- row_qa_fail + col_qa_fail
  return(qa_total)
}
