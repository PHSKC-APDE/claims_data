# This code QAs table claims.stage_mcaid_claim_moud
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2024-06
# Jeremy Whitehurst (building on SQL from Eli Kern)
#
# QA checks:
# 1) IDs are all found in the elig tables
# 2) Check for new NDCs
# 3) Check there were as many or more NDCs for each calendar year


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded


qa_stage_mcaid_claim_moud_f <- function(conn = NULL,
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
  final_schema <- config[[server]][["final_schema"]]
  final_table <- ifelse(is.null(config[[server]][["final_table"]]), '',
                        config[[server]][["final_table"]])
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- ifelse(is.null(config[[server]][["ref_table"]]), '',
                      config[[server]][["ref_table"]])
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                     config[[server]][["qa_table"]])
  
  
  message("Running QA on ", to_schema, ".", to_table)
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  last_run <- as.POSIXct(DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}",
                         .con = conn))[[1]])
  
  
  #### Check all IDs are also found in the elig_demo and time_var tables ####
  ids_demo_chk <- as.integer(DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
                         FROM {`to_schema`}.{`to_table`} AS a
                         LEFT JOIN {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_demo AS b
                         ON a.id_mcaid = b.id_mcaid
                         WHERE b.id_mcaid IS NULL",
                         .con = conn)))
  
  ids_timevar_chk <- as.integer(DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
                         FROM {`to_schema`}.{`to_table`} AS a
                         LEFT JOIN {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_timevar AS b
                         ON a.id_mcaid = b.id_mcaid
                         WHERE b.id_mcaid IS NULL",
                         .con = conn)))
  
  # Write findings to metadata
  if (ids_demo_chk == 0 & ids_timevar_chk == 0) {
    ids_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Distinct IDs compared to elig tables', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were the same number of IDs as in the final mcaid_elig_demo ", 
                                  "and mcaid_elig_timevar tables')",
                                  .con = conn))
  } else {
    ids_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Distinct IDs compared to elig tables', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {ids_demo_chk} {DBI::SQL(ifelse(ids_demo_chk >= 0, 'more', 'fewer'))} ",
                                  "IDs than in the final mcaid_elig_demo table and ", 
                                  "{ids_timevar_chk} {DBI::SQL(ifelse(ids_timevar_chk >= 0, 'more', 'fewer'))} ", 
                                  "IDs than in the final mcaid_elig_timevar table')",
                                  .con = conn))
  }
  
  
  #### Check for new NDCs ####
  ndc_cnt <- DBI::dbGetQuery(conn, glue::glue_sql("select * from ##mcaid_moud_pharm_2 where admin_method is null ", .con = conn))
  
  if (nrow(ndc_cnt) == 0) {
    qa_check_1 <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'No new NDCs', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'All rows of ndc formatted properly')",
                                  .con = conn))
  } else {
    qa_check_1 <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   '{nrow(ndc_cnt)} new NDC(s) missing from ref.ndc_codes', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'ndc field had some rows with length != 11 or numeric')",
                                  .con = conn))
  }
  
  
  #### Check for no rows with unspec_proc_flag and non-zero MOUD supply ####
  non_zero <- DBI::dbGetQuery(conn, 
                              glue::glue_sql("select count(*) from {`to_schema`}.{`to_table`} where unspec_proc_flag = 1 and moud_days_supply > 0;",
                                             .con = conn))
  
  # Write findings to metadata
  if (nrow(non_zero) == 0) {
    qa_check_2 <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'No rows with unspec_proc_flag AND non-zero MOUD supply', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'All rows of ndc formatted properly')",
                                  .con = conn))
  } else {
    qa_check_2 <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   '{nrow(non_zero)} row(s) with unspec_proc_flag AND non-zero MOUD supply', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'rows where ndc field not formatted properly')",
                                  .con = conn))
  }
  
  #### Check for no rows with more than one type of MOUD flag ####
  multi_moud <- DBI::dbGetQuery(conn, 
                                glue::glue_sql("select count(*) from {`to_schema`}.{`to_table`} where moud_flag_count > 1;",
                                               .con = conn))
  
  # Write findings to metadata
  if (nrow(multi_moud) == 0) {
    qa_check_3 <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'No rows with more than one type of MOUD flag', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'All rows of ndc formatted properly')",
                                  .con = conn))
  } else {
    qa_check_3 <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   '{nrow(non_zero)} rows with more than one type of MOUD flag', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'ndc field had some rows with more than one type of MOUD flag')",
                                  .con = conn))
  }
  
  
  #### SUM UP FAILURES ####
  fail_tot <- sum(ids_fail, qa_check_1, qa_check_2, qa_check_3)
  return(fail_tot)
}
