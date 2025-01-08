# This code QAs table claims.stage_mcaid_claim_pref_episode
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2024-06
# Jeremy Whitehurst (building on SQL from Spencer Hensley)
#
# QA checks:
# 1) IDs are all found in the elig tables


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded


qa_stage_mcaid_claim_preg_episode_f <- function(conn = NULL,
                                                conn_qa = NULL,
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
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Distinct IDs compared to elig tables', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were the same number of IDs as in the final mcaid_elig_demo ", 
                                  "and mcaid_elig_timevar tables')",
                                  .con = conn_qa))
  } else {
    ids_fail <- 1
    DBI::dbExecute(conn = conn_qa,
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
                                  .con = conn_qa))
  }  
  
  min_age <- as.integer(DBI::dbGetQuery(conn, glue::glue_sql("select count(*) from {`to_schema`}.{`to_table`} where age_at_outcome < 12", .con = conn)))
  if (min_age == 0) {
    qa1 <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Minimum age >= 12 as expected', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Minimum age >= 12 as expected')",
                                  .con = conn_qa))
  } else {
    qa1 <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Minimum age is under 12, lower than expected', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   '{min_age} row(s) with age lower than expected minimum (12)')",
                                  .con = conn_qa))
  }
           
  max_age <- as.integer(DBI::dbGetQuery(conn, glue::glue_sql("select count(*) from {`to_schema`}.{`to_table`} where age_at_outcome > 55", .con = conn)))
  if (max_age == 0) {
    qa2 <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Maximum age <= 55 as expected', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Maximum age <= 55 as expected')",
                                  .con = conn_qa))
  } else {
    qa2 <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Maximum age is over 55, higher than expected', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   '{max_age} row(s) with age higher than expected maximum (55)')",
                                  .con = conn_qa))
  }
  
  null_date <- as.integer(DBI::dbGetQuery(conn, glue::glue_sql("select count(*) from {`to_schema`}.{`to_table`} where preg_start_date is null or preg_end_date is null;", .con = conn)))
  if (null_date == 0) {
    qa3 <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'All rows with non-null start and end dates', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'All rows with non-null start and end dates')",
                                  .con = conn_qa))
  } else {
    qa3 <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Rows with null start or end dates', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   '{null_date} row(s) with null start or end dates')",
                                  .con = conn_qa))
  }
  
  null_ga <- as.integer(DBI::dbGetQuery(conn, glue::glue_sql("select count(*) from {`to_schema`}.{`to_table`} where valid_ga = 1 and (ga_days is null or ga_weeks is null or ga_estimation_step is null);", .con = conn)))
  if (null_ga == 0) {
    qa4 <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'All valid GA rows with non-null GA columns', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'All valid GA rows with non-null GA columns')",
                                  .con = conn_qa))
  } else {
    qa4 <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Valid GA rows with null GA columns', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   '{null_ga} valid GA row(s) with null GA columns')",
                                  .con = conn_qa))
  }
  
  end_types <- as.integer(DBI::dbGetQuery(conn, glue::glue_sql("select count(distinct preg_endpoint) from {`to_schema`}.{`to_table`};", .con = conn)))
  if (end_types == 7) {
    qa5 <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Expected # of distict preg endpoint types (7)', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Expected # of distict preg endpoint types (7)')",
                                  .con = conn_qa))
  } else {
    qa5 <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Incorrect # of distict preg endpoint types (expecting 7)', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   '{end_tpes} distinct preg endpoint types (expecting 7)')",
                                  .con = conn_qa))
  }
  
  null_lb <- as.integer(DBI::dbGetQuery(conn, glue::glue_sql("select count(*) from {`to_schema`}.{`to_table`} where preg_endpoint = 'lb' and valid_ga = 1 and lb_type is null;", .con = conn)))
  if (null_lb == 0) {
    qa6 <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'All LB records with valid GA with non-null lb_type', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'All LB records with valid GA with non-null lb_type')",
                                  .con = conn_qa))
  } else {
    qa6 <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'LB records with valid GA with null lb_type', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   '{null_lb} LB row(s) with valid GA with null lb_type')",
                                  .con = conn_qa))
  }
  #### SUM UP FAILURES ####
  fail_tot <- sum(ids_fail, qa1, qa2, qa3, qa4, qa5, qa6)
  return(fail_tot)
}
