# This code QAs the stage mcaid claim header table
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-12
# Alastair Matheson (building on SQL from Philip Sylling)
#
# QA checks:
# 1) IDs are all found in the elig tables
# 2) Claim header is distinct
# 3) Check there were as many or more claims for each calendar year
# 4) Check there were as many or more ED visits for each calendar year


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded


qa_stage_mcaid_claim_header_f <- function(conn = NULL,
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
  stage_schema <- config[[server]][["stage_schema"]]
  stage_table <- ifelse(is.null(config[[server]][["stage_table"]]), '',
                        config[[server]][["stage_table"]])
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                     config[[server]][["qa_table"]])
  
  
  message("Running QA on ", to_schema, ".", to_table)
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  last_run <- as.POSIXct(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}",
                   .con = conn))[[1]])
  
  
  #### Check all IDs are also found in the elig_demo and time_var tables ####
  ids_demo_chk <- as.integer(DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
                         FROM {`to_schema`}.{`to_table`} AS a
                         LEFT JOIN {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_elig_demo AS b
                         ON a.id_mcaid = b.id_mcaid
                         WHERE b.id_mcaid IS NULL",
                         .con = conn)))
  
  ids_timevar_chk <- as.integer(DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
                         FROM {`to_schema`}.{`to_table`} AS a
                         LEFT JOIN {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_elig_timevar AS b
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
  
  
  #### Check claim headers are distinct ####
  cnt_claims <- DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT(claim_header_id) as rows_tot, COUNT(DISTINCT claim_header_id) as rows_distinct
                         FROM {`to_schema`}.{`to_table`}", .con = conn))
  
  # Write findings to metadata
  if (cnt_claims$rows_tot == cnt_claims$rows_distinct) {
    distinct_fail <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'All claim headers are distinct', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {cnt_claims$rows_tot} claim headers and all were distinct')",
                                  .con = conn_qa))
  } else {
    distinct_fail <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'All claim headers are distinct', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {cnt_claims$rows_tot} claim headers but {cnt_claims$rows_distinct} were distinct')",
                                  .con = conn_qa))
  }
  
  
  
  if (DBI::dbExistsTable(conn, DBI::Id(schema = final_schema, table = paste0(final_table, "mcaid_claim_header")))) {
    #### Compare number of claim headers in current vs. prior analytic tables ####
    
    num_header_current <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS current_num_header
                           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_header
                           GROUP BY YEAR(first_service_date) ORDER BY YEAR(first_service_date)",
                           .con = conn))
    
    num_header_new <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS new_num_header
                         FROM {`to_schema`}.{`to_table`}
                         GROUP BY YEAR(first_service_date) ORDER by YEAR(first_service_date)", .con = conn))
    
    num_header_overall <- left_join(num_header_new, num_header_current, by = "claim_year") %>%
      mutate(pct_change = round((new_num_header - current_num_header) / current_num_header * 100, 4))
    
    # Write findings to metadata
    if (max(num_header_overall$pct_change, na.rm = T) > 0 & 
        min(num_header_overall$pct_change, na.rm = T) >= 0) {
      num_header_fail <- 0
      DBI::dbExecute(conn = conn_qa, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of claim headers', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The following years had more claim headers than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_header_overall$claim_year[num_header_overall$pct_change > 0], 
                                            pct = round(abs(num_header_overall$pct_change[num_header_overall$pct_change > 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                    .con = conn_qa))
    } else if (min(num_header_overall$pct_change, na.rm = T) + max(num_header_overall$pct_change, na.rm = T) == 0) {
      num_header_fail <- 1
      DBI::dbExecute(conn = conn_qa, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of claim headers', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'No change in the number of claim headers compared to final schema table')",
                                    .con = conn_qa))
    } else if (min(num_header_overall$pct_change, na.rm = T) < 0) {
      num_header_fail <- 1
      DBI::dbExecute(conn = conn_qa, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of claim headers', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The following years had fewer claim headers than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_header_overall$claim_year[num_header_overall$pct_change < 0], 
                                            pct = round(abs(num_header_overall$pct_change[num_header_overall$pct_change < 0]), 2)),
                                 '{year} ({pct}% fewer)'), sep = ', ', last = ' and '))}')",
                                    .con = conn_qa))
    }
    
    
    
    #### Compare number of ED visits in current vs. prior analytic tables ####
    num_ed_current <- DBI::dbGetQuery(
      conn_qa, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS current_num_ed
                           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_header
                           GROUP BY YEAR(first_service_date) ORDER BY YEAR(first_service_date)",
                           .con = conn_qa))
    
    num_ed_new <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS new_num_ed
                         FROM {`to_schema`}.{`to_table`}
                         GROUP BY YEAR(first_service_date) ORDER by YEAR(first_service_date)", .con = conn))
    
    num_ed_overall <- left_join(num_ed_new, num_ed_current, by = "claim_year") %>%
      mutate(pct_change = round((new_num_ed - current_num_ed) / current_num_ed * 100, 4))
    
    # Write findings to metadata
    if (max(num_ed_overall$pct_change, na.rm = T) > 0 & 
        min(num_ed_overall$pct_change, na.rm = T) >= 0) {
      num_ed_fail <- 0
      DBI::dbExecute(conn = conn_qa, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of ED visits', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The following years had more ED visits than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(num_ed_overall$claim_year[num_ed_overall$pct_change > 0], 
                        sep = ', ', last = ' and '))}')",
                                    .con = conn_qa))
    } else if (min(num_ed_overall$pct_change, na.rm = T) + max(num_ed_overall$pct_change, na.rm = T) == 0) {
      num_ed_fail <- 1
      DBI::dbExecute(conn = conn_qa, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of ED visits', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'No change in the number of ED visits compared to final schema table')",
                                    .con = conn_qa))
    } else if (min(num_ed_overall$pct_change, na.rm = T) < 0) {
      num_ed_fail <- 1
      DBI::dbExecute(conn = conn_qa, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of ED visits', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The following years had fewer ED visits than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(num_ed_overall$claim_year[num_ed_overall$pct_change < 0], 
                        sep = ', ', last = ' and '))}')",
                                    .con = conn_qa))
    }
  } else {
    num_header_fail <- 0
    num_ed_fail <- 0
  }
  
  
  #### Could add in other checks here ####
  # Check against each temp table that goes into claim header, e.g., sum of mental_dx_rda_any
  
  
  #### SUM UP FAILURES ####
  fail_tot <- sum(distinct_fail, ids_fail, num_header_fail, num_ed_fail)
  return(fail_tot)
}
