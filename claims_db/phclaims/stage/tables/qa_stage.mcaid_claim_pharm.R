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


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded


qa_stage_mcaid_claim_pharm_f <- function(conn = NULL,
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
  last_run <- as.POSIXct(odbc::dbGetQuery(conn, "SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}")[[1]])
  
  
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
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Distinct IDs compared to elig tables', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were the same number of IDs as in the final mcaid_elig_demo ", 
                                  "and mcaid_elig_timevar tables')",
                                  .con = conn))
  } else {
    ids_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Distinct IDs compared to elig tables', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {ids_demo_chk} {DBI::SQL(ifelse(ids_demo_chk >= 0, 'more', 'fewer'))} ",
                                  "IDs than in the final mcaid_elig_demo table and ", 
                                  "{ids_timevar_chk} {DBI::SQL(ifelse(ids_timevar_chk >= 0, 'more', 'fewer'))} ", 
                                  "IDs than in the final mcaid_elig_timevar table')",
                                  .con = conn))
  }
  
  
  #### Check format of ndc ####
  ndc_format <- as.integer(odbc::dbGetQuery(
    conn = conn,
    glue::glue_sql("SELECT count(*) FROM {`to_schema`}.{`to_table`}
                 WHERE [ndc] IS NOT NULL AND 
                 (len([ndc]) <> 11 OR isnumeric([ndc]) = 0)")))
  
  if (ndc_format == 0) {
    ndc_format_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Format of ndc field', 
                   'PASS', 
                   {Sys.time()}, 
                   'All rows of ndc formatted properly')",
                                  .con = conn))
  } else {
    ndc_format_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Format of ndc field', 
                   'FAIL', 
                   {Sys.time()}, 
                   'ndc field had some rows with length != 11 or numeric')",
                                  .con = conn))
  }
  
  
  #### Compare number of claim lines in current vs. prior analytic tables ####
  if (DBI::dbExistsTable(conn,
                         DBI::Id(schema = final_schema, table = paste0(final_table, "mcaid_claim_pharm")))) {
    
    num_rx_current <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(rx_fill_date) AS claim_year, COUNT(*) AS current_num_rx
                           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_pharm
                           GROUP BY YEAR(rx_fill_date) ORDER BY YEAR(rx_fill_date)",
                           .con = conn))
    
    num_rx_new <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(rx_fill_date) AS claim_year, COUNT(*) AS new_num_rx
                         FROM {`to_schema`}.{`to_table`}
                         GROUP BY YEAR(rx_fill_date) ORDER by YEAR(rx_fill_date", .con = conn))
    
    num_rx_overall <- left_join(num_rx_new, num_rx_current, by = "claim_year") %>%
      mutate(pct_change = round((new_num_rx - current_num_rx) / current_num_rx * 100, 4))
    
    # Write findings to metadata
    if (max(num_rx_overall$pct_change, na.rm = T) > 0 & 
        min(num_rx_overall$pct_change, na.rm = T) >= 0) {
      num_rx_fail <- 0
      DBI::dbExecute(conn = conn, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of pharmacy claim rows', 
                   'PASS', 
                   {Sys.time()}, 
                   'The following years had more pharmacy claim rows than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_rx_overall$claim_year[num_rx_overall$pct_change > 0], 
                                            pct = round(abs(num_rx_overall$pct_change[num_rx_overall$pct_change > 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                    .con = conn))
    } else if (min(num_rx_overall$pct_change, na.rm = T) + max(num_rx_overall$pct_change, na.rm = T) == 0) {
      num_rx_fail <- 1
      DBI::dbExecute(conn = conn, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of pharmacy claim row', 
                   'FAIL', 
                   {Sys.time()}, 
                   'No change in the number of claim lines compared to final schema table')",
                                    .con = conn))
    } else if (min(num_rx_overall$pct_change, na.rm = T) < 0) {
      num_rx_fail <- 1
      DBI::dbExecute(conn = conn, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of pharmacy claim row', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The following years had fewer pharmacy claim rows than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_rx_overall$claim_year[num_rx_overall$pct_change < 0], 
                                            pct = round(abs(num_rx_overall$pct_change[num_rx_overall$pct_change < 0]), 2)),
                                 '{year} ({pct}% fewer)'), sep = ', ', last = ' and '))}')",
                                    .con = conn))
    }
  } else {
    num_rx_fail <- 0
  }
  
  
  
  #### SUM UP FAILURES ####
  fail_tot <- sum(ids_fail, ndc_format_fail, num_rx_fail)
  return(fail_tot)
}
