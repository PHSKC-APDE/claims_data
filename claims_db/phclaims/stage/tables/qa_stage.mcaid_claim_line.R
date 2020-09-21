
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


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded


qa_stage_mcaid_claim_line_f <- function(conn = NULL,
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
  ids_demo_chk <- as.integer(DBI::dbGetQuery(conn,
                                             "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM {`to_schema`}.{`to_table`} AS a
  LEFT JOIN {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_demo AS b
  ON a.id_mcaid = b.id_mcaid
  WHERE b.id_mcaid IS NULL"))
  
  ids_timevar_chk <- as.integer(DBI::dbGetQuery(conn,
                                                "SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
  FROM {`to_schema`}.{`to_table`} AS a
  LEFT JOIN {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_demo AS b
  ON a.id_mcaid = b.id_mcaid
  WHERE b.id_mcaid IS NULL"))
  
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
  
  
  #### Check number of rows compared to raw ####
  rows_line <- as.integer(odbc::dbGetQuery(
    conn = conn, "SELECT COUNT(DISTINCT [claim_line_id]) FROM {`to_schema`}.{`to_table`}"))
  rows_raw <- as.integer(odbc::dbGetQuery(
    conn = conn, "SELECT COUNT(DISTINCT [CLM_LINE_TCN]) FROM {`from_schema`}.{`from_table`}"))
  
  if (rows_line == rows_raw) {
    rows_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Number of distinct claim lines compared to raw data', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were the same number of distinct claim lines as in the raw data')",
                                  .con = conn))
  } else {
    rows_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Number of distinct claim lines compared to raw data', 
                   'FAIL', 
                   {Sys.time()}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)} had {rows_line} distinct claim lines ", 
                                  "compared to {rows_raw} in {DBI::SQL(from_schema)}.{DBI::SQL(from_table)}')",
                                  .con = conn))
  }
  
  
  #### Check format of rev_code ####
  rev_format <- as.integer(odbc::dbGetQuery(
    conn = conn,
    "SELECT count(*) FROM {`to_schema`}.{`to_table`}
  WHERE rev_code IS NOT NULL AND (len(rev_code) <> 4 OR isnumeric(rev_code) = 0)"))
  
  if (rev_format == 0) {
    rev_code_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Format of rev_code field', 
                   'PASS', 
                   {Sys.time()}, 
                   'All rows of rev_code formatted properly')",
                                  .con = conn))
  } else {
    rev_code_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Format of rev_code field', 
                   'FAIL', 
                   {Sys.time()}, 
                   'rev_code field had some rows with length != 4 or characters')",
                                  .con = conn))
  }
  
  
  #### Check if any RAC codes do not join to reference table ####
  rac_chk <- as.integer(DBI::dbGetQuery(
    conn,
    glue::glue_sql("SELECT count(distinct 'RAC Code - ' + CAST([rac_code_line] AS VARCHAR(255)))
  FROM {`to_schema`}.{`to_table`} as a
  WHERE NOT EXISTS
  (SELECT 1 FROM {`ref_schema`}.{DBI::SQL(ref_table)}mcaid_rac_code as b
  WHERE a.rac_code_line = b.rac_code)", .con = conn)))
  
  
  
  # Write findings to metadata
  if (rac_chk < 50) {
    rac_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Almost all RAC codes join to reference table', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {rac_chk} RAC values not in {DBI::SQL(ref_schema)}.{DBI::SQL(ref_table)}mcaid_rac_code (acceptable is < 50)')",
                                  .con = conn))
  } else {
    rac_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Almost all RAC codes join to reference table', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {rac_chk} RAC not in {DBI::SQL(ref_schema)}.{DBI::SQL(ref_table)}mcaid_rac_code table (acceptable is < 50)')",
                                  .con = conn))
  }
  
  
  #### Compare number of claim lines in current vs. prior analytic tables ####
  if (DBI::dbExistsTable(conn, DBI::Id(schema = final_schema, table = paste0(final_table, "mcaid_claim_line")))) {
    num_claim_current <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS current_claim_line
 FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_line
 GROUP BY YEAR(first_service_date) ORDER BY YEAR(first_service_date)", .con = conn))
    
    num_claim_new <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS new_claim_line
 FROM {`to_schema`}.{`to_table`}
 GROUP BY YEAR(first_service_date) ORDER by YEAR(first_service_date)", .con = conn))
    
    num_claim_overall <- left_join(num_claim_new, num_claim_current, by = "claim_year") %>%
      mutate(pct_change = round((new_claim_line - current_claim_line) / current_claim_line * 100, 4))
    
    # Write findings to metadata
    if (max(num_claim_overall$pct_change, na.rm = T) > 0 & 
        min(num_claim_overall$pct_change, na.rm = T) >= 0) {
      num_claim_fail <- 0
      DBI::dbExecute(conn = conn, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of claim lines', 
                   'PASS', 
                   {Sys.time()}, 
                   'The following years had more claim lines than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_claim_overall$claim_year[num_claim_overall$pct_change > 0], 
                                            pct = round(abs(num_claim_overall$pct_change[num_claim_overall$pct_change > 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                    .con = conn))
    } else if (min(num_claim_overall$pct_change, na.rm = T) + max(num_claim_overall$pct_change, na.rm = T) == 0) {
      num_claim_fail <- 1
      DBI::dbExecute(conn = conn, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of claim lines', 
                   'FAIL', 
                   {Sys.time()}, 
                   'No change in the number of claim lines compared to final schema table')",
                                    .con = conn))
    } else if (min(num_claim_overall$pct_change, na.rm = T) < 0) {
      num_claim_fail <- 1
      DBI::dbExecute(conn = conn, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of claim lines', 
                   'FAIL', 
                   {Sys.time()}, 
                   'The following years had fewer claim lines than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_claim_overall$claim_year[num_claim_overall$pct_change < 0], 
                                            pct = round(abs(num_claim_overall$pct_change[num_claim_overall$pct_change < 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                    .con = conn))
    }
  } else {
    num_claim_fail <- 0
  }
  
  
  #### SUM UP FAILURES ####
  fail_tot <- sum(ids_fail, rows_fail, rev_code_fail, rac_fail, num_claim_fail)
  return(fail_tot)
}










