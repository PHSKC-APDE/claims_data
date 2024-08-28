
# This code QAs table claims.stage_mcaid_claim_procedure
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-12
# Alastair Matheson (building on SQL from Philip Sylling)
#
# QA checks:
# 1) IDs are all found in the elig tables
# 2) Procedure codes are formatted appropriately
# 3) procedure_code_number falls in an acceptable range
# 4) Check there were as many or more procedure codes for each calendar year


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded


qa_stage_mcaid_claim_procedure_f <- function(conn = NULL,
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
  last_run <- as.POSIXct(odbc::dbGetQuery(
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
  
  
  
  #### Check format of procedure codes ####
  procedure_format_chk <- as.integer(DBI::dbGetQuery(
    conn = conn, glue::glue_sql("WITH CTE AS
                         (SELECT
                           CASE WHEN LEN([procedure_code]) = 5 AND ISNUMERIC([procedure_code]) = 1 THEN 'CPT Category I'
                           WHEN LEN([procedure_code]) = 5 AND 
                              ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND 
                              SUBSTRING([procedure_code], 5, 1) = 'F' THEN 'CPT Category II'
                           WHEN LEN([procedure_code]) = 5 AND 
                              ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND 
                              SUBSTRING([procedure_code], 5, 1) = 'T' THEN 'CPT Category III'
                           WHEN LEN([procedure_code]) = 5 AND 
                              ISNUMERIC(SUBSTRING([procedure_code], 1, 4)) = 1 AND 
                              SUBSTRING([procedure_code], 5, 1) IN ('M', 'U') THEN 'CPT Other'
                           WHEN LEN([procedure_code]) = 5 AND 
                              SUBSTRING([procedure_code], 1, 1) LIKE '[A-Z]' AND 
                              ISNUMERIC(SUBSTRING([procedure_code], 2, 4)) = 1 THEN 'HCPCS'
                           WHEN LEN([procedure_code]) IN (3, 4) AND 
                              ISNUMERIC([procedure_code]) = 1 THEN 'ICD-9-PCS'
                           WHEN LEN([procedure_code]) = 7 THEN 'ICD-10-PCS'
                           ELSE 'UNKNOWN' END AS [code_system]
                           ,*
                             FROM {`to_schema`}.{`to_table`}
                         )
                         
                         SELECT COUNT(DISTINCT [procedure_code])
                         FROM CTE
                         WHERE [code_system] = 'UNKNOWN';",
                       .con = conn)))
  
  
  # Write findings to metadata
  if (procedure_format_chk < 50) {
    procedure_format_fail <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Format of procedure codes', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {procedure_format_chk} distinct procedure codes with an unknown format (<50 ok)')",
                                  .con = conn_qa))
  } else {
    procedure_format_fail <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Format of procedure codes', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {procedure_format_chk} distinct procedure codes with an unknown format')",
                                  .con = conn_qa))
  }
  
  
  #### Check that procedure_code_number in ('01':'12','line') ####
  procedure_num_chk <- as.integer(
    DBI::dbGetQuery(conn,
                    glue::glue_sql("SELECT count([procedure_code_number])
                  FROM {`to_schema`}.{`to_table`}
                  where [procedure_code_number] not in
                                 ('01','02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', 'line')", 
                                   .con = conn)))
  
  # Write findings to metadata
  if (procedure_num_chk == 0) {
    procedure_num_fail <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'procedure_code_number = 01-12 or line', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'All procedure_code_number values were 01:12 or line')",
                                  .con = conn_qa))
  } else {
    procedure_num_fail <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'procedure_code_number = 01-12 or line', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {procedure_num_chk} procedure_code_number values not 01 through 12 or line')",
                                  .con = conn_qa))
  }
  
  
  #### Compare number of procedure codes in current vs. prior analytic tables ####
  if (DBI::dbExistsTable(conn,
                         DBI::Id(schema = final_schema, table = paste0(final_table, "mcaid_claim_procedure")))) {
    
    
    num_procedure_current <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS current_num_procedure
                           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_procedure
                           GROUP BY YEAR(first_service_date) ORDER BY YEAR(first_service_date)",
                           .con = conn))
    
    num_procedure_new <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS new_num_procedure
                         FROM {`to_schema`}.{`to_table`}
                         GROUP BY YEAR(first_service_date) ORDER by YEAR(first_service_date)", .con = conn))
    
    num_procedure_overall <- left_join(num_procedure_new, num_procedure_current, by = "claim_year") %>%
      mutate_at(vars(new_num_procedure, current_num_procedure), list(~ replace_na(., 0))) %>%
      mutate(pct_change = round((new_num_procedure - current_num_procedure) / current_num_procedure * 100, 4))
    
    # Write findings to metadata
    if (max(num_procedure_overall$pct_change, na.rm = T) > 0 & 
        min(num_procedure_overall$pct_change, na.rm = T) >= 0) {
      num_procedure_fail <- 0
      DBI::dbExecute(conn = conn_qa, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of procedures', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The following years had more procedures than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_procedure_overall$claim_year[num_procedure_overall$pct_change > 0], 
                                            pct = round(abs(num_procedure_overall$pct_change[num_procedure_overall$pct_change > 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                    .con = conn_qa))
    } else if (min(num_procedure_overall$pct_change, na.rm = T) + max(num_procedure_overall$pct_change, na.rm = T) == 0) {
      num_procedure_fail <- 1
      DBI::dbExecute(conn = conn_qa, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of procedures', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'No change in the number of procedures compared to final schema table')",
                                    .con = conn_qa))
    } else if (min(num_procedure_overall$pct_change, na.rm = T) < 0) {
      num_procedure_fail <- 1
      DBI::dbExecute(conn = conn_qa, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of procedures', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The following years had fewer procedures than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_procedure_overall$claim_year[num_procedure_overall$pct_change < 0], 
                                            pct = round(abs(num_procedure_overall$pct_change[num_procedure_overall$pct_change < 0]), 2)),
                                 '{year} ({pct}% fewer)'), sep = ', ', last = ' and '))}')",
                                    .con = conn_qa))
    }
  } else {
    num_procedure_fail <- 0
  }
  
  
  #### SUM UP FAILURES ####
  fail_tot <- sum(ids_fail, procedure_format_fail, procedure_num_fail, num_procedure_fail)
  return(fail_tot)
}
