# This code QAs table claims.final_mcaid_housing_status
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2026-03
# Jeremy Whitehurst (building on SQL from Eli Kern)
#
# QA checks:
# 1) No duplicate IDs
# 2) Check month-to-month trends

### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded


qa_stage_mcaid_housing_status <- function(conn = NULL,
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

  to_schema <- config[[server]][["schema"]]
  to_table <- config[[server]][["to_table"]]
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                     config[[server]][["qa_table"]])
  
  
  message("Running QA on ", to_schema, ".", to_table)
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  last_run <- as.POSIXct(DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}",
                         .con = conn))[[1]])
  
  
  #### Check for duplicate IDs ####
  ids_chk <- as.integer(DBI::dbGetQuery(
    conn, glue::glue_sql("
                          WITH temp1 AS (
                          SELECT id_mcaid, from_date, COUNT(*) AS row_count
                          FROM {`to_schema`}.{`to_table`}
                          GROUP BY id_mcaid, from_date)
                          
                          SELECT COUNT(*) AS qa_expect0
                          FROM temp1
                          WHERE row_count > 1",
                         .con = conn)))

  # Write findings to metadata
  if (ids_chk == 0) {
    ids_fail <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Confirmed there are zero duplicate IDs per [from_date]', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There are no duplicate IDs per [from_date]')",
                                  .con = conn_qa))
  } else {
    ids_fail <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'There are {ids_chk} duplicate IDs per [from_date]', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There are duplicate IDs per [from_date]')",
                                  .con = conn_qa))
  }
  
  
  #### Check for drastic changes (>=5% difference) in month-to-month trends ####
  diff_chk <- DBI::dbGetQuery(conn, 
                              glue::glue_sql("
                                             WITH tmp1 AS
                                              (SELECT b.year_month, COUNT(DISTINCT id_mcaid) AS id_dcount, ROW_NUMBER() OVER (ORDER BY b.year_month) AS num
                                                FROM claims.final_mcaid_housing_status AS a
                                                LEFT JOIN claims.ref_date AS b ON a.from_date = b.[date]
                                                GROUP BY b.year_month)
                                             SELECT COUNT(*) AS row_cnt, MAX(diff) AS max_diff FROM
	                                              (SELECT year_month, next_year_month, CAST(ROUND(diff * 100, 4) AS FLOAT) AS diff FROM
		                                              (SELECT a.year_month, b.year_month AS next_year_month, CAST(ABS(b.id_dcount - a.id_dcount) AS DECIMAL(10, 4)) / CAST(a.id_dcount AS DECIMAL(10, 4)) AS diff
		                                                FROM tmp1 AS a
		                                                INNER JOIN tmp1 AS b ON b.num = a.num + 1) AS z) AS y
                                              WHERE diff >= 5
                                             ", .con = conn))
  
  if (diff_chk[1,1] == 0) {
    diff_fail <- 0
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Confirmed there are zero months with >= 5% difference compared to previous months', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Checked for Month-to-Month trends of >= 5% difference')",
                                  .con = conn_qa))
  } else {
    diff_fail <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   '{diff_check[1,1]} months with up to {diff_check[1,2]}% difference', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There are trends of >= 5% difference')",
                                  .con = conn_qa))
  }
  
  
  fail_tot <- sum(ids_fail, diff_fail)
  return(fail_tot)
}
