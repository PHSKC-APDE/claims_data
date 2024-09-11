# Alastair Matheson
# 2019-05

# Code to QA stage_mcaid_elig_demo



### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded
# load_only = only enter new values to that table, no other QA

qa_mcaid_elig_demo_f <- function(conn = NULL,
                                 conn_qa = NULL,
                                 server = c("hhsaw", "phclaims"),
                                 config = NULL,
                                 get_config = F,
                                 load_only = F) {
  
  
  # If this is the first time ever loading data, skip some checks.
  #   Otherwise, check against existing QA values
  
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
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                      config[[server]][["qa_table"]])
  
  
  message("Running QA on ", to_schema, ".", to_table)
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # Rows in current table
  row_count <- as.numeric(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}",
                         .con = conn)))
  
  
  ### Pull out run date of stage_mcaid_elig_demo
  last_run <- as.POSIXct(odbc::dbGetQuery(conn, 
                                          glue::glue_sql("SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}",
                                                         .con = conn))[[1]])
  
  if (load_only == F) {
    #### COUNT NUMBER OF ROWS ####
    # Pull in the reference value
    previous_rows <- as.numeric(
      odbc::dbGetQuery(conn_qa, 
                       glue::glue_sql("SELECT a.qa_value FROM
                       (SELECT * FROM {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid_values
                         WHERE table_name = '{DBI::SQL(`to_schema`)}.{DBI::SQL(`to_table`)}' AND
                          qa_item = 'row_count') a
                       INNER JOIN
                       (SELECT MAX(qa_date) AS max_date 
                         FROM {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid_values
                         WHERE table_name = '{DBI::SQL(`to_schema`)}.{DBI::SQL(`to_table`)}' AND
                          qa_item = 'row_count') b
                       ON a.qa_date = b.max_date",
                                      .con = conn_qa)))
    
    row_diff <- row_count - previous_rows
    
    if (row_diff < 0) {
      row_qa_fail <- 1
      DBI::dbExecute(
        conn = conn_qa,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Number new rows compared to most recent run', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {row_diff} fewer rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn_qa))
      
      message(glue::glue("Fewer rows than found last time.  
                  Check {qa_schema}.{qa_table}qa_mcaid for details (last_run = {format(last_run, usetz = FALSE)}"))
    } else {
      row_qa_fail <- 0
      DBI::dbExecute(
        conn = conn_qa,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Number new rows compared to most recent run', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {row_diff} more rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn_qa))
    }
    
    
    #### CHECK DISTINCT IDS = NUMBER OF ROWS ####
    id_count <- as.numeric(odbc::dbGetQuery(
      conn, glue::glue_sql("SELECT COUNT (DISTINCT id_mcaid) 
                         FROM {`to_schema`}.{`to_table`}", .con = conn)))
    
    if (id_count != row_count) {
      id_distinct_qa_fail <- 1
      DBI::dbExecute(
        conn = conn_qa,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({format(last_run, usetz = FALSE)}, 
                       '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                       'Number distinct IDs', 
                       'FAIL', 
                       {format(Sys.time(), usetz = FALSE)}, 
                       'There were {id_count} distinct IDs but {row_count} rows (should be the same)')",
                       .con = conn_qa))
      
      message(glue::glue("Number of distinct IDs doesn't match the number of rows. 
                      Check {qa_schema}.{qa_table}qa_mcaid for details (last_run = {format(last_run, usetz = FALSE)}"))
    } else {
      id_distinct_qa_fail <- 0
      DBI::dbExecute(
        conn = conn_qa,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({format(last_run, usetz = FALSE)}, 
                       '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                       'Number distinct IDs', 
                       'PASS', 
                       {format(Sys.time(), usetz = FALSE)}, 
                       'The number of distinct IDs matched the number of rows ({id_count})')",
                       .con = conn_qa))
    }
    
    
    #### CHECK DISTINCT IDS = DISTINCT IDS IN STAGE.MCAID_ELIG ####
    id_count_raw <- as.numeric(odbc::dbGetQuery(
      conn, glue::glue_sql("SELECT COUNT (DISTINCT MBR_H_SID) 
                         FROM {`from_schema`}.{`from_table`}", .con = conn)))
    
    if (id_count != id_count_raw) {
      id_stage_qa_fail <- 1
      DBI::dbExecute(
        conn = conn_qa,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({format(last_run, usetz = FALSE)}, 
                       '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                       'Number distinct IDs matches raw data', 
                       'FAIL', 
                       {format(Sys.time(), usetz = FALSE)}, 
                       'There were {id_count} distinct IDs but {id_count_raw} IDs in the raw data (should be the same)')",
                       .con = conn_qa))
      
      message(glue::glue("Number of distinct IDs doesn't match the number of rows. 
                      Check {qa_schema}.{qa_table}qa_mcaid for details (last_run = {format(last_run, usetz = FALSE)}"))
    } else {
      id_stage_qa_fail <- 0
      DBI::dbExecute(
        conn = conn_qa,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({format(last_run, usetz = FALSE)}, 
                       '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                       'Number distinct IDs matches raw data', 
                       'PASS', 
                       {format(Sys.time(), usetz = FALSE)}, 
                       'The number of distinct IDs matched the number in the raw data ({id_count})')",
                       .con = conn_qa))
    }
  }
  
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
  message("Loading values to ", qa_schema, ".", qa_table, "qa_mcaid_values")
  
  load_sql <- glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid_values
                             (table_name, qa_item, qa_value, qa_date, note) 
                             VALUES ('{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                     'row_count', 
                                     {row_count}, 
                                     {format(Sys.time(), usetz = FALSE)}, 
                                     'Count after refresh')",
                             .con = conn_qa)
  
  DBI::dbExecute(conn = conn_qa, load_sql)
  
  message("QA complete, see above for any error messages")
  
  if (load_only == F) {
    qa_total <- row_qa_fail + id_distinct_qa_fail + id_stage_qa_fail
  } else {
    qa_total <- id_distinct_qa_fail + id_stage_qa_fail
  }
  
  return(qa_total)
  
}
