###############################################################################
# Alastair Matheson
# 2019-05

# Code to QA claims.stage_mcaid_elig_timevar

###############################################################################


# No overlaps

qa_mcaid_elig_timevar_f <- function(conn = NULL,
                                    conn_qa = NULL,
                                    server = c("hhsaw", "phclaims"),
                                    config = NULL,
                                    get_config = F,
                                    load_only = F) {
  
  # If this is the first time ever loading data, only load values.
  #   Otherwise, check against existing QA values
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T) {
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
  
  ### Pull out run date of claims.stage_mcaid_elig_timevar
  last_run <- as.POSIXct(odbc::dbGetQuery(
    conn, 
    glue::glue_sql("SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}",
                   .con = conn))[[1]])
  
  if(load_only == F) {
    ### COUNT NUMBER OF ROWS ###
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
      DBI::dbExecute(conn_qa,
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Number new rows compared to most recent run', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {row_diff} fewer rows in the most recent table ({row_count} vs. {previous_rows})')",
                                    .con = conn_qa))
      
      warning(glue::glue("Fewer rows than found last time.  
                  Check {qa_schema}.{qa_table}qa_mcaid for details (last_run = {format(last_run, usetz = FALSE)}"))
    } else {
      row_qa_fail <- 0
      DBI::dbExecute(conn_qa,
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Number new rows compared to most recent run', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {row_diff} more rows in the most recent table ({row_count} vs. {previous_rows})')",
                                    .con = conn_qa))
      
    }
  }
  
  #### CHECK DISTINCT IDS = DISTINCT IN stage_mcaid_elig ####
  id_count_timevar <- as.numeric(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT id_mcaid) AS count FROM {`to_schema`}.{`to_table`}",
                         .con = conn)))
  
  id_count_elig <- as.numeric(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT MEDICAID_RECIPIENT_ID) as count FROM {`from_schema`}.{`from_table`}",
                         .con = conn)))
  
  if (id_count_timevar != id_count_elig) {
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
                       'There were {id_count_timevar} distinct IDs but {id_count_elig} in the raw data (should be the same)')",
                     .con = conn_qa))
    
    warning(glue::glue("Number of distinct IDs doesn't match the number of rows. 
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
                       'The number of distinct IDs matched number in raw data ({id_count_timevar})')",
                     .con = conn_qa))
  }
  
  
  
  #### CHECK FOR DUPLICATE ROWS  ####
  dup_row_count <- as.numeric(odbc::dbGetQuery(
    conn, 
    glue::glue_sql("SELECT COUNT (*) AS count FROM 
    (SELECT DISTINCT id_mcaid, from_date, to_date, 
    dual, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip,
    cov_time_day 
    FROM {`to_schema`}.{`to_table`}) a",
                   .con = conn)))
  
  
  if (dup_row_count != row_count) {
    dup_row_qa_fail <- 1
    DBI::dbExecute(conn = conn_qa,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({format(last_run, usetz = FALSE)}, 
                       '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                       'Duplicate rows', 
                       'FAIL', 
                       {format(Sys.time(), usetz = FALSE)}, 
                       'There were {dup_row_count} distinct rows (excl. ref_geo vars) \\
                    but {row_count} rows overall (should be the same)')",
                                  .con = conn_qa))
    
    warning(glue::glue("There appear to be duplicate rows. 
                      Check {qa_schema}.{qa_table}qa_mcaid for details (last_run = {format(last_run, usetz = FALSE)}"))
  } else {
    dup_row_qa_fail <- 0
    DBI::dbExecute(
      conn = conn_qa,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({format(last_run, usetz = FALSE)}, 
                       '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                       'Duplicate rows', 
                       'PASS', 
                       {format(Sys.time(), usetz = FALSE)}, 
                       'The number of distinct rows (excl. ref_geo vars) \\
                     matched number total rows ({row_count})')",
                     .con = conn_qa))
  }
  
  
  
  #### MIN AND MAX DATES IN DATA ####
  date_range_timevar <- odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT MIN(from_date) AS from_date, max(to_date) as to_date 
                           FROM {`to_schema`}.{`to_table`}", .con = conn))
  date_range_elig <- odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT MIN(CLNDR_YEAR_MNTH) AS from_date, max(CLNDR_YEAR_MNTH) as to_date 
                           FROM {`from_schema`}.{`from_table`}", .con = conn))
  date_range_elig <- date_range_elig %>%
    mutate(from_date = as.character(as.Date(paste0(from_date, "01"), format = "%Y%m%d")),
           to_date = as.character(as.Date(paste0(to_date, "01"), format = "%Y%m%d") + months(1) - ddays(1)))
  
  
  if (date_range_timevar$from_date < date_range_elig$from_date | 
      date_range_timevar$to_date > date_range_elig$to_date) {
    date_qa_fail <- 1
    DBI::dbExecute(
      conn = conn_qa,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({format(last_run, usetz = FALSE)}, 
                             '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                             'Date range',
                             'FAIL',
                             {format(Sys.time(), usetz = FALSE)}, 
                             'Some from/to dates fell outside the CLNDR_YEAR_MNTH range \\
                             (min: {`date_range_timevar$from_date`}, max: {`date_range_timevar$to_date`})')",
                     .con = conn_qa))
    
    warning(glue::glue("Some from/to dates fell outside the CLNDR_YEAR_MNTH range. 
                    Check {qa_schema}.{qa_table}qa_mcaid for details (last_run = {format(last_run, usetz = FALSE)}"))
  } else {
    date_qa_fail <- 0
    DBI::dbExecute(
      conn = conn_qa,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({format(last_run, usetz = FALSE)}, 
                             '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                             'Date range',
                             'PASS',
                             {format(Sys.time(), usetz = FALSE)}, 
                             'All from/to dates fell within the CLNDR_YEAR_MNTH range \\
                             (min: {`date_range_elig$from_date`}, max: {`date_range_elig$to_date`})')",
                     .con = conn_qa))
  }
  
  
  
  #### CHECK SPECIFIC INDIVIDUALS TO ENSURE THEIR DATES WORK ####
  # # Problem with this is that the to_dates in the csv don't keep up with the actual data
  # # Best done manually on occasion
  # timevar_ind <- read.csv("//dchs-shares01/dchsdata/DCHSPHClaimsData/Data/QA_specific/claims.stage_mcaid_elig_timevar_qa_ind.csv",
  #                         stringsAsFactors = F)
  # timevar_ind <- timevar_ind %>%
  #   mutate_at(vars(from_date, to_date), list(~ as.Date(.)))
  # 
  # timevar_ind_sql <- glue::glue_sql("SELECT id_mcaid, from_date, to_date FROM {`to_schema`}.{`to_table`} 
  #                                   WHERE id_mcaid IN ({ind_ids*})
  #                                   ORDER BY id_mcaid, from_date",
  #                                   ind_ids = unlist(distinct(timevar_ind, id_mcaid)),
  #                                   .con = conn)
  # 
  # timevar_ind_stage <- odbc::dbGetQuery(conn, timevar_ind_sql)
  # 
  # if (all_equal(timevar_ind_stage, select(timevar_ind, -notes)) == FALSE) {
  #   ind_date_qa_fail <- 1
  #   DBI::dbExecute(
  #     conn = conn,
  #     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid 
  #                    (last_run, table_name, qa_item, qa_result, qa_date, note) 
  #                    VALUES ({format(last_run, usetz = FALSE)}, 
  #                            '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
  #                            'Specific IDs',
  #                            'FAIL',
  #                            {format(Sys.time(), usetz = FALSE)}, 
  #                            'Some from/to dates did not match expected results for specific IDs')",
  #                    .con = conn))
  # } else {
  #   ind_date_qa_fail <- 0
  #   DBI::dbExecute(
  #     conn = conn,
  #     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid 
  #                    (last_run, table_name, qa_item, qa_result, qa_date, note) 
  #                    VALUES ({format(last_run, usetz = FALSE)}, 
  #                            '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
  #                            'Specific IDs',
  #                            'PASS',
  #                            {format(Sys.time(), usetz = FALSE)}, 
  #                            'All from/to dates matched expected results for specific IDs')",
  #                    .con = conn))
  # }
  
  
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
  load_sql <- glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid_values
                             (table_name, qa_item, qa_value, qa_date, note) 
                             VALUES ('{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                     'row_count', 
                                     {row_count}, 
                                     {format(Sys.time(), usetz = FALSE)}, 
                                     '')",
                             .con = conn_qa)
  
  DBI::dbExecute(conn = conn_qa, load_sql)
  
  
  if (load_only == F) {
    qa_total <- row_qa_fail + id_distinct_qa_fail + dup_row_qa_fail + date_qa_fail 
  } else {
    qa_total <- id_distinct_qa_fail + dup_row_qa_fail + date_qa_fail 
  }
  
  return(qa_total)
  
}
