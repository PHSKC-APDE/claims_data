#### FUNCTIONS TO LOAD DATA TO metadata.etl_log TABLE AND RETRIEVE DATA
# Alastair Matheson
# Created:        2019-05-07
# Last modified:  2019-05-07


load_metadata_etl_log_f <- function(conn = NULL,
                                    batch_type = c("incremental", "full"),
                                    data_source = NULL,
                                    delivery_date = NULL,
                                    note = NULL) {
  
  #### ERROR CHECKS ####
  if (is.null(conn)) {
    print("No DB connection specificed, trying PHClaims51")
    conn <- odbc::dbConnect(odbc(), "PHClaims51")
  }
  
  batch_type <- match.arg(batch_type)
  if (!batch_type %in% c("incremental", "full")) {
    stop("batch_type must be 'incremental' or 'full'")
  }
  
  if (is.null(data_source) | !data_source %in% c("APCD", "Medicaid", "Medicare")) {
    stop("Enter a data source (one of 'APCD', 'Medicaid', or 'Medicare'")
  }
  
  if (is.null(delivery_date)) {
    stop("Enter a delivery date")
  }
  
  if (is.na(as.Date(as.character(delivery_date), format = "%Y-%m-%d"))) {
    stop("Delivery date must be in YYYY-MM-DD format and in quotes")
  }
  
  if (is.null(note)) {
    stop("Enter a note to describe this data")
  }
  
  
  #### CHECK EXISTING ENTRIES ####
  latest <- odbc::dbGetQuery(conn, 
                             "SELECT TOP (1) * FROM metadata.etl_log
                             ORDER BY etl_batch_id DESC")
  
  latest_source <- odbc::dbGetQuery(conn, 
                                    glue::glue_sql(
                                      "SELECT TOP (1) * FROM metadata.etl_log
                                      WHERE data_source = {data_source} 
                                      ORDER BY etl_batch_id DESC",
                                      .con = conn))
  
  #### SET DEFAULTS ####
  batch_type <- ifelse(batch_type == "incremental", "Incremental refresh",
                       "Full refresh")
  
  proceed <- T # Move ahead with the load
  if (nrow(latest) > 0) {
    etl_batch_id <- latest$etl_batch_id + 1
  } else {
    etl_batch_id <- 1
  }
  
  
  
  #### CHECK AGAINST EXISTING ENTRIES ####
  # (assume if there is no record for this data source already then yes)
  if (nrow(latest) > 0 & nrow(latest_source) > 0) {
    proceed_msg <- glue::glue("
The most recent entry in the etl_log is as follows:
etl_batch_id: {latest[1]}
batch_type: {latest[2]}
data_source: {latest[3]}
delivery_date: {latest[4]}
note: {latest[5]}

The most recent entry in the etl_log FOR THIS DATA SOURCE is as follows:
etl_batch_id: {latest_source[1]}
batch_type: {latest_source[2]}
data_source: {latest_source[3]}
delivery_date: {latest_source[4]}
note: {latest_source[5]}

Do you still want to make a new entry?",
                              .con = conn)
    
    proceed <- askYesNo(msg = proceed_msg)
    
  }
  
  if (proceed == F | is.na(proceed)) {
    print("ETL log load cancelled at user request")
  } else {
    sql_load <- glue::glue_sql(
      "INSERT INTO metadata.etl_log 
      (etl_batch_id, batch_type, data_source, delivery_date, note) 
      VALUES ({etl_batch_id}, {batch_type}, {data_source}, 
      {delivery_date}, {note})",
      .con = conn
    )
    
    odbc::dbGetQuery(conn, sql_load)
    
    print(glue::glue("ETL batch #{etl_batch_id} loaded"))
  }
}


#### FUNCTION TO DISPLAY DATA ASSOCIATED WITH AN ETL_BATCH_ID
# Used to check that the right current_etl_id is being used
retrieve_metadata_etl_log_f <- function(conn = NULL, etl_batch_id = NULL) {
  ### Error checks
  if (is.null(conn)) {
    print("No DB connection specificed, trying PHClaims51")
    conn <- odbc::dbConnect(odbc(), "PHClaims51")
  }
  
  if (is.null(etl_batch_id)) {
    stop("Enter an etl_batch_id")
  }
  
  ### run query
  odbc::dbGetQuery(conn, 
                   glue::glue_sql("SELECT * FROM metadata.etl_log
                                  WHERE etl_batch_id = {etl_batch_id}",
                                  .con = conn))
}