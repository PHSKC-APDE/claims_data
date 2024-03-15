#### FUNCTIONS TO LOAD DATA TO METADATA ETL LOG TABLE AND RETRIEVE DATA
# Alastair Matheson

# Note: these functions are for claims data
# Use https://github.com/PHSKC-APDE/DOHdata/blob/main/ETL/general/scripts_general/etl_log.R
#  for non-claims data

# auto_proceed = T allows skipping of checks against existing ETL entries.
# Use with caution to avoid creating duplicate entries.
# Note that this will not overwrite checking for near-exact matches but will auto-reuse them. 

# 3/14/24 Eli update: Tweaked code that produces "proceed" object to 1) fix parentheses around OR statement
  # and 2) set proceed <- T if auto_proceed == T and nrow(matches) >= 1


load_metadata_etl_log_file_f <- function(conn = NULL,
                                    server = NULL,
                                    batch_type = c("incremental", "full"),
                                    data_source = NULL,
                                    date_min = NULL,
                                    date_max = NULL,
                                    delivery_date = NULL,
                                    file_name = NULL,
                                    file_loc = NULL,
                                    row_cnt = NULL,
                                    note = NULL,
                                    auto_proceed = F) {
  
  #### ERROR CHECKS ####
  if (is.null(conn)) {
    stop("No DB connection specificed")
  }
  
  batch_type <- match.arg(batch_type)
  
  if (is.null(data_source) | !data_source %in% c("APCD", "Medicaid", "Medicare")) {
    stop("Enter a data source (one of 'APCD', 'Medicaid', or 'Medicare'")
  }
  
  if (is.null(date_min) | is.null(date_max)) {
    stop("Both date_min and date_max must be entered. 
         Use YYYY-01-01 and YYYY-12-31 for full-year data.")
  }
  
  
  if (is.null(delivery_date)) {
    stop("Enter a delivery date")
  }
  
  
  if (is.na(as.Date(as.character(date_min), format = "%Y-%m-%d")) |
      is.na(as.Date(as.character(date_max), format = "%Y-%m-%d")) |
      is.na(as.Date(as.character(delivery_date), format = "%Y-%m-%d"))) {
    stop("Dates must be in YYYY-MM-DD format and in quotes")
  }
  
  if (is.null(file_name)) {
    stop("Enter a file name")
  }
  
  if (is.null(file_loc)) {
    stop("Enter a file location")
  }
  
  if (is.null(row_cnt)) {
    stop("Enter a row count")
  }
  
  if (is.null(note)) {
    stop("Enter a note to describe this data")
  }
  
  
  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  if (server == "phclaims") {
    schema <- "metadata"
    table <- "etl_log"
  } else if (server == "hhsaw") {
    schema <- "claims"
    table <- "metadata_etl_log"
  }
  
  
  #### REDO batch_type VARIABLE ####
  batch_type <- ifelse(batch_type == "incremental", 
                       "Incremental refresh",
                       "Full refresh")
  
  
  #### CHECK EXISTING ENTRIES ####
  latest <- DBI::dbGetQuery(conn, 
                            glue::glue_sql("SELECT TOP (1) * FROM {`schema`}.{`table`}
                             ORDER BY etl_batch_id DESC",
                                           .con = conn))
  
  latest_source <- DBI::dbGetQuery(conn, 
                                   glue::glue_sql(
                                     "SELECT TOP (1) * FROM {`schema`}.{`table`}
                                      WHERE data_source = {data_source} 
                                      ORDER BY etl_batch_id DESC",
                                     .con = conn))
  
  matches <- DBI::dbGetQuery(conn, 
                             glue::glue_sql(
                               "SELECT * FROM {`schema`}.{`table`}
                                      WHERE batch_type = {batch_type} AND
                                      data_source = {data_source} AND 
                                      delivery_date = {delivery_date}
                                      ORDER BY etl_batch_id DESC",
                               .con = conn))
  
  #### SET UP ID ####
  if (nrow(latest) > 0) {
    etl_batch_id <- latest$etl_batch_id + 1
  } else {
    etl_batch_id <- 1
  }
  
  
  #### CHECK ABOUT CREATING NEW ENTRY ####
  if (auto_proceed == T & (nrow(matches) == 0 | nrow(latest_source) == 0)) {
    proceed <- T
  } else if (auto_proceed == T & nrow(matches) >= 1) {
    proceed <- T
  }
  
  if (auto_proceed == F) {
    # Check if wanting to make a new ID when there is a close match
    if (nrow(matches) > 0) {
      print(matches)
      
      proceed_msg <- glue::glue("There are already entries in the table that \\
                              look similar to what you are attempting to enter. \\
                              See the console window.
                              
                              Do you still want to make a new entry?")
      proceed <- askYesNo(msg = proceed_msg)
    } else {
      # Check against most recent entry for this data source
      if (nrow(latest_source) > 0 & auto_proceed == F) {
        proceed_msg <- glue::glue("
The most recent entry in the etl_log FOR THIS DATA SOURCE is as follows:
etl_batch_id: {latest_source[1]}
batch_type: {latest_source[2]}
data_source: {latest_source[3]}
date_min: {format(latest_source[4], format = '%Y-%m-%d')}
date_max: {format(latest_source[5], format = '%Y-%m-%d')}
delivery_date: {format(latest_source[6], format = '%Y-%m-%d')}
file_name: {latest_source[9]}
note: {latest_source[7]}

Do you still want to make a new entry?")
        
        proceed <- askYesNo(msg = proceed_msg)
      }
    }
  }
  
  
  if (is.na(proceed)) {
    stop("ETL log load cancelled at user request")
  }
  
  
  #### CHECK ABOUT REUSING ID ####
  if (auto_proceed == T & nrow(matches) > 0) {
    # Reuse most recent etl_batch_id automatically if there is a match
    reuse <- T
  } else if (proceed == F & nrow(matches) > 0) {
    reuse <- askYesNo(msg = "Would you like to reuse an existing entry that matches?")
    
    if (is.na(reuse)) {stop("ETL log load cancelled at user request")}
  } else {
    reuse <- F
  }
  
  ### Use existing etl_batch_id
  if (reuse == T) {
    if (auto_proceed == F) {
      etl_batch_id <- select.list(choices = matches$etl_batch_id,
                                  title = "Which existing entry would you like to reuse?")
      
      message("Reusing ETL batch #", etl_batch_id)
      return(etl_batch_id)
    } else if (auto_proceed == T) {
      etl_batch_id <- max(matches$etl_batch_id)
      message("Reusing most recent matching ETL batch (#", etl_batch_id, ")")
    }
  } 
  
  
  #### REGISTER NEW ID ####
  if (proceed == T) {
    sql_load <- glue::glue_sql(
      "INSERT INTO {`schema`}.{`table`} 
      (etl_batch_id, batch_type, data_source, date_min, date_max, delivery_date, 
      note, file_name, file_location, row_count) 
      VALUES ({etl_batch_id}, {batch_type}, {data_source}, {date_min}, {date_max}, 
      {delivery_date}, {note}, {file_name}, {file_loc}, {row_cnt})",
      .con = conn)
    
    DBI::dbGetQuery(conn, sql_load)
    
    # Finish with a message and return the latest etl_batch_id
    # (users should be assigning this to a current_batch_id object)
    message("ETL batch #", etl_batch_id, " loaded")
  } else if (proceed == F & reuse == F) {
    message("No ETL batch ID was created or reused")
  }
  
  #### RETURN etl_batch_id ####
  return(etl_batch_id)
}



load_metadata_etl_log_f <- function(conn = NULL,
                                    server = NULL,
                                    batch_type = c("incremental", "full"),
                                    data_source = NULL,
                                    date_min = NULL,
                                    date_max = NULL,
                                    delivery_date = NULL,
                                    note = NULL,
                                    auto_proceed = F) {
  
  #### ERROR CHECKS ####
  if (is.null(conn)) {
    stop("No DB connection specificed")
  }
  
  batch_type <- match.arg(batch_type)
  
  if (is.null(data_source) | !data_source %in% c("APCD", "Medicaid", "Medicare")) {
    stop("Enter a data source (one of 'APCD', 'Medicaid', or 'Medicare'")
  }
  
  if (is.null(date_min) | is.null(date_max)) {
    stop("Both date_min and date_max must be entered. 
         Use YYYY-01-01 and YYYY-12-31 for full-year data.")
  }
  
  
  if (is.null(delivery_date)) {
    stop("Enter a delivery date")
  }
  
  
  if (is.na(as.Date(as.character(date_min), format = "%Y-%m-%d")) |
      is.na(as.Date(as.character(date_max), format = "%Y-%m-%d")) |
      is.na(as.Date(as.character(delivery_date), format = "%Y-%m-%d"))) {
    stop("Dates must be in YYYY-MM-DD format and in quotes")
  }
  
  
  if (is.null(note)) {
    stop("Enter a note to describe this data")
  }
  
  
  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  if (server == "phclaims") {
    schema <- "metadata"
    table <- "etl_log"
  } else if (server == "hhsaw") {
    schema <- "claims"
    table <- "metadata_etl_log"
  }
  
  
  #### REDO batch_type VARIABLE ####
  batch_type <- ifelse(batch_type == "incremental", 
                       "Incremental refresh",
                       "Full refresh")
  
  
  #### CHECK EXISTING ENTRIES ####
  latest <- DBI::dbGetQuery(conn, 
                            glue::glue_sql("SELECT TOP (1) * FROM {`schema`}.{`table`}
                             ORDER BY etl_batch_id DESC",
                                           .con = conn))
  
  latest_source <- DBI::dbGetQuery(conn, 
                                    glue::glue_sql(
                                      "SELECT TOP (1) * FROM {`schema`}.{`table`}
                                      WHERE data_source = {data_source} 
                                      ORDER BY etl_batch_id DESC",
                                      .con = conn))
  
  matches <- DBI::dbGetQuery(conn, 
                              glue::glue_sql(
                                "SELECT * FROM {`schema`}.{`table`}
                                      WHERE batch_type = {batch_type} AND
                                      data_source = {data_source} AND 
                                      delivery_date = {delivery_date}
                                      ORDER BY etl_batch_id DESC",
                                .con = conn))

  #### SET UP ID ####
  if (nrow(latest) > 0) {
    etl_batch_id <- latest$etl_batch_id + 1
  } else {
    etl_batch_id <- 1
  }
  
  
  #### CHECK ABOUT CREATING NEW ENTRY ####
  if (auto_proceed == T & (nrow(matches) == 0 | nrow(latest_source) == 0)) {
    proceed <- T
  } else if (auto_proceed == T & nrow(matches) >= 1) {
    proceed <- T
  }
  
  if (auto_proceed == F) {
    # Check if wanting to make a new ID when there is a close match
    if (nrow(matches) > 0) {
      print(matches)
      
      proceed_msg <- glue::glue("There are already entries in the table that \\
                              look similar to what you are attempting to enter. \\
                              See the console window.
                              
                              Do you still want to make a new entry?")
      proceed <- askYesNo(msg = proceed_msg)
    } else {
      # Check against most recent entry for this data source
      if (nrow(latest_source) > 0 & auto_proceed == F) {
        proceed_msg <- glue::glue("
The most recent entry in the etl_log FOR THIS DATA SOURCE is as follows:
etl_batch_id: {latest_source[1]}
batch_type: {latest_source[2]}
data_source: {latest_source[3]}
date_min: {format(latest_source[4], format = '%Y-%m-%d')}
date_max: {format(latest_source[5], format = '%Y-%m-%d')}
delivery_date: {format(latest_source[6], format = '%Y-%m-%d')}
note: {latest_source[7]}

Do you still want to make a new entry?")
        
        proceed <- askYesNo(msg = proceed_msg)
      }
    }
  }
  
 
  if (is.na(proceed)) {
    stop("ETL log load cancelled at user request")
  }
  
  
  #### CHECK ABOUT REUSING ID ####
  if (auto_proceed == T & nrow(matches) > 0) {
    # Reuse most recent etl_batch_id automatically if there is a match
    reuse <- T
  } else if (proceed == F & nrow(matches) > 0) {
    reuse <- askYesNo(msg = "Would you like to reuse an existing entry that matches?")
    
    if (is.na(reuse)) {stop("ETL log load cancelled at user request")}
  } else {
    reuse <- F
  }
  
  ### Use existing etl_batch_id
  if (reuse == T) {
    if (auto_proceed == F) {
      etl_batch_id <- select.list(choices = matches$etl_batch_id,
                                  title = "Which existing entry would you like to reuse?")
      
      message("Reusing ETL batch #", etl_batch_id)
      return(etl_batch_id)
    } else if (auto_proceed == T) {
      etl_batch_id <- max(matches$etl_batch_id)
      message("Reusing most recent matching ETL batch (#", etl_batch_id, ")")
    }
  } 
  
  
  #### REGISTER NEW ID ####
  if (proceed == T) {
    sql_load <- glue::glue_sql(
      "INSERT INTO {`schema`}.{`table`} 
      (etl_batch_id, batch_type, data_source, date_min, date_max, delivery_date, note) 
      VALUES ({etl_batch_id}, {batch_type}, {data_source}, {date_min}, {date_max}, 
      {delivery_date}, {note})",
      .con = conn)
  
    DBI::dbGetQuery(conn, sql_load)
    
    # Finish with a message and return the latest etl_batch_id
    # (users should be assigning this to a current_batch_id object)
    message("ETL batch #", etl_batch_id, " loaded")
  } else if (proceed == F & reuse == F) {
    message("No ETL batch ID was created or reused")
  }
  
  #### RETURN etl_batch_id ####
  return(etl_batch_id)
}


#### FUNCTION TO DISPLAY DATA ASSOCIATED WITH AN ETL_BATCH_ID
# Used to check that the right current_etl_id is being used
retrieve_metadata_etl_log_f <- function(conn = NULL, 
                                        server = NULL,
                                        etl_batch_id = NULL) {
  #### ERROR CHECKS ####
  if (is.null(conn)) {
    print("No DB connection specificed, trying PHClaims51")
    conn <- odbc::dbConnect(odbc(), "PHClaims51")
  }
  
  if (is.null(etl_batch_id)) {
    stop("Enter an etl_batch_id")
  }
  
  #### SET UP SERVER ####
  if (is.null(server) | !server %in% c("phclaims", "hhsaw")) {
    message("Server must be NULL, 'phclaims', or 'hhsaw'")
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  }
  
  if (server == "phclaims") {
    schema <- "metadata"
    table <- "etl_log"
  } else if (server == "hhsaw") {
    schema <- "claims"
    table <- "metadata_etl_log"
  }
  
  
  ### run query
  DBI::dbGetQuery(conn, 
                   glue::glue_sql("SELECT * FROM {`schema`}.{`table`}
                                  WHERE etl_batch_id = {etl_batch_id}",
                                  .con = conn))
}


#### FUNCTION TO DISPLAY DATA ASSOCIATED WITH AN ETL_BATCH_ID
get_unloaded_etl_batches_f <- function(conn = NULL,
                                       server = NULL,
                                       type = c("claim", "elig")) {
  #### ERROR CHECKS ####
  if (is.null(conn)) {
    print("No DB connection specificed, trying PHClaims51")
    conn <- odbc::dbConnect(odbc(), "PHClaims51")
  }

  #### SET UP SERVER ####
  if (is.null(server) | !server %in% c("phclaims", "hhsaw")) {
    message("Server must be NULL, 'phclaims', or 'hhsaw'")
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  }
  
  if (server == "phclaims") {
    schema <- "metadata"
    table <- "etl_log"
  } else if (server == "hhsaw") {
    schema <- "claims"
    table <- "metadata_etl_log"
  }
  
  ### run query
  results <- DBI::dbGetQuery(conn, 
                  glue::glue_sql("SELECT * FROM {`schema`}.{`table`}
                                  WHERE date_load_raw IS NULL
                                   AND CHARINDEX({type}, note) > 0
                                   AND data_source = 'Medicaid'
                                   AND batch_type = 'Incremental Refresh'
                                  ORDER BY date_min, date_max",
                                 .con = conn))
  return(results)
}
