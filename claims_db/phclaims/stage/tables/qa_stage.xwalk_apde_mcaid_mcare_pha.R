## Header ####
# Author: Danny Colombara
# 
# R version: 4.3.1
#
# Purpose: Simple QA for [claims].[stage_xwalk_apde_mcaid_mcare_pha]
# 
# Notes: Type the <Alt> + <o> at the same time to collapse the code and view the structure
# 
# This code is designed to be run as part of the master Medicaid/Medicare script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid_mcare/master_mcaid_mcare_analytic.R
#

# Set up ----
  options(error = NULL, scipen = 999)
  db_hhsaw <- rads::validate_hhsaw_key() # connects to Azure 16 HHSAW
  
  db_idh <- DBI::dbConnect(odbc::odbc(), driver = "ODBC Driver 17 for SQL Server", 
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433", 
                           database = "inthealth_dwhealth", 
                           uid = keyring::key_list("hhsaw")[["username"]], 
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]), 
                           Encrypt = "yes", TrustServerCertificate = "yes", 
                           Authentication = "ActiveDirectoryPassword")

# This is all one function ----
qa_xwalk_apde_mcaid_mcare_pha_f <- function(conn = db_hhsaw,
                                            skip_mcare = T, # until Medicare is abel to be linked
                                            load_only = F) {
  
  # If this is the first time ever loading data, only load values.
  #   Otherwise, check against existing QA values
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # Rows in current table
  row_count <- as.integer(odbc::dbGetQuery(conn, 
                                           "SELECT COUNT (*) FROM [claims].[stage_xwalk_apde_mcaid_mcare_pha]"))
  
  ### Pull out run date of [claims].[stage_xwalk_apde_mcaid_mcare_pha]
  last_run <- as.POSIXct(odbc::dbGetQuery(conn, "SELECT MAX (last_run) FROM [claims].[stage_xwalk_apde_mcaid_mcare_pha]")[[1]])
  
  #### If load_only == F (meaning, want to QA vs previous values) ####
  if (load_only == F) {
  #### COMPARE COUNT OF XWALK ROWS TO PREVIOUS COUNT ####
    # Pull in the reference value
    previous_rows <- as.numeric(
      odbc::dbGetQuery(conn, 
                       "SELECT TEMP.qa_value
                        FROM [claims].[metadata_qa_xwalk_values] TEMP
                        INNER JOIN (
                            SELECT MAX(qa_date) AS max_date
                            FROM [claims].[metadata_qa_xwalk_values]
                            WHERE table_name = 'claims.stage_xwalk_apde_mcaid_mcare_pha' 
                            AND qa_item = 'row_count'
                        ) max_dates ON TEMP.qa_date = max_dates.max_date
                        WHERE TEMP.table_name = 'claims.stage_xwalk_apde_mcaid_mcare_pha'
                        AND TEMP.qa_item = 'row_count'"
                          ))
    

    if(is.na(previous_rows)){previous_rows = 0}
    
    row_diff <- row_count - previous_rows
    
    if (row_diff < 0) {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'claims.stage_xwalk_apde_mcaid_mcare_pha',
                       'Number new rows compared to most recent run', 
                       'FAIL', 
                       {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                       'There were {row_diff} fewer rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      problem.row_diff <- glue::glue("Fewer rows than found last time.  
                      Check [claims].[metadata_qa_xwalk] for details (last_run = {last_run})
                      \n")
    } else {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'claims.stage_xwalk_apde_mcaid_mcare_pha',
                       'Number new rows compared to most recent run', 
                       'PASS', 
                       {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                       'There were {row_diff} more rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      problem.row_diff <- glue::glue(" ") # no problem, so empty error message
      
    }

  #### CHECK DISTINCT KCMASTER_ID >= PREVIOUS ####
    distinct_KCMASTER_ID <- as.integer(DBI::dbGetQuery(conn, "SELECT COUNT(DISTINCT(KCMASTER_ID)) FROM [claims].[stage_xwalk_apde_mcaid_mcare_pha]"))
    
    prev_count_kcmaster_id <- as.integer(odbc::dbGetQuery(
      conn,
      "SELECT TEMP.qa_value
    FROM [claims].[metadata_qa_xwalk_values] TEMP
    INNER JOIN (
        SELECT MAX(qa_date) AS max_date
        FROM [claims].[metadata_qa_xwalk_values]
        WHERE table_name = 'claims.stage_xwalk_apde_mcaid_mcare_pha' 
        AND qa_item = 'distinct_KCMASTER_ID'
    ) max_dates ON TEMP.qa_date = max_dates.max_date
    WHERE TEMP.table_name = 'claims.stage_xwalk_apde_mcaid_mcare_pha'
    AND TEMP.qa_item = 'distinct_KCMASTER_ID'"
    ))
    
    
    if (distinct_KCMASTER_ID < prev_count_kcmaster_id) {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES (
                      {last_run}, 
                     'claims.stage_xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs - KCMASTER_ID', 
                     'FAIL', 
                      {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                     'There were {distinct_KCMASTER_ID} distinct KCMASTER_IDs but {prev_count_kcmaster_id} in the most recent [claims].[metadata_qa_xwalk_values] (shoudl be >=)'
                     )
                     ",
                       .con = conn))
      
      problem.KCMASTER_ID  <- glue::glue("Number of distinct KCMASTER_IDs is less than the most recent in [claims].[metadata_qa_xwalk_values]. 
                    Check [claims].[metadata_qa_xwalk] for details (last_run = {last_run})
                   \n")
    } else {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'claims.stage_xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs - KCMASTER_ID', 
                     'PASS', 
                      {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                     'The number of distinct KCMASTER_IDs ({distinct_KCMASTER_ID}) is >= the most recent number in [claims].[metadata_qa_xwalk_values] 
                     ({prev_count_kcmaster_id})')",
                       .con = conn))
      
      problem.KCMASTER_ID  <- glue::glue(" ") # no problem
    }
    
  #### CHECK DISTINCT ID_APDE >= PREVIOUS ####
    distinct_id_apde <- as.integer(DBI::dbGetQuery(conn, "SELECT COUNT(DISTINCT(id_apde)) FROM [claims].[stage_xwalk_apde_mcaid_mcare_pha]"))
    
    prev_count_id_apde <- as.integer(odbc::dbGetQuery(
      conn,
      "SELECT TEMP.qa_value
    FROM [claims].[metadata_qa_xwalk_values] TEMP
    INNER JOIN (
        SELECT MAX(qa_date) AS max_date
        FROM [claims].[metadata_qa_xwalk_values]
        WHERE table_name = 'claims.stage_xwalk_apde_mcaid_mcare_pha' 
        AND qa_item = 'distinct_id_apde'
    ) max_dates ON TEMP.qa_date = max_dates.max_date
    WHERE TEMP.table_name = 'claims.stage_xwalk_apde_mcaid_mcare_pha'
    AND TEMP.qa_item = 'distinct_id_apde'"
    ))
    
    
    if (distinct_id_apde < prev_count_id_apde) {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES (
                      {last_run}, 
                     'claims.stage_xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs - ID_APDE', 
                     'FAIL', 
                      {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                     'There were {distinct_id_apde} distinct ID_APDEs but {prev_count_id_apde} in the most recent [claims].[metadata_qa_xwalk_values] (shoudl be >=)'
                     )
                     ",
                       .con = conn))
      
      problem.ID_APDE  <- glue::glue("Number of distinct ID_APDEs is less than the most recent in [claims].[metadata_qa_xwalk_values]. 
                    Check [claims].[metadata_qa_xwalk] for details (last_run = {last_run})
                   \n")
    } else {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'claims.stage_xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs - ID_APDE', 
                     'PASS', 
                      {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                     'The number of distinct ID_APDEs ({distinct_id_apde}) is >= the most recent number in [claims].[metadata_qa_xwalk_values] 
                     ({prev_count_id_apde})')",
                       .con = conn))
      
      problem.ID_APDE  <- glue::glue(" ") # no problem
    }
  
  #### CHECK DISTINCT MCARE IDS >= DISTINCT IN MCARE ELIG DEMO ####
  if(skip_mcare == F){
      id_count_mcare <- as.integer(odbc::dbGetQuery(
        conn, "SELECT COUNT (DISTINCT id_mcare) AS count FROM [claims].[stage_xwalk_apde_mcaid_mcare_pha]"))
      
      id_count_mcare_elig_demo <- as.integer(odbc::dbGetQuery(
        conn, 
        "SELECT TEMP.qa_value
        FROM [claims].[metadata_qa_mcare_values] TEMP
        INNER JOIN (
            SELECT MAX(qa_date) AS max_date
            FROM [claims].[metadata_qa_mcare_values]
            WHERE table_name = 'claims.stage_mcare_elig_demo' 
            AND qa_item = 'row_count'
        ) max_dates ON TEMP.qa_date = max_dates.max_date
        WHERE TEMP.table_name = 'claims.stage_mcare_elig_demo'
        AND TEMP.qa_item = 'row_count'" 
        ))
      
      # NOTE ... it is possible / probable that the linkage has more IDS than the ELIG DEMO because not everyone is in ELIG DEMO
      if (id_count_mcare < id_count_mcare_elig_demo) {
        odbc::dbGetQuery(
          conn = conn,
          glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES (
                        {last_run}, 
                       'claims.stage_xwalk_apde_mcaid_mcare_pha',
                       'Number distinct IDs - Medicare', 
                       'FAIL', 
                        {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                       'There were {id_count_mcare} distinct MCARE IDs but {id_count_mcare_elig_demo} in the most recent [claims].[stage_mcare_elig_demo] (xwalk should have >= # in elig demo)'
                       )
                       ",
                         .con = conn))
        
        problem.mcare_id  <- glue::glue("Number of distinct MCARE IDs is less than the number in [claims].[stage_mcare_elig_demo] data. 
                      Check [claims].[metadata_qa_xwalk] for details (last_run = {last_run})
                     \n")
      } else {
        odbc::dbGetQuery(
          conn = conn,
          glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'claims.stage_xwalk_apde_mcaid_mcare_pha',
                       'Number distinct IDs - Medicare', 
                       'PASS', 
                        {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                       'The number of distinct MCARE IDs ({id_count_mcare}) is >= the number in [claims].[stage_mcare_elig_demo]  
                       ({id_count_mcare_elig_demo})')",
                         .con = conn))
        
        problem.mcare_id  <- glue::glue(" ") # no problem
      }
  } 
  
  #### CHECK DISTINCT MCAID IDS == DISTINCT IN MCAID ELIG DEMO ####
  id_count_mcaid <- as.integer(odbc::dbGetQuery(
    conn, "SELECT COUNT (DISTINCT id_mcaid) AS count FROM [claims].[stage_xwalk_apde_mcaid_mcare_pha]"))
  
  id_count_mcaid_elig_demo <- as.integer(odbc::dbGetQuery(
    conn,
    "SELECT TEMP.qa_value
    FROM [claims].[metadata_qa_mcaid_values] TEMP
    INNER JOIN (
        SELECT MAX(qa_date) AS max_date
        FROM [claims].[metadata_qa_mcaid_values]
        WHERE table_name = 'claims.stage_mcaid_elig_demo' 
        AND qa_item = 'row_count'
    ) max_dates ON TEMP.qa_date = max_dates.max_date
    WHERE TEMP.table_name = 'claims.stage_mcaid_elig_demo'
    AND TEMP.qa_item = 'row_count'"
  ))
  
  
  if (id_count_mcaid != id_count_mcaid_elig_demo) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES (
                      {last_run}, 
                     'claims.stage_xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs - Medicaid', 
                     'FAIL', 
                      {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                     'There were {id_count_mcaid} distinct MCAID IDs but {id_count_mcaid_elig_demo} in the most recent [claims].[stage_mcaid_elig_demo] (they should be equal)'
                     )
                     ",
                     .con = conn))
    
    problem.mcaid_id  <- glue::glue("Number of distinct MCAID IDs is different from the number in [claims].[stage_mcaid_elig_demo] data. 
                    Check [claims].[metadata_qa_xwalk] for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'claims.stage_xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs - Medicaid', 
                     'PASS', 
                      {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                     'The number of distinct MCAID IDs ({id_count_mcaid}) is equal to the number in [claims].[stage_mcaid_elig_demo] 
                     ({id_count_mcaid_elig_demo})')",
                     .con = conn))
    
    problem.mcaid_id  <- glue::glue(" ") # no problem
  }
  
  #### CHECK DISTINCT PHOUSING_ID == DISTINCT PHOUSING_ID IN [IDMatch].[IM_HISTORY_TABLE] in IDH ####
  id_count_pha <- as.integer(odbc::dbGetQuery(
    conn, "SELECT COUNT (DISTINCT PHOUSING_ID) AS count FROM [claims].[stage_xwalk_apde_mcaid_mcare_pha]"))
  
  id_count_pha_orig <- as.integer(odbc::dbGetQuery(
    conn = db_idh, "SELECT COUNT (DISTINCT PHOUSING_ID) AS count FROM [IDMatch].[IM_HISTORY_TABLE] 
    WHERE IS_HISTORICAL = 'N' AND KCMASTER_ID IS NOT NULL"))
  
  if (id_count_pha != id_count_pha_orig) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES (
                      {last_run}, 
                     'claims.stage_xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs - phousing_id', 
                     'FAIL', 
                      {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                     'There were {id_count_pha} distinct PHOUSING_IDs but {id_count_pha_orig} in the most recent [IDMatch].[IM_HISTORY_TABLE] in the IDH (they should be equal)'
                     )
                     ",
                     .con = conn))
    
    problem.id_pha  <- glue::glue("Number of distinct PHOUSING_IDs is different from the number in [IDMatch].[IM_HISTORY_TABLE] in the IDH. 
                    Check [claims].[metadata_qa_xwalk] for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO [claims].[metadata_qa_xwalk]
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'claims.stage_xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs - phousing_id', 
                     'PASS', 
                      {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}, 
                     'The number of distinct PHOUSING_IDs ({id_count_pha}) is equal to the number in [IDMatch].[IM_HISTORY_TABLE] in the IDH  
                     ({id_count_pha_orig})')",
                     .con = conn))
    
    problem.id_pha  <- glue::glue(" ") # no problem
  }
  
  #### CREATE FUNCTION TO CHECK THAT IDs in XWALk TABLE ARE DISTINCT (ONLY IN ONE ROW) ----
      check_duplicate_ids <- function(conn, last_run, identifier = NULL) {
        # Construct the SQL query using paste0()
        query <- paste0(
          "SELECT ", identifier, ", freq = COUNT(", identifier, ") ",
          "FROM [claims].[stage_xwalk_apde_mcaid_mcare_pha] ",
          "GROUP BY ", identifier, " ",
          "HAVING COUNT(", identifier, ") > 1"
        )
        distinct_ids <- setDT(odbc::dbGetQuery(conn, query))
        
        if (nrow(distinct_ids) != 0) {
          # Construct the INSERT query for the case of duplicates
          insert_query <- paste0(
            "INSERT INTO [claims].[metadata_qa_xwalk] ",
            "(last_run, table_name, qa_item, qa_result, qa_date, note) ",
            "VALUES ('", last_run, "', ",
            "'claims.stage_xwalk_apde_mcaid_mcare_pha', ",
            "'Duplicate ", identifier, "', ",
            "'WARNING', ",
            "'", format(Sys.time(), '%Y-%m-%d %H:%M:%S'), "', ",
            "'There were ", nrow(distinct_ids), " duplicate ", identifier, " (i.e., a given ID appeared in more than one row)')"
          )
          odbc::dbGetQuery(conn, insert_query)
          
          problem_message <- paste0("There appear to be duplicate ", identifier, 
                                    " Check [claims].[metadata_qa_xwalk] for details (last_run = ", last_run, ")\n")
        } else {
          # Construct the INSERT query for the case of no duplicates
          insert_query <- paste0(
            "INSERT INTO [claims].[metadata_qa_xwalk] ",
            "(last_run, table_name, qa_item, qa_result, qa_date, note) ",
            "VALUES ('", last_run, "', ",
            "'claims.stage_xwalk_apde_mcaid_mcare_pha', ",
            "'Duplicate ", identifier, "', ",
            "'PASS', ",
            "'", format(Sys.time(), '%Y-%m-%d %H:%M:%S'), "', ",
            "'There were NO duplicate ", identifier, " (good job!)')"
          )
          odbc::dbGetQuery(conn, insert_query)
          
          problem_message <- ""
        }
        
        return(problem_message)
      }
  
    # CHECK THAT id_mcare ARE DISTINCT (ONLY IN ONE ROW) ----
      if(skip_mcare == F){
        problem.dup.id_mcare <- check_duplicate_ids(conn, last_run, "id_mcare")
      } 
    
    # CHECK THAT id_mcaid ARE DISTINCT (ONLY IN ONE ROW) ----
      problem.dup.id_mcaid <- check_duplicate_ids(conn, last_run, "id_mcaid")
    
    # CHECK THAT phousing_id ARE DISTINCT (ONLY IN ONE ROW) ----
      problem.dup.phousing_id <- check_duplicate_ids(conn, last_run, "phousing_id")
  
    # CHECK THAT id_apde ARE DISTINCT (ONLY IN ONE ROW) ----
      problem.dup.id_apde <- check_duplicate_ids(conn, last_run, "id_apde")
    
    # CHECK THAT id_apde ARE DISTINCT (ONLY IN ONE ROW) ----
      problem.dup.KCMASTER_ID <- check_duplicate_ids(conn, last_run, "KCMASTER_ID")
  
  } # close load_only condition above
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
      # ROW COUNT ----
      value_row_count <- data.table(table_name = 'claims.stage_xwalk_apde_mcaid_mcare_pha', 
                                    qa_item = 'row_count', 
                                    qa_value = row_count, 
                                    qa_date = as.POSIXct(Sys.time()), 
                                    note = NA_character_)
      DBI::dbWriteTable(conn = db_hhsaw,
                        name = DBI::Id(schema = 'claims', table = 'metadata_qa_xwalk_values'),
                        value = as.data.frame(value_row_count),
                        overwrite = F, 
                        append = T)
      
      # COUNT DISTINCT KCMASTER_ID ---- 
      value_distinct_KCMASTER_ID <- data.table(table_name = 'claims.stage_xwalk_apde_mcaid_mcare_pha', 
                                               qa_item = 'distinct_KCMASTER_ID', 
                                               qa_value = distinct_KCMASTER_ID, 
                                               qa_date = as.POSIXct(Sys.time()), 
                                               note = NA_character_)
      DBI::dbWriteTable(conn = db_hhsaw,
                        name = DBI::Id(schema = 'claims', table = 'metadata_qa_xwalk_values'),
                        value = as.data.frame(value_distinct_KCMASTER_ID),
                        overwrite = F, 
                        append = T)

      # COUNT DISTINCT id_apde ----
      value_distinct_id_apde <- data.table(table_name = 'claims.stage_xwalk_apde_mcaid_mcare_pha', 
                                               qa_item = 'distinct_id_apde', 
                                               qa_value = distinct_id_apde, 
                                               qa_date = as.POSIXct(Sys.time()), 
                                               note = NA_character_)
      DBI::dbWriteTable(conn = db_hhsaw,
                        name = DBI::Id(schema = 'claims', table = 'metadata_qa_xwalk_values'),
                        value = as.data.frame(value_distinct_id_apde),
                        overwrite = F, 
                        append = T)
      

  #### Identify problems / fails ####
    if(load_only == F){
      if(any(sapply(grep('^problem\\.', ls(), value = TRUE), function(x) nchar(get(x)) > 1))){
        problems <- glue::glue("****STOP!!!!!!!!****\nPlease address the following issues that have been logged in [claims].[metadata_qa_xwalk] ... \n\n", 
                               paste(mget(grep('^problem\\.', ls(), value = T)), collapse = '\n'))}else{
                                 problems <- glue::glue("All QA checks passed and recorded to [claims].[metadata_qa_xwalk]")       
                               }
      message(problems)
      
    }
}

# The end! ----

  