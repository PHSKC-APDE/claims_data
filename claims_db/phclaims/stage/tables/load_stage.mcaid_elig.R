#### CODE TO CREATE STAGE MCAID ELIG TABLE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08

### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_partial.R


load_stage.mcaid_elig_f <- function(conn_db = NULL,
                                    conn_dw = NULL,
                                    server = NULL,
                                    full_refresh = F, 
                                    config = NULL) {
  
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/alter_schema.R")
  
  ### Error checks
  if (is.null(conn_dw)) {stop("No DW connection specificed")}
  if (is.null(conn_db)) {stop("No DB connection specificed")}
  if (is.null(config)) {stop("Specify a list with config details")}
  
  
  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  # Set up both connections so they work in either server
  if (server == "phclaims") {conn_dw <- conn_db}
  
  
  #### GET VARS FROM CONFIG FILE ####
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  archive_schema <- config[[server]][["archive_schema"]]
  archive_table <- ifelse(is.null(config[[server]][["archive_table"]]), '',
                          config[[server]][["archive_table"]])
  tmp_schema <- config[[server]][["tmp_schema"]]
  tmp_table <- ifelse(is.null(config[[server]][["tmp_table"]]), '',
                      config[[server]][["tmp_table"]])
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                     config[[server]][["qa_table"]])
  
  duplicate_type <- NA
  
  if (!is.null(config$etl$date_var)) {
    date_var <- config$etl$date_var
  } else {
    date_var <- config$date_var
  }
  
  
  vars <- unlist(names(config$vars))
  # also make version without geo_hash_raw for deduplication section
  vars_dedup <- vars[!vars %in% c("geo_hash_raw")]
  
  # Need to keep only the vars that come before the named ones below
  # This is so we can recreate the address hash field
  vars_prefix <- vars[!vars %in% c("geo_hash_raw", "MBR_ACES_IDNTFR", "etl_batch_id")]
  vars_suffix <- c("MBR_ACES_IDNTFR", "etl_batch_id")
  
  
  if (full_refresh == F) {
    etl_batch_type <- "incremental"
    date_truncate <- DBI::dbGetQuery(
      conn_dw,
      glue::glue_sql("SELECT MIN({`date_var`}) FROM {`from_schema`}.{`from_table`}",
                     .con = conn_dw))
  } else {
    etl_batch_type <- "full"
  }
  
  
  #### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
  current_batch_id <- as.numeric(odbc::dbGetQuery(
    conn_dw,
    glue::glue_sql("SELECT MAX(etl_batch_id) FROM {`from_schema`}.{`from_table`}",
                   .con = conn_dw)))
  
  if (is.na(current_batch_id)) {
    stop(glue::glue_sql("Missing etl_batch_id in {`from_schema`}.{`from_table`}"))
  }
  
  
  #### ARCHIVE EXISTING TABLE ####
  # Different approaches between Azure data warehouse (rename) and on-prem SQL DB (alter schema)
  # Check that the stage table actually exists so we don't accidentally wipe the archive table
  if (full_refresh == F & DBI::dbExistsTable(conn_dw, DBI::Id(schema = to_schema, table = to_table))) {
    if (server == "hhsaw") {
      if(DBI::dbExistsTable(conn_dw, DBI::Id(schema = to_schema, table = paste0(archive_table, '_bak')))) {
        try(DBI::dbSendQuery(conn_dw, 
                             glue::glue_sql("DROP TABLE {`to_schema`}.{`paste0(archive_table, '_bak')`}", 
                                            .con = conn_dw)))
      }
      try(DBI::dbSendQuery(conn_dw, 
                           glue::glue_sql("RENAME OBJECT {`to_schema`}.{`archive_table`} TO {`paste0(archive_table, '_bak')`}",
                                          .con = conn_dw)))
      try(DBI::dbSendQuery(conn_dw, 
                           glue::glue_sql("RENAME OBJECT {`to_schema`}.{`to_table`} TO {`archive_table`}",
                                          .con = conn_dw)))
    } else if (server == "phclaims") {
      if(DBI::dbExistsTable(conn_dw, DBI::Id(schema = archive_schema, table = paste0(archive_table, '_bak')))) {
        try(DBI::dbSendQuery(conn_db, 
                             glue::glue_sql("DROP TABLE {`archive_schema`}.{`paste0(archive_table, '_bak')`}", 
                                            .con = conn_db)))
      }
      try(DBI::dbSendQuery(conn_db, 
                           glue::glue("EXEC sp_rename '{archive_schema}.{archive_table}',  '{paste0(archive_table, '_bak')}'")))
      alter_schema_f(conn = conn_db, 
                     from_schema = to_schema, 
                     to_schema = archive_schema,
                     table_name = to_table, 
                     rename_index = F)
    }
  }
  
  
  #### CHECK FOR DUPLICATES AND ADDRESS THEM ####
  # Some months have multiple rows per person-month-RAC combo. There are currently 
  #   3 main reasons for this:
  # 1) Multiple end reasons (mostly dates prior to 2018-09)
  # 2) One row with missing HOH_ID and one row with non-missing HOH_ID (mostly mid-2019)
  # 3) RAC name (usually secondary) spelled incorrectly 
  #  (Involuntary Inpatient Psychiactric Treatment (ITA) vs Involuntary Inpatient Psychiatric Treatment (ITA))
  #
  
  message("Checking for any duplicates")
  
  rows_load_raw <- as.numeric(dbGetQuery(
    conn_dw,
    glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", 
                   .con = conn_dw)))
  
  distinct_rows_load_raw <- as.numeric(dbGetQuery(
    conn_dw,
    glue::glue_sql("SELECT COUNT (*) FROM
  (SELECT DISTINCT CLNDR_YEAR_MNTH, MBR_H_SID, MEDICAID_RECIPIENT_ID, RAC_FROM_DATE, RAC_TO_DATE, RAC_CODE, RAC_NAME, DUALELIGIBLE_INDICATOR
  FROM {`from_schema`}.{`from_table`}) a", .con = conn_dw)))
  
  
  # If a match, don't bother checking any further
  if (rows_load_raw != distinct_rows_load_raw) {
    message("Processing duplicate rows")
    
    # Need three different approaches to fix these. Check for each type of error.
    message("Duplicates found, checking for multiple END_REASON_NAME rows per id/month/RAC combo")
    duplicate_check_reason <- as.numeric(dbGetQuery(
      conn_dw,
      glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MBR_H_SID, MEDICAID_RECIPIENT_ID, RAC_FROM_DATE, 
             RAC_TO_DATE, RAC_CODE, RAC_NAME, DUALELIGIBLE_INDICATOR
                 FROM {`from_schema`}.{`from_table`}) a",
                     .con = conn_dw)))
    
    message("Checking for multiple HOH_ID rows per id/month/RAC combo")
    duplicate_check_hoh <- as.numeric(dbGetQuery(
      conn_dw,
      glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MBR_H_SID, MEDICAID_RECIPIENT_ID, RAC_FROM_DATE, 
             RAC_TO_DATE, RAC_CODE, END_REASON_NAME, RAC_NAME, DUALELIGIBLE_INDICATOR
                 FROM {`from_schema`}.{`from_table`}) a",
                     .con = conn_dw)))
    
    message("Checking for misspelled RAC names rows per id/month/RAC combo")
    duplicate_check_rac <- as.numeric(dbGetQuery(
      conn_dw,
      glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MBR_H_SID, MEDICAID_RECIPIENT_ID, RAC_FROM_DATE, 
             RAC_TO_DATE, RAC_CODE, END_REASON_NAME, DUALELIGIBLE_INDICATOR
                 FROM {`from_schema`}.{`from_table`}) a",
                     .con = conn_dw)))
    
    duplicate_type <- case_when(
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "All three types of duplicates present",
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac == rows_load_raw ~ "Duplicate END_REASON_NAME AND HOH_ID rows present",
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh == rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "Duplicate END_REASON_NAME AND RAC_NAME rows present",
      duplicate_check_reason == rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "Duplicate HOH_ID AND RAC_NAME rows present",
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh == rows_load_raw & 
        duplicate_check_rac == rows_load_raw ~ "Only END_REASON_NAME duplicates present",
      duplicate_check_reason == rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac == rows_load_raw ~ "Only HOH_ID duplicates present",
      duplicate_check_reason == rows_load_raw & duplicate_check_hoh == rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "Only RAC_NAME duplicates present")
    if(!exists('duplicate_type')) { duplicate_type <- NA }
    
    # Note: solution assumes only one duplicate type present in any given id/month/RAC combo
    if (!is.na(duplicate_type)) {
      message(glue::glue("{duplicate_type}. Using temp table code to fix."))
      
      ### Pull in vars for making temp tables
      
      # Need to specify which temp table the vars come from
      # Can't handle this just with glue_sql
      # (see https://community.rstudio.com/t/using-glue-sql-s-collapse-with-table-name-identifiers/11633)
      var_names <- lapply(vars_dedup, function(nme) DBI::Id(table = "a", column = nme))
      
      
      # Use priority set out below (higher resaon score = higher priority)
      ### Set up temporary table
      message("Setting up a temp table to remove duplicate rows")
      # This can then be used to deduplicate rows with differing end reasons
      # The Azure connection keeps dropping, causing temp tables to vanish
      # Switching to creating actual tables for stability
      
      # Remove temp table if it exists
      try(odbc::dbRemoveTable(conn_dw, DBI::Id(schema = tmp_schema, 
                                               table = paste0(tmp_table, "mcaid_elig"))), 
          silent = T)
      
      system.time(DBI::dbExecute(conn_dw,
                                 glue::glue_sql(
                                   "SELECT {`vars_dedup`*},
      CASE WHEN END_REASON_NAME IS NULL THEN 1
        WHEN END_REASON_NAME = 'Other' THEN 2
        WHEN END_REASON_NAME = 'Other - For User Generation Only' THEN 3
        WHEN END_REASON_NAME = 'Review Not Complete' THEN 4
        WHEN END_REASON_NAME = 'No Eligible Household Members' THEN 5
        WHEN END_REASON_NAME = 'Already Eligible for Program in Different AU' THEN 6
        ELSE 7 END AS reason_score 
      INTO {`tmp_schema`}.{DBI::SQL(tmp_table)}mcaid_elig 
      FROM {`from_schema`}.{`from_table`}",
                                   .con = conn_dw)))
      
      
      
      # Fix spelling of RAC if needed
      if (duplicate_check_rac != rows_load_raw) {
        DBI::dbExecute(conn_dw, glue::glue_sql(
          "UPDATE {`tmp_schema`}.{DBI::SQL(tmp_table)}mcaid_elig  
               SET RAC_NAME = 'Involuntary Inpatient Psychiatric Treatment (ITA)' 
               WHERE RAC_NAME = 'Involuntary Inpatient Psychiactric Treatment (ITA)'", .con = conn_dw))
      }
      
      # Check no dups exist by recording row counts
      temp_rows_01 <- as.numeric(dbGetQuery(
        conn_dw,
        glue::glue_sql("SELECT COUNT (*) FROM {`tmp_schema`}.{DBI::SQL(tmp_table)}mcaid_elig ", 
                       .con = conn_dw)))
      
      if (rows_load_raw != temp_rows_01) {
        stop("Not all rows were copied to the temp table")
      } else {
        message(glue::glue("The {tmp_schema}.{tmp_table}mcaid_elig table has {temp_rows_01} rows, as expected"))
      }
      
      
      ### Manipulate the temporary table to deduplicate
      # Remove temp table if it exists
      try(odbc::dbRemoveTable(conn_dw, DBI::Id(schema = tmp_schema, 
                                               table = paste0(tmp_table, "mcaid_elig_dedup"))), 
          silent = T)
      
      dedup_sql <- glue::glue_sql(
        "SELECT DISTINCT {`var_names`*}
        INTO {`tmp_schema`}.{DBI::SQL(tmp_table)}mcaid_elig_dedup
        FROM
      (SELECT {`vars_dedup`*}, reason_score FROM {`tmp_schema`}.{DBI::SQL(tmp_table)}mcaid_elig ) a
        LEFT JOIN
        (SELECT CLNDR_YEAR_MNTH, MBR_H_SID, MEDICAID_RECIPIENT_ID, RAC_FROM_DATE,
          RAC_TO_DATE, RAC_CODE, 
          MAX(reason_score) AS max_score 
          FROM {`tmp_schema`}.{DBI::SQL(tmp_table)}mcaid_elig 
          GROUP BY CLNDR_YEAR_MNTH, MBR_H_SID, MEDICAID_RECIPIENT_ID, RAC_FROM_DATE, RAC_TO_DATE, 
          RAC_CODE) b
        ON a.CLNDR_YEAR_MNTH = b.CLNDR_YEAR_MNTH AND
        a.MBR_H_SID = b.MBR_H_SID AND 
        a.MEDICAID_RECIPIENT_ID = b.MEDICAID_RECIPIENT_ID AND
        (a.RAC_FROM_DATE = b.RAC_FROM_DATE OR (a.RAC_FROM_DATE IS NULL AND b.RAC_FROM_DATE IS NULL)) AND
        (a.RAC_TO_DATE = b.RAC_TO_DATE OR (a.RAC_TO_DATE IS NULL AND b.RAC_TO_DATE IS NULL)) AND
        (a.RAC_CODE = b.RAC_CODE OR (a.RAC_CODE IS NULL AND b.RAC_CODE IS NULL))
        WHERE a.reason_score = b.max_score",
        .con = conn_dw)
      
      DBI::dbExecute(conn_dw, dedup_sql)
      
      # Keep track of how many of the duplicate rows are accounted for
      temp_rows_02 <- as.numeric(dbGetQuery(
        conn_dw, glue::glue_sql("SELECT COUNT (*) 
                                FROM {`tmp_schema`}.{DBI::SQL(tmp_table)}mcaid_elig_dedup", 
                                .con = conn_dw)))
      
      dedup_row_diff <- temp_rows_01 - temp_rows_02
      
      if (temp_rows_02 == distinct_rows_load_raw) {
        message(glue::glue("All duplicates accounted for (new row total = {distinct_rows_load_raw})"))
      } else if (temp_rows_02 < distinct_rows_load_raw) {
        message(glue::glue("The {tmp_schema}.{tmp_table}mcaid_elig_dedup table has {temp_rows_02} rows ",
                           "({dedup_row_diff} fewer than {tmp_table}mcaid_elig)",
                           " but only {temp_rows_01 - distinct_rows_load_raw} duplicate rows were expected"))
      } else {
        message(glue::glue("The {tmp_schema}.{tmp_table}mcaid_elig_dedup table has {temp_rows_02} rows ",
                           "({dedup_row_diff} fewer than {tmp_table}mcaid_elig)",
                           " but {distinct_rows_load_raw - temp_rows_02} duplicate rows remain"))
      }
      
    } else {
      message(" A new type of duplicate is present. Investigate further")
    }
  }
  
  
  
  #### LOAD TABLE ####
  # Combine relevant parts of archive and new data
  message("Loading to stage table")
  
  # First drop existing table
  try(odbc::dbRemoveTable(conn_dw, DBI::Id(schema = to_schema, table = to_table)), silent = T)
  
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
  
  
  # Then set up first block of SQL, which varies by server
  if (server == "hhsaw") {
    load_intro <- glue::glue_sql("CREATE TABLE {`to_schema`}.{`to_table`} 
                                    WITH (CLUSTERED COLUMNSTORE INDEX, 
                                          DISTRIBUTION = HASH ({`date_var`})) AS ",
                                 .con = conn_dw)
  } else if (server == "phclaims") {
    create_table_f(conn = conn_dw, 
                   server = server,
                   config = config,
                   overwrite = T)
    load_intro <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK) ({`vars`*}) ", .con = conn_dw)
  }
  
  if (full_refresh == F) {
    # Select the source, depending on if deduplication has been carried out
    if (is.na(duplicate_type)) {
      sql_combine <- glue::glue_sql("{DBI::SQL(load_intro)}
                                    SELECT {`vars`*} FROM 
                                    {`archive_schema`}.{`archive_table`}
                                    WHERE {`date_var`} < {date_truncate}
                                    UNION
                                    SELECT {`vars_prefix`*}, 
                                    CONVERT(char(64),
                                            HASHBYTES('SHA2_256',
                                                      -- NOTE: NEED FILLER BECAUSE THERE IS NO geo_add3_raw
                                                      CAST(UPPER(CONCAT(RSDNTL_ADRS_LINE_1, '|', RSDNTL_ADRS_LINE_2, 
                                                      '|', '|', RSDNTL_CITY_NAME, '|', RSDNTL_STATE_CODE, '|', 
                                                      RSDNTL_POSTAL_CODE)) AS VARCHAR(1275))),2) AS geo_hash_raw, 
                                    {`vars_suffix`*} FROM
                                    {`from_schema`}.{`from_table`}
                                    WHERE {`date_var`} >= {date_truncate}",
                                    .con = conn_dw)
    } else {
      sql_combine <- glue::glue_sql("{DBI::SQL(load_intro)} 
                                     SELECT {`vars`*} FROM 
                                     {`archive_schema`}.{`archive_table`}
                                    WHERE {`date_var`} < {date_truncate}
                                    UNION
                                    SELECT {`vars_prefix`*}, 
                                    CONVERT(char(64),
                                            HASHBYTES('SHA2_256',
                                                      -- NOTE: NEED FILLER BECAUSE THERE IS NO geo_add3_raw
                                                      CAST(UPPER(CONCAT(RSDNTL_ADRS_LINE_1, '|', RSDNTL_ADRS_LINE_2, 
                                                      '|', '|', RSDNTL_CITY_NAME, '|', RSDNTL_STATE_CODE, '|', 
                                                      RSDNTL_POSTAL_CODE)) AS VARCHAR(1275))),2) AS geo_hash_raw, 
                                    {`vars_suffix`*} FROM 
                                    {`tmp_schema`}.{DBI::SQL(tmp_table)}mcaid_elig_dedup
                                    WHERE {`date_var`} >= {date_truncate}",
                                    .con = conn_dw)
    }
  } else if (full_refresh == T) {
    # Select the source, depending on if deduplication has been carried out
    if (is.na(duplicate_type)) {
      sql_combine <- glue::glue_sql("{DBI::SQL(load_intro)} 
                                    SELECT {`vars_prefix`*}, 
                                    CONVERT(char(64),
                                            HASHBYTES('SHA2_256',
                                                      -- NOTE: NEED FILLER BECAUSE THERE IS NO geo_add3_raw
                                                      CAST(UPPER(CONCAT(RSDNTL_ADRS_LINE_1, '|', RSDNTL_ADRS_LINE_2, 
                                                      '|', '|', RSDNTL_CITY_NAME, '|', RSDNTL_STATE_CODE, '|', 
                                                      RSDNTL_POSTAL_CODE)) AS VARCHAR(1275))),2) AS geo_hash_raw, 
                                    {`vars_suffix`*} FROM 
                                    FROM {`from_schema`}.{`from_table`} ",
                                    .con = conn_dw)
      
    } else {
      sql_combine <- glue::glue_sql("{DBI::SQL(load_intro)} 
                                    SELECT {`vars_prefix`*}, 
                                    CONVERT(char(64),
                                            HASHBYTES('SHA2_256',
                                                      -- NOTE: NEED FILLER BECAUSE THERE IS NO geo_add3_raw
                                                      CAST(UPPER(CONCAT(RSDNTL_ADRS_LINE_1, '|', RSDNTL_ADRS_LINE_2, 
                                                      '|', '|', RSDNTL_CITY_NAME, '|', RSDNTL_STATE_CODE, '|', 
                                                      RSDNTL_POSTAL_CODE)) AS VARCHAR(1275))),2) AS geo_hash_raw,
                                    {`vars_suffix`*} FROM 
                                    FROM {`tmp_schema`}.{DBI::SQL(tmp_table)}mcaid_elig_dedup",
                                    .con = conn_dw)
    }
  }
  
  sql_combine <- glue::glue_sql("{DBI::SQL(sql_combine)};
                                UPDATE {`to_schema`}.{`to_table`}
                                SET MEDICAID_RECIPIENT_ID = UPPER(MEDICAID_RECIPIENT_ID)",
                                .con = conn_dw)
  
  ### Load table
  system.time(DBI::dbExecute(conn_dw, sql_combine))
  
  
  
  #### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
  message("Running QA checks")
  
  # Obtain row counts for other tables (rows_load_raw already calculated above)
  rows_stage <- as.numeric(dbGetQuery(
    conn_dw, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = conn_dw)))
  
  if (full_refresh == F) {
    rows_archive <- as.numeric(dbGetQuery(
      conn_dw, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`archive_table`} 
                            WHERE {`date_var`} < {date_truncate}",
                              .con = conn_dw)))
    
    if (exists("dedup_row_diff")) {
      row_diff = rows_stage - (rows_archive + rows_load_raw) + dedup_row_diff
    } else {
      row_diff = rows_stage - (rows_archive + rows_load_raw)
    }
  } else {
    if (exists("dedup_row_diff")) {
      row_diff = rows_stage - rows_load_raw + dedup_row_diff
    } else {
      row_diff = rows_stage - rows_load_raw
    }
  }
  
  if (full_refresh == F) {
    row_diff_qa_type <- 'Rows passed from load_raw AND archive to stage'
  } else {
    row_diff_qa_type <- 'Rows passed from load_raw to stage'
  }
  
  # Load to metadata table
  if (row_diff != 0) {
    row_diff_qa_fail <- 1
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  {row_diff_qa_type}, 
                                  'FAIL',
                                  {format(Sys.time(), usetz = FALSE)},
                                  'Issue even after accounting for any duplicate rows. Investigate further.')",
                                  .con = conn_db))
    warning("Number of rows does not match total expected")
  } else {
    row_diff_qa_fail <- 0
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  {row_diff_qa_type}, 
                                  'PASS',
                                  {format(Sys.time(), usetz = FALSE)},
                                  'Number of rows in stage matches expected (n = {rows_stage})')",
                                  .con = conn_db))
  }
  
  
  #### QA CHECK: NULL IDs ####
  null_ids <- as.numeric(dbGetQuery(conn_dw, glue::glue_sql(
    "SELECT COUNT (*) FROM {`to_schema`}.{`to_table`} 
                                    WHERE MEDICAID_RECIPIENT_ID IS NULL OR MBR_H_SID IS NULL",
    .con = conn_dw)))
  
  if (null_ids != 0) {
    null_ids_qa_fail <- 1
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'Null Medicaid IDs or MBR_H_SID', 
                                  'FAIL',
                                  {format(Sys.time(), usetz = FALSE)},
                                  'Null IDs found. Investigate further.')",
                                  .con = conn_db))
    warning("Null Medicaid IDs found in claims.stage_mcaid_elig")
  } else {
    null_ids_qa_fail <- 0
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'Null Medicaid IDs', 
                                  'PASS',
                                  {format(Sys.time(), usetz = FALSE)},
                                  'No null IDs found')",
                                  .con = conn_db))
  }
  
  
  #### ADD VALUES TO QA_VALUES TABLE ####
  DBI::dbExecute(
    conn = conn_db,
    glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'row_count', 
                   '{rows_stage}', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   {refresh_type})",
                   refresh_type = ifelse(full_refresh == F, 
                                         'Count after partial refresh', 
                                         'Count after full refresh'),
                   .con = conn_db))
  
  
  #### ADD OVERALL QA RESULT ####
  # This creates an overall QA result to feed the stage.v_mcaid_status view, 
  #    which is used by the integrated data hub to check for new data to run
  if (max(row_diff_qa_fail, null_ids_qa_fail) == 1) {
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'Overall QA result', 
                                  'FAIL',
                                  {format(Sys.time(), usetz = FALSE)},
                                  'One or more QA steps failed')",
                                  .con = conn_db))
    stop("One or more QA steps failed. See claims.metadata_qa_mcaid for more details")
  } else {
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'Overall QA result', 
                                  'PASS',
                                  {format(Sys.time(), usetz = FALSE)},
                                  'All QA steps passed')",
                                  .con = conn_db))
  }
  
  
  #### CLEAN UP ####
  # Drop global temp table
  rm(vars, var_names)
  rm(duplicate_check_reason, duplicate_check_hoh, duplicate_check_rac, duplicate_type,
     temp_rows_01, temp_rows_02, dedup_sql)
  rm(from_schema, from_table, to_schema, to_table, archive_schema, date_truncate)
  rm(rows_stage, rows_load_raw, rows_archive, distinct_rows_load_raw, null_ids)
  rm(row_diff_qa_fail, null_ids_qa_fail)
  rm(config)
  rm(sql_combine)
  rm(current_batch_id)
}

