#### CODE TO CREATE STAGE MCAID ELIG TABLE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08

### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


load_stage.mcaid_elig_f <- function(conn = NULL, full_refresh = F, config = NULL) {
  ### Error checks
  if (is.null(conn)) {stop("No DB connection specificed")}
  if (is.null(config)) {stop("Specify a list with config details")}
  
  
  #### GET VARS FROM CONFIG FILE ####
  from_schema <- config$from_schema
  from_table <- config$from_table
  to_schema <- config$to_schema
  to_table <- config$to_table
  
  if (!is.null(config$etl$date_var)) {
    date_var <- config$etl$date_var
  } else {
    date_var <- config$date_var
  }
  
  # Remove etl_batch_id from list of vars as it is added at the end
  vars <- unlist(names(config$vars))
  vars <- vars[!vars %in% "etl_batch_id"]
  
  if (full_refresh == F) {
    archive_schema <- config$archive_schema
    archive_table <- config$archive_table
    date_truncate <- config$etl$date_min
    
    etl_batch_type <- "incremental"
  } else {
    etl_batch_type <- "full"
  }

  
  #### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
  # Now need to make ETL batch ID here as it is added to stage.
  # Raw data are in an external table that points to the data warehouse. Can't add an ETL column to that.
  
  # First set up dates for ETL log
  etl_date_min <- as.Date(paste0(str_sub(config$etl$date_min, 1, 4), "-",
                                       str_sub(config$etl$date_min, 5, 6), "-",
                                       "01"), format = "%Y-%m-%d")
  etl_date_max <- as.Date(paste0(str_sub(config$etl$date_max, 1, 4), "-",
                                       str_sub(config$etl$date_max, 5, 6), "-",
                                       "01"), format = "%Y-%m-%d") %m+% months(1) - days(1)
  
  
  current_batch_id <- load_metadata_etl_log_f(conn = db_claims, 
                                              batch_type = etl_batch_type, 
                                              data_source = "Medicaid", 
                                              date_min = etl_date_min,
                                              date_max = etl_date_max,
                                              delivery_date = config$etl$date_delivery, 
                                              note = config$etl$note)

  
  #### QA RAW DATA ####
  # Now that we have a batch_etl_id we can do some basic QA on the raw data
  
  #### QA CHECK: ROW COUNTS MATCH SOURCE FILE COUNT ####
  message("Checking loaded row counts vs. expected")
  # Use the load config file for the list of tables to check and their expected row counts
  qa_rows_sql <- qa_load_row_count_f(conn = db_claims, schema = config$from_schema,
                                     table = config$from_table, row_count = config$etl$row_count)
  
  # Report individual results out to SQL table
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'claims.raw_mcaid_elig',
                                        'Number rows loaded to SQL vs. expected value(s)', 
                                        {qa_rows_sql$outcome[1]},
                                        {Sys.time()},
                                        {qa_rows_sql$note[1]})",
                                  .con = db_claims))
  
  if (qa_rows_sql$outcome[1] == "FAIL") {
    qa_row_fail <- 1L
    warning(glue::glue("Mismatching row count between source file and SQL table. 
                  Check claims.metadata_qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  } else {
    qa_row_fail <- 0L
  }
  
  
  #### QA CHECK: COUNT OF DISTINCT ID, CLNDR_YEAR_MNTH, FROM DATE, TO DATE, SECONDARY RAC ####
  message("Running additional QA items")
  # Should be no combo of ID, CLNDR_YEAR_MNTH, from_date, to_date, and secondary RAC with >1 row
  # However, there are cases where there is a duplicate row but the only difference is
  # a NULL or different END_REASON. Include END_REASON to account for this.
  distinct_rows_load_raw <- as.numeric(dbGetQuery(db_claims,
                                         glue::glue_sql("SELECT COUNT (*) FROM
                                         (SELECT DISTINCT CLNDR_YEAR_MNTH, 
                                         MEDICAID_RECIPIENT_ID, FROM_DATE, TO_DATE,
                                         RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, END_REASON 
                                         FROM {`config$from_schema`}.{`config$from_table`}) a",
                                                        .con = db_claims)))
  
  rows_load_raw <- as.numeric(dbGetQuery(db_claims, glue::glue_sql(
    "SELECT COUNT (*) FROM {`config$from_schema`}.{`config$from_table`}", .con = db_claims)))
  
  
  if (distinct_rows_load_raw != rows_load_raw) {
    DBI::dbExecute(conn = db_claims,
                     glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                    (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                    VALUES ({current_batch_id}, 
                                    'claims.raw_mcaid_elig',
                                    'Distinct rows (ID, CLNDR_YEAR_MNTH, FROM/TO DATE, RPRTBL_RAC_CODE, SECONDARY RAC, END_REASON)', 
                                    'FAIL',
                                    {Sys.time()},
                                    'Number distinct rows ({distinct_rows_load_raw}) != total rows ({rows_load_raw})')",
                                    .con = db_claims))
    warning(glue("Number of distinct rows ({distinct_rows_load_raw}) does not match total expected ({rows_load_raw})"))
  } else {
    DBI::dbExecute(conn = db_claims,
                     glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.raw_mcaid_elig',
                                  'Distinct rows (ID, CLNDR_YEAR_MNTH, FROM/TO DATE, RPRTBL_RAC_CODE, SECONDARY RAC, END_REASON)', 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of distinct rows equals total # rows ({rows_load_raw})')",
                                    .con = db_claims))
  }
  
  
  #### QA CHECK: DATE RANGE MATCHES EXPECTED RANGE ####
  qa_date_range <- qa_date_range_f(conn = db_claims,
                                   schema = config$from_schema,
                                   table = config$from_table,
                                   date_min_exp = config$etl$date_min,
                                   date_max_exp = config$etl$date_max,
                                   date_var = config$etl$date_var)
  
  # Report individual results out to SQL table
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'claims.raw_mcaid_elig',
                                        'Actual vs. expected date range in data', 
                                        {qa_date_range$outcome[1]},
                                        {Sys.time()},
                                        {qa_date_range$note[1]})",
                                  .con = db_claims))
  
  if (qa_date_range$outcome[1] == "FAIL") {
    qa_date_fail <- 1L
    warning(glue::glue("Mismatching date range between source file and SQL table. 
                  Check claims.metadata_qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  } else {
    qa_date_fail <- 0L
  }
  
  
  #### QA CHECK: LENGTH OF MCAID ID = 11 CHARS ####
  id_len <- dbGetQuery(db_claims, glue::glue_sql(
                       "SELECT MIN(LEN(MEDICAID_RECIPIENT_ID)) AS min_len, 
                     MAX(LEN(MEDICAID_RECIPIENT_ID)) AS max_len 
                     FROM {`config$from_schema`}.{`config$from_table`}",
                       .con = db_claims))
  
  if (id_len$min_len != 11 | id_len$max_len != 11) {
    DBI::dbExecute(conn = db_claims,
      glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'claims.raw_mcaid_elig',
                   'Length of Medicaid ID', 
                   'FAIL', 
                   {Sys.time()}, 
                   'Minimum ID length was {id_len$min_len}, maximum was {id_len$max_len}')",
                     .con = db_claims))
    
    qa_id_len_fail <- 1L
    warning(glue::glue("Some Medicaid IDs are not 11 characters long.  
                  Check claims.metadata_qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  } else {
    qa_id_len_fail <- 0L
    DBI::dbExecute(conn = db_claims,
      glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'claims.raw_mcaid_elig',
                   'Length of Medicaid ID', 
                   'PASS', 
                   {Sys.time()}, 
                   'All Medicaid IDs were 11 characters')",
                     .con = db_claims))
  }
  
  
  #### QA CHECK: LENGTH OF RAC CODES = 4 CHARS ####
  rac_len <- dbGetQuery(db_claims, glue::glue_sql(
                        "SELECT MIN(LEN(RPRTBL_RAC_CODE)) AS min_len, 
                     MAX(LEN(RPRTBL_RAC_CODE)) AS max_len, 
                     MIN(LEN(SECONDARY_RAC_CODE)) AS min_len2, 
                     MAX(LEN(SECONDARY_RAC_CODE)) AS max_len2 
                     FROM {`config$from_schema`}.{`config$from_table`}",
                        .con = db_claims))
  
  if (rac_len$min_len != 4 | rac_len$max_len != 4 | rac_len$min_len2 != 4 | rac_len$max_len2 != 4) {
    qa_rac_len_fail <- 1L
    DBI::dbExecute(conn = db_claims,
      glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'claims.raw_mcaid_elig',
                   'Length of RAC codes', 
                   'FAIL', 
                   {Sys.time()}, 
                   'Min RPRTBLE_RAC_CODE length was {rac_len$min_len}, max was {rac_len$max_len};
                   Min SECONDARY_RAC_CODE length was {rac_len$min_len2}, max was {rac_len$max_len2}')",
                     .con = db_claims))
    
    message(glue::glue("Some RAC codes are not 4 characters long.  
                  Check claims.metadata_qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  } else {
    qa_rac_len_fail <- 0L
    DBI::dbExecute(conn = db_claims,
      glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'claims.raw_mcaid_elig',
                   'Length of RAC codes', 
                   'PASS', 
                   {Sys.time()}, 
                   'All RAC codes (reportable and secondary) were 4 characters')",
                     .con = db_claims))
  }
  
  
  #### QA CHECK: NUMBER NULLs IN FROM_DATE ####
  from_nulls <- dbGetQuery(db_claims, glue::glue_sql(
                           "SELECT a.null_dates, b.total_rows 
                      FROM
                      (SELECT 
                        COUNT (*) AS null_dates, ROW_NUMBER() OVER (ORDER BY NEWID()) AS seqnum
                        FROM {`config$from_schema`}.{`config$from_table`}
                        WHERE FROM_DATE IS NULL) a
                      LEFT JOIN
                      (SELECT COUNT(*) AS total_rows, ROW_NUMBER() OVER (ORDER BY NEWID()) AS seqnum
                        FROM {`config$from_schema`}.{`config$from_table`}) b
                      ON a.seqnum = b.seqnum", .con = db_claims))
  
  pct_null <- round(from_nulls$null_dates / from_nulls$total_rows  * 100, 3)
  
  if (pct_null > 2.0) {
    qa_from_null_fail <- 1L
    DBI::dbExecute(conn = db_claims,
      glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'claims.raw_mcaid_elig',
                   'NULL from dates', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {from_nulls$null_dates} NULL from dates ({pct_null}% of total rows)')",
                     .con = db_claims))
    
    message(glue::glue(">2% FROM_DATE rows are null.  
                  Check claims.metadata_qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  } else {
    qa_from_null_fail <- 0L
    DBI::dbExecute(conn = db_claims,
      glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'claims.raw_mcaid_elig',
                   'NULL from dates', 
                   'PASS', 
                   {Sys.time()}, 
                   '<2% of from date rows were null ({pct_null}% of total rows)')",
                     .con = db_claims))
  }
  
  
  
  #### QA CHECK: SUMMARY ####
  if (qa_row_fail + qa_date_fail + qa_id_len_fail + qa_rac_len_fail + qa_from_null_fail > 0) {
    stop("One or more critical QA checks failed, see results in claims.metadata_qa_mcaid")
  } else {
    message("All QA items complete, see results in claims.metadata_qa_mcaid")
  }
  
  
  
  #### ARCHIVE EXISTING TABLE ####
  # No longer switching schema, instead just renaming table. Need to drop existing archive table first
  if (full_refresh == F) {
    try(DBI::dbSendQuery(conn, glue::glue("DROP TABLE {archive_schema}.{archive_table}")))
    DBI::dbSendQuery(conn, glue::glue("EXEC sp_rename '{to_schema}.{to_table}', '{archive_table}'"))
  }
  
  
  #### CHECK FOR DUPLICATES AND ADDRESS THEM ####
  # Some months have multiple rows per person-month-RAC combo. There are currently 
  #   3 main reasons for this:
  # 1) Multiple end reasons (mostly dates prior to 2018-09)
  # 2) One row with missing HOH_ID and one row with non-missing HOH_ID (mostly mid-2019)
  # 3) RAC name (usually secondary) spelled incorrectly 
  #  (Involuntary Inpatient Psychiactric Treatment (ITA) vs Involuntary Inpatient Psychiatric Treatment (ITA))
  #
  # Note: the initial check is now done above in the QA steps. Can use results here.
  
  # If a match, don't bother checking any further
  if (rows_load_raw != distinct_rows_load_raw) {
    message("Processing duplicate rows")
    
    # Need three different approaches to fix these. Check for each type of error.
    message("Duplicates found, checking for multiple END_REASON rows per id/month/RAC combo")
    duplicate_check_reason <- as.numeric(dbGetQuery(
      conn,
      glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
             TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, HOH_ID, RPRTBL_RAC_NAME,
             SECONDARY_RAC_NAME 
                 FROM {`from_schema`}.{`from_table`}) a",
                     .con = conn)))
    
    message("Checking for multiple HOH_ID rows per id/month/RAC combo")
    duplicate_check_hoh <- as.numeric(dbGetQuery(
      conn,
      glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
             TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, END_REASON, RPRTBL_RAC_NAME,
             SECONDARY_RAC_NAME 
                 FROM {`from_schema`}.{`from_table`}) a",
                     .con = conn)))
    
    message("Checking for misspelled RAC names rows per id/month/RAC combo")
    duplicate_check_rac <- as.numeric(dbGetQuery(
      conn,
      glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
             TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, HOH_ID, END_REASON
                 FROM {`from_schema`}.{`from_table`}) a",
                     .con = conn)))
    
    duplicate_type <- case_when(
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "All three types of duplicates present",
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac == rows_load_raw ~ "Duplicate END_REASON AND HOH_ID rows present",
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh == rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "Duplicate END_REASON AND RAC_NAME rows present",
      duplicate_check_reason == rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "Duplicate HOH_ID AND RAC_NAME rows present",
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh == rows_load_raw & 
        duplicate_check_rac == rows_load_raw ~ "Only END_REASON duplicates present",
      duplicate_check_reason == rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac == rows_load_raw ~ "Only HOH_ID duplicates present",
      duplicate_check_reason == rows_load_raw & duplicate_check_hoh == rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "Only RAC_NAME duplicates present")
    
    
    # Note: solution assumes only one duplicate type present in any given id/month/RAC combo
    if (!is.na(duplicate_type)) {
      message(glue::glue("{duplicate_type}. Using temp table code to fix."))
      
      ### Pull in vars for making temp tables
      
      # Need to specify which temp table the vars come from
      # Can't handle this just with glue_sql
      # (see https://community.rstudio.com/t/using-glue-sql-s-collapse-with-table-name-identifiers/11633)
      var_names <- lapply(vars, function(nme) DBI::Id(table = "a", column = nme))
      
      
      # Use priority set out below (higher resaon score = higher priority)
      ### Set up temporary table
      message("Setting up a temp table to remove duplicate rows")
      # This can then be used to deduplicate rows with differing end reasons
      # The Azure connection keeps dropping, causing temp tables to vanish
      # Switching to creating actual tables for stability
      
      # Remove temp table if it exists
      try(odbc::dbRemoveTable(conn, DBI::Id(schema = from_schema, table = "tmp_mcaid_elig")), silent = T)
      
      system.time(odbc::dbGetQuery(conn,
        glue::glue_sql(
          "SELECT {`vars`*},
      CASE WHEN END_REASON IS NULL THEN 1
        WHEN END_REASON = 'Other' THEN 2
        WHEN END_REASON = 'Other - For User Generation Only' THEN 3
        WHEN END_REASON = 'Review Not Complete' THEN 4
        WHEN END_REASON = 'No Eligible Household Members' THEN 5
        WHEN END_REASON = 'Already Eligible for Program in Different AU' THEN 6
        ELSE 7 END AS reason_score 
      INTO {`from_schema`}.tmp_mcaid_elig 
      FROM {`from_schema`}.{`from_table`}",
          .con = conn)))
      
      
      
      
      
      # Fix spelling of RAC if needed
      if (duplicate_check_rac != rows_load_raw) {
        dbGetQuery(conn, glue::glue_sql(
                   "UPDATE {`from_schema`}.tmp_mcaid_elig 
               SET RPRTBL_RAC_NAME = 'Involuntary Inpatient Psychiatric Treatment (ITA)' 
               WHERE RPRTBL_RAC_NAME = 'Involuntary Inpatient Psychiactric Treatment (ITA)'", .con = conn))
        
        dbGetQuery(conn, glue::glue_sql(
                   "UPDATE {`from_schema`}.tmp_mcaid_elig
               SET SECONDARY_RAC_NAME = 'Involuntary Inpatient Psychiatric Treatment (ITA)' 
               WHERE SECONDARY_RAC_NAME = 'Involuntary Inpatient Psychiactric Treatment (ITA)'", .con = conn))
      }
      
      # Check no dups exist by recording row counts
      temp_rows_01 <- as.numeric(dbGetQuery(conn, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.tmp_mcaid_elig", .con = conn)))
      if (rows_load_raw != temp_rows_01) {
        stop("Not all rows were copied to the temp table")
      } else {
        message(glue::glue("The {from_schema}.tmp_mcaid_elig table has {temp_rows_01} rows, as expected"))
      }
      
      
      ### Manipulate the temporary table to deduplicate
      # Remove temp table if it exists
      try(odbc::dbRemoveTable(conn, DBI::Id(schema = from_schema, table = "tmp_mcaid_elig_dedup")), silent = T)
      
      dedup_sql <- glue::glue_sql(
        "SELECT DISTINCT {`var_names`*}
        INTO {`from_schema`}.tmp_mcaid_elig_dedup
        FROM
      (SELECT {`vars`*}, reason_score FROM {`from_schema`}.tmp_mcaid_elig) a
        LEFT JOIN
        (SELECT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE,
          TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, 
          MAX(reason_score) AS max_score, MAX(HOH_ID) AS max_hoh
          FROM {`from_schema`}.tmp_mcaid_elig
          GROUP BY CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, TO_DATE, 
          RPRTBL_RAC_CODE, SECONDARY_RAC_CODE) b
        ON a.CLNDR_YEAR_MNTH = b.CLNDR_YEAR_MNTH AND
        a.MEDICAID_RECIPIENT_ID = b.MEDICAID_RECIPIENT_ID AND
        (a.FROM_DATE = b.FROM_DATE OR (a.FROM_DATE IS NULL AND b.FROM_DATE IS NULL)) AND
        (a.TO_DATE = b.TO_DATE OR (a.TO_DATE IS NULL AND b.TO_DATE IS NULL)) AND
        (a.RPRTBL_RAC_CODE = b.RPRTBL_RAC_CODE OR
          (a.RPRTBL_RAC_CODE IS NULL AND b.RPRTBL_RAC_CODE IS NULL)) AND
        (a.SECONDARY_RAC_CODE = b.SECONDARY_RAC_CODE OR
          (a.SECONDARY_RAC_CODE IS NULL AND b.SECONDARY_RAC_CODE IS NULL))
        WHERE a.reason_score = b.max_score AND 
        (a.HOH_ID = b.max_hoh OR (a.HOH_ID IS NULL AND b.max_hoh IS NULL))",
        .con = conn)
      
      odbc::dbGetQuery(conn, dedup_sql)
      
      # Keep track of how many of the duplicate rows are accounted for
      temp_rows_02 <- as.numeric(dbGetQuery(conn, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.tmp_mcaid_elig_dedup", .con = conn)))
      dedup_row_diff <- temp_rows_01 - temp_rows_02
      
      if (temp_rows_02 == distinct_rows_load_raw) {
        message(glue::glue("All duplicates accounted for (new row total = {distinct_rows_load_raw})"))
      } else {
        message(glue::glue("The from_schema.tmp_mcaid_elig_dedup table has {temp_rows_02} rows ",
                           "({dedup_row_diff} fewer than tmp_mcaid_elig)",
                           " but {distinct_rows_load_raw - temp_rows_02} duplicate rows remain"))
      }
      
    } else {
      message(" A new type of duplicate is present. Investigate further")
    }
  }
  
  
  
  temp <- dbGetQuery(db_claims, 
                     "select a.* from 
                     (SELECT DISTINCT CLNDR_YEAR_MNTH, 
                       MEDICAID_RECIPIENT_ID, FROM_DATE, TO_DATE,
                       RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, END_REASON,
                       1 as dummy
                       FROM claims.raw_mcaid_elig_init) a
                     left join
                     (select CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
                     TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, END_REASON
                     from ##mcaid_elig_dedup) b
                     on 
                     a.CLNDR_YEAR_MNTH = b.CLNDR_YEAR_MNTH AND 
                     a.MEDICAID_RECIPIENT_ID = b.MEDICAID_RECIPIENT_ID AND
                     (a.FROM_DATE = b.FROM_DATE OR (a.FROM_DATE IS NULL AND b.FROM_DATE IS NULL)) AND
                     (a.TO_DATE = b.TO_DATE OR (a.TO_DATE IS NULL AND b.TO_DATE IS NULL)) AND
                     (a.RPRTBL_RAC_CODE = b.RPRTBL_RAC_CODE OR
                       (a.RPRTBL_RAC_CODE IS NULL AND b.RPRTBL_RAC_CODE IS NULL)) AND
                     (a.SECONDARY_RAC_CODE = b.SECONDARY_RAC_CODE OR
                       (a.SECONDARY_RAC_CODE IS NULL AND b.SECONDARY_RAC_CODE IS NULL)) AND
                     (a.END_REASON = b.END_REASON OR
                       (a.END_REASON IS NULL AND b.END_REASON IS NULL))
                     where a.dummy is null")
  
  
  
  #### LOAD TABLE ####
  # Combine relevant parts of archive and new data
  message("Loading to stage table")
  
  # Need to recreate stage table first (true if full_refresh == F or T)
  # Assumes create_table_f loaded as part of the master script
  create_table_f(conn = conn, 
                 config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml", 
                 overwrite = T)
  
  if (full_refresh == F) {
    # Select the source, depending on if deduplication has been carried out
    if (is.na(duplicate_type)) {
      sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                  SELECT {`vars`*}, etl_batch_id FROM
                                  {`archive_schema`}.{`archive_table`}
                                  WHERE {`date_var`} < {date_truncate}
                                  UNION
                                  SELECT {`vars`*}, {current_batch_id} AS etl_batch_id FROM
                                  {`from_schema`}.{`from_table`}
                                  WHERE {`date_var`} >= {date_truncate}",
                                    .con = conn)
    } else {
      
      # This is taking around 30 minutes
      sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                    SELECT {`vars`*}, etl_batch_id FROM
                                    {`archive_schema`}.{`archive_table`}
                                    WHERE {`date_var`} < {date_truncate}
                                    UNION
                                    SELECT {`vars`*}, {current_batch_id} AS etl_batch_id FROM
                                    ##mcaid_elig_dedup
                                    WHERE {`date_var`} >= {date_truncate}",
                                    .con = conn)
    }
  } else if (full_refresh == T) {
    # Select the source, depending on if deduplication has been carried out
    if (is.na(duplicate_type)) {
      sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                  SELECT {`vars`*}, {current_batch_id} AS etl_batch_id 
                                  FROM {`from_schema`}.{`from_table`}",
                                    .con = conn)
    } else {
      sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                    SELECT {`vars`*}, {current_batch_id} AS etl_batch_id 
                                    FROM ##mcaid_elig_dedup",
                                    .con = conn)
    }
  }
  
  
  #### LOAD TABLE TESTS ####
  ### Normal approach (32 mins)
  create_table_f(conn = conn, 
                 config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml", 
                 overwrite = T)
  
  sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                    SELECT {`vars`*}, etl_batch_id FROM
                                    {`archive_schema`}.{`archive_table`}
                                    WHERE {`date_var`} < {date_truncate}
                                    UNION
                                    SELECT {`vars`*}, {current_batch_id} AS etl_batch_id FROM
                                    ##mcaid_elig_dedup
                                    WHERE {`date_var`} >= {date_truncate}",
                                .con = conn)

  system.time(DBI::dbExecute(conn, sql_combine))
  
  
  ### Create table as select (CTAS)
  sql_combine <- glue::glue_sql("CREATE TABLE {`to_schema`}.{`to_table`} 
                                    WITH (
                                      DISTRIBUTION = ROUND_ROBIN,
                                      CLUSTERED COLUMNSTORE INDEX
                                    )
                                    AS 
                                    SELECT {`vars`*}, etl_batch_id FROM
                                    {`archive_schema`}.{`archive_table`}
                                    WHERE {`date_var`} < {date_truncate}
                                    UNION
                                    SELECT {`vars`*}, {current_batch_id} AS etl_batch_id FROM
                                    ##mcaid_elig_dedup
                                    WHERE {`date_var`} >= {date_truncate}",
                                .con = conn)
  
  system.time(DBI::dbExecute(conn, sql_combine))
  
  
  
  
  
  
  
  #### ADD INDEX ####
  add_index_f(conn = conn, table_config = config)
  
  
  #### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
  message("Running QA checks")
  
  # Obtain row counts for other tables (rows_load_raw already calculated above)
  rows_stage <- as.numeric(dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = conn)))
  
  if (full_refresh == F) {
    rows_archive <- as.numeric(dbGetQuery(
      conn, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`archive_table`} 
                            WHERE {`date_var`} < {date_truncate}",
                                .con = conn)))
      
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
  
  if (row_diff != 0) {
    row_diff_qa_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_elig',
                                  {row_diff_qa_type}, 
                                  'FAIL',
                                  {Sys.time()},
                                  'Issue even after accounting for any duplicate rows. Investigate further.')",
                                  .con = conn))
    warning("Number of rows does not match total expected")
  } else {
    row_diff_qa_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_elig',
                                  {row_diff_qa_type}, 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of rows in stage matches expected (n = {rows_stage})')",
                                  .con = conn))
  }
  
  
  #### QA CHECK: NULL IDs ####
  null_ids <- as.numeric(dbGetQuery(conn, 
                                    "SELECT COUNT (*) FROM claims.stage_mcaid_elig 
                                    WHERE MEDICAID_RECIPIENT_ID IS NULL"))
  
  if (null_ids != 0) {
    null_ids_qa_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_elig',
                                  'Null Medicaid IDs', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Null IDs found. Investigate further.')",
                                  .con = conn))
    warning("Null Medicaid IDs found in claims.stage_mcaid_elig")
  } else {
    null_ids_qa_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_elig',
                                  'Null Medicaid IDs', 
                                  'PASS',
                                  {Sys.time()},
                                  'No null IDs found')",
                                  .con = conn))
  }
  
  
  #### ADD VALUES TO QA_VALUES TABLE ####
  DBI::dbExecute(
    conn = conn,
    glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('claims.stage_mcaid_elig',
                   'row_count', 
                   '{rows_stage}', 
                   {Sys.time()}, 
                   {refresh_type})",
                   refresh_type = ifelse(full_refresh == F, 
                                         'Count after partial refresh', 
                                         'Count after full refresh'),
                   .con = conn))
  
  
  #### ADD OVERALL QA RESULT ####
  # This creates an overall QA result to feed the stage.v_mcaid_status view, 
  #    which is used by the integrated data hub to check for new data to run
  if (max(row_diff_qa_fail, null_ids_qa_fail) == 1) {
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_elig',
                                  'Overall QA result', 
                                  'FAIL',
                                  {Sys.time()},
                                  'One or more QA steps failed')",
                                  .con = conn))
    stop("One or more QA steps failed. See claims.metadata_qa_mcaid for more details")
  } else {
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_elig',
                                  'Overall QA result', 
                                  'PASS',
                                  {Sys.time()},
                                  'All QA steps passed')",
                                  .con = conn))
  }
  
  
  #### CLEAN UP ####
  # Drop global temp table
  suppressWarnings(try(odbc::dbRemoveTable(conn, "##mcaid_elig_temp", temporary = T)))
  suppressWarnings(try(odbc::dbRemoveTable(conn, "##mcaid_elig_dedup", temporary = T)))
  rm(dedup_sql)
  rm(vars, var_names)
  rm(duplicate_check_reason, duplicate_check_hoh, duplicate_check_rac, duplicate_type,
     temp_rows_01, temp_rows_02, dedup_sql)
  rm(from_schema, from_table, to_schema, to_table, archive_schema, date_truncate)
  rm(rows_stage, rows_load_raw, rows_archive, distinct_rows_load_raw, null_ids)
  rm(row_diff_qa_fail, row_diff_qa_note, null_ids_qa_fail)
  rm(config)
  rm(sql_combine, sql_archive)
  rm(current_batch_id)
  
}

