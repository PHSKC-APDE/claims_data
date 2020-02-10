#### CODE TO CREATE STAGE MCAID ELIG TABLE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08

### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


load_stage.mcaid_elig_f <- function(full_refresh = F) {
  #### CALL IN CONFIG FILES TO GET VARS ####
  table_config_stage_elig <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml"
  ))
  
  table_config_load_elig <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig.yaml"
  ))
  
  from_schema <- table_config_stage_elig$from_schema
  from_table <- table_config_stage_elig$from_table
  to_schema <- table_config_stage_elig$to_schema
  to_table <- table_config_stage_elig$to_table
  
  if (full_refresh == F) {
    archive_schema <- table_config_stage_elig$archive_schema
  }
  
  date_truncate <- table_config_load_elig$overall$date_min
  
  
  #### CALL IN FUNCTIONS IF NOT ALREADY LOADED ####
  if (exists("alter_schema_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
  }
  if (exists("add_index_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
  }
  
  
  #### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
  current_batch_id <- as.numeric(odbc::dbGetQuery(db_claims,
                                                  glue::glue_sql("SELECT MAX(etl_batch_id) FROM {`from_schema`}.{`from_table`}",
                                                                 .con = db_claims)))
  
  if (is.na(current_batch_id)) {
    stop(glue::glue_sql("Missing etl_batch_id in {`from_schema`}.{`from_table`}"))
  }
  
  
  #### ARCHIVE EXISTING TABLE ####
  if (full_refresh == F) {
    alter_schema_f(conn = db_claims, from_schema = to_schema, to_schema = archive_schema,
                   table_name = to_table)
  }
 
  
  #### LOAD TABLE ####
  # Some months have multiple rows per person-month-RAC combo. There are currently 
  #   3 main reasons for this:
  # 1) Multiple end reasons (mostly dates prior to 2018-09)
  # 2) One row with missing HOH_ID and one row with non-missing HOH_ID (mostly mid-2019)
  # 3) RAC name (usually secondary) spelled incorrectly 
  #  (Involuntary Inpatient Psychiactric Treatment (ITA) vs Involuntary Inpatient Psychiatric Treatment (ITA))
  
  message("Checking for any duplicates")
  rows_load_raw <- as.numeric(dbGetQuery(
    db_claims,
    glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", .con = db_claims)))
  
  distinct_rows_load_raw <- as.numeric(dbGetQuery(
    db_claims,
    glue::glue_sql("SELECT COUNT (*) FROM
  (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, TO_DATE,
  RPRTBL_RAC_CODE, SECONDARY_RAC_CODE 
  FROM {`from_schema`}.{`from_table`}) a", .con = db_claims)))
  
  # If a match, don't bother checking any further
  if (rows_load_raw != distinct_rows_load_raw) {
    # Need three different approaches to fix these. Check for each type of error.
    message("Duplicates found, checking for multiple END_REASON rows per id/month/RAC combo")
    duplicate_check_reason <- as.numeric(dbGetQuery(
      db_claims,
      glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
             TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, HOH_ID, RPRTBL_RAC_NAME,
             SECONDARY_RAC_NAME 
                 FROM {`from_schema`}.{`from_table`}) a",
                     .con = db_claims)))
    
    message("Checking for multiple HOH_ID rows per id/month/RAC combo")
    duplicate_check_hoh <- as.numeric(dbGetQuery(
      db_claims,
      glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
             TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, END_REASON, RPRTBL_RAC_NAME,
             SECONDARY_RAC_NAME 
                 FROM {`from_schema`}.{`from_table`}) a",
                     .con = db_claims)))
    
    message("Checking for misspelled RAC names rows per id/month/RAC combo")
    duplicate_check_rac <- as.numeric(dbGetQuery(
      db_claims,
      glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
             TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, HOH_ID, END_REASON
                 FROM {`from_schema`}.{`from_table`}) a",
                     .con = db_claims)))
    
    duplicate_type <- case_when(
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "All three types of duplicates present",
      duplicate_check_reason == rows_load_raw & duplicate_check_hoh != rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "Duplicate HOH_ID and RAC_NAME rows present",
      duplicate_check_reason != rows_load_raw & duplicate_check_hoh == rows_load_raw & 
        duplicate_check_rac != rows_load_raw ~ "Duplicate END_REASON AND HOH_ID rows present",
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
      vars <- unlist(names(table_config_stage_elig$vars))
      # Need to specify which temp table the vars come from
      # Can't handle this just with glue_sql
      # (see https://community.rstudio.com/t/using-glue-sql-s-collapse-with-table-name-identifiers/11633)
      var_names <- lapply(names(table_config_stage_elig$vars), 
                          function(nme) DBI::Id(table = "a", column = nme))
      vars_dedup <- lapply(var_names, DBI::dbQuoteIdentifier, conn = db_claims)
      
      
      # Use priority set out below (higher resaon score = higher priority)
      ### Set up temporary table
      message("Setting up a temp table to remove duplicate rows")
      # This can then be used to deduplicate rows with differing end reasons
      # Remove temp table if it exists
      try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_temp", temporary = T), silent = T)
      
      odbc::dbGetQuery(db_claims,
        glue::glue_sql(
          "SELECT {`vars`*},
      CASE WHEN END_REASON IS NULL THEN 1
        WHEN END_REASON = 'Other' THEN 2
        WHEN END_REASON = 'Other - For User Generation Only' THEN 3
        WHEN END_REASON = 'Review Not Complete' THEN 4
        WHEN END_REASON = 'No Eligible Household Members' THEN 5
        WHEN END_REASON = 'Already Eligible for Program in Different AU' THEN 6
        ELSE 7 END AS reason_score 
      INTO ##mcaid_elig_temp
      FROM {`from_schema`}.{`from_table`}",
          .con = db_claims))
      
      # Fix spelling of RAC if needed
      if (duplicate_check_rac != rows_load_raw) {
        dbGetQuery(db_claims,
                   "UPDATE ##mcaid_elig_temp 
               SET RPRTBL_RAC_NAME = 'Involuntary Inpatient Psychiatric Treatment (ITA)' 
               WHERE RPRTBL_RAC_NAME = 'Involuntary Inpatient Psychiactric Treatment (ITA)'")
        
        dbGetQuery(db_claims,
                   "UPDATE ##mcaid_elig_temp 
               SET SECONDARY_RAC_NAME = 'Involuntary Inpatient Psychiatric Treatment (ITA)' 
               WHERE SECONDARY_RAC_NAME = 'Involuntary Inpatient Psychiactric Treatment (ITA)'")
      }
      
      # Check no dups exist by recording row counts
      temp_rows_01 <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM ##mcaid_elig_temp"))
      if (rows_load_raw != temp_rows_01) {
        stop("Not all rows were copied to the temp table")
      } else {
        message(glue::glue("The ##mcaid_elig_temp table has {temp_rows_01} rows, as expected"))
      }
      
      
      ### Manipulate the temporary table to deduplicate
      # Remove temp table if it exists
      try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_dedup", temporary = T), silent = T)
      
      dedup_sql <- glue::glue_sql(
        "SELECT DISTINCT {`vars_dedup`*}
      INTO ##mcaid_elig_dedup
      FROM
      (SELECT {`vars`*}, reason_score FROM ##mcaid_elig_temp) a
        LEFT JOIN
        (SELECT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE,
          TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, 
          MAX(reason_score) AS max_score, MAX(HOH_ID) AS max_hoh
          FROM ##mcaid_elig_temp
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
        .con = db_claims)
      
      odbc::dbGetQuery(db_claims, dedup_sql)
      
      # Keep track of how many of the duplicate rows are accounted for
      temp_rows_02 <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM ##mcaid_elig_dedup"))
      dedup_row_diff <- temp_rows_01 - temp_rows_02
      
      if (temp_rows_02 == distinct_rows_load_raw) {
        message(glue::glue("All duplicates accounted for (new row total = {distinct_rows_load_raw})"))
      } else {
        message(glue::glue("The ##mcaid_elig_dedup table has {temp_rows_02} rows ",
                           "({dedup_row_diff} fewer than ##mcaid_elig_temp)",
                           " but {distinct_rows_load_raw - temp_rows_02} duplicate rows remain"))
      }
      
    } else {
      message(" A new type of duplicate is present. Investigate further")
    }
  }
  
  
  ### Combine relevant parts of archive and new data
  message("Recreating stage table")
  
  # First create new table
  create_table_f(db_claims,
                 config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml",
                 overall = T, ind_yr = F, overwrite = F)
  
  if (full_refresh == F) {
    # Select the source, depending on if deduplication has been carried out
    if (is.na(duplicate_type)) {
      sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                  SELECT {`vars`*} FROM
                                  {`archive_schema`}.{`to_table`}
                                  WHERE {`date_var`} < {date_truncate}
                                  UNION
                                  SELECT {`vars`*} FROM
                                  {`from_schema`}.{`from_table`}
                                  WHERE {`date_var`} >= {date_truncate}",
                                    .con = db_claims,
                                    date_var = table_config_stage_elig$date_var)
    } else {
      sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                    SELECT {`vars`*} FROM
                                    archive.{`to_table`}
                                    WHERE {`date_var`} < {date_truncate}
                                    UNION
                                    SELECT {`vars`*} FROM
                                    ##mcaid_elig_dedup
                                    WHERE {`date_var`} >= {date_truncate}",
                                    .con = db_claims,
                                    date_var = table_config_stage_elig$date_var)
    }
  } else {
    # Select the source, depending on if deduplication has been carried out
    if (is.na(duplicate_type)) {
      sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                  SELECT {`vars`*} FROM
                                  {`from_schema`}.{`from_table`}",
                                    .con = db_claims)
    } else {
      sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                    SELECT {`vars`*} FROM ##mcaid_elig_dedup",
                                    .con = db_claims)
    }
  }

  DBI::dbExecute(db_claims, sql_combine)
  
  
  #### ADD INDEX ####
  add_index_f(conn = db_claims, table_config = table_config_stage_elig)
  
  
  #### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
  message("Running QA checks")
  
  # Obtain row counts for other tables (rows_load_raw already calculated above)
  rows_stage <- as.numeric(dbGetQuery(
    db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = db_claims)))
  
  if (full_refresh == F) {
    rows_archive <- as.numeric(dbGetQuery(
      db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`to_table`} 
                            WHERE {`table_config_stage_elig$date_var`} < {date_truncate}",
                                .con = db_claims)))
      
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

  if (row_diff != 0) {
    if (full_refresh == F) {
      row_diff_qa_note <- 'Rows passed from load_raw AND archive to stage'
    } else {
      row_diff_qa_note <- 'Rows passed from load_raw to stage'
    }
    
    row_diff_qa_fail <- 1
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  {row_diff_qa_note}, 
                                  'FAIL',
                                  {Sys.time()},
                                  'Issue even after accounting for any duplicate rows. Investigate further.')",
                                  .con = db_claims))
    warning("Number of rows does not match total expected")
  } else {
    row_diff_qa_fail <- 0
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  {row_diff_qa_note}, 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of rows in stage matches expected (n = {rows_stage})')",
                                  .con = db_claims))
  }
  
  
  #### QA CHECK: NULL IDs ####
  null_ids <- as.numeric(dbGetQuery(db_claims, 
                                    "SELECT COUNT (*) FROM stage.mcaid_elig 
                                    WHERE MEDICAID_RECIPIENT_ID IS NULL"))
  
  if (null_ids != 0) {
    null_ids_qa_fail <- 1
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Null Medicaid IDs', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Null IDs found. Investigate further.')",
                                  .con = db_claims))
    warning("Null Medicaid IDs found in stage.mcaid_elig")
  } else {
    null_ids_qa_fail <- 0
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Null Medicaid IDs', 
                                  'PASS',
                                  {Sys.time()},
                                  'No null IDs found')",
                                  .con = db_claims))
  }
  
  
  #### ADD VALUES TO QA_VALUES TABLE ####
  DBI::dbExecute(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('stage.mcaid_elig',
                   'row_count', 
                   '{rows_stage}', 
                   {Sys.time()}, 
                   'Count after partial refresh')",
                   .con = db_claims))
  
  
  #### ADD OVERALL QA RESULT ####
  # This creates an overall QA result to feed the stage.v_mcaid_status view, 
  #    which is used by the integrated data hub to check for new data to run
  if (max(row_diff_qa_fail, null_ids_qa_fail) == 1) {
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Overall QA result', 
                                  'FAIL',
                                  {Sys.time()},
                                  'One or more QA steps failed')",
                                  .con = db_claims))
    stop("One or more QA steps failed. See metadata.qa_mcaid for more details")
  } else {
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Overall QA result', 
                                  'PASS',
                                  {Sys.time()},
                                  'All QA steps passed')",
                                  .con = db_claims))
  }
  
  
  #### CLEAN UP ####
  # Drop global temp table
  suppressWarnings(try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_temp", temporary = T)))
  suppressWarnings(try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_dedup", temporary = T)))
  rm(dedup_sql)
  rm(vars, var_names, vars_dedup)
  rm(duplicate_check_reason, duplicate_check_hoh, duplicate_check_rac, duplicate_type,
     temp_rows_01, temp_rows_02, dedup_sql)
  rm(from_schema, from_table, to_schema, to_table, archive_schema, date_truncate)
  rm(rows_stage, rows_load_raw, rows_archive, distinct_rows_load_raw, null_ids)
  rm(row_diff_qa_fail, row_diff_qa_note, null_ids_qa_fail)
  rm(table_config_stage_elig)
  rm(sql_combine, sql_archive)
  rm(current_batch_id)
  
}

