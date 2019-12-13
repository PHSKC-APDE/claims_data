#### CODE TO LOAD MCAID CLAIMS TABLES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08

### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


load_load_raw.mcaid_claim_partial_f <- function(etl_date_min = NULL,
                                                etl_date_max = NULL,
                                                etl_delivery_date = NULL,
                                                etl_note = NULL,
                                                qa_file_row = F) {
  
  # qa_file_row flag will determine whether to count the number of rows in the txt files
  # Note this is VERY slow over the network so better to check row counts once in SQL
  
  
  ### Check entries are in place for ETL function
  if (is.null(etl_delivery_date) | is.null(etl_note)) {
    stop("Enter a delivery date and note for the ETL batch ID function")
  }
  
  
  # Load ETL and QA functions if not already present
  if (exists("load_metadata_etl_log_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
  }
  
  if (exists("qa_file_row_count_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
  }
  
  
  
  #### SET UP BATCH ID ####
  # Eventually switch this function over to using glue_sql to stop unwanted SQL behavior
  current_batch_id <- load_metadata_etl_log_f(conn = db_claims, 
                                              batch_type = "incremental", 
                                              data_source = "Medicaid", 
                                              date_min = etl_date_min,
                                              date_max = etl_date_max,
                                              delivery_date = etl_delivery_date, 
                                              note = etl_note)
  
  if (is.na(current_batch_id)) {
    stop("No etl_batch_id. Check metadata.etl_log table")
  }
  
  
  #### QA CHECK: ACTUAL VS EXPECTED ROW COUNTS ####
  if (qa_file_row == T) {
    print("Checking expected vs. actual row counts (will take a while")
    # Use the load config file for the list of tables to check and their expected row counts
    qa_rows_file <- qa_file_row_count_f(config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml",
                                        overall = F, ind_yr = T)
    
    # Report results out to SQL table
    odbc::dbGetQuery(conn = db_claims,
                     glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_claim',
                                        'Number of rows in source file(s) match(es) expected value', 
                                        {qa_rows_file$outcome},
                                        {Sys.time()},
                                        {qa_rows_file$note})",
                                    .con = db_claims))
    
    if (qa_rows_file$outcome == "FAIL") {
      stop(glue::glue("Mismatching row count between source file and expected number. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
    }
  }
  
  

  #### QA CHECK: ORDER OF COLUMNS IN SOURCE FILE MATCH TABLE SHELLS IN SQL ####
  print("Checking column order")
  qa_column <- qa_column_order_f(conn = db_claims,
                                 config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml",
                                 overall = T, ind_yr = F)
  
  # Report results out to SQL table
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_claim',
                                        'Order of columns in source file matches SQL table', 
                                        {qa_column$outcome},
                                        {Sys.time()},
                                        {qa_column$note})",
                                  .con = db_claims))
  
  if (qa_column$outcome == "FAIL") {
    stop(glue::glue("Mismatching column order between source file and SQL table. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id})"))
  }
  
  
  
  #### LOAD TABLES ####
  print("Loading tables to SQL")
  load_table_from_file_f(conn = db_claims,
                         config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml",
                         overall = T, ind_yr = F, combine_yr = F)
  
  
  #### QA CHECK: ROW COUNTS MATCH SOURCE FILE COUNT ####
  print("Checking loaded row counts vs. expected")
  # Use the load config file for the list of tables to check and their expected row counts
  qa_rows_sql <- qa_load_row_count_f(conn = db_claims,
                                    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml",
                                    overall = T, ind_yr = F, combine_yr = F)
  
  # Report individual results out to SQL table
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_claim',
                                        'Number rows loaded to SQL vs. expected value(s)', 
                                        {qa_rows_sql$outcome[1]},
                                        {Sys.time()},
                                        {qa_rows_sql$note[1]})",
                                  .con = db_claims))
  
  if (qa_rows_sql$outcome[1] == "FAIL") {
    stop(glue::glue("Mismatching row count between source file and SQL table. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  }
  
  
  
  #### QA CHECK: COUNT OF DISTINCT ROWS (MINUS ADDRESS FIELDS) ####
  print("Running additional QA items")
  # Should be no duplicate TCNs once address fields are ignored
  
  # Currently fields are hard coded. Switch over to reading in YAML file and 
  # excluding the address fields
  
  distinct_rows <- as.numeric(dbGetQuery(
    db_claims,
    "SELECT COUNT (*) FROM
    (SELECT DISTINCT MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN, 
      ORGNL_TCN, RAC_CODE_H, RAC_CODE_L, FROM_SRVC_DATE, TO_SRVC_DATE, 
      BLNG_PRVDR_LCTN_IDNTFR, BLNG_NATIONAL_PRVDR_IDNTFR, BLNG_PRVDR_LCTN_TXNMY_CODE, 
      BLNG_PRVDR_TYPE_CODE, BLNG_PRVDR_SPCLTY_CODE, SRVCNG_PRVDR_LCTN_IDNTFR, 
      SRVCNG_NATIONAL_PRVDR_IDNTFR, SRVCNG_PRVDR_LCTN_TXNMY_CODE, 
      SRVCNG_PRVDR_TYPE_CODE, SRVCNG_PRVDR_SPCLTY_CODE, CLM_TYPE_CID, CLM_TYPE_NAME, 
      CLM_CTGRY_LKPCD, CLM_CTGRY_NAME, REVENUE_CODE, TYPE_OF_BILL, CLAIM_STATUS, 
      CLAIM_STATUS_DESC, DRG_CODE, DRG_NAME, UNIT_SRVC_H, UNIT_SRVC_L, 
      PRIMARY_DIAGNOSIS_CODE, DIAGNOSIS_CODE_2, DIAGNOSIS_CODE_3, DIAGNOSIS_CODE_4, 
      DIAGNOSIS_CODE_5, DIAGNOSIS_CODE_6, DIAGNOSIS_CODE_7, DIAGNOSIS_CODE_8, 
      DIAGNOSIS_CODE_9, DIAGNOSIS_CODE_10, DIAGNOSIS_CODE_11, DIAGNOSIS_CODE_12, 
      PRIMARY_DIAGNOSIS_CODE_LINE, DIAGNOSIS_CODE_2_LINE, DIAGNOSIS_CODE_3_LINE, 
      DIAGNOSIS_CODE_4_LINE, DIAGNOSIS_CODE_5_LINE, DIAGNOSIS_CODE_6_LINE, 
      DIAGNOSIS_CODE_7_LINE, DIAGNOSIS_CODE_8_LINE, DIAGNOSIS_CODE_9_LINE, 
      DIAGNOSIS_CODE_10_LINE, DIAGNOSIS_CODE_11_LINE, DIAGNOSIS_CODE_12_LINE, 
      PRCDR_CODE_1, PRCDR_CODE_2, PRCDR_CODE_3, PRCDR_CODE_4, PRCDR_CODE_5, PRCDR_CODE_6, 
      PRCDR_CODE_7, PRCDR_CODE_8, PRCDR_CODE_9, PRCDR_CODE_10, PRCDR_CODE_11, PRCDR_CODE_12, 
      LINE_PRCDR_CODE, MDFR_CODE1, MDFR_CODE2, MDFR_CODE3, MDFR_CODE4, NDC, NDC_DESC, 
      DRUG_STRENGTH, PRSCRPTN_FILLED_DATE, DAYS_SUPPLY, DRUG_DOSAGE, PACKAGE_SIZE_UOM, 
      SBMTD_DISPENSED_QUANTITY, PRSCRBR_ID, PRVDR_LCTN_H_SID, NPI, PRVDR_LAST_NAME, 
      PRVDR_FIRST_NAME, TXNMY_CODE, TXNMY_NAME, PRVDR_TYPE_CODE, SPCLTY_CODE, SPCLTY_NAME, 
      ADMSN_SOURCE_LKPCD, PATIENT_STATUS_LKPCD, ADMSN_DATE, ADMSN_HOUR, ADMTNG_DIAGNOSIS_CODE, 
      BLNG_PRVDR_FIRST_NAME, BLNG_PRVDR_LAST_NAME, BLNG_PRVDR_NAME, DRVD_DRG_CODE, 
      DRVD_DRG_NAME, DSCHRG_DATE, FCLTY_TYPE_CODE, INSRNC_CVRG_CODE, INVC_TYPE_LKPCD, 
      MDCL_RECORD_NMBR, PRIMARY_DIAGNOSIS_POA_LKPCD, PRIMARY_DIAGNOSIS_POA_NAME, 
      PRVDR_COUNTY_CODE, SPCL_PRGRM_LKPCD, BSP_GROUP_CID, LAST_PYMNT_DATE, BILL_DATE, 
      SYSTEM_IN_DATE, TCN_DATE
      FROM load_raw.mcaid_claim) a"))
  
  distinct_tcn <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (DISTINCT CLM_LINE_TCN) FROM load_raw.mcaid_claim"))
  
  
  if (distinct_rows != distinct_tcn) {
    odbc::dbGetQuery(conn = db_claims,
                     glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                    (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                    VALUES ({current_batch_id}, 
                                    'load_raw.mcaid_claim',
                                    'Distinct TCNs', 
                                    'FAIL',
                                    {Sys.time()},
                                    'No. distinct TCNs did not match rows even after excluding addresses')",
                                    .con = db_claims))
    stop("Number of distinct rows does not match total expected")
    } else {
    odbc::dbGetQuery(conn = db_claims,
                     glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'load_raw.mcaid_claim',
                                  'Distinct TCNs', 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of distinct TCNs equals total # rows (after excluding address fields)')",
                                    .con = db_claims))
  }
  
  
  #### QA CHECK: DATE RANGE MATCHES EXPECTED RANGE ####
  qa_date_range <- qa_date_range_f(conn = db_claims,
                                   config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml",
                                   overall = T, ind_yr = F, combine_yr = F,
                                   date_var = "FROM_SRVC_DATE")
  
  # Report individual results out to SQL table
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_claim',
                                        'Actual vs. expected date range in data', 
                                        {qa_date_range$outcome[1]},
                                        {Sys.time()},
                                        {qa_date_range$note[1]})",
                                  .con = db_claims))
  
  if (qa_date_range$outcome[1] == "FAIL") {
    stop(glue::glue("Mismatching date range between source file and SQL table. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  }
  

  print("All QA items passed, see results in metadata.qa_mcaid")
  
  #### ADD BATCH ID COLUMN ####
  print("Adding batch ID to SQL table")
  # Add column to the SQL table and set current batch to the default
  odbc::dbGetQuery(db_claims,
                   glue::glue_sql(
                     "ALTER TABLE load_raw.mcaid_claim 
                   ADD etl_batch_id INTEGER 
                   DEFAULT {current_batch_id} WITH VALUES",
                     .con = db_claims))
  

  print("All claims data loaded to SQL and QA checked")

}

