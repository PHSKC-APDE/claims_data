#### FUNCTION TO CREATE LOAD_RAW MCAID CLAIM TABLES
# Alastair Matheson
# Created:        2019-04-04
# Last modified:  2019-04-04


### Plans for future improvements:
# Allow for non-contiguous year tables to be created (e.g., 2013 and 2016)
# Add warning when overall mcaid_claim is about to be overwritten


#### PARAMETERS ####
# conn = name of the connection to the SQL database
# overall = create overall mcaid_claim table (default is TRUE)
# ind_yr = create mcaid_claim tables for individual years (default is TRUE)
# min_yr = the starting point of individual year tables (must be from 2012-2022)
# min_yr = the ending point of individual year tables (must be from 2012-2022)
# overwrite = drop table first before creating it, if it exists (default is TRUE)
# test_mode = write things to the tmp schema to test out functions (default is FALSE)


#### FUNCTION ####
load_raw.mcaid_claim_f <- function(
  conn,
  overall = T,
  ind_yr = T,
  min_yr = 2012,
  max_yr = 2018,
  overwrite = T,
  test_mode = F
) {
  
  #### ERROR CHECKS AND OVERALL MESSAGES ####
  # Check that something will be run
  if (overall == F & ind_yr == F) {
    stop("At least one of 'overall and 'ind_yr' must be set to TRUE")
  }
  
  # Check date range for years (only if ind_yr == T)
  if (ind_yr == T) {
    if (!(between(min_yr, 2012, 2022))) {
      stop("min_yr must be between 2012 and 2022 (inclusive")
    }
    if (!(between(max_yr, 2012, 2022))) {
      stop("max_yr must be between 2012 and 2022 (inclusive")
    }
    if (min_yr > max_yr) {
      stop("min_yr must be <= max_yr")
    }
  }
  
  # Alert users they are in test mode
  if (test_mode == T) {
    print("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO TMP SCHEMA")
    test_msg <- " (function is in test mode)"
  } else {
    test_msg <- ""
  }
  
  
  #### VARIABLES ####
  if (test_mode == T) {
    schema <- "tmp"
  } else {
    schema <- "load_raw"
  }
  
  
  vars <- c("MBR_H_SID" = "INT",
            "MEDICAID_RECIPIENT_ID" = "VARCHAR(200)",
            "BABY_ON_MOM_IND" = "VARCHAR(200)",
            "TCN" = "BIGINT",
            "CLM_LINE_TCN" = "BIGINT",
            "CLM_LINE" = "SMALLINT",
            "ORGNL_TCN" = "BIGINT",
            "RAC_CODE_H" = "INT",
            "RAC_CODE_L" = "INT",
            "FROM_SRVC_DATE" = "DATE",
            "TO_SRVC_DATE" = "DATE",
            "BLNG_PRVDR_LCTN_IDNTFR" = "BIGINT",
            "BLNG_NATIONAL_PRVDR_IDNTFR" = "BIGINT",
            "BLNG_PRVDR_LCTN_TXNMY_CODE" = "VARCHAR(200)",
            "BLNG_PRVDR_TYPE_CODE" = "VARCHAR(200)",
            "BLNG_PRVDR_SPCLTY_CODE" = "VARCHAR(200)",
            "BILLING_PRVDR_ADDRESS" = "VARCHAR(200)",
            "SRVCNG_PRVDR_LCTN_IDNTFR" = "BIGINT",
            "SRVCNG_NATIONAL_PRVDR_IDNTFR" = "BIGINT",
            "SRVCNG_PRVDR_LCTN_TXNMY_CODE" = "VARCHAR(200)",
            "SRVCNG_PRVDR_TYPE_CODE" = "VARCHAR(200)",
            "SVRCNG_PRVDR_SPCLTY_CODE" = "VARCHAR(200)",
            "SERVICING_PRVDR_ADDRESS" = "VARCHAR(200)",
            "CLM_TYPE_CID" = "INT",
            "CLM_TYPE_NAME" = "VARCHAR(200)",
            "CLM_CTGRY_LKPCD" = "VARCHAR(200)",
            "CLM_CTGRY_NAME" = "VARCHAR(200)",
            "REVENUE_CODE" = "VARCHAR(200)",
            "TYPE_OF_BILL" = "VARCHAR(200)",
            "CLAIM_STATUS" = "INT",
            "CLAIM_STATUS_DESC" = "VARCHAR(200)",
            "DRG_CODE" = "VARCHAR(200)",
            "DRG_NAME" = "VARCHAR(200)",
            "UNIT_SRVC_H" = "VARCHAR(200)",
            "UNIT_SRVC_L" = "VARCHAR(200)",
            "PRIMARY_DIAGNOSIS_CODE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_2" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_3" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_4" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_5" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_6" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_7" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_8" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_9" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_10" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_11" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_12" = "VARCHAR(200)",
            "PRIMARY_DIAGNOSIS_CODE_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_2_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_3_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_4_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_5_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_6_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_7_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_8_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_9_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_10_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_11_LINE" = "VARCHAR(200)",
            "DIAGNOSIS_CODE_12_LINE" = "VARCHAR(200)",
            "PRCDR_CODE_1" = "VARCHAR(200)",
            "PRCDR_CODE_2" = "VARCHAR(200)",
            "PRCDR_CODE_3" = "VARCHAR(200)",
            "PRCDR_CODE_4" = "VARCHAR(200)",
            "PRCDR_CODE_5" = "VARCHAR(200)",
            "PRCDR_CODE_6" = "VARCHAR(200)",
            "PRCDR_CODE_7" = "VARCHAR(200)",
            "PRCDR_CODE_8" = "VARCHAR(200)",
            "PRCDR_CODE_9" = "VARCHAR(200)",
            "PRCDR_CODE_10" = "VARCHAR(200)",
            "PRCDR_CODE_11" = "VARCHAR(200)",
            "PRCDR_CODE_12" = "VARCHAR(200)",
            "LINE_PRCDR_CODE" = "VARCHAR(200)",
            "MDFR_CODE1" = "VARCHAR(200)",
            "MDFR_CODE2" = "VARCHAR(200)",
            "MDFR_CODE3" = "VARCHAR(200)",
            "MDFR_CODE4" = "VARCHAR(200)",
            "NDC" = "VARCHAR(200)",
            "NDC_DESC" = "VARCHAR(200)",
            "DRUG_STRENGTH" = "VARCHAR(200)",
            "PRSCRPTN_FILLED_DATE" = "DATE",
            "DAYS_SUPPLY" = "INT",
            "DRUG_DOSAGE" = "VARCHAR(200)",
            "PACKAGE_SIZE_UOM" = "VARCHAR(200)",
            "SBMTD_DISPENSED_QUANTITY" = "NUMERIC(19,3)",
            "PRSCRBR_ID" = "VARCHAR(200)",
            "PRVDR_LCTN_H_SID"  = "BIGINT",
            "NPI" = "BIGINT",
            "PRVDR_LAST_NAME" = "VARCHAR(200)",
            "PRVDR_FIRST_NAME" = "VARCHAR(200)",
            "TXNMY_CODE"   = "VARCHAR(200)",
            "TXNMY_NAME" = "VARCHAR(200)",
            "PRVDR_TYPE_CODE" = "VARCHAR(200)",
            "SPCLTY_CODE"  = "VARCHAR(200)",
            "SPCLTY_NAME" = "VARCHAR(200)",
            "MCO_PRVDR_ADDRESS" = "VARCHAR(200)",
            "MCO_PRVDR_COUNTY" = "VARCHAR(200)",
            "ADMSN_SOURCE_LKPCD" = "VARCHAR(200)",
            "PATIENT_STATUS_LKPCD" = "VARCHAR(200)",
            "ADMSN_DATE" = "DATE",
            "ADMSN_HOUR" = "INT",
            "ADMTNG_DIAGNOSIS_CODE" = "VARCHAR(200)",
            "BLNG_PRVDR_FIRST_NAME" = "VARCHAR(200)",
            "BLNG_PRVDR_LAST_NAME" = "VARCHAR(200)",
            "BLNG_PRVDR_NAME" = "VARCHAR(200)",
            "DRVD_DRG_CODE" = "VARCHAR(200)",
            "DRVD_DRG_NAME" = "VARCHAR(200)",
            "DSCHRG_DATE" = "DATE",
            "FCLTY_TYPE_CODE" = "VARCHAR(200)",
            "INSRNC_CVRG_CODE" = "VARCHAR(200)",
            "INVC_TYPE_LKPCD" = "VARCHAR(200)",
            "MDCL_RECORD_NMBR" = "VARCHAR(200)",
            "PRIMARY_DIAGNOSIS_POA_LKPCD" = "VARCHAR(200)",
            "PRIMARY_DIAGNOSIS_POA_NAME" = "VARCHAR(200)",
            "PRVDR_COUNTY_CODE" = "VARCHAR(200)",
            "SPCL_PRGRM_LKPCD" = "VARCHAR(200)",
            "BSP_GROUP_CID" = "INT",
            "LAST_PYMNT_DATE" = "DATE",
            "BILL_DATE" = "DATE",
            "SYSTEM_IN_DATE" = "DATE",
            "TCN_DATE" = "DATE",
            'etl_batch_id' = "INT"
  )


  #### OVERALL TABLE ####
  if (overall == T) {
    print(paste0("Creating overall [", schema, "].[mcaid_claim] table", test_msg))
    
    tbl_name <- DBI::Id(schema = schema, table = "mcaid_claim")
    
    if (overwrite == T) {
      if (dbExistsTable(conn, tbl_name)) {
        dbRemoveTable(conn, tbl_name)
      }
    }

    DBI::dbCreateTable(conn, tbl_name, fields = vars)
  }
  
  
  #### CALENDAR YEAR TABLES ####
  if (ind_yr == T) {
    print(paste0("Creating calendar year [", schema, "].[mcaid_claim] tables", test_msg))
    
    # Set up list to run over
    range <- seq(min_yr, max_yr)
    names(range) <- seq(min_yr, max_yr)
    
    lapply(range, function(x) {
      tbl_name <- DBI::Id(schema = schema, table = paste0("mcaid_claim_", x))
      
      if (overwrite == T) {
        if (dbExistsTable(conn, tbl_name)) {
          dbRemoveTable(conn, tbl_name)
        }
      }
      
      DBI::dbCreateTable(conn, tbl_name, fields = vars)
    })
  }
}