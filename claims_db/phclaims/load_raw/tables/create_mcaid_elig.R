#### FUNCTION TO CREATE LOAD_RAW MCAID ELIG TABLES
# Alastair Matheson
# Created:        2019-04-04
# Last modified:  2019-04-04


### Plans for future improvements:
# Allow for non-contiguous year tables to be created (e.g., 2013 and 2016)
# Add warning when overall mcaid_elig is about to be overwritten


#### PARAMETERS ####
# conn = name of the connection to the SQL database
# overall = create overall mcaid_elig table (default is TRUE)
# ind_yr = create mcaid_elig tables for individual years (default is TRUE)
# min_yr = the starting point of individual year tables (must be from 2012-2022)
# min_yr = the ending point of individual year tables (must be from 2012-2022)
# overwrite = drop table first before creating it, if it exists (default is TRUE)
# test_mode = write things to the tmp schema to test out functions (default is FALSE)


#### FUNCTION ####
load_raw.mcaid_elig_f <- function(
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
  
  
  vars <- c("CLNDR_YEAR_MNTH" = "INTEGER", 
            "MEDICAID_RECIPIENT_ID" = "VARCHAR(200)", 
            "HOH_ID" = "VARCHAR(200)", 
            "SOCIAL_SECURITY_NMBR" = "VARCHAR(200)", 
            "FIRST_NAME" = "VARCHAR(200)", 
            "MIDDLE_NAME" = "VARCHAR(200)", 
            "LAST_NAME" = "VARCHAR(200)", 
            "GENDER" = "VARCHAR(200)", 
            "RACE1_NAME" = "VARCHAR(200)", 
            "RACE2_NAME" = "VARCHAR(200)", 
            "RACE3_NAME" = "VARCHAR(200)", 
            "RACE4_NAME" = "VARCHAR(200)", 
            "HISPANIC_ORIGIN_NAME" = "VARCHAR(200)", 
            "BIRTH_DATE" = "DATE", 
            "SPOKEN_LNG_NAME" = "VARCHAR(200)", 
            "WRTN_LNG_NAME" = "VARCHAR(200)", 
            "PRGNCY_DUE_DATE" = "DATE", 
            "RPRTBL_RAC_CODE" = "INTEGER", 
            "RPRTBL_RAC_NAME" = "VARCHAR(200)", 
            "RAC_CODE" = "VARCHAR(200)", 
            "RAC_NAME" = "VARCHAR(200)", 
            "RPRTBL_BSP_GROUP_CID" = "INTEGER", 
            "RPRTBL_BSP_GROUP_NAME" = "VARCHAR(200)", 
            "FROM_DATE" = "DATE", 
            "TO_DATE" = "DATE", 
            "END_REASON" = "VARCHAR(200)", 
            "COVERAGE_TYPE_IND" = "VARCHAR(200)", 
            "MC_PRVDR_ID" = "VARCHAR(200)", 
            "MC_PRVDR_NAME" = "VARCHAR(200)", 
            "DUAL_ELIG" = "VARCHAR(200)", 
            "TPL_FULL_FLAG" = "VARCHAR(200)", 
            "RSDNTL_ADRS_LINE_1" = "VARCHAR(200)", 
            "RSDNTL_ADRS_LINE_2" = "VARCHAR(200)", 
            "RSDNTL_CITY_NAME" = "VARCHAR(200)", 
            "RSDNTL_STATE_CODE" = "VARCHAR(200)", 
            "RSDNTL_POSTAL_CODE" = "VARCHAR(200)", 
            "RSDNTL_COUNTY_CODE" = "VARCHAR(200)", 
            "RSDNTL_COUNTY_NAME" = "VARCHAR(200)", 
            "MBR_ACES_IDNTFR" = "INTEGER", 
            "MBR_H_SID" = "INTEGER", 
            "etl_batch_id" = "INTEGER")


  #### OVERALL TABLE ####
  if (overall == T) {
    print(paste0("Creating overall [", schema, "].[mcaid_elig] table", test_msg))
    
    tbl_name <- DBI::Id(schema = schema, table = "mcaid_elig")
    
    if (overwrite == T) {
      if (dbExistsTable(conn, tbl_name)) {
        dbRemoveTable(conn, tbl_name)
      }
    }

    DBI::dbCreateTable(conn, tbl_name, fields = vars)
  }
  
  
  #### CALENDAR YEAR TABLES ####
  if (ind_yr == T) {
    print(paste0("Creating calendar year [", schema, "].[mcaid_elig] tables", test_msg))
    
    # Set up list to run over
    range <- seq(min_yr, max_yr)
    names(range) <- seq(min_yr, max_yr)
    
    lapply(range, function(x) {
      tbl_name <- DBI::Id(schema = schema, table = paste0("mcaid_elig_", x))
      
      if (overwrite == T) {
        if (dbExistsTable(conn, tbl_name)) {
          dbRemoveTable(conn, tbl_name)
        }
      }
      
      DBI::dbCreateTable(conn, tbl_name, fields = vars)
    })
  }
}