#### CODE TO UPDATE ADDRESS_CLEAN TABLES WITH MONTHLY MEDICAID REFRESHES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-09


### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


#### PARTIAL ADDRESS_CLEAN SETUP ####
# THIS CODE: STEP 1
# STEP 1A: Take address data from Medicaid that don't match to the ref table
# STEP 1B: Output data to run through Informatica

# FUTURE CODE: STEP 2
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step2.R
# STEP 2A: Pull in Informatica results
# STEP 2B: Remove any records already in the manually corrected data
# STEP 2C: APPEND to SQL

### NOTE
# Make sure only finding rows where geo_add3_raw IS NULL since this isn't used 
#   in the Medicaid data.


load_stage.address_clean_partial_1 <- function(conn_db = NULL) {
  
  #### STEP 1A: Take address data from Medicaid that don't match to the ref table ####
  ### Bring in all Medicaid addresses not in the ref table
  # Include ETL batch ID to know where the addresses are coming from
  new_add <- dbGetQuery(conn_db,
                        "SELECT DISTINCT a.geo_add1_raw, a.geo_add2_raw, a.geo_city_raw,
            a.geo_state_raw, a.geo_zip_raw, a.etl_batch_id, 1 AS geo_source_mcaid,
            b.[exists]
           FROM
           (SELECT 
            CASE WHEN RSDNTL_ADRS_LINE_1 IN ('NA', 'N/A') THEN NULL ELSE RSDNTL_ADRS_LINE_1 END AS 'geo_add1_raw', 
            CASE WHEN RSDNTL_ADRS_LINE_2 IN ('NA', 'N/A') THEN NULL ELSE RSDNTL_ADRS_LINE_2 END AS 'geo_add2_raw', 
            RSDNTL_CITY_NAME AS 'geo_city_raw', 
            RSDNTL_STATE_CODE AS 'geo_state_raw', 
            RSDNTL_POSTAL_CODE AS 'geo_zip_raw', 
            etl_batch_id
           FROM claims.stage_mcaid_elig) a
           LEFT JOIN
           (SELECT geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
             geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
             1 AS [exists] 
             FROM ref.address_clean
             WHERE geo_add3_raw IS NULL) b
           ON 
           (a.geo_add1_raw = b.geo_add1_raw OR (a.geo_add1_raw IS NULL AND b.geo_add1_raw IS NULL)) AND
           (a.geo_add2_raw = b.geo_add2_raw OR (a.geo_add2_raw IS NULL AND b.geo_add2_raw IS NULL)) AND 
           (a.geo_city_raw = b.geo_city_raw OR (a.geo_city_raw IS NULL AND b.geo_city_raw IS NULL)) AND 
           (a.geo_state_raw = b.geo_state_raw OR (a.geo_state_raw IS NULL AND b.geo_state_raw IS NULL)) AND 
           (a.geo_zip_raw = b.geo_zip_raw OR (a.geo_zip_raw IS NULL AND b.geo_zip_raw IS NULL))
           where b.[exists] IS NULL")
  
  #### STEP 1B: Output data to run through Informatica ####
  new_add_out <- new_add %>%
    distinct(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw)
  
  write.csv(new_add_out, 
            glue::glue("//kcitetldepim001/Informatica/address/adds_for_informatica_{Sys.Date()}.csv"),
            row.names = F)
  
  message(glue::glue("{nrow(new_add_out)} addresses were exported for Informatica cleanup"))
  
  #### CLEAN UP ####
  rm(update_source, update_sql)
  rm(list = ls(pattern = "^new_add"))
}