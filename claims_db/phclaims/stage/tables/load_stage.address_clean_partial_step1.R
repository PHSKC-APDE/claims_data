#### CODE TO UPDATE ADDRESS_CLEAN TABLES WITH MONTHLY MEDICAID REFRESHES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-09


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999, warning.length = 8170)

library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(RCurl) # Read files from Github

if (!exists("db_apde51")) {
  db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")  
}
if (!exists("db_claims")) {
  db_claims <- dbConnect(odbc(), "PHClaims51") 
}

if (!exists("create_table_f")) {
  source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")  
}

geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"


#### PARTIAL ADDRESS_CLEAN SETUP ####
# THIS CODE: STEP 1
# STEP 1A: Take address data from Medicaid that don't match to the ref table
# STEP 1B: Output data to run through Informatica

# FUTURE CODE: STEP 2
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step2.R
# STEP 2A: Pull in Informatica results
# STEP 2B: Remove any records already in the manually corrected data
# STEP 2C: APPEND to SQL


#### STEP 1A: Take address data from Medicaid that don't match to the ref table ####
### Bring in all Medicaid addresses not in the ref table
# Include ETL batch ID to know where the addresses are coming from
new_add <- dbGetQuery(db_claims,
           "SELECT DISTINCT a.geo_add1_raw, a.geo_add2_raw, a.geo_city_raw,
              a.geo_state_raw, a.geo_zip_raw, a.geo_hash_raw, a.etl_batch_id,
              b.[exists]
              FROM
              (SELECT 
                RSDNTL_ADRS_LINE_1 AS 'geo_add1_raw', 
                RSDNTL_ADRS_LINE_2 AS 'geo_add2_raw', 
                RSDNTL_CITY_NAME AS 'geo_city_raw', 
                RSDNTL_STATE_CODE AS 'geo_state_raw', 
                RSDNTL_POSTAL_CODE AS 'geo_zip_raw', 
                geo_hash_raw
                FROM PHClaims.stage.mcaid_elig) a
              LEFT JOIN
              (SELECT geo_hash_raw, 1 AS [exists] FROM ref.address_clean) b
              ON a.geo_hash_raw = b.geo_hash_raw
              WHERE b.[exists] IS NULL")


#### STEP 1B: Output data to run through Informatica ####
new_add_out <- new_add %>%
  distinct(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw, geo_hash_raw) %>%
  mutate(add_id = n())

write.csv(new_add_out, 
          glue::glue("//kcitetldepim001/Informatica/address/adds_for_informatica_{Sys.Date()}.csv"),
          row.names = F)

message(glue::glue("{nrow(new_add_out)} addresses were exported for Informatica cleanup"))

#### CLEAN UP ####
rm(update_source, update_sql)
rm(list = ls(pattern = "^new_add"))
rm(geocode_path)