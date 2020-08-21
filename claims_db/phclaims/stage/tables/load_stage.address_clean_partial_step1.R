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
# STEP 1A: Find address data from Medicaid that was previously only in the PHA data
# STEP 1B: Update geo_source_mcaid column for addresses now in both sources
# STEP 1C: Take address data from Medicaid that don't match to the ref table
# STEP 1D: Output data to run through Informatica

# FUTURE CODE: STEP 2
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step2.R
# STEP 2A: Pull in Informatica results
# STEP 2B: Remove any records already in the manually corrected data
# STEP 2C: APPEND to SQL

### NOTE
# Make sure only finding rows where geo_add3_raw IS NULL. 
# This column is only found in the PHA data (so if not null, there couldn't 
# be a match in the Medicaid data)


#### STEP 1A: Find address data from Medicaid that was previously only in the PHA data ####
# Need to use this to update geo_source_mcaid
update_source <- dbGetQuery(
  db_claims,
  "SELECT DISTINCT a.geo_add1_raw, a.geo_add2_raw, b.geo_add3_raw, 
  a.geo_city_raw, a.geo_state_raw, a.geo_zip_raw, 1 AS geo_source_mcaid,
  b.geo_source_pha 
  FROM
    (SELECT RSDNTL_ADRS_LINE_1 AS 'geo_add1_raw', 
             RSDNTL_ADRS_LINE_2 AS 'geo_add2_raw',
             RSDNTL_CITY_NAME as 'geo_city_raw', 
             RSDNTL_STATE_CODE AS 'geo_state_raw', 
             RSDNTL_POSTAL_CODE AS 'geo_zip_raw'
             FROM PHClaims.stage.mcaid_elig) a
    INNER JOIN
      (SELECT geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
        geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
        geo_source_pha 
        FROM ref.address_clean
        WHERE geo_source_mcaid = 0 AND geo_source_pha = 1 AND geo_add3_raw IS NULL) b
    ON 
    (a.geo_add1_raw = b.geo_add1_raw OR (a.geo_add1_raw IS NULL AND b.geo_add1_raw IS NULL)) AND
    (a.geo_add2_raw = b.geo_add2_raw OR (a.geo_add2_raw IS NULL AND b.geo_add2_raw IS NULL)) AND 
    (a.geo_city_raw = b.geo_city_raw OR (a.geo_city_raw IS NULL AND b.geo_city_raw IS NULL)) AND
    (a.geo_state_raw = b.geo_state_raw OR (a.geo_state_raw IS NULL AND b.geo_state_raw IS NULL)) AND 
    (a.geo_zip_raw = b.geo_zip_raw OR (a.geo_zip_raw IS NULL AND b.geo_zip_raw IS NULL))")


#### STEP 1B: Update geo_source_mcaid column for addresses now in both sources ####
# Need to account for NULL values and glue's transformer functions don't seem
# to work with glue_data_sql so using this approach
update_sql <- glue::glue_data(
  update_source,
  "UPDATE stage.address_clean 
  SET geo_source_mcaid = 1 WHERE 
  (geo_add1_raw = '{geo_add1_raw}' AND geo_add2_raw = '{geo_add2_raw}' AND 
  geo_add3_raw IS NULL AND geo_city_raw = '{geo_city_raw}' AND geo_zip_raw = '{geo_zip_raw}')",
)

update_sql <- str_replace_all(update_sql, "= 'NA'", "Is NULL")

if (nrow(update_source) > 0) {
  DBI::dbExecute(db_claims, glue::glue_collapse(update_sql, sep = "; "))
  
  message(glue::glue("{nrow(update_source)} addresses were found in the new Medicaid ",
                     "data that were previously only in PHA data"))
} else {
  message(glue::glue("{nrow(update_source)} addresses were found in the new Medicaid ",
                     "data that were previously only in PHA data"))
}



#### STEP 1C: Take address data from Medicaid that don't match to the ref table ####
### Bring in all Medicaid addresses not in the ref table
# Include ETL batch ID to know where the addresses are coming from
new_add <- dbGetQuery(db_claims,
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
           FROM PHClaims.stage.mcaid_elig) a
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



### SUBSEQUENT ADDRESS_CLEAN SETUP
# MORE CODE TO COME?



#### STEP 1D: Output data to run through Informatica ####
new_add_out <- new_add %>%
  distinct(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw)

write.csv(new_add_out, 
          glue::glue("//kcitsqlutpdbh51/importdata/data/PHClaimsAddress/adds_for_informatica_{Sys.Date()}.csv"),
          row.names = F)

#write.csv(new_add_out, 
#          glue::glue("//kcitetldepim001/Informatica/address/adds_for_informatica_{Sys.Date()}.csv"),
#          row.names = F)

message(glue::glue("{nrow(new_add_out)} addresses were exported for Informatica cleanup"))

#### CLEAN UP ####
rm(update_source, update_sql)
rm(list = ls(pattern = "^new_add"))
rm(geocode_path)