#### CODE TO UPDATE ADDRESS_CLEAN TABLES WITH MONTHLY MEDICAID REFRESHES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999, warning.length = 8170)

library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(RCurl) # Read files from Github
library(sf) # Read shape files

db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db_claims <- dbConnect(odbc(), "PHClaims51")

geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")


#### PARTIAL ADDRESS_CLEAN SETUP ####
# STEP 1: Find address data from Medicaid that was previously only in the PHA data
# STEP 2: Update geo_source_mcaid column for addresses now in both sources
# STEP 3: Take address data from Medicaid that don't match to the ref table
# STEP 4: Run through Informatica
# STEP 5: Remove any records already in the manually corrected data
# STEP 6: APPEND to SQL

### NOTE
# Make sure only finding rows where geo_add3_raw IS NULL. 
# This column is only found in the PHA data (so if not null, there couldn't 
# be a match in the Medicaid data)


#### STEP 1: Find address data from Medicaid that was previously only in the PHA data ####
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


#### STEP 2: Update geo_source_mcaid column for addresses now in both sources ####
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

dbGetQuery(db_claims, glue::glue_collapse(update_sql, sep = "; "))


#### STEP 3: Take address data from Medicaid that don't match to the ref table ####
### Bring in all Medicaid addresses not in the ref table
# Include ETL batch ID to know where the addresses are coming from
new_add <- dbGetQuery(db_claims,
           "SELECT DISTINCT a.geo_add1_raw, a.geo_add2_raw, a.geo_city_raw,
            a.geo_state_raw, a.geo_zip_raw, a.etl_batch_id, 1 AS geo_source_mcaid
           FROM
           (SELECT RSDNTL_ADRS_LINE_1 AS 'geo_add1_raw', 
             RSDNTL_ADRS_LINE_2 AS 'geo_add2_raw',
             RSDNTL_CITY_NAME as 'geo_city_raw', 
             RSDNTL_STATE_CODE AS 'geo_state_raw', 
             RSDNTL_POSTAL_CODE AS 'geo_zip_raw',
             etl_batch_id
             FROM PHClaims.stage.mcaid_elig) a
           LEFT JOIN
           (SELECT geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
             geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean
             FROM ref.address_clean
             WHERE geo_add3_raw IS NULL) b
           ON 
           (a.geo_add1_raw = b.geo_add1_raw OR (a.geo_add1_raw IS NULL AND b.geo_add1_raw IS NULL)) AND
           (a.geo_add2_raw = b.geo_add2_raw OR (a.geo_add2_raw IS NULL AND b.geo_add2_raw IS NULL)) AND 
           (a.geo_city_raw = b.geo_city_raw OR (a.geo_city_raw IS NULL AND b.geo_city_raw IS NULL)) AND 
           (a.geo_state_raw = b.geo_state_raw OR (a.geo_state_raw IS NULL AND b.geo_state_raw IS NULL)) AND 
           (a.geo_zip_raw = b.geo_zip_raw OR (a.geo_zip_raw IS NULL AND b.geo_zip_raw IS NULL))
           where b.geo_zip_clean IS NULL")



### SUBSEQUENT ADDRESS_CLEAN SETUP
# MORE CODE TO COME?



#### STEP 4: Run through Informatica ####
### Write out to Informatica
new_add_out <- new_add %>%
  distinct(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw)

write.csv(new_add_out, 
          glue::glue("//kcitetldepim001/Informatica/address/adds_for_informatica_{Sys.Date()}.csv"))


### Read in addresses run though Informatica
# First pull in list of files in folder
informatica_add <- list.files(path = "//kcitetldepim001/Informatica/address/", pattern = "cleaned_addresses_[0-9|-]*.csv")

new_add_in <- data.table::fread(
  file = glue::glue("//kcitetldepim001/Informatica/address/{max(informatica_add)}"),
  stringsAsFactors = F)


### Convert missing to NA so joins work and take distinct
new_add_in <- new_add_in %>%
  mutate_at(vars(add1, add2, po_box, city, state, zip, 
                 old_add1, old_add2, old_city, old_state, old_zip),
            funs(ifelse(. == "" | . == "NA" | is.na(.), NA_character_, .))) %>%
  select(-`#id`, -mailabilty_score) %>%
  distinct()


### Informatica seems to drop secondary designators when they start with #
# Move over from old address
new_add_in <- new_add_in %>%
  mutate(add2 = ifelse(is.na(add2) & str_detect(old_add1, "^#"),
                       old_add1, add2))


### Tidy up some PO box and other messiness
new_add_in <- new_add_in %>%
  mutate(add1 = case_when(
    is.na(add1) & !is.na(po_box) ~ po_box,
    TRUE ~ add1),
  add2 = case_when(
    is.na(add2) & !is.na(po_box) & !is.na(add1) ~ po_box,
    !is.na(add2) & !is.na(po_box) & !is.na(add1) ~ paste(add2, po_box, sep = " "),
    TRUE ~ add2),
  po_box = as.numeric(ifelse(!is.na(po_box), 1, 0))
  )


### Tidy up columns
new_add_in <- new_add_in %>%
  rename(geo_add1_raw = old_add1,
         geo_add2_raw = old_add2,
         geo_city_raw = old_city,
         geo_state_raw = old_state,
         geo_zip_raw = old_zip,
         geo_add1_clean = add1,
         geo_add2_clean = add2,
         geo_city_clean = city,
         geo_state_clean = state,
         geo_zip_clean = zip) %>%
  mutate(geo_zip_clean = as.character(geo_zip_clean),
         geo_zip_raw = as.character(geo_zip_raw))


#### STEP 5: Remove any records already in the manually corrected data ####
# Note, some addresses that are run through Informatica may still require
# manual correction. Need to remove them from the Informatica data and use the
# manually corrected version.

### Bring in manual corrections
manual_add <- read.csv(file.path(geocode_path,
                                 "Medicaid_eligibility_specific_addresses_fix - DO NOT SHARE FILE.csv"),
                       stringsAsFactors = F)

manual_add <- manual_add %>% 
  mutate_all(list(~ ifelse(. == "", NA_character_, .))) %>%
  mutate(geo_zip_raw = as.character(geo_zip_raw),
         geo_zip_clean = as.character(geo_zip_clean))


### Find any manually corrected rows in the addresses
in_manual <- inner_join(manual_add, 
                        select(new_add_in, geo_add1_raw, geo_add2_raw, geo_city_raw,
                               geo_state_raw, geo_zip_raw),
                       by = c("geo_add1_raw", "geo_add2_raw", "geo_city_raw",
                              "geo_state_raw", "geo_zip_raw")) %>%
  select(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
         geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
         overridden)


### Remove them from the Informatica addresses
new_add_trim <- left_join(new_add_in,
                          select(in_manual, geo_add1_raw, geo_add2_raw, geo_city_raw,
                                 geo_state_raw, geo_zip_raw, overridden),
                          by = c("geo_add1_raw", "geo_add2_raw", "geo_city_raw",
                                 "geo_state_raw", "geo_zip_raw")) %>%
  filter(is.na(overridden)) %>%
  select(-overridden)


### Bring it all together
new_add_final <- bind_rows(new_add_trim, in_manual) %>%
  # Set up columns only found in the PHA data
  mutate(geo_add3_raw = NA_character_,
         geo_source_mcaid = 1,
         geo_source_pha = 0) %>%
  select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
         geo_state_raw, geo_zip_raw,
         geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
         geo_source_mcaid, geo_source_pha)


#### STEP 6: APPEND to SQL ####
dbWriteTable(db_claims, 
             name = DBI::Id(schema = "stage",  table = "address_clean"),
             new_add_final,
             overwrite = F, append = T)


#### CLEAN UP ####
rm(update_source, update_sql)
rm(list = ls(pattern = "^new_add"))
rm(informatica_add)
rm(manual_add, in_manual)
rm(geocode_path)