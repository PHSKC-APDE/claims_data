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

db_claims <- dbConnect(odbc(), "PHClaims51")

geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")


#### PARTIAL ADDRESS_CLEAN SETUP ####
# PREVIOUS CODE: STEPS 1-4
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step1.R
# STEP 1A: Find address data from Medicaid that was previously only in the PHA data
# STEP 1B: Update geo_source_mcaid column for addresses now in both sources
# STEP 1C: Take address data from Medicaid that don't match to the ref table
# STEP 1D: Output data to run through Informatica

# THIS CODE: STEPS 5-7
# STEP 2A: Pull in Informatica results
# STEP 2B: Remove any records already in the manually corrected data
# STEP 2C: APPEND to SQL


#### STEP 2A: Pull in Informatica results ####
### First pull in list of files in folder
informatica_add <- list.files(path = "//kcitetldepim001/Informatica/address/", pattern = "cleaned_addresses_[0-9|-]*.csv")

new_add_in <- data.table::fread(
  file = glue::glue("//kcitetldepim001/Informatica/address/{max(informatica_add)}"),
  stringsAsFactors = F)


### Convert missing to NA so joins work and take distinct
new_add_in <- new_add_in %>%
  mutate_at(vars(add1, add2, po_box, city, state, zip, 
                 old_add1, old_add2, old_city, old_state, old_zip),
            list( ~ ifelse(. == "" | . == "NA" | is.na(.), NA_character_, .))) %>%
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


#### STEP 2B: Remove any records already in the manually corrected data ####
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
         geo_source_pha = 0,
         last_run = Sys.time()) %>%
  select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
         geo_state_raw, geo_zip_raw,
         geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
         geo_source_mcaid, geo_source_pha, last_run)


#### STEP 2C: APPEND to SQL ####
dbWriteTable(db_claims, 
             name = DBI::Id(schema = "stage",  table = "address_clean"),
             new_add_final,
             overwrite = F, append = T)


#### CLEAN UP ####
rm(list = ls(pattern = "^new_add"))
rm(informatica_add)
rm(manual_add, in_manual)
rm(geocode_path)