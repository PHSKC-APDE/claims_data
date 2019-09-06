#### CODE TO SET UP OR UPDATE ADDRESS_CLEAN TABLES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-04


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)

library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(sf) # Read shape files

db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db_claims <- dbConnect(odbc(), "PHClaims51")

geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")


#### INITAL ADDRESS_CLEAN SETUP ####
# Take address data from all claims sources (Medicaid)
# Add in addresses from other sources (Housing)
# Add in manually corrected addresses
# combine into a single file


### Create SQL table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_clean.yaml",
               overall = T, ind_yr = F)


### Bring in manual corrections
manual_add <- read.csv(file.path(geocode_path,
                                 "Medicaid_eligibility_specific_addresses_fix_updated - DO NOT SHARE FILE.csv"),
                       stringsAsFactors = F)

manual_add <- manual_add %>% 
  mutate_all(funs(ifelse(. == "", NA_character_, .))) %>%
  mutate(geo_zip_raw = as.character(geo_zip_raw),
         geo_zip_clean = as.character(geo_zip_clean))


### Bring in all Medicaid addresses
# Seems slightly quicker to push addresses to a temp table than take distinct
try(dbRemoveTable(db_claims, "##add_temp", temporary = T))
dbGetQuery(db_claims, 
           "SELECT 
           RSDNTL_ADRS_LINE_1 AS geo_add1_raw, 
           RSDNTL_ADRS_LINE_2 AS geo_add2_raw,
           RSDNTL_CITY_NAME AS geo_city_raw, 
           RSDNTL_STATE_CODE AS geo_state_raw, 
           RSDNTL_POSTAL_CODE AS geo_zip_raw,
           1 AS geo_source_mcaid,
           NULL AS geo_source_pha 
           INTO ##add_temp 
           FROM stage.mcaid_elig")


mcaid_add <- dbGetQuery(db_claims, 
                        "SELECT DISTINCT geo_add1_raw, geo_add2_raw, 
                        geo_city_raw, geo_state_raw, geo_zip_raw, 
                        geo_source_mcaid, geo_source_pha
                        FROM ##add_temp ")


### Bring in complete public housing authority addresses
pha_add_full <- dbGetQuery(db_apde51, 
                      "SELECT unit_add, unit_apt, unit_apt2, 
                      unit_city, unit_state, unit_zip,
                      geo_add1_raw, geo_add2_raw, 
                      geo_city_raw, geo_state_raw, geo_zip_raw,
                      NULL AS geo_source_mcaid,
                      1 AS geo_source_pha  
                      FROM stage.pha_address_new")


# Subset to partially cleaned addresses and set missing add to NA to match Medicaid
pha_add <- pha_add_full %>%
  distinct(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
           geo_source_mcaid, geo_source_pha) %>%
  mutate_all(funs(ifelse(. == "" | . == "NA", NA_character_, .)))


### Find any address common to both data sets
joint_add <- inner_join(select(mcaid_add, geo_add1_raw, geo_add2_raw, 
                               geo_city_raw, geo_state_raw, geo_zip_raw, geo_source_mcaid),
                        select(pha_add, geo_add1_raw, geo_add2_raw, 
                               geo_city_raw, geo_state_raw, geo_zip_raw, geo_source_pha),
                        by = c("geo_add1_raw", "geo_add2_raw", 
                               "geo_city_raw", "geo_state_raw", "geo_zip_raw"))



# Remove common addresses from source
# Make second version of mcaid_add because it takes so long to read in
mcaid_add2 <- anti_join(mcaid_add, 
                     select(joint_add, geo_add1_raw, geo_add2_raw, 
                            geo_city_raw, geo_state_raw, geo_zip_raw),
                     by = c("geo_add1_raw", "geo_add2_raw", 
                            "geo_city_raw", "geo_state_raw", "geo_zip_raw"))

pha_add <- anti_join(pha_add, 
                     select(joint_add, geo_add1_raw, geo_add2_raw, 
                            geo_city_raw, geo_state_raw, geo_zip_raw),
                     by = c("geo_add1_raw", "geo_add2_raw", 
                            "geo_city_raw", "geo_state_raw", "geo_zip_raw"))


### Combine files
combined_add <- bind_rows(mcaid_add2, pha_add, joint_add)


### Remove manually corrected addresses from combined list
combined_add <- anti_join(combined_add, manual_add,
                          by = c("geo_add1_raw", "geo_add2_raw",  
                                 "geo_city_raw", "geo_state_raw", "geo_zip_raw"))


# Make all NA actual NA
combined_add <- combined_add %>%
  mutate_at(vars(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw),
            funs(ifelse(. == "" | . == "NA" | is.na(.), NA_character_, .)))



#### SUBSEQUENT ADDRESS_CLEAN SETUP ####
# Take addresses from source
# Anti-join to existing ref.address_clean

### Create SQL table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_clean.yaml",
               overall = T, ind_yr = F)

# MORE CODE TO COME



#### ADDRESS_CLEAN INFORMATICA STEP ####
# Run through Informatica
# Make any manual changes
# Create/add to existing table

### Write out to Informatica
combined_add_out <- combined_add %>%
  distinct(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw)

write.csv(combined_add_out, paste0("//kcitetldepim001/Informatica/address/combined_add_",
                                   Sys.Date(), ".csv"))


### Read in addresses run though Informatica
combined_add_in <- data.table::fread(file = "//kcitetldepim001/Informatica/address/cleaned_addresses_new.csv")


### Convert missing to NA so joins work and take distinct
combined_add_in_clean <- combined_add_in %>%
  mutate_at(vars(add1, add2, po_box, city, state, zip, 
                 old_add1, old_add2, old_city, old_state, old_zip),
            funs(ifelse(. == "" | . == "NA" | is.na(.), NA_character_, .))) %>%
  select(-`#id`) %>%
  distinct()

### Tidy up some PO box and other messiness
combined_add_in_clean <- combined_add_in_clean %>%
  mutate(movetype = case_when(
    # PO and BOX were split = movetype 1
    str_detect(add1, "^BOX[:space:]?[0-9]*$") & str_detect(po_box, "PO BOX") ~ 1,
    # add1 had number before PO BOX = movetype 2
    str_detect(add1, "^[0-9]* PO BOX$") & str_detect(po_box, "PO BOX") ~ 2,
    # add1 one has PO Box number but nothing else = movetype 3
    str_detect(add1, "^[0-9]*$") & po_box == "PO BOX" ~ 3,
    # add2 has secondary designator and po_box has details = movetype 4
    !is.na(add1) & !is.na(add2) & !is.na(po_box) ~ 4,
    # add2 has orphan secondary designator and po_box has details = movetype 5
    is.na(add1) & !is.na(add2) & !is.na(po_box) ~ 5,
    # add1 is street address, add2 is NA and po_box has box details = movetype 6
    !is.na(add1) & is.na(add2) & !is.na(po_box) ~ 6,
    # only address is the po_box = movetype 8
    is.na(add1) & is.na(add2) & !is.na(po_box) ~ 7
  )
  ) %>%
  mutate(add1 = case_when(
    movetype == 1 ~ paste0("PO ", str_sub(add1, 1, nchar(add1))),
    movetype == 2 ~ po_box,
    movetype == 3 ~ paste(po_box, add1),
    movetype == 4 ~ add1,
    movetype == 5 ~ po_box,
    movetype == 6 ~ add1,
    movetype == 7 ~ po_box,
    TRUE ~ add1
  ),
  add2 = case_when(
    movetype == 1 ~ add2,
    movetype == 2 ~ NA_character_,
    movetype == 3 ~ NA_character_,
    movetype == 4 ~ paste(add2, po_box),
    movetype == 5 ~ add2,
    movetype == 6 ~ po_box,
    TRUE ~ add2
  ),
  # Now set PO box field to be a simple flag
  po_box = as.numeric(ifelse(!is.na(po_box), 1, 0)),
  # Fix up when some numbers when leading zeros stripped
  old_add1 = case_when(
    old_add1 == "10040" ~ "00010040",
    old_add1 == "241" ~ "0241",
    old_add1 == "269001164" ~ "0269001164",
    old_add1 == "3204" ~ "03204",
    old_add1 == "1.00E-101" ~ "1E-101",
    old_add1 == "1.10E-201" ~ "11E-202",
    TRUE ~ old_add1),
  old_zip = as.character(old_zip),
  zip = as.character(zip)
  ) %>%
  # The 2019-04-29 run through Informatica had secondary addresses as dates
  # This is now fixed but must be corrected here for join to work
  mutate_at(vars(old_add1, old_add2),
            funs(case_when(
              . == "1-Jan" ~ "1-1",
              . == "2-Jan" ~ "1/2",
              . == "4-Jan" ~ "1-4",
              . == "7-Jan" ~ "1-7",
              . == "1-Feb" ~ "2-1",
              . == "2-Feb" ~ "2-2",
              . == "5-Feb" ~ "2-5",
              . == "6-Feb" ~ "2-6",
              . == "8-Feb" ~ "2-8",
              . == "13-Feb" ~ "2-13",
              . == "4-Mar" ~ "3-4",
              . == "Mar-32" ~ "3-32",
              . == "1-Apr" ~ "4-1",
              . == "4-Apr" ~ "4-4",
              . == "6-Apr" ~ "4-6",
              . == "7-Apr" ~ "4-7",
              . == "16-Apr" ~ "4-16",
              . == "3-May" ~ "5-3",
              . == "4-May" ~ "5-4",
              . == "7-May" ~ "5-7",
              . == "2-Jun" ~ "6-2",
              . == "3-Jun" ~ "6-3",
              . == "4-Jun" ~ "6-4",
              . == "5-Jun" ~ "6-5",
              . == "6-Jun" ~ "6-6",
              . == "9-Jun" ~ "6-9",
              . == "Jun-31" ~ "6-31",
              . == "Jun-45" ~ "6-45",
              . == "3-Jul" ~ "7-3",
              . == "5-Jul" ~ "7-5",
              . == "8-Jul" ~ "7-8",
              . == "1-Aug" ~ "8-1",
              . == "2-Aug" ~ "8-2",
              . == "3-Aug" ~ "8-3",
              . == "28-Aug" ~ "8-28",
              . == "1-Sep" ~ "9-1",
              . == "2-Sep" ~ "9-2",
              . == "4-Sep" ~ "9-4",
              . == "8-Sep" ~ "9-8",
              . == "Sep-24" ~ "9-24",
              . == "Sep-40" ~ "9-40",
              . == "1-Oct" ~ "10-1",
              . == "3-Oct" ~ "10-3",
              . == "4-Oct" ~ "10-4",
              . == "5-Oct" ~ "10-5",
              . == "1-Nov" ~ "11-1",
              . == "3-Nov" ~ "11-3",
              . == "4-Nov" ~ "11-4",
              . == "6-Nov" ~ "11-6",
              . == "2-Dec" ~ "12-2",
              . == "8-Dec" ~ "12-08",
              . == "22-Dec" ~ "12-22",
              TRUE ~ .
            ))) %>%
  select(-`#id`, -movetype) %>%
  distinct()

# Tidy up some situations where Informatica stripped out the secondary unit
#    but it seems to actually exist
secondary_init <- c("^#", "^# ", "^\\$", "^APT", "^APPT","^APARTMENT", "^APRT", "^ATPT", 
                    "^BOX", "^BLDG", "^BLD", "^BLG", "^BUILDING", "^DUPLEX", "^FL ", 
                    "^FLOOR", "^HOUSE", "^LOT", "^LOWER", "^LOWR", "^LWR", "^REAR", 
                    "^RM", "^ROOM", "^SLIP", "^STE", "^SUITE", "^SPACE", "^SPC", 
                    "^STUDIO", "^TRAILER", "^TRAILOR", "^TLR", "^TRL", "^TRLR", 
                    "^UNIT", "^UPPER", "^UPPR", "^UPSTAIRS")

combined_add_in_clean <- combined_add_in_clean %>%
  mutate(add2 = case_when(
    is.na(add2) & !is.na(old_add2) & 
      str_detect(old_add2, paste(secondary_init, collapse = "|")) ~ old_add2,
    TRUE ~ add2
  ),
  add2 = case_when(
    is.na(add2) & !is.na(old_add1) & 
      str_detect(old_add1, paste(secondary_init, collapse = "|")) ~ old_add1,
    TRUE ~ add2
  )
  ) %>%
  distinct()


### Make new combined file
combined_add_clean <- left_join(combined_add, combined_add_in_clean, 
                          by = c("geo_add1_raw" = "old_add1", 
                                 "geo_add2_raw" = "old_add2", 
                                 "geo_city_raw" = "old_city", 
                                 "geo_state_raw" = "old_state", 
                                 "geo_zip_raw" = "old_zip")) %>%
  rename(geo_add1_clean = add1,
         geo_add2_clean = add2,
         geo_city_clean = city,
         geo_state_clean = state,
         geo_zip_clean = zip)


#### COMBINE ALL ADDDRESSES ####
combined_add_full <- bind_rows(combined_add_clean, manual_add) %>%
  distinct()

# Show how many matched
combined_add_full %>% mutate(matched = ifelse(is.na(mailabilty_score), 0, 1)) %>%
  group_by(matched) %>% summarise(count = n())
# How many matched after removing manual corrections
combined_add_full %>% mutate(matched = ifelse(is.na(mailabilty_score), 0, 1)) %>%
  filter(is.na(overridden)) %>%
  group_by(matched) %>% summarise(count = n())

# Tidy up columns
combined_add_full <- combined_add_full %>%
  mutate(po_box = ifelse(po_box == 1 | !is.na(mailbox), 1, 0)) %>%
  #select(-homeless, -mailbox, -care_of, -notes) %>%
  distinct()


### Join back to PHA address full to add manual corrections and retrieve original
#  PHA addresses


combined_add_full2 <- full_join(combined_add_full, 
                                pha_add_full %>% 
                                  mutate_all(funs(ifelse(. == "" | . == "NA", NA_character_, .))),
                                by = c("geo_add1_raw", "geo_add2_raw",
                                       "geo_city_raw", "geo_state_raw", "geo_zip_raw"))
combined_add_full2 <- combined_add_full2 %>%
  mutate(pha_xfer = ifelse(!is.na(geo_source_pha.y) & is.na(geo_add1_clean) & 
                             is.na(geo_add2_clean) & is.na(geo_city_clean) & 
                             is.na(geo_state_clean) & is.na(geo_zip_clean), 1, 0),
         geo_add1_clean = ifelse(pha_xfer == 1, geo_add1_raw, geo_add1_clean),
         geo_add2_clean = ifelse(pha_xfer == 1, geo_add2_raw, geo_add2_clean),
         geo_city_clean = ifelse(pha_xfer == 1, geo_city_raw, geo_city_clean),
         geo_state_clean = ifelse(pha_xfer == 1, geo_state_raw, geo_state_clean),
         geo_zip_clean = ifelse(pha_xfer == 1, geo_zip_raw, geo_zip_clean),
         geo_add1_raw = ifelse(geo_source_pha.y == 1 & !is.na(geo_source_pha.y), 
                               unit_add, geo_add1_raw),
         geo_add2_raw = ifelse(geo_source_pha.y == 1 & !is.na(geo_source_pha.y),
                               unit_apt, geo_add2_raw),
         geo_add3_raw = unit_apt2,
         geo_city_raw = ifelse(geo_source_pha.y == 1 & !is.na(geo_source_pha.y), 
                               unit_city, geo_city_raw),
         geo_state_raw = ifelse(geo_source_pha.y == 1 & !is.na(geo_source_pha.y), 
                                unit_state, geo_state_raw),
         geo_zip_raw = ifelse(geo_source_pha.y == 1 & !is.na(geo_source_pha.y), 
                              unit_zip, geo_zip_raw),
         geo_source_mcaid = case_when(
           geo_source_mcaid.x == 1 & !is.na(geo_source_mcaid.x) ~ 1,
           overridden == 1 & !is.na(overridden) ~ 1,
           TRUE ~ 0),
         geo_source_pha = case_when(
           geo_source_pha.x == 1 & !is.na(geo_source_pha.x) ~ 1,
           geo_source_pha.y == 1 & !is.na(geo_source_pha.y) ~ 1,
           TRUE ~ 0),
         overridden = ifelse(is.na(overridden), 0, 1)
         ) %>%
  select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
         geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
         geo_source_mcaid, geo_source_pha, po_box, overridden) %>%
  # Need to update the sources now that the original PHA addresses are back
  group_by(geo_add1_raw, geo_add2_raw, geo_add3_raw, 
           geo_city_raw, geo_state_raw, geo_zip_raw) %>%
  mutate(geo_source_mcaid = max(geo_source_mcaid),
         geo_source_pha = max(geo_source_pha)) %>%
  ungroup() %>%
  distinct()





#### WRITE TO SQL ####
### Pull out relevant fields
combined_add_full_load <- combined_add_full2 %>%
  distinct(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
           geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
           geo_source_mcaid, geo_source_pha)


tbl_id_meta <- DBI::Id(schema = "stage", table = "address_clean")

dbWriteTable(db_claims, tbl_id_meta, combined_add_full_load, overwrite = T)







#### GEOCODING STEP ####
### Eventually the results of the geocoding might be used to clean addresses further,
#      though ArcGIS doesn't do a great job of this

### Bring in previously geocoded data (ESRI)
geocoded_2018_06_12 <- read_sf(file.path(geocode_path, 
                                         "Distinct_addresses_geocoded_2018-06-20.shp"))

geocoded_2018_06_12 <- geocoded_2018_06_12 %>%
  as.data.frame() %>%
  filter(Status == "M" & Loc_name != "zip_5_digit_gc" & !is.na(Loc_name)) %>%
  select(add1, city, state, zip_1, -geometry) %>%
  rename(geo_add1_raw = add1,
         geo_city_raw = city,
         geo_state_raw = state,
         geo_zip_raw = zip_1) %>%
  distinct() %>%
  mutate(geocoded = 1,
         geo_zip_raw = as.character(geo_zip_raw))


### Flag those that have been geocoded already
combined_add_for_geo <- left_join(combined_add_full, geocoded_2018_06_12,
                                  by = c("geo_add1_raw", "geo_city_raw", 
                                         "geo_state_raw", "geo_zip_raw"))


### Write out file for geocoding (drop add2)
combined_add_for_geo <- combined_add_for_geo %>%
  filter(is.na(geocoded)) %>%
  distinct(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean) %>%
  # Add a unique ID also for binding later
  mutate(row_id = row_number())

write.csv(combined_add_for_geo,
          paste0("//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding/",
                    "distinct_addresses_", Sys.Date(), ".csv"),
          row.names = F)





##### TEMP #####
combined_add_clean %>% as.data.frame() %>% sample_n(10)




library(data.table)
combined_add_check <- setDT(combined_add_clean)
combined_add_check[, rows := .N, 
                   by = .(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw)]
combined_add_check <- combined_add_check[order(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw)]


combined_add_check <- combined_add_clean %>%
  group_by(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw) %>%
  mutate(rows = n()) %>%
  ungroup() %>% 
  arrange(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw)


combined_add_check %>% filter(rows > 1) %>% head() %>% as.data.frame()



manual_add_check = left_join(manual_add, combined_add_in,
                             by = c("geo_add1_raw" = "old_add1",
                                    "geo_add2_raw" = "old_add2",
                                    "geo_city_raw" = "old_city",
                                    "geo_state_raw" = "old_state",
                                    "geo_zip_raw" = "old_zip")) %>%
  select(-overridden, -`#id`)

manual_add_check_same <- manual_add_check %>%
  distinct() %>%
  mutate(rows = row_number() + 1) %>%
  filter(notes == "" & is.na(homeless) & is.na(care_of) & !is.na(mailabilty_score) &
           paste0("APT ", geo_add2_raw) == geo_add2_clean) %>%
  select(-notes, -homeless, -care_of, -mailabilty_score, -mailbox, -geo_state_clean)



