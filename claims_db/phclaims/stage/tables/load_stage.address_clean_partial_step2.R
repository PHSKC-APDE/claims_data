#### CODE TO UPDATE ADDRESS_CLEAN TABLES WITH MONTHLY MEDICAID REFRESHES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-09



#### PARTIAL ADDRESS_CLEAN SETUP ####
# PREVIOUS CODE: STEP 1
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step1.R
# STEP 1A: Take address data from Medicaid that don't match to the ref table
# STEP 1B: Output data to run through Informatica

# THIS CODE: STEP 2
# STEP 2A: Pull in Informatica results
# STEP 2B: Remove any records already in the manually corrected data
# STEP 2C: APPEND to SQL


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage.address_clean_partial_2 <- function(conn = NULL,
                                               server = c("hhsaw", "phclaims"),
                                               config = NULL,
                                               get_config = F) {
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- config[[server]][["ref_table"]]
  
  geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
  
  if (!exists("create_table_f")) {
    source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
  }
  
  #### STEP 2A: Pull in Informatica results ####
  ### First pull in list of files in folder
  informatica_add <- list.files(path = "//kcitetldepim001/Informatica/address/", pattern = "cleaned_addresses_[0-9|-]*.csv")
  
  new_add_in <- data.table::fread(
    file = glue::glue("//kcitetldepim001/Informatica/address/{max(informatica_add)}"),
    stringsAsFactors = F)
  
  
  #### NEED TO SEE HOW geo_hash_raw LOOKS AFTER INFORMATICA PROCESS ####
  ### THEN EDIT CODE HERE
  ### ALSO ADD IN IDS TO JOIN TO OLD DATA BECAUSE <NA>s are being converted to 
  #     'NA' strings at some point, which messes up joins
  
  
  ### Convert missing to NA so joins work and take distinct
  # The latest version produced by Informatica had a different column structure
  # so need to account for that
  if ("#id" %in% names(new_add_in)) {
    new_add_in <- new_add_in %>%
      mutate_at(vars(add1, add2, po_box, city, state, zip, 
                     old_add1, old_add2, old_city, old_state, old_zip),
                list( ~ ifelse(. == "" | . == "NA" | is.na(.), NA_character_, .))) %>%
      select(-`#id`, -mailabilty_score) %>%
      distinct()
  } else {
    new_add_in <- new_add_in %>%
      rename(add1 = "#add1") %>%
      mutate_at(vars(add1, add2, po_box, city, state, zip,
                     old_add1, old_add2, old_city, old_state, old_zip),
                list( ~ ifelse(. == "" | . == "NA" | is.na(.), NA_character_, .))) %>%
      select(-mailabilty_score) %>%
      distinct()
  }
  
  
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
  
  
  ### Add in geo_has columns if needed
  if (!"geo_hash_raw" %in% names(new_add_trim)) {
    new_add_trim <- new_add_trim %>% mutate(geo_hash_raw = NA_character_)
  }
  if (!"geo_hash_clean" %in% names(new_add_trim)) {
    new_add_trim <- new_add_trim %>% mutate(geo_hash_clean = NA_character_)
  }
  
  
  
  #### Bring it all together ####
  ## NB THE PASTE COMMAND IN R WILL ADD THE STRING 'NA' WHEN IT ENCOUNTERS A TRUE NA VALUE.
  # THIS IS UNDESIREABLE WHEN IT COMES OT MAKING HASHES SO NAs ARE REPLACED BY EMPTY STRINGS.
  # THIS MEANS THE HAS WILL MATCH WHAT IS MADE IN SQL WITH THE SAME INPUTS.

  new_add_final <- bind_rows(new_add_trim, in_manual) %>%
    # Set up columns only found in the PHA data or used for skipping geocoding later
    mutate(geo_add3_raw = NA_character_,
           geo_geocode_skip = 0L,
           geo_hash_raw = ifelse(is.na(geo_hash_raw),
                                 toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_raw, ''), 
                                                               stringr::str_replace_na(geo_add2_raw, ''), 
                                                               stringr::str_replace_na(geo_add3_raw, ''), 
                                                               stringr::str_replace_na(geo_city_raw, ''), 
                                                               stringr::str_replace_na(geo_state_raw, ''), 
                                                               stringr::str_replace_na(geo_zip_raw, ''), 
                                                               sep = "|"))),
                                 geo_hash_raw),
           geo_hash_clean = toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''), 
                                                          stringr::str_replace_na(geo_add2_clean, ''), 
                                                          stringr::str_replace_na(geo_city_clean, ''), 
                                                          stringr::str_replace_na(geo_state_clean, ''), 
                                                          stringr::str_replace_na(geo_zip_clean, ''), 
                                                          sep = "|"))),
           last_run = Sys.time()) %>%
    select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
           geo_state_raw, geo_zip_raw, geo_hash_raw,
           geo_add1_clean, geo_add2_clean, geo_city_clean, 
           geo_state_clean, geo_zip_clean, geo_hash_clean,
           geo_geocode_skip, last_run) %>%
    # Convert all blank fields to be NA
    mutate_if(is.character, list(~ ifelse(. == "", NA_character_, .)))
  
  
  #### STEP 2C: APPEND to SQL ####
  dbWriteTable(conn, 
               name = DBI::Id(schema = to_schema,  table = to_table),
               new_add_final,
               overwrite = F, append = T)
  
  
  #### CLEAN UP ####
  rm(list = ls(pattern = "^new_add"))
  rm(informatica_add)
  rm(manual_add, in_manual)
  rm(geocode_path)
}

