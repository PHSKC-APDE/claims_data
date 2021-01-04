#### CODE TO UPDATE ADDRESS_CLEAN TABLES WITH MONTHLY MEDICAID REFRESHES
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12


### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


#### PARTIAL ADDRESS_CLEAN SETUP ####
# STEP 1
# STEP 1A: Take address data from Medicaid that don't match to the ref table
# STEP 1B: Output data to Azure table [ref].[informatica_address_input] to run through Informatica overnight

# STEP 2
# STEP 2A: Pull in Informatica results from Azure table [ref].[informatica_address_output]
# STEP 2B: Remove any records already in the manually corrected data
# STEP 2C: APPEND to SQL




### Function elements
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# source = mcade, mcare, apcd
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage.address_clean_partial_step1 <- function(server = NULL,
                                                   config = NULL,
                                                   source = NULL,
                                                   get_config = F) {
  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  conn <- create_db_connection(server)
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- config[[server]][["ref_table"]]
  informatica_ref_schema <- config[["informatica_ref_schema"]]
  informatica_input_table <- config[["informatica_input_table"]]
  informatica_output_table <- config[["informatica_output_table"]]
  
  #### STEP 1A: Take address data from Medicaid that don't match to the ref table ####
  ### Bring in all Medicaid addresses not in the ref table
  # Include ETL batch ID to know where the addresses are coming from
  new_add <- dbGetQuery(
    conn,
    glue::glue_sql("SELECT DISTINCT a.geo_add1_raw, a.geo_add2_raw, a.geo_city_raw,
                   a.geo_state_raw, a.geo_zip_raw, a.geo_hash_raw, a.etl_batch_id,
                   b.[exists]
                   FROM
                   (SELECT
                     RSDNTL_ADRS_LINE_1 AS 'geo_add1_raw', 
                     RSDNTL_ADRS_LINE_2 AS 'geo_add2_raw', 
                     RSDNTL_CITY_NAME AS 'geo_city_raw', 
                     RSDNTL_STATE_CODE AS 'geo_state_raw', 
                     RSDNTL_POSTAL_CODE AS 'geo_zip_raw', 
                     geo_hash_raw, etl_batch_id
                     FROM {`from_schema`}.{`from_table`}) a
                   LEFT JOIN
                   (SELECT geo_hash_raw, 1 AS [exists] FROM {`ref_schema`}.{`ref_table`}) b
                   ON a.geo_hash_raw = b.geo_hash_raw
                   WHERE b.[exists] IS NULL",
                   .con = conn))
  
  #### STEP 1B: Output data to run through Informatica ####
  # Record current time
  cur_timestamp <- Sys.time()
  
  new_add_out <- new_add %>% 
    distinct(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw, geo_hash_raw) %>%
    # Keep geo_source blank so it is not obvious which addresses come from Medicaid
    mutate(geo_source = "",
           timestamp = cur_timestamp) %>%
    select(-geo_hash_raw)

  if (nrow(new_add_out) > 0) {
    # Make connection to HHSAW
    conn_hhsaw <- create_db_connection("hhsaw")
    DBI::dbAppendTable(conn_hhsaw, DBI::Id(schema = informatica_ref_schema, table = informatica_input_table), 
                       new_add_out)
    message(nrow(new_add_out), " addresses were exported for Informatica cleanup")
    return(cur_timestamp)
  } else {
    message("There were ", nrow(new_add_out), " new addresses. Nothing was exported for Informatica cleanup")
    return(nrow(new_add_out))
  }

}


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# informatica_timestamp is the timestamp used from the input table that is returned from the step 1 function
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage.address_clean_partial_step2 <- function(server = NULL,
                                                   config = NULL,
                                                   source = NULL,
                                                   informatica_timestamp = NULL,
                                                   get_config = F) {

  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  conn <- create_db_connection("hhsaw")
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- config[[server]][["ref_table"]]
  informatica_ref_schema <- config[["informatica_ref_schema"]]
  informatica_input_table <- config[["informatica_input_table"]]
  informatica_output_table <- config[["informatica_output_table"]]
  
  geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
  
  if (!exists("create_table_f")) {
    source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
  }
  
  #### STEP 2A: Pull in Informatica results ####
  
  new_add_in <- dbGetQuery(
                  conn,
                  glue::glue_sql("SELECT
                                    [geo_add1_clean] AS 'add1', 
                                    [geo_add2_clean] AS 'add2', 
                                    [geo_po_box_clean] AS 'po_box', 
                                    [geo_city_clean] AS 'city', 
                                    [geo_state_clean] AS 'state', 
                                    [geo_zip_clean] AS 'zip',
                                    [geo_add1_raw] AS 'old_add1', 
                                    [geo_add2_raw] AS 'old_add2', 
                                    [geo_city_raw] AS 'old_city', 
                                    [geo_state_raw] AS 'old_state', 
                                    [geo_zip_raw] AS 'old_zip'
                                  FROM {`informatica_ref_schema`}.{`informatica_output_table`}
                                  WHERE convert(varchar, timestamp, 20) = 
                                 {lubridate::with_tz(stage_address_clean_timestamp, 'utc')}"
                   ,.con = conn))
  
  
  #### NEED TO SEE HOW geo_hash_raw LOOKS AFTER INFORMATICA PROCESS ####
  ### THEN EDIT CODE HERE
  ### ALSO ADD IN IDS TO JOIN TO OLD DATA BECAUSE <NA>s are being converted to 
  #     'NA' strings at some point, which messes up joins
  
  
  ### Convert missing to NA so joins work and take distinct
  # The latest version produced by Informatica had a different column structure
  # so need to account for that
  new_add_in <- new_add_in %>%
      mutate_at(vars(add1, add2, po_box, city, state, zip,
                     old_add1, old_add2, old_city, old_state, old_zip),
                list( ~ ifelse(. == "" | . == "NA" | is.na(.), NA_character_, .))) %>%
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
  
  
  ### Add in geo_has columns if needed
  if (!"geo_hash_raw" %in% names(new_add_trim)) {
    new_add_trim <- new_add_trim %>% mutate(geo_hash_raw = NA_character_)
  }
  if (!"geo_hash_clean" %in% names(new_add_trim)) {
    new_add_trim <- new_add_trim %>% mutate(geo_hash_clean = NA_character_)
  }
  
  
  
  #### Bring it all together ####
  ## NB THE PASTE COMMAND IN R WILL ADD THE STRING 'NA' WHEN IT ENCOUNTERS A TRUE NA VALUE.
  # THIS IS UNDESIREABLE WHEN IT COMES TO MAKING HASHES SO NAs ARE REPLACED BY EMPTY STRINGS.
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
  conn <- create_db_connection(server)
  dbWriteTable(conn, 
               name = DBI::Id(schema = to_schema,  table = to_table),
               new_add_final,
               overwrite = F, append = T)
  
  
  #### CLEAN UP ####
  rm(list = ls(pattern = "^new_add"))
  rm(manual_add, in_manual)
  rm(geocode_path)
  
}