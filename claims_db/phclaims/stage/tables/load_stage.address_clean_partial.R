#### CODE TO CLEAN MEDICAID ADDRESSES
# Partial update version
#
# Alastair Matheson, PHSKC (APDE)
# 2019-06
#
# Note if a full refresh of the ref.address_clean table is desired, use
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/load_stage.address_clean_full.R


load_stage.address_clean_partial_f <- function(informatica = F) {
  #### SET UP PATHS AND CONFIG ####
  geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
  
  table_config_create <- yaml::yaml.load(getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_clean.yaml"))
  
  #### PULL IN ADDRESS DATA ####
  # Make temp tables of existing addresses and index the hash for quicker join
  try(dbRemoveTable(db_claims, "##address_raw", temporary = T))
  dbGetQuery(db_claims, glue_sql(
    "SELECT a.*,
      CAST (HASHBYTES('MD5', CONCAT(a.geo_add1_raw, a.geo_add2_raw, a.geo_city_raw, a.geo_state_raw, a.geo_zip_raw)) AS VARBINARY(16)) AS add_hash
      INTO ##address_raw
      FROM
      (SELECT DISTINCT 
        RSDNTL_ADRS_LINE_1 AS geo_add1_raw,
        RSDNTL_ADRS_LINE_2 AS geo_add2_raw,
        RSDNTL_CITY_NAME AS geo_city_raw,
        RSDNTL_STATE_CODE AS geo_state_raw,
        RSDNTL_POSTAL_CODE AS geo_zip_raw
        FROM stage.mcaid_elig) a;
      CREATE CLUSTERED INDEX idx_raw ON ##address_raw (add_hash);"
  ))
  
  try(dbRemoveTable(db_claims, "##address_clean", temporary = T))
  dbGetQuery(db_claims, glue_sql(
    "SELECT a.*,
      CAST (HASHBYTES('MD5', CONCAT(a.geo_add1_raw, a.geo_add2_raw, a.geo_city_raw, a.geo_state_raw, a.geo_zip_raw)) AS VARBINARY(16)) AS add_hash
      INTO ##address_clean
      FROM
      (SELECT geo_add1_raw, geo_add2_raw, geo_city_raw,
        geo_state_raw, geo_zip_raw, 1 AS 'cleaned'
        FROM ref.address_clean) a;
      CREATE CLUSTERED INDEX idx_raw ON ##address_clean (add_hash);"
  ))
  
  
  address_raw <- dbGetQuery(db_claims, glue_sql(
    "SELECT a.geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw, 1 AS geo_source_mcaid FROM 
  (SELECT geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw, add_hash FROM ##address_raw) a
  LEFT JOIN 
  (SELECT add_hash, 1 AS cleaned FROM ##address_clean) b
  ON a.add_hash = b.add_hash
  WHERE b.cleaned IS NULL",
    .con = db_claims))
  
  try(dbRemoveTable(db_claims, "##address_raw", temporary = T))
  try(dbRemoveTable(db_claims, "##address_clean", temporary = T))
  
  
  
  #### JOIN TO MANUAL CORRECTIONS ####
  ### Bring in data and make sure blanks are missing
  manual_add <- read.csv(file.path(geocode_path,
                                   "Medicaid_eligibility_specific_addresses_fix - DO NOT SHARE FILE.csv"),
                         stringsAsFactors = F)
  
  manual_add <- manual_add %>% 
    mutate_all(funs(ifelse(. == "", NA_character_, .))) %>%
    mutate(geo_zip_raw = as.character(geo_zip_raw),
           geo_zip_clean = as.character(geo_zip_clean))
  
  
  ### Combine data to make clean
  address_clean <- left_join(address_raw, select(manual_add, geo_add1_raw:geo_zip_clean, overridden),
                             by = c("geo_add1_raw", "geo_add2_raw",  
                                    "geo_city_raw", "geo_state_raw", "geo_zip_raw"))
  
  ### Bring over other fields that didn't join
  address_clean <- address_clean %>%
    mutate(
      geo_add1_clean = ifelse(is.na(overridden), geo_add1_raw, geo_add1_clean),
      geo_add2_clean = ifelse(is.na(overridden), geo_add2_raw, geo_add2_clean),
      geo_city_clean = ifelse(is.na(overridden), geo_city_raw, geo_city_clean),
      geo_state_clean = ifelse(is.na(overridden), geo_state_raw, geo_state_clean),
      geo_zip_clean = ifelse(is.na(overridden), geo_zip_raw, geo_zip_clean)
    )
  
  
  #### MOVE SECONDARY DESIGNATORS INTO ADD2 ####
  # Secondary designators
  secondary <- c("#", "\\$", "APT", "APPT", "APARTMENT", "APRT", "ATPT","BOX", "BLDG", 
                 "BLD", "BLG", "BUILDING", "DUPLEX", "FL ", "FLOOR", "HOUSE", "LOT", 
                 "LOWER", "LOWR", "LWR", "REAR", "RM", "ROOM", "SLIP", "STE", "SUITE", 
                 "SPACE", "SPC", "STUDIO", "TRAILER", "TRAILOR", "TLR", "TRL", "TRLR", 
                 "UNIT", "UPPER", "UPPR", "UPSTAIRS")
  
  address_clean <- address_clean %>%
    mutate(
      unit_length_diff = case_when(
        is.na(geo_add2_clean) ~ str_length(geo_add1_clean),
        TRUE ~ str_length(geo_add1_clean) - str_length(geo_add2_clean)),
      unit_apt_length = case_when(
        is.na(geo_add2_clean) ~ 0L,
        TRUE ~ str_length(geo_add2_clean) - 
          str_locate(geo_add2_clean, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2]),
      # Remove straight duplicates of apt numbers in address and apt fields
      geo_add1_clean = if_else(!is.na(geo_add2_clean) &
                                 str_sub(geo_add1_clean, unit_length_diff + 1, str_length(geo_add1_clean)) ==
                                 str_sub(geo_add2_clean, 1, str_length(geo_add2_clean)),
                               str_sub(geo_add1_clean, 1, unit_length_diff),
                               geo_add1_clean),
      # Remove duplicates that are a little more complicated (where the secondary designator isn't repeated but the secondary number is)
      geo_add1_clean = if_else(!is.na(geo_add2_clean) & str_detect(geo_add2_clean, paste(secondary, collapse = "|")) == TRUE &
                                 str_sub(geo_add2_clean, 
                                         str_locate(geo_add2_clean, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2] + 1, 
                                         str_length(geo_add2_clean)) ==
                                 str_sub(geo_add1_clean, str_length(geo_add1_clean) - unit_apt_length + 1, str_length(geo_add1_clean)) &
                                 !str_sub(geo_add1_clean, str_length(geo_add1_clean) - 1, 
                                          str_length(geo_add1_clean)) %in% c("LA", "N", "NE", "NW", "S", "SE", "SW"),
                               str_sub(geo_add1_clean, 1, str_length(geo_add1_clean) - unit_apt_length),
                               geo_add1_clean),
      # ID apartment numbers that need to move into the appropriate column (1, 2)
      # Also include addresses that end in a number as many seem to be apartments (3, 4)
      unit_apt_move = case_when(
        is.na(geo_add2_clean) & is.na(overridden) & 
          str_detect(geo_add1_clean, paste0("[:space:]+(", 
                                            paste(secondary, collapse = "|"), ")")) == TRUE ~ 1,
        !is.na(geo_add2_clean) & is.na(overridden) &
          str_detect(geo_add1_clean, paste0("[:space:]+(", paste(secondary, collapse = "|"), ")")) == TRUE ~ 2,
        is.na(geo_add2_clean) & is.na(overridden) &
          str_detect(geo_add1_clean, "[:space:]+[:alnum:]*[-]*[:digit:]+$") == TRUE &
          str_detect(geo_add1_clean, "PO BOX|PMB") == FALSE & str_detect(geo_add1_clean, "HWY 99$") == FALSE ~ 3,
        !is.na(geo_add2_clean) & is.na(overridden) &
          str_detect(geo_add1_clean, "[:space:]+[:alnum:]*[-]*[:digit:]+$") == TRUE &
          str_detect(geo_add1_clean, "PO BOX|PMB") == FALSE & str_detect(geo_add1_clean, "HWY 99$") == FALSE ~ 4,
        TRUE ~ 0
      ),
      # Move apartment numbers to geo_add2_clean if that field currently blank
      geo_add2_clean = if_else(unit_apt_move == 1,
                               str_sub(geo_add1_clean, 
                                       str_locate(geo_add1_clean, 
                                                  paste0("[:space:]+(", paste(secondary, collapse = "|"), ")"))[, 1], 
                                       str_length(geo_add1_clean)),
                               geo_add2_clean),
      geo_add2_clean = if_else(unit_apt_move == 3,
                               str_sub(geo_add1_clean, 
                                       str_locate(geo_add1_clean, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], 
                                       str_length(geo_add1_clean)),
                               geo_add2_clean),
      # Merge apt data from geo_add1_clean with geo_add2_clean if the latter is currently not blank
      geo_add2_clean = if_else(unit_apt_move == 2,
                               paste(str_sub(geo_add1_clean, 
                                             str_locate(geo_add1_clean, 
                                                        paste0("[:space:]*(", paste(secondary, collapse = "|"), ")"))[, 1], 
                                             str_length(geo_add1_clean)),
                                     geo_add2_clean, sep = " "),
                               geo_add2_clean),
      geo_add2_clean = 
        case_when(
          unit_apt_move == 4 & str_detect(geo_add2_raw, "#") == FALSE ~ 
            paste(str_sub(geo_add1_clean, 
                          str_locate(geo_add1_clean, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], 
                          str_length(geo_add1_clean)), geo_add2_clean, sep = " "),
          unit_apt_move == 4 & str_detect(geo_add2_raw, "#") == TRUE ~ 
            paste(str_sub(geo_add1_clean, str_locate(geo_add1_clean, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1],
                          str_length(geo_add1_clean)),
                  str_sub(geo_add2_clean, str_locate(geo_add2_clean, "[:digit:]")[, 1], str_length(geo_add2_clean)),
                  sep = " "),
          TRUE ~ geo_add2_clean
        ),
      # Remove apt data from the address field (this needs to happen after the above code)
      geo_add1_clean = if_else(unit_apt_move %in% c(1, 2),
                               str_sub(geo_add1_clean, 1, 
                                       str_locate(geo_add1_clean, 
                                                  paste0("[:space:]+(", paste(secondary, collapse = "|"), ")"))[, 1] - 1),
                               geo_add1_clean),
      geo_add1_clean = if_else(unit_apt_move %in% c(3, 4),
                               str_sub(geo_add1_clean, 1, str_locate(geo_add1_clean, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1] - 1),
                               geo_add1_clean),
      # Now pull over any straggler apartments ending in a single letter or letter prefix
      unit_apt_move = if_else(str_detect(geo_add1_clean, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$") == TRUE, 5, unit_apt_move),
      geo_add2_clean = if_else(unit_apt_move == 5 & str_detect(geo_add2_clean, "#") == FALSE,
                               paste0(str_sub(geo_add1_clean, 
                                              str_locate(geo_add1_clean, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*$")[, 1] + 1,
                                              str_length(geo_add1_clean)), 
                                      geo_add2_clean),
                               geo_add2_clean),
      geo_add2_clean = if_else(unit_apt_move == 5 & str_detect(geo_add2_clean, "#") == TRUE,
                               paste0(str_sub(geo_add1_clean, 
                                              str_locate(geo_add1_clean, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$")[, 1] + 1,
                                              str_length(geo_add1_clean)), 
                                      str_sub(geo_add2_clean, str_locate(geo_add2_clean, "#")[, 1] + 1, str_length(geo_add2_clean))),
                               geo_add2_clean),
      # Remove apt data from the address field (this needs to happen after the above code)
      geo_add1_clean = if_else(unit_apt_move == 5,
                               str_sub(geo_add1_clean, 1, str_locate(geo_add1_clean, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$")[, 1] - 1),
                               geo_add1_clean)
    ) %>%
    # Remove any whitespace generated in the process
    mutate_at(vars(geo_add1_clean, geo_add2_clean), funs(str_trim(.))) %>%
    mutate_at(vars(geo_add1_clean, geo_add2_clean), funs(str_replace_all(., "[:space:]+", " "))) %>%
    select(-unit_length_diff, -unit_apt_length)
  
  
  if (informatica = T) {
    # If needed, prepare addresses to be run through Informatics
    # For periodic updates probably not necessary
    
    # Code TBD
    
  }
  
  
  #### ADD DATA TO EXISTING TABLE ####
  ### First prep data to match columns
  address_clean_load <- address_clean %>%
    mutate(geo_add3_raw = NA_character_,
           geo_source_pha = 0) %>%
    select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
           geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
           geo_source_mcaid, geo_source_pha)
  
  
  ### Then bring in data from the final ref table
  # Assume new table has been created in the mcaid master script
  load_table_from_sql_f(
    conn = db_claims,
    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean.yaml", 
    truncate = T, truncate_date = F)
  
  
  
  ### Load data
  # Set up table name
  tbl_id <- DBI::Id(schema = "stage", table = "address_clean")
  
  # Write data
  dbWriteTable(db_claims, tbl_id, 
               value = as.data.frame(address_clean_load),
               overwrite = F, append = T,
               field.types = paste(names(table_config_create$vars),
                                   table_config_create$vars,
                                   collapse = ", ", sep = " = "))

  
  #### QA ROWS LOADED ####
  rows_ref <- as.integer(dbGetQuery(db_claims, "SELECT COUNT(*) FROM ref.address_clean"))
  rows_stage <- as.integer(dbGetQuery(db_claims, "SELECT COUNT(*) FROM stage.address_clean"))
  
  if (rows_ref + nrow(address_clean_load) != rows_stage) {
    stop("Number rows brought in from ref.address_clean + new rows != stage.address_clean row count")
  }
  
  print("stage.address_clean loaded")
  
}

