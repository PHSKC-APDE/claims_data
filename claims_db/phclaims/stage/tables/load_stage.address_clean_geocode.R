#### CODE TO UPDATE ADDRESS_CLEAN AND ADDRESS_GEOCODE TABLES WITH MONTHLY MEDICAID REFRESHES
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2022-06


### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_partial.R

### Function elements
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# source = mcade, mcare, apcd
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage.address_clean_geocode <- function(server = NULL,
                                                   config = NULL,
                                                   source = NULL,
                                                   get_config = F,
                                                   interactive_auth = NULL) {
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
  
  conn <- create_db_connection(server, interactive = interactive_auth)
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[["hhsaw"]][["to_schema"]]
  to_table <- config[["hhsaw"]][["to_table"]]
  ref_schema <- config[["hhsaw"]][["ref_schema"]]
  ref_table <- config[["hhsaw"]][["ref_table"]]
  
  #### Take address data from Medicaid that don't match to the ref table ####
  ### Bring in all Medicaid addresses not in the ref table and send into new geocoding function to clean addresses and geocode
  # Include ETL batch ID to know where the addresses are coming from
  conn <- create_db_connection(server, interactive = interactive_auth)
  stage_adds <- DBI::dbGetQuery(
    conn,
    glue::glue_sql("SELECT DISTINCT 
                     RSDNTL_ADRS_LINE_1 AS 'geo_add1_raw', 
                     RSDNTL_ADRS_LINE_2 AS 'geo_add2_raw', 
                     NULL AS 'geo_add3_raw',
                     RSDNTL_CITY_NAME AS 'geo_city_raw', 
                     RSDNTL_STATE_CODE AS 'geo_state_raw', 
                     RSDNTL_POSTAL_CODE AS 'geo_zip_raw', 
                     geo_hash_raw, etl_batch_id
                   FROM {`from_schema`}.{`from_table`}",
                   .con = conn))
  
  conn_hhsaw <- create_db_connection("hhsaw", interactive = interactive_auth)
  ref_hashes <- DBI::dbGetQuery(
    conn_hhsaw,
    glue::glue_sql("SELECT geo_hash_raw
                   FROM {`ref_schema`}.{`ref_table`}",
                   .con = conn_hhsaw))
  
  new_adds <- anti_join(stage_adds, ref_hashes)
  if(nrow(new_adds) == 0) {
    message("[", Sys.time(), "] No New Addresses to Clean...")
    return(0)
  }
  uped_adds <- submit_ads_for_cleaning(new_adds, conn_hhsaw)
  conn_hhsaw <- create_db_connection("hhsaw", interactive = interactive_auth)
  log_schema <- getOption('kcg.log_upload')@name["schema"]
  log_table <- getOption('kcg.log_upload')@name["table"]
  log <- DBI::dbGetQuery(conn_hhsaw,
                         glue::glue_sql("SELECT TOP (1) * 
                                        FROM {`log_schema`}.{`log_table`}
                                        WHERE [username] = {Sys.getenv('USERNAME')}
                                        ORDER BY [timestamp] DESC",
                                        .con = conn_hhsaw))
  message("[", Sys.time(), "] ", log$nrow, " Addresses were Exported for Informatica Cleanup...")
  return(log$id)
}

load_stage.address_clean_geocode_check <- function(interactive_auth = T,
                                                   upid) {
  if(upid == 0) {
    stop("No New Addresses")
  }
  conn_hhsaw <- create_db_connection("hhsaw", interactive = interactive_auth)
  log_schema <- getOption('kcg.log_geoclean')@name["schema"]
  log_table <- getOption('kcg.log_geoclean')@name["table"]
  address_schema <- getOption('kcg.address_clean')@name["schema"]
  address_table <- getOption('kcg.address_clean')@name["table"]
  geocode_schema <- getOption('kcg.address_geocode')@name["schema"]
  geocode_table <- getOption('kcg.address_geocode')@name["table"]
  
  clean <- DBI::dbGetQuery(conn_hhsaw,
                         glue::glue_sql("SELECT TOP (1) * 
                                        FROM {`log_schema`}.{`log_table`}
                                        WHERE [upid] = {upid}
                                          AND [destination] = {paste0(address_schema, '.', address_table)}",
                                        .con = conn_hhsaw))  
  if(nrow(clean) > 0) {
    message("[", Sys.time(), "] Address Cleaning Complete: ", clean$nrow, " Addresses Added to [",
            address_schema, "].[", address_table, "]")
    geocode <- DBI::dbGetQuery(conn_hhsaw,
                             glue::glue_sql("SELECT TOP (1) * 
                                        FROM {`log_schema`}.{`log_table`}
                                        WHERE [destination] = {paste0(geocode_schema, '.', geocode_table)}
                                            AND [timestamp] > {format(clean$timestamp, usetz = FALSE)}
                                          ORDER BY [timestamp] ASC",
                                          
                                            .con = conn_hhsaw))  
    if(nrow(geocode) > 0) {
      message("[", Sys.time(), "] Address Geocoding Complete: ", geocode$nrow, " Addresses Added to [",
              geocode_schema, "].[", geocode_table, "]")
    } else {
      message("[", Sys.time(), "] Address Geocoding NOT Complete...")
    }
  } else {
    message("[", Sys.time(), "] Address Cleaning and Geocoding NOT Complete...")
  }
}
