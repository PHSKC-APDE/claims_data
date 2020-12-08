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
# conn = database connection
# conn_iat = database connection for Informatica address tables
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# source = mcade, mcare, apcd
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage.address_clean_partial_step1 <- function(conn_ = NULL,
                                                   conn_iat = NULL,
                                                   server = NULL,
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
  
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- config[[server]][["ref_table"]]
  informatica_ref_schema <- config[["informatica_ref_schema"]]
  informatica_input_table <- config[["informatica_input_table"]]
  informatica_output_table <- config[["informatica_address_output"]]
  
  
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
  new_add_out <- new_add %>%
    distinct(geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw, geo_hash_raw) %>%
    mutate(add_id = n())
  cur_timestamp <- Sys.time()
  
  
  message(nrow(new_add_out), " addresses were exported for Informatica cleanup")
}

