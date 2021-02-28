#### MASTER CODE TO RUN A MONTHYL MEDICAID DATA UPDATE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(sf) # Read shape files
library(keyring) # Access stored credentials
library(dplyr)
library(magrittr)

# These are use for geocoding new addresses
geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
s_shapes <- "//phshare01/epe_share/WORK/REQUESTS/Maps/Shapefiles/"
g_shapes <- "//gisdw/kclib/Plibrary2/"

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/copy_into.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/mcaid/create_db_connection.R")


#### CHOOSE SERVER AND CREATE CONNECTION ####
server <- select.list(choices = c("phclaims", "hhsaw"))
interactive_auth <- select.list(choices = c("TRUE", "FALSE"))
if (server == "hhsaw") {
  prod <- select.list(choices = c("TRUE", "FALSE"))
} else {
  prod <- F
}


db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)

if (server == "hhsaw") {
  dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
}


#### RAW ELIG ####
### Bring in yaml file and function
load_mcaid_elig_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.yaml"))
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.R")

### Extract dates
load_elig_date_min <- as.Date(paste0(str_sub(load_mcaid_elig_config[["date_min"]], 1, 4), "-",
                                     str_sub(load_mcaid_elig_config[["date_min"]], 5, 6), "-",
                                     "01"), format = "%Y-%m-%d")
load_elig_date_max <- as.Date(paste0(str_sub(load_mcaid_elig_config[["date_max"]], 1, 4), "-",
                                     str_sub(load_mcaid_elig_config[["date_max"]], 5, 6), "-",
                                     "01"), format = "%Y-%m-%d") %m+% months(1) - days(1)


if (server == "hhsaw") {
  load_load_raw.mcaid_elig_partial_f(conn = db_claims,
                                     conn_dw = dw_inthealth,
                                     server = server,
                                     config = load_mcaid_elig_config,
                                     etl_date_min = load_elig_date_min, 
                                     etl_date_max = load_elig_date_max,
                                     etl_delivery_date = load_mcaid_elig_config[["date_delivery"]], 
                                     etl_note = "Partial refresh of Medicaid elig data")
} else if (server == "phclaims") {
  ### Create tables
  # Need to do this each time because of the etl_batch_id variable
  create_table_f(conn = db_claims, 
                 server = server,
                 config = load_mcaid_elig_config,
                 overwrite = T)
  
  
  ### Load tables
  load_load_raw.mcaid_elig_partial_f(conn = db_claims,
                                     server = server,
                                     config = load_mcaid_elig_config,
                                     etl_date_min = load_elig_date_min, 
                                     etl_date_max = load_elig_date_max,
                                     etl_delivery_date = load_mcaid_elig_config[["date_delivery"]], 
                                     etl_note = "Partial refresh of Medicaid elig data")
}
### Clean up
rm(load_mcaid_elig_config, load_elig_date_min, load_elig_date_max, load_load_raw.mcaid_elig_partial_f)


#### RAW CLAIMS ####
### Bring in yaml file and function
load_mcaid_claim_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml"))
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.R")



if (server == "hhsaw") {
  load_load_raw.mcaid_claim_partial_f(conn = db_claims,
                                      conn_dw = dw_inthealth,
                                      server = server,
                                      config = load_mcaid_claim_config,
                                      etl_date_min = load_mcaid_claim_config[["date_min"]],
                                      etl_date_max = load_mcaid_claim_config[["date_max"]],
                                      etl_delivery_date = load_mcaid_claim_config[["date_delivery"]], 
                                      etl_note = "Partial refresh of Medicaid claims data",
                                      qa_file_row = F)
} else if (server == "phclaims") {
  ### Create tables
  create_table_f(conn = db_claims, 
                 server = server,
                 config = load_mcaid_claim_config,
                 overwrite = T)
  
  ### Load tables
  load_load_raw.mcaid_claim_partial_f(conn = db_claims,
                                      server = server,
                                      config = load_mcaid_claim_config,
                                      etl_date_min = load_mcaid_claim_config[["date_min"]],
                                      etl_date_max = load_mcaid_claim_config[["date_max"]],
                                      etl_delivery_date = load_mcaid_claim_config[["date_delivery"]], 
                                      etl_note = "Partial refresh of Medicaid claims data",
                                      qa_file_row = F)
}

### Clean up
rm(load_mcaid_claim_config)



#### STAGE ELIG ####
# Call in config file to get vars (and check for errors)
table_config_stage_elig <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml"))
if (table_config_stage_elig[[1]] == "Not Found") {stop("Error in config file. Check URL")}
# Load and run function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.R")

if (server == "hhsaw") {
  system.time(load_stage.mcaid_elig_f(conn_dw = dw_inthealth, 
                                      conn_db = db_claims, 
                                      server = server,
                                      full_refresh = F, 
                                      config = table_config_stage_elig))
} else if (server == "phclaims") {
  system.time(load_stage.mcaid_elig_f(conn_dw = db_claims, 
                                      conn_db = db_claims, 
                                      server = server,
                                      full_refresh = F, 
                                      config = table_config_stage_elig))
}


#### STAGE CLAIM ####
# Call in config file to get vars (and check for errors)
table_config_stage_claims <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.yaml"))
if (table_config_stage_claims[[1]] == "Not Found") {stop("Error in config file. Check URL")}
# Load and run function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.R")
if (server == "hhsaw") {
 system.time(load_claims.stage_mcaid_claim_f(conn_dw = dw_inthealth, 
                                            conn_db = db_claims, 
                                            server = server,
                                            full_refresh = F, 
                                            config = table_config_stage_claims))
} else if (server == "phclaims") {
  system.time(load_claims.stage_mcaid_claim_f(conn_dw = db_claims, 
                                              conn_db = db_claims, 
                                              server = server,
                                              full_refresh = F, 
                                              config = table_config_stage_claims))
}


#### STAGE.ADDRESS_CLEAN ####
# Call in config file to get vars
stage_address_clean_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean.yaml"))
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial.R")

# Run step 1, which identifies new addresses and sets them up to be run through Informatica
stage_address_clean_timestamp <- load_stage.address_clean_partial_step1(server = server,
                                                                        config = stage_address_clean_config,
                                                                        source = 'mcaid',
                                                                        interactive_auth = interactive_auth)

if (stage_address_clean_timestamp != 0) {
  # Load time stamp value to metadata table in case R breaks and needs a restart
  db_claims <- create_db_connection(server, interactive = interactive_auth)
  elig_etl <- as.integer(DBI::dbGetQuery(db_claims, 
                                         glue::glue_sql("SELECT max(etl_batch_id) 
                               FROM {`stage_address_clean_config[[server]][['from_schema']]`}.{`stage_address_clean_config[[server]][['from_table']]`}",
                                                        .con = db_claims)))
  
  timestamp_record <- data.frame(table_name = "stage.address_clean",
                                 qa_item = "informatica_time_stamp",
                                 qa_value = stage_address_clean_timestamp,
                                 qa_date = Sys.time(),
                                 note = paste0("Addresses from ETL batch ", elig_etl))
  
  DBI::dbWriteTable(db_claims, 
                    name = DBI::Id(schema = stage_address_clean_config[[server]][['qa_schema']], 
                                   table = paste0(stage_address_clean_config[[server]][['qa_table']], "qa_mcaid_values")),
                    value = timestamp_record,
                    append = T)
  
  
  
  #### PAUSE ####
  # Wait for Informatica process overnight
  
  ### Check to see if the results are in the output table
  # Set up specific HHSAW connection
  conn_hhsaw <- create_db_connection("hhsaw", interactive = interactive_auth)
  # Check to see if any addresses exist
  # Note need to round SQL time stamp to nearest second
  # NB. The code adds the current timestamp in Pacific time to the server, but the server
  #     stores it as UTC. Same is true when loading to the qa_mcaid_values table.
  #     However, the same is not true when checking the Informatica output table.
  #     Therefore need to do some timezone conversion.
  #     If loading from the qa_mcaid_values table, use as.POSIXct(<value>, tz = "UTC")
  add_output <- DBI::dbGetQuery(conn_hhsaw, 
                                glue::glue_sql("SELECT TOP (1) * 
                               FROM {`stage_address_clean_config[['informatica_ref_schema']]`}.{`stage_address_clean_config[['informatica_output_table']]`} 
                               WHERE convert(varchar, timestamp, 20) = 
                                               {lubridate::with_tz(stage_address_clean_timestamp, 'utc')}",
                                               .con = conn_hhsaw))
  
  while(nrow(add_output) == 0) {
    # Wait an hour before checking again
    Sys.sleep(3600)
    
    # Likely need to re-establish the HHSAW connection due to timeouts
    conn_hhsaw <- create_db_connection("hhsaw", interactive = interactive_auth)
    add_output <- DBI::dbGetQuery(conn_hhsaw, 
                                  glue::glue_sql("SELECT TOP (1) * 
                               FROM {`stage_address_clean_config[['informatica_ref_schema']]`}.{`stage_address_clean_config[['informatica_output_table']]`} 
                               WHERE convert(varchar, timestamp, 20) = 
                                                 {lubridate::with_tz(stage_address_clean_timestamp, 'utc')}",
                                                 .con = conn_hhsaw))
  }
  
  
  ### Likely need to re-establish the server connections due to timeouts
  db_claims <- create_db_connection(server, interactive = interactive_auth)
  if (server == "hhsaw") {
    dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth)
  }
  
  
  ### Run step 2, which processes addresses that were through Informatica and loads to SQL
  load_stage.address_clean_partial_step2(server = server,
                                         config = stage_address_clean_config,
                                         source = 'mcaid',
                                         informatica_timestamp = stage_address_clean_timestamp,
                                         interactive_auth = interactive_auth)
  
  # QA stage.address_clean
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.address_clean_partial.R")
  qa_stage_address_clean <- qa.address_clean_partial(conn = db_claims,
                                                     server = server,
                                                     config = stage_address_clean_config)
  
  
  #### FINAL.ADDRESS_CLEAN ####
  # Check that things passed QA before loading final table
  if (qa_stage_address_clean == 0) {
    # Pull out run date
    last_run_stage_address_clean <- as.POSIXct(odbc::dbGetQuery(
      db_claims, glue::glue_sql("SELECT MAX (last_run) 
                              FROM {`stage_address_clean_config[[server]][['to_schema']]`}.{`stage_address_clean_config[[server]][['to_table']]`}",
                                .con = db_claims))[[1]])
    
    # Pull in the config file
    ref_address_clean_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml"))
    
    to_schema <- ref_address_clean_config[[server]][["to_schema"]]
    to_table <- ref_address_clean_config[[server]][["to_table"]]
    qa_schema <- ref_address_clean_config[[server]][["qa_schema"]]
    qa_table <- ifelse(is.null(ref_address_clean_config[[server]][["qa_table"]]), '',
                       ref_address_clean_config[[server]][["qa_table"]])
    
    # Check if the table exists and, if not, create it
    if (DBI::dbExistsTable(db_claims, DBI::Id(schema = to_schema, table = to_table)) == F) {
      create_table_f(db_claims, server = server, config = ref_address_clean_config)
    }
    
    # Load final table (assumes no changes to table structure)
    load_table_from_sql_f(conn = db_claims, 
                          server = server,
                          config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml",
                          truncate = T, truncate_date = F)
    
    # QA final table
    message("QA final address clean table")
    qa_rows_ref_address_clean <- qa_sql_row_count_f(conn = db_claims, 
                                                    server = server,
                                                    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml")
    
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_stage_address_clean}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                 'Number final rows compared to stage', 
                 {qa_rows_ref_address_clean$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_ref_address_clean$note})",
                     .con = db_claims))
    
    rm(last_run_stage_address_clean, ref_address_clean_config,
       qa_rows_ref_address_clean, to_schema, to_table, qa_schema, qa_table)
  } else {
    stop(glue::glue("Something went wrong with the stage.address_clean run. See {`ref_address_clean_config[[server]][['qa_schema']]`}.
    {DBI::SQL(ref_address_clean_config[[server]][['qa_table']])}qa_mcaid"))
  }
  
  ### Clean up
  rm(add_output, elig_etl, timestamp_record)
  rm(stage_address_clean_config, qa.address_clean_partial, qa_stage_address_clean)
}




#### STAGE.ADDRESS_GEOCODE ####
# Call in config file to get vars
stage_address_geocode_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_geocode.yaml"))

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_geocode_partial.R")

qa_stage_address_geocode <- stage_address_geocode_f(conn = db_claims, 
                                                    server = server, 
                                                    config = stage_address_geocode_config, 
                                                    full_refresh = F)


#### REF.ADDRESS_GEOCODE ####
if (qa_stage_address_geocode == 0) {
  # Pull out run date
  last_run_stage_address_geocode <- as.POSIXct(odbc::dbGetQuery(
    db_claims, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_address_geocode_config[[server]][['to_schema']]`}.{`stage_address_geocode_config[[server]][['to_table']]`}",
                              .con = db_claims))[[1]])

  
  # Pull in the config file
  ref_address_geocode_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml"))
  
  to_schema <- ref_address_geocode_config[[server]][["to_schema"]]
  to_table <- ref_address_geocode_config[[server]][["to_table"]]
  qa_schema <- ref_address_geocode_config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(ref_address_geocode_config[[server]][["qa_table"]]), '',
                     ref_address_geocode_config[[server]][["qa_table"]])
  
  # Check if the table exists and, if not, create it
  if (DBI::dbExistsTable(db_claims, DBI::Id(schema = to_schema, table = to_table)) == F) {
    create_table_f(db_claims, server = server, config = ref_address_geocode_config)
  }
  
  
  # Load final table (assumes no changes to table structure)
  load_table_from_sql_f(conn = db_claims,
                        server = server,
                        config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml",
                        truncate = T, truncate_date = F)
  
  
  # QA final table
  message("QA final address geocode table")
  qa_rows_final <- qa_sql_row_count_f(conn = db_claims,
                                      server = server,
                                      config = ref_address_geocode_config)
  
  DBI::dbExecute(
    conn = db_claims,
    glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_stage_address_geocode}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                 'Number final rows compared to stage', 
                 {qa_rows_final$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final$note})",
                   .con = db_claims))
  
  rm(last_run_stage_address_geocode, qa_rows_final, to_schema, to_table, qa_schema, qa_table)
}
rm(stage_address_geocode_config, qa_stage_address_geocode, stage_address_geocode_f)


#### PHCLAIM AND HHSAW ADDRESS SYNC ####
# While Medicaid claims are being loaded to both servers, need to keep ref tables synced.
# Not always clear which server will be loaded first with new addresses, so check both 
#   and update the other table accordingly

### Set up server-specific connections
conn_hhsaw <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
conn_phclaims <- create_db_connection("phclaims", interactive = interactive_auth, prod = prod)

#### stage_address_clean table ####
# Call in config file to get vars
stage_address_clean_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean.yaml"))

hhsaw_schema <- stage_address_clean_config[['hhsaw']][['to_schema']]
hhsaw_table <- stage_address_clean_config[['hhsaw']][['to_table']]
phclaims_schema <- stage_address_clean_config[['phclaims']][['to_schema']]
phclaims_table <- stage_address_clean_config[['phclaims']][['to_table']]


# Bring in all addresses
address_clean_hhsaw <- DBI::dbGetQuery(
  conn_hhsaw,
  glue::glue_sql("SELECT * FROM {`hhsaw_schema`}.{`hhsaw_table`}", .con = conn_hhsaw))

address_clean_phclaims <- DBI::dbGetQuery(
  conn_phclaims,
  glue::glue_sql("SELECT * FROM {`phclaims_schema`}.{`phclaims_table`}", .con = conn_phclaims))

# Compare and find differences
update_hhsaw <- anti_join(address_clean_phclaims, address_clean_hhsaw,
                          by = "geo_hash_raw")

update_phclaims <- anti_join(address_clean_hhsaw, address_clean_phclaims,
                             by = "geo_hash_raw")


# Update tables so they are in sync
if (nrow(update_hhsaw) > 0) {
  DBI::dbWriteTable(conn_hhsaw, 
                    name = DBI::Id(schema = hhsaw_schema, table = hhsaw_table),
                    value = update_hhsaw,
                    append = T)
}
message(nrow(update_hhsaw), " stage address rows loaded from PHClaims to HHSAW")

if (nrow(update_phclaims) > 0) {
  DBI::dbWriteTable(conn_phclaims, 
                    name = DBI::Id(schema = phclaims_schema, table = phclaims_table),
                    value = update_phclaims,
                    append = T)
}
message(nrow(update_phclaims), " stage address rows loaded from HHSAW to PHClaims")


#### address_clean table ####
# Call in config file to get vars
ref_address_clean_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml"))

hhsaw_schema <- ref_address_clean_config[['hhsaw']][['to_schema']]
hhsaw_table <- ref_address_clean_config[['hhsaw']][['to_table']]
phclaims_schema <- ref_address_clean_config[['phclaims']][['to_schema']]
phclaims_table <- ref_address_clean_config[['phclaims']][['to_table']]


# Bring in all addresses
address_clean_hhsaw <- DBI::dbGetQuery(
  conn_hhsaw,
  glue::glue_sql("SELECT * FROM {`hhsaw_schema`}.{`hhsaw_table`}", .con = conn_hhsaw))

address_clean_phclaims <- DBI::dbGetQuery(
  conn_phclaims,
  glue::glue_sql("SELECT * FROM {`phclaims_schema`}.{`phclaims_table`}", .con = conn_phclaims))

# Compare and find differences
update_hhsaw <- anti_join(address_clean_phclaims, address_clean_hhsaw,
                          by = "geo_hash_raw")

update_phclaims <- anti_join(address_clean_hhsaw, address_clean_phclaims,
                          by = "geo_hash_raw")


# Update tables so they are in sync
if (nrow(update_hhsaw) > 0) {
  DBI::dbWriteTable(conn_hhsaw, 
                    name = DBI::Id(schema = hhsaw_schema, table = hhsaw_table),
                    value = update_hhsaw,
                    append = T)
}
message(nrow(update_hhsaw), " address rows loaded from PHClaims to HHSAW")

if (nrow(update_phclaims) > 0) {
  DBI::dbWriteTable(conn_phclaims, 
                    name = DBI::Id(schema = phclaims_schema, table = phclaims_table),
                    value = update_phclaims,
                    append = T)
}
message(nrow(update_phclaims), " address rows loaded from HHSAW to PHClaims")


#### address_geocode table ####
# Call in config file to get vars
stage_address_geocode_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_geocode.yaml"))

hhsaw_schema <- stage_address_geocode_config[['hhsaw']][['to_schema']]
hhsaw_table <- stage_address_geocode_config[['hhsaw']][['to_table']]
phclaims_schema <- stage_address_geocode_config[['phclaims']][['to_schema']]
phclaims_table <- stage_address_geocode_config[['phclaims']][['to_table']]

# Bring in all addresses
address_geocode_hhsaw <- DBI::dbGetQuery(
  conn_hhsaw,
  glue::glue_sql("SELECT * FROM {`hhsaw_schema`}.{`hhsaw_table`}", .con = conn_hhsaw))

address_geocode_phclaims <- DBI::dbGetQuery(
  conn_phclaims,
  glue::glue_sql("SELECT * FROM {`phclaims_schema`}.{`phclaims_table`}",
                 .con = conn_phclaims))

# Compare and find differences
update_hhsaw <- anti_join(address_geocode_phclaims, address_geocode_hhsaw,
                          by = c("geo_add1_clean", "geo_city_clean", "geo_state_clean", "geo_zip_clean"))

update_phclaims <- anti_join(address_geocode_hhsaw, address_geocode_phclaims,
                             by = c("geo_add1_clean", "geo_city_clean", "geo_state_clean", "geo_zip_clean"))


# Update tables so they are in sync
if (nrow(update_hhsaw) > 0) {
  DBI::dbWriteTable(conn_hhsaw, 
                    name = DBI::Id(schema = hhsaw_schema, table = hhsaw_table),
                    value = update_hhsaw,
                    append = T)
}
message(nrow(update_hhsaw), " stage geocode rows loaded from PHClaims to HHSAW")

if (nrow(update_phclaims) > 0) {
  DBI::dbWriteTable(conn_phclaims, 
                    name = DBI::Id(schema = phclaims_schema, table = phclaims_table),
                    value = update_phclaims,
                    append = T)
}
message(nrow(update_phclaims), " stage geocode rows loaded from HHSAW to PHClaims")


#### address_geocode table ####
# Call in config file to get vars
ref_address_geocode_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml"))

hhsaw_schema <- ref_address_geocode_config[['hhsaw']][['to_schema']]
hhsaw_table <- ref_address_geocode_config[['hhsaw']][['to_table']]
phclaims_schema <- ref_address_geocode_config[['phclaims']][['to_schema']]
phclaims_table <- ref_address_geocode_config[['phclaims']][['to_table']]

# Bring in all addresses
address_geocode_hhsaw <- DBI::dbGetQuery(
  conn_hhsaw,
  glue::glue_sql("SELECT * FROM {`hhsaw_schema`}.{`hhsaw_table`}", .con = conn_hhsaw))

address_geocode_phclaims <- DBI::dbGetQuery(
  conn_phclaims,
  glue::glue_sql("SELECT * FROM {`phclaims_schema`}.{`phclaims_table`}",
                 .con = conn_phclaims))

# Compare and find differences
update_hhsaw <- anti_join(address_geocode_phclaims, address_geocode_hhsaw,
                          by = c("geo_add1_clean", "geo_city_clean", "geo_state_clean", "geo_zip_clean"))

update_phclaims <- anti_join(address_geocode_hhsaw, address_geocode_phclaims,
                             by = c("geo_add1_clean", "geo_city_clean", "geo_state_clean", "geo_zip_clean"))


# Update tables so they are in sync
if (nrow(update_hhsaw) > 0) {
  DBI::dbWriteTable(conn_hhsaw, 
                    name = DBI::Id(schema = hhsaw_schema, table = hhsaw_table),
                    value = update_hhsaw,
                    append = T)
}
message(nrow(update_hhsaw), " geocode rows loaded from PHClaims to HHSAW")

if (nrow(update_phclaims) > 0) {
  DBI::dbWriteTable(conn_phclaims, 
                    name = DBI::Id(schema = phclaims_schema, table = phclaims_table),
                    value = update_phclaims,
                    append = T)
}
message(nrow(update_phclaims), " geocode rows loaded from HHSAW to PHClaims")


### Should think about updating QA tables here too
