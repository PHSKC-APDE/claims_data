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


server <- select.list(choices = c("phclaims", "hhsaw"))



if (server == "phclaims") {
  db_claims <- DBI::dbConnect(odbc::odbc(), "PHClaims51")
} else if (server == "hhsaw") {
  db_claims <- DBI::dbConnect(odbc::odbc(),
                              driver = "ODBC Driver 17 for SQL Server",
                              server = "tcp:kcitazrhpasqldev20.database.windows.net,1433",
                              database = "hhs_analytics_workspace",
                              uid = keyring::key_list("hhsaw_dev")[["username"]],
                              pwd = keyring::key_get("hhsaw_dev", keyring::key_list("hhsaw_dev")[["username"]]),
                              Encrypt = "yes",
                              TrustServerCertificate = "yes",
                              Authentication = "ActiveDirectoryPassword")
  
  dw_inthealth <- DBI::dbConnect(odbc::odbc(),
                                 driver = "ODBC Driver 17 for SQL Server",
                                 server = "tcp:kcitazrhpasqldev20.database.windows.net,1433",
                                 database = "inthealth_edw",
                                 uid = keyring::key_list("hhsaw_dev")[["username"]],
                                 pwd = keyring::key_get("hhsaw_dev", keyring::key_list("hhsaw_dev")[["username"]]),
                                 Encrypt = "yes",
                                 TrustServerCertificate = "yes",
                                 Authentication = "ActiveDirectoryPassword")
}


# These are use for geocoding new addresses
geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
s_shapes <- "//phshare01/epe_share/WORK/REQUESTS/Maps/Shapefiles/"
g_shapes <- "//gisdw/kclib/Plibrary2/"



#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/copy_into.R")


#### RAW ELIG ####
### Bring in yaml file and function
load_mcaid_elig_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.yaml"))
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
load_mcaid_claim_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml"))
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
table_config_stage_elig <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml"))
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
  system.time(load_stage.mcaid_elig_f(conn_db = db_claims, 
                                      server = server,
                                      full_refresh = F, 
                                      config = table_config_stage_elig))
}



#### STAGE CLAIM ####
# Call in config file to get vars (and check for errors)
table_config_stage_claims <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.yaml"))
if (table_config_stage_claims[[1]] == "Not Found") {stop("Error in config file. Check URL")}
# Load and run function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.R")
system.time(load_claims.stage_mcaid_claim_f(conn_dw = dw_inthealth, 
                                            conn_db = db_claims, 
                                            full_refresh = F, 
                                            config = table_config_stage_claims))


#### ADDRESS CLEANING ####
### stage.address_clean
# Call in config file to get vars
stage_address_clean_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean.yaml"))

# Run step 1, which identifies new addresses and sets them up to be run through Informatica
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step1.R")

load_stage.address_clean_partial_1(conn = db_claims, 
                                   server = server, 
                                   config = stage_address_clean_config)


#### MANUAL PAUSE ####
# Need to get the Informatica process automated

# Run step 2, which processes addresses that were through Informatica and loads to SQL
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step2.R")
load_stage.address_clean_partial_2(conn = db_claims, 
                                   server = server, 
                                   config = stage_address_clean_config)

# QA stage.address_clean
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.address_clean_partial.R")
qa_stage_address_clean <- qa.address_clean_partial(conn = db_claims, 
                                                   server = server, 
                                                   config = stage_address_clean_config)


# Check that things passed QA before loading final table
if (qa_stage_address_clean == 0) {
  # Pull out run date
  last_run_stage_address_clean <- as.POSIXct(odbc::dbGetQuery(
    db_claims, glue::glue_sql("SELECT MAX (last_run) 
                              FROM {`stage_address_clean_config[[server]][['to_schema']]`}.{`stage_address_clean_config[[server]][['to_table']]`}",
                              .con = db_claims))[[1]])
  
  # Pull in the config file
  ref_address_clean_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml"))
  
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
rm(stage_address_clean_config, load_stage.address_clean_partial_1, load_stage.address_clean_partial_2, 
   qa.address_clean_partial, qa_stage_address_clean)



#### STAGE.ADDRESS_GEOCODE ####
# Call in config file to get vars
stage_address_geocode_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_geocode.yaml"))


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
  ref_address_geocode_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml"))
  
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
                        config = ref_address_geocode_config,
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
