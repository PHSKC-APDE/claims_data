#### MASTER CODE TO RUN A MONTHYL MEDICAID DATA UPDATE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Githuba
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(sf) # Read shape files
library(keyring) # Access stored credentials
library(stringr) # Various string functions
library(svDialogs)
library(R.utils)
library(kcgeocode)
library(pool)

# These are use for geocoding new addresses
geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
s_shapes <- "//phshare01/epe_share/WORK/REQUESTS/Maps/Shapefiles/"
g_shapes <- "//Kcitfsrprpgdw01/kclib/Plibrary2/"

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/copy_into.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
#memory.limit(size = 56000) # Only necessary for R version < 4.2

#### CHOOSE SERVER AND CREATE CONNECTION ####
server <- dlg_list(c("hhsaw", "phclaims"), title = "Select Server.")$res
if(server == "hhsaw") {
  interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication?")$res
  prod <- dlg_list(c("TRUE", "FALSE"), title = "Production Server?")$res
} else {
  interactive_auth <- T  
  prod <- T
}

db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)

if (server == "hhsaw") {
  dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
}


#### RAW ELIG ####
### Bring in yaml file and function
load_mcaid_elig_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/mcaid_synapse/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.yaml"))
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/mcaid_synapse/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.R")


### Select File
raw_list <- get_unloaded_etl_batches_f(db_claims,
                                       server,
                                       "elig")
batch_select <- dlg_list(raw_list[,"file_name"], title = "Select Raw File")$res
batch <- raw_list[raw_list$file_name == batch_select, ]

if (server == "hhsaw") {
  load_load_raw.mcaid_elig_partial_f(conn = db_claims,
                                     conn_dw = dw_inthealth,
                                     server = server,
                                     config = load_mcaid_elig_config,
                                     batch = batch)
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
                                     batch = batch)
}
### Clean up
rm(load_mcaid_elig_config)


#### RAW CLAIMS ####
### Bring in yaml file and function
load_mcaid_claim_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml"))
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.R")

db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)

### Select File
raw_list <- get_unloaded_etl_batches_f(db_claims,
                                       server,
                                       "claims")
batch_select <- dlg_list(raw_list[,"file_name"], title = "Select Raw File")$res
batch <- raw_list[raw_list$file_name == batch_select, ]

if (server == "hhsaw") {
  load_load_raw.mcaid_claim_partial_f(conn = db_claims,
                                      conn_dw = dw_inthealth,
                                      server = server,
                                      config = load_mcaid_claim_config,
                                      batch = batch)
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
                                      batch = batch)
}

### Clean up
rm(load_mcaid_claim_config)



#### STAGE ELIG ####
# Call in config file to get vars (and check for errors)
table_config_stage_elig <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/mcaid_synapse/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml"))
#if (table_config_stage_elig[[1]] == "Not Found") {stop("Error in config file. Check URL")}
# Load and run function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/mcaid_synapse/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.R")

db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
if (server == "hhsaw") {
  dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
}
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
table_config_stage_claims <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.yaml"))
#if (table_config_stage_claims[[1]] == "Not Found") {stop("Error in config file. Check URL")}
# Load and run function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.R")

db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
if (server == "hhsaw") {
  dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
}
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


#### ADDRESS_CLEAN AND ADDRESS_GEOCODE ####
# Call in config file to get vars
stage_address_clean_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.address_clean.yaml"))
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.address_clean_geocode.R")
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
# Identifies new addresses and sends them through the kcgeocode process
logid <- load_stage.address_clean_geocode(server = server,
                                        config = stage_address_clean_config,
                                        interactive_auth = interactive_auth)
# Checks status of kcgeocode process
load_stage.address_clean_geocode_check(upid = logid)
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
check_status(logid,
            type = 'upid', con = db_claims,
              DBI::Id(schema = 'ref', table = 'address_status'))
rm(list = ls())    

