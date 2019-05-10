#### CREATE ETL LOGGING TABLE ####

#### Set up global parameter and call in libraries ####
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Piece together queries

db_claims <- dbConnect(odbc(), "PHClaims51")

### Set up functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")

### Run function
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/metadata/tables/create_metadata.qa_mcaid.yaml",
               overall = T, ind_yr = F)
