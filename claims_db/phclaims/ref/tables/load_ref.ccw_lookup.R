## Script name: load_ref.ccw_lookup
##
## Purpose of script: Code to create and load data to ref.ccw_lookup
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2022-05-10
## Email: alastair.matheson@kingcounty.gov
##
## Notes: Based on earlier code by Eli Kern   
##


# SET OPTIONS AND BRING IN PACKAGES ----
options(scipen = 6, digits = 4, warning.length = 8170)

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(svDialogs, tidyverse, lubridate, odbc, configr, glue, openxlsx)

## Bring in relevant functions ----
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")

## Set up server connection ----
server <- dlg_list(c("phclaims", "hhsaw"), title = "Select Server.")$res
if(server == "hhsaw") {
  interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication?")$res
  prod <- dlg_list(c("TRUE", "FALSE"), title = "Production Server?")$res
} else {
  interactive_auth <- T  
  prod <- T
}

db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)

# DATA ----
## Bring in config file ----
ccw_yaml <- yaml::read_yaml(file.path(here::here(), "claims_db/phclaims/ref/tables/load_ref.ccw_lookup.yaml"))

## Bring in from reference github ----
ccw_desc <- openxlsx::read.xlsx("https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/ccw_lookup.xlsx?raw=true", sheet = "ccw_definitions_kc_99_xx")


# MAKE BLANK TABLE ----
create_table(conn = db_claims,
             server = server,
             config = ccw_yaml,
             overall = T,
             ind_yr = F,
             overwrite = T)


# WRITE DATA ----
DBI::dbWriteTable(conn = db_claims, 
                  name = DBI::Id(schema = ccw_yaml[[server]][["to_schema"]], 
                                 table = ccw_yaml[[server]][["to_table"]]),
                  value = ccw_desc,
                  overwrite = F, append = T)
