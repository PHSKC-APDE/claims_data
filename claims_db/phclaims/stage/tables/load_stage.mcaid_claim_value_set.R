
# This code loads table ([stage].[mcaid_claim_value_set]) to hold DISTINCT 
# claim headers that meet RDA and/or HEDIS value set definitions.
# 
# Created by: Philip Sylling, 2019-11-14
# 
#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170)

library(configr) # Read in YAML files
library(DBI)
library(dbplyr)
library(devtools)
library(dplyr)
library(glue)
library(janitor)
library(lubridate)
library(odbc)
library(openxlsx)
library(RCurl) # Read files from Github
library(tidyr)
library(tidyverse) # Manipulate data

db_claims <- dbConnect(odbc(), "PHClaims")
print("Loading stage.mcaid_claim_value_set")

step1_sql <- glue::glue_sql("
EXEC [stage].[sp_load_mcaid_claim_value_set];
", .con = conn)
odbc::dbGetQuery(conn = db_claims, step1_sql)
dbDisconnect(db_claims)

