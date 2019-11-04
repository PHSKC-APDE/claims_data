
library(chron)
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
library(stringr)
library(tidyr)
library(tidyverse) # Manipulate data

db_claims <- dbConnect(odbc(), "PHClaims")

print("Creating stage.mcaid_claim_header")
source("C:/Users/psylling/github/claims_data/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_header.R")
print("Creating stage.mcaid_claim_icdcm_header")
source("C:/Users/psylling/github/claims_data/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_icdcm_header.R")
print("Creating stage.mcaid_claim_line")
source("C:/Users/psylling/github/claims_data/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_line.R")
print("Creating stage.mcaid_claim_pharm")
source("C:/Users/psylling/github/claims_data/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_pharm.R")
print("Creating stage.mcaid_claim_procedure")
source("C:/Users/psylling/github/claims_data/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_procedure.R")

dbDisconnect(db_claims)
