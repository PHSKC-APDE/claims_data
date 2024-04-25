#### MASTER CODE TO COPY DATA FROM INTHEALTH_EDW EXTERNAL TABLES TO HHSAW TABLE SHELLS
#
# Eli Kern, PHSKC-APDE
#
# Code developed with assistance from Philip Sylling (KCIT)
# Table shells with CCI indexes were created in HHSAW by Philip
# Stored procedures (agnostic to columns) were created in HHSAW by Philip
#
# 2024-04


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

pacman::p_load(tidyverse, odbc, configr, glue, keyring, svDialogs, R.utils) # Load list of packages


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")


#### STEP 1: CREATE CONNECTIONS ####

##Establish connection to HHSAW prod
interactive_auth <- FALSE
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)


#### STEP 2: COPY DATA FOR ALL TABLES ####

## Beginning message (before loop begins)
message(paste0("Beginning process to copy data from INTHEALTH_EDW to HHSAW - ", Sys.time()))

#Establish list of Azure Blob Storage folders for which GZIP files will be copied to inthealth_edw
table_list <- list("claim_icdcm_raw", "claim_line_raw", "claim_procedure_raw", "claim_provider_raw", "dental_claim", "eligibility",
                   "medical_claim_header", "member_month_detail", "pharmacy_claim", "provider", "provider_master")

#One-time list to just run tables not yet done by Philip
table_list <- list("claim_icdcm_raw", "claim_line_raw", "claim_provider_raw",
                   "member_month_detail", "provider")

#Begin loop
lapply(table_list, function(table_list) {

  table_name <- glue::glue_sql(table_list)
  message(paste0("Working on table: ", table_name))
  system.time(DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("execute claims.usp_load_stage_apcd_{`table_name`}_cci;",
                                .con = db_claims)))
  
  inthealth_row_count <- DBI::dbGetQuery(conn = db_claims,
                                         glue::glue_sql("select count(*) as row_count from claims.stage_apcd_{`table_name`};",
                                                        .con = db_claims))
  hhsaw_row_count <- DBI::dbGetQuery(conn = db_claims,
                                     glue::glue_sql("select count(*) as row_count from claims.stage_apcd_{`table_name`}_cci;",
                                                    .con = db_claims))
  
  if (inthealth_row_count$row_count != hhsaw_row_count$row_count) {
    stop(glue::glue("Mismatching row count between inthealth_edw external table and HHSAW table."))
  }
  
})

## Closing message
message(paste0("All tables have been successfully copied to inthealth_edw - ", Sys.time()))