#### MASTER CODE TO COPY DATA FROM INTHEALTH_EDW EXTERNAL TABLES TO HHSAW TABLE SHELLS
#
# Eli Kern, PHSKC-APDE
#
# Code developed with assistance from Philip Sylling (KCIT)
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

#Full table list
table_list <- list(
  "final_apcd_elig_demo",
  "final_apcd_elig_timevar",
  "final_apcd_elig_month",
  "final_apcd_elig_plr",
  "final_apcd_claim_line",
  "final_apcd_claim_icdcm_header",
  "final_apcd_claim_procedure",
  "final_apcd_claim_provider",
  "final_apcd_claim_header",
  "final_apcd_claim_ccw",
  "final_apcd_claim_preg_episode",
  "final_apcd_claim_dental",
  "final_apcd_claim_pharmacy",
  "ref_apcd_provider",
  "ref_apcd_provider_master"
)

#Define modified table list if needed (e.g., when loop breaks after some tables have been copied)
#table_list <- list()

#Begin loop
lapply(table_list, function(table_list) {

  table_name <- glue::glue_sql(table_list)
  message(paste0("Working on table: ", table_name, " - ", Sys.time()))
  db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("execute claims.usp_load_{`table_name`};",
                                .con = db_claims))
  
  #Row count comparison for all tables except PLR tables
  if(table_name != "final_apcd_elig_plr") {
    table_name_inthealth <- gsub("final_", "stage_", table_name)
    inthealth_row_count <- DBI::dbGetQuery(conn = db_claims,
                                           glue::glue_sql("select count(*) as row_count from claims.{`table_name_inthealth`};",
                                                          .con = db_claims))
    hhsaw_row_count <- DBI::dbGetQuery(conn = db_claims,
                                       glue::glue_sql("select count(*) as row_count from claims.{`table_name`};",
                                                      .con = db_claims))
    
    if (inthealth_row_count$row_count != hhsaw_row_count$row_count) {
      stop(glue::glue("Mismatching row count between inthealth_edw external table and HHSAW table."))
    }
  }
  
  message(paste0("Done working on table: ", table_name, " - ", Sys.time()))
})

## Closing message
message(paste0("All tables have been successfully copied to inthealth_edw - ", Sys.time()))