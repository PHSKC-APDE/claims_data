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

pacman::p_load(tidyverse, odbc, configr, glue, keyring, svDialogs, R.utils, apde.etl) # Load list of packages

#### STEP 1: CREATE CONNECTIONS ####

##Establish connection to HHSAW prod
interactive_auth <- FALSE
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

#### STEP 2: COPY DATA FOR ALL TABLES ####

## Beginning message (before loop begins)
message(paste0("Beginning process to copy data from INTHEALTH_EDW to HHSAW - ", Sys.time()))

#Full table list
table_list <- list(
  "mcare_claim_bh",
  "mcare_claim_ccw",
  "mcare_claim_header",
  "mcare_claim_icdcm_header",
  "mcare_claim_line",
  "mcare_claim_moud",
  "mcare_claim_naloxone",
  "mcare_claim_pharm",
  "mcare_claim_pharm_char",
  "mcare_claim_procedure",
  "mcare_claim_provider",
  "mcare_elig_demo",
  "mcare_elig_timevar",
  "mcare_elig_month",
  "mcare_bene_enrollment",
  "mcare_bene_names",
  "mcare_bene_ssn_xwalk"
)

#Define modified table list if needed (e.g., when loop breaks after some tables have been copied)
#table_list <- list()

#Begin loop
lapply(table_list, function(table_list) {

  if(survPen::instr(table_list, "bene") == 0) {
    ext_table <- paste0("stage_", table_list)
    to_table <- paste0("final_", table_list)
  } else {
    ext_table <- table_list
    to_table <- paste0("stage_", table_list)
  }
  
  message(paste0("Working on table: ", to_table, " - ", Sys.time()))
  db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("execute claims.usp_external_table_load @fromtable = N{ext_table}, @totable = N{to_table};",
                                .con = db_claims))
  
  #Row count comparison for all tables
  inthealth_row_count <- DBI::dbGetQuery(conn = db_claims,
                                         glue::glue_sql("select count(*) as row_count from claims.{`ext_table`};",
                                                        .con = db_claims))
  hhsaw_row_count <- DBI::dbGetQuery(conn = db_claims,
                                     glue::glue_sql("select count(*) as row_count from claims.{`to_table`};",
                                                    .con = db_claims))
  
  if (inthealth_row_count$row_count != hhsaw_row_count$row_count) {
    stop(glue::glue("Mismatching row count between inthealth_edw external table and HHSAW table."))
  }
  
  message(paste0("Done working on table: ", to_table, " - ", Sys.time()))
})

## Closing message
message(paste0("All tables have been successfully copied to inthealth_edw - ", Sys.time()))
rm(list = ls())