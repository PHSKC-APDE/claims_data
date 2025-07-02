library(tidyverse) # Manipulate data
library(data.table) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(svDialogs)
library(R.utils)

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/external_table_check.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/table_duplicate.R")

table_list <- c("elig_demo", "elig_timevar", "elig_month",
                "claim_line", "claim_icdcm_header",
                "claim_procedure", "claim_pharm",
                "claim_header", "claim_naloxone",
                "claim_moud", "claim_preg_episode",
                "claim_ccw", "claim_bh")

from_conn <- create_db_connection("inthealth", interactive = F, prod = T)
to_conn <- create_db_connection("hhsaw", interactive = F, prod = T)
file <- paste0("c:/temp/mcaid/prod_claims_ext_", format(Sys.Date(), "%Y%m%d"), ".sql")

for(i in 1:length(table_list)) {
  table <- table_list[[i]]
  external_table_check_f(
    conn = from_conn,
    db = "inthealth_edw",
    schema = "stg_claims",
    table = paste0("stage_mcaid_", table),
    db_ext = "hhs_analytics_workspace",
    conn_ext = to_conn,
    schema_ext = "claims",
    table_ext = paste0("stage_mcaid_", table),
    sql_display = T,
    sql_file_path = file,
    overwrite = F
  )
}


from_conn <- create_db_connection("inthealth", interactive = F, prod = T)
to_conn <- create_db_connection("inthealth", interactive = F, prod = F)

for(i in 1:length(table_list)) {
  table <- table_list[[i]]
  table_duplicate_f(conn_from = from_conn, 
                    conn_to = to_conn, 
                    server_to = "inthealth_dev", 
                    db_to = "inthealth_edw",
                    from_schema = "stg_claims",
                    from_table = paste0("stage_mcaid_", table),
                    to_schema = "stg_claims",
                    to_table = paste0("stage_mcaid_", table),
                    confirm_tables = F,
                    delete_table = T,
                    table_structure_only = T)
}

from_conn <- create_db_connection("inthealth", interactive = F, prod = F)
to_conn <- create_db_connection("hhsaw", interactive = F, prod = F)
file <- paste0("c:/temp/mcaid/dev_claims_ext_", format(Sys.Date(), "%Y%m%d"), ".sql")

for(i in 1:length(table_list)) {
  table <- table_list[[i]]
  external_table_check_f(
    conn = from_conn,
    db = "inthealth_edw",
    schema = "stg_claims",
    table = paste0("stage_mcaid_", table),
    db_ext = "hhs_analytics_workspace",
    conn_ext = to_conn,
    schema_ext = "claims",
    table_ext = paste0("stage_mcaid_", table),
    sql_display = T,
    sql_file_path = file,
    overwrite = F
  )
}

