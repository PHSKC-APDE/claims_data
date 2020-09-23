#### MASTER CODE TO UPDATE MEDICAID ANALYTIC TABLES
#
# Alastair Matheson, PHSKC (APDE)
#
# 2020-01


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(data.table) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
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
}


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/db_loader/scripts_general/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/db_loader/scripts_general/claim_ccw.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/db_loader/scripts_general/yaml_import.R")



#### CREATE ELIG TABLES --------------------------------------------------------
#### MCAID_ELIG_DEMO ####
### Bring in function and config file
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.R")
stage_mcaid_elig_demo_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.yaml")

# Run function
load_stage_mcaid_elig_demo_f(conn = db_claims, server = server, config = stage_mcaid_elig_demo_config)

# Pull out run date
last_run_elig_demo <- as.POSIXct(odbc::dbGetQuery(
  db_claims, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_mcaid_elig_demo_config[[server]][['to_schema']]`}{`stage_mcaid_elig_demo_config[[server]][['to_table']]`}",
                            .con = db_claims))[[1]])

### QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_demo.R")
qa_stage_mcaid_elig_demo <- qa_mcaid_elig_demo_f(conn = db_claims, server = server, 
                                                 config = stage_mcaid_elig_demo_config, load_only = F)


# Check that things passed QA before loading final table
if (qa_stage_mcaid_elig_demo == 0) {
  # Check if the table exists and, if not, create it
  final_mcaid_elig_demo_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml")
  
  if (DBI::dbExistsTable(db_claims, DBI::Id(schema = final_mcaid_elig_demo_config[[server]][["to_schema"]],
                                            table = final_mcaid_elig_demo_config[[server]][["to_table"]])) == F) {
    create_table_f(db_claims, server = server, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml")
  }
  
  #### Load final table (assumes no changes to table structure)
  load_table_from_sql_f(conn = db_claims,
                        server = server,
                        config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml", 
                        truncate = T, truncate_date = F)
  
  # QA final table
  qa_rows_final_elig_demo <- qa_sql_row_count_f(conn = db_claims, 
                                                server = server,
                                                config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml")
  
  DBI::dbExecute(
    conn = db_claims,
    glue::glue_sql("INSERT INTO {`final_mcaid_elig_demo_config[[server]][['qa_schema']]`}.
    {DBI::SQL(final_mcaid_elig_demo_config[[server]][['qa_table']])}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_elig_demo}, 
                 '{DBI::SQL(final_mcaid_elig_demo_config[[server]][['to_schema']])}.{DBI::SQL(final_mcaid_elig_demo_config[[server]][['to_table']])}',
                 'Number final rows compared to stage', 
                 {qa_rows_final_elig_demo$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_elig_demo$note})",
                   .con = db_claims))
  
  rm(qa_rows_final_elig_demo)
} else {
  stop(glue::glue("Something went wrong with the mcaid_elig_demo run. See {`final_mcaid_elig_demo_config[[server]][['qa_schema']]`}.
    {DBI::SQL(final_mcaid_elig_demo_config[[server]][['qa_table']])}qa_mcaid"))
}


### Clean up
rm(qa_stage_mcaid_elig_demo, stage_mcaid_elig_demo_config, load_stage_mcaid_elig_demo_f, 
   last_run_elig_demo, final_mcaid_elig_demo_config)



#### MCAID_ELIG_TIMEVAR ####
### Bring in function and config file
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_timevar.R")
stage_mcaid_elig_timevar_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_timevar.yaml")

# Run function
load_stage_mcaid_elig_timevar_f(conn = db_claims, server = server, config = stage_mcaid_elig_timevar_config)

# Pull out run date
last_run_elig_timevar <- as.POSIXct(odbc::dbGetQuery(
  db_claims, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_mcaid_elig_timevar_config[[server]][['to_schema']]`}{`stage_mcaid_elig_timevar_config[[server]][['to_table']]`}",
                            .con = db_claims))[[1]])

### QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_timevar.R")
qa_stage_mcaid_elig_timevar <- qa_mcaid_elig_timevar_f(conn = db_claims, server = server, 
                                                 config = stage_mcaid_elig_timevar_config, load_only = F)

# Check that things passed QA before loading final table
if (qa_stage_mcaid_elig_timevar == 0) {
  # Check if the table exists and, if not, create it
  final_mcaid_elig_timevar_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml")
  
  if (DBI::dbExistsTable(db_claims, DBI::Id(schema = final_mcaid_elig_timevar_config[[server]][["to_schema"]],
                                            table = final_mcaid_elig_timevar_config[[server]][["to_table"]])) == F) {
    create_table_f(db_claims, server = server, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml")
  }
  
  #### Load final table (assumes no changes to table structure)
  load_table_from_sql_f(conn = db_claims,
                        server = server,
                        config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml", 
                        truncate = T, truncate_date = F)
  
  # QA final table
  qa_rows_final_elig_timevar <- qa_sql_row_count_f(conn = db_claims, 
                                                server = server,
                                                config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml")
  
  DBI::dbExecute(
    conn = db_claims,
    glue::glue_sql("INSERT INTO {`final_mcaid_elig_timevar_config[[server]][['qa_schema']]`}.
    {DBI::SQL(final_mcaid_elig_timevar_config[[server]][['qa_table']])}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_elig_timevar}, 
                 '{DBI::SQL(final_mcaid_elig_timevar_config[[server]][['to_schema']])}.{DBI::SQL(final_mcaid_elig_timevar_config[[server]][['to_table']])}',
                 'Number final rows compared to stage', 
                 {qa_rows_final_elig_timevar$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_elig_timevar$note})",
                   .con = db_claims))
  
  rm(qa_rows_final_elig_timevar)
} else {
  stop(glue::glue("Something went wrong with the mcaid_elig_timevar run. See {`final_mcaid_elig_timevar_config[[server]][['qa_schema']]`}.
    {DBI::SQL(final_mcaid_elig_timevar_config[[server]][['qa_table']])}qa_mcaid"))
}


### Clean up
rm(qa_stage_mcaid_elig_timevar, stage_mcaid_elig_timevar_config, load_stage_mcaid_elig_timevar_f, 
   last_run_elig_timevar, final_mcaid_elig_timevar_config)



#### CREATE CLAIMS TABLES ------------------------------------------------------
# Need to follow this order when making tables because of dependencies
# These scripts depend only on [stage].[mcaid_claim]:
#    mcaid_claim_line
#    mcaid_claim_icdcm_header
#    mcaid_claim_procedure
#    mcaid_claim_pharm
#
# The mcaid_claim_header table relies on the tables above
# The mcaid_claim_value_set table relies on the tables above


#### GENERIC CLAIM LOAD PROCESS ####
# The general loading process for many claim tables is the same so this function can 
# be used. Will look for the value of server in the general environment. Fine for 
# now but might want to tighten that up at some point.

claim_load_f <- function(table = c("icdcm_header", "header", "line", 
                                   "pharm", "procedure")) {
  
  table <- match.arg(table)
  
  
  ### Bring in function and config file
  devtools::source_url(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_", table, ".R"))
  stage_config <- yaml::read_yaml(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_", table, ".yaml"))
  
  
  # Run function, which also adds index
  if (table == "icdcm_header") {
    load_stage_mcaid_claim_icdcm_header_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "header") {
    load_stage_mcaid_claim_header_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "") {
    load_stage_mcaid_claim_line_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "pharm") {
    load_stage_mcaid_claim_pharm_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "procedure") {
    load_stage_mcaid_claim_procedure_f(conn = db_claims, server = server, config = stage_config)
  }
  
  # Pull out run date
  last_run_claim <- as.POSIXct(odbc::dbGetQuery(
    db_claims, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_config[[server]][['to_schema']]`}{`stage_config[[server]][['to_table']]`}",
                              .con = db_claims))[[1]])
  
  
  ### QA table and load to final
  devtools::source_url(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_", table, ".R"))
  
  if (table == "icdcm_header") {
    qa_stage <- qa_stage_mcaid_claim_icdcm_header_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "header") {
    qa_stage <- qa_stage_mcaid_claim_header_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "") {
    qa_stage <- qa_stage_mcaid_claim_line_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "pharm") {
    qa_stage <- qa_stage_mcaid_claim_pharm_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "procedure") {
    qa_stage <- qa_stage_mcaid_claim_procedure_f(conn = db_claims, server = server, config = stage_config)
  }
  
  
  if (qa_stage > 0) {
    message("One or more QA checks on ", stage_config[[server]][['to_schema']], ".", stage_config[[server]][['to_table']], " failed. See ", stage_config[[server]][['qa_schema']], ".", stage_config[[server]][['qa_table']], "qa_mcaid for details")
    table_fail <- 1
  } else {
    ### Load to final
    message("All QA checks on ", stage_config[[server]][['to_schema']], ".", stage_config[[server]][['to_table']], " passed, loading to final table")
    
    
    # Bring in config file
    final_config <- yaml::read_yaml(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_claim_", table, ".yaml"))
    
    # Track how many rows in stage
    rows_claim_stage <- as.integer(odbc::dbGetQuery(
      db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`final_config[[server]][['from_schema']]`}.
                              {`final_config[[server]][['from_table']]`}",
                                .con = db_claims)))
    
    # Remove final table
    try(DBI::dbSendQuery(db_claims, glue::glue_sql(
      "DROP TABLE {`final_config[[server]][['to_schema']]`}.{`final_config[[server]][['to_table']]`}",
      .con = db_claims)))
    
    # Rename to final table
    if (server == "hhsaw") {
      DBI::dbSendQuery(db_claims, glue::glue_sql(
        "EXEC sp_rename '{DBI::SQL(final_config[[server]][['from_schema']])}.{DBI::SQL(final_config[[server]][['from_table']])}',  {final_config[[server]][['to_table']]}", .con = db_Claims))
    } else if (server == "phclaims") {
      alter_schema_f(conn = db_claims, 
                     from_schema = final_config[[server]][['from_schema']], 
                     to_schema = final_config[[server]][['to_table']],
                     table_name = final_config[[server]][['to_table']],
                     rename_index = F)
    }
    
    # QA final table
    rows_claim_final <- as.integer(odbc::dbGetQuery(
      db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`final_config[[server]][['to_schema']]`}.
                              {`final_config[[server]][['to_table']]`}",
                                .con = db_claims)))
    
    
    if (rows_claim_stage == rows_claim_final) {
      DBI::dbExecute(
        conn = db_claims,
        glue::glue_sql("INSERT INTO {`final_config[[server]][['qa_schema']]`}.
    {DBI::SQL(final_config[[server]][['qa_table']])}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_line}, 
                 '{DBI::SQL(final_config[[server]][['to_schema']])}.{DBI::SQL(final_config[[server]][['to_table']])}',
                 'Number final rows compared to stage', 
                 'PASS', 
                 {Sys.time()}, 
                 'All rows transferred to final table ({rows_claim_stage})')",
                       .con = db_claims))
      
      # Track success
      table_fail <- 0
    } else {
      DBI::dbExecute(
        conn = db_claims,
        glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_line}, 
                 '{DBI::SQL(final_config[[server]][['to_schema']])}.{DBI::SQL(final_config[[server]][['to_table']])}',
                 'Number final rows compared to stage', 
                 'FAIL', 
                 {Sys.time()}, 
                 '{rows_claim_final} rows in final table (expecting {rows_claim_stage})')",
                       .con = db_claims))
      
      # Note failure
      table_fail <- 1
    }
  }
  
  # Export out results of load
  return(table_fail)
}


#### MCAID_CLAIM_LINE ####
claim_line_fail <- claim_load_f(table = "line")


#### MCAID_CLAIM_ICDCM_HEADER ####
claim_icdcm_header_fail <- claim_load_f(table = "icdcm_header")


#### MCAID_CLAIM_PROCEDURE ####
claim_procedure_fail <- claim_load_f(table = "procedure")


#### MCAID_CLAIM_PHARM ####
claim_pharm_fail <- claim_load_f(table = "pharm")


#### MCAID_CLAIM_HEADER ####
if (sum(claim_line_fail, claim_icdcm_header_fail, claim_procedure_fail, claim_pharm_fail) > 0) {
  stop("One or more claims analytic tables failed, mcaid_claim_header not created. See metadata.mcaid_qa for details")
} else {
  claim_load_f(table = "header")
}


#### MCAID_CLAIM_VALUE_SET ####




#### MCAID_CLAIM_CCW ####
# Load table to SQL
ccw_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_ccw.yaml")
load_ccw(conn = db_claims, server = server, source = "mcaid",
         config = ccw_config)

# QA table
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_ccw.R")
ccw_qa_result <- qa_stage_mcaid_claim_ccw_f(conn = db_claims, server = server, config = ccw_config)


# If QA passes, load to final table
if (ccw_qa_result == "PASS") {
  # Check if the table exists and, if not, create it
  final_mcaid_claim_ccw_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml")
  
  if (DBI::dbExistsTable(db_claims, DBI::Id(schema = final_mcaid_claim_ccw_config[[server]][["to_schema"]],
                                            table = final_mcaid_claim_ccw_config[[server]][["to_table"]])) == F) {
    create_table_f(db_claims, server = server, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml")
  }
  
  #### Load final table (assumes no changes to table structure)
  load_table_from_sql_f(conn = db_claims,
                        server = server,
                        config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml", 
                        truncate = T, truncate_date = F)
  
  # QA final table
  qa_rows_final_claim_ccw <- qa_sql_row_count_f(conn = db_claims, 
                                                server = server,
                                                config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml")
  
  DBI::dbExecute(
    conn = db_claims,
    glue::glue_sql("INSERT INTO {`final_mcaid_claim_ccw_config[[server]][['qa_schema']]`}.
    {DBI::SQL(final_mcaid_claim_ccw_config[[server]][['qa_table']])}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_ccw}, 
                 '{DBI::SQL(final_mcaid_claim_ccw_config[[server]][['to_schema']])}.{DBI::SQL(final_mcaid_claim_ccw_config[[server]][['to_table']])}',
                 'Number final rows compared to stage', 
                 {qa_rows_final_claim_ccw$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_claim_ccw$note})",
                   .con = db_claims))
  
  rm(qa_rows_final_claim_ccw)
} else {
  stop(glue::glue("Something went wrong with the mcaid_claim_ccw run. See {`final_mcaid_claim_ccw_config[[server]][['qa_schema']]`}.
    {DBI::SQL(final_mcaid_claim_ccw_config[[server]][['qa_table']])}qa_mcaid"))
}



#### PERFORMANCE MEASURES ------------------------------------------------------
# All these tables will be renamed to have a mcaid_ prefix at some point

#### PERF_ELIG_MEMBER_MONTH ####
DBI::dbExecute(db_claims, "EXEC [stage].[sp_perf_elig_member_month]")

#### PERF_ENROLL_DENOM ####
# Need to find which months have been refreshed and run for those
# Assumes a 12-month refresh. Change manually for other options
max_elig_month <- odbc::dbGetQuery(db_claims, "SELECT MAX (CLNDR_YEAR_MNTH) FROM stage.mcaid_elig")[[1]]
min_elig_month <- max_elig_month - 99

# Run stored procedure
DBI::dbExecute(db_claims, 
               glue_sql("EXEC [stage].[sp_perf_enroll_denom] 
                        @start_date_int = {min_elig_month}, @end_date_int = {max_elig_month}",
               .con = db_claims))

rm(max_elig_month, min_elig_month)

#### PERF_DISTINCT_MEMBER ####
DBI::dbExecute(db_claims, "EXEC [stage].[sp_perf_distinct_member]")


#### ASTHMA MEDICATION RATIO ####



#### DROP TABLES NO LONGER NEEDED ####

