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


db_claims <- dbConnect(odbc(), "PHClaims51")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/claim_ccw.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")



#### CREATE ELIG TABLES --------------------------------------------------------
#### MCAID_ELIG_DEMO ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.R")

# Pull out run date of stage.mcaid_elig_demo
last_run_elig_demo <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_elig_demo")[[1]])

# QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_demo.R")
qa_mcaid_elig_demo_f(conn = db_claims, load_only = F)

# Load final table (assumes no changes to table structure)
load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml", 
  truncate = T, truncate_date = F)

# QA final table
qa_rows_final_elig_demo <- qa_sql_row_count_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml",
  overall = T, ind_yr = F)

odbc::dbGetQuery(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_elig_demo}, 
                 'final.mcaid_elig_demo',
                 'Number final rows compared to stage', 
                 {qa_rows_final_elig_demo$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_elig_demo$note})",
                 .con = db_claims))

rm(qa_rows_final_elig_demo, last_run_elig_demo)


#### MCAID_ELIG_TIMEVAR ####
# Create and load stage version
create_table_f(conn = db_claims,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_elig_timevar.yaml",
               overall = T, ind_yr = F, overwrite = T)

system.time(devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_timevar.R"))


# Pull out run date of stage.mcaid_elig_timevar
last_run_elig_timevar <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_elig_timevar")[[1]])

# QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_timevar.R")
qa_mcaid_elig_timevar_f(conn = db_claims, load_only = F)


# Create and load final table
create_table_f(conn = db_claims,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml",
               overall = T, ind_yr = F, overwrite = T)

load_table_from_sql_f(conn = db_claims,
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml",
                      truncate = T, truncate_date = F)

# QA final table
qa_rows_final_elig_timevar <- qa_sql_row_count_f(conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml",
  overall = T, ind_yr = F)

DBI::dbExecute(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_elig_timevar}, 
                 'final.mcaid_elig_timevar',
                 'Number final rows compared to stage', 
                 {qa_rows_final_elig_timevar$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_elig_timevar$note})",
                 .con = db_claims))

rm(last_run_elig_timevar, qa_rows_final_elig_timevar)





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


#### MCAID_CLAIM_LINE ####
### Create and load table, add index
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_line.R")

### QA table and load to final
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_line.R",
                     echo = T)

if (fail_tot > 0) {
  message("One or more QA checks on stage.mcaid_claim_line failed. See metadata.qa_mcaid for details")
  claim_line_fail <- 1
} else {
  message("All QA checks on stage.mcaid_claim_line passed, loading to final schema")
  claim_line_fail <- 0
  
  # Pull out run date of stage.mcaid_claim_line
  last_run_claim_line <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_claim_line")[[1]])
  
  # Pull in config files
  stage_mcaid_claim_line_config <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_line.yaml"))
  final_mcaid_claim_line_config <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_line.yaml"))
  
  # Track how many rows in stage
  rows_claim_line_stage <- as.integer(odbc::dbGetQuery(
    db_claims, "SELECT COUNT (*) FROM stage.mcaid_claim_line"))
  
  # Alter schema to final table
  alter_schema_f(conn = db_claims, 
                 from_schema = final_mcaid_claim_line_config$from_schema, 
                 to_schema = final_mcaid_claim_line_config$to_schema,
                 table_name = final_mcaid_claim_line_config$to_table)
  
  # Rename index
  DBI::dbExecute(db_claims,
    glue::glue_sql("EXEC sp_rename N'final.mcaid_claim_line.{`stage_mcaid_claim_line_config$index_name`}', 
                   N'{`final_mcaid_claim_line_config$index_name`}', N'INDEX';",
                   .con = db_claims))
  
  # QA final table
  rows_claim_line_final <- as.integer(odbc::dbGetQuery(
    db_claims, "SELECT COUNT (*) FROM final.mcaid_claim_line"))
  
  if (rows_claim_line_stage == rows_claim_line_final) {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_line}, 
                 'final.mcaid_claim_line',
                 'Number final rows compared to stage', 
                 'PASS', 
                 {Sys.time()}, 
                 'All rows transferred to final schema')",
                     .con = db_claims))
  } else {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_line}, 
                 'final.mcaid_claim_line',
                 'Number final rows compared to stage', 
                 'FAIL', 
                 {Sys.time()}, 
                 '{rows_claim_line_final} rows in final schema (expecting {rows_claim_line_stage})')",
                     .con = db_claims))
  }
  
  rm(last_run_claim_line, stage_mcaid_claim_line_config, 
     final_mcaid_claim_line_config, rows_claim_line_stage, rows_claim_line_final)
}
rm(fail_tot)


#### MCAID_CLAIM_ICDCM_HEADER ####
### Create and load table, add index
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_icdcm_header.R")

### QA table
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_icdcm_header.R",
                     echo = T)

if (fail_tot > 0) {
  message("One or more QA checks on stage.mcaid_claim_icdcm_header failed. See metadata.qa_mcaid for details")
  claim_icdcm_fail <- 1
} else {
  message("All QA checks on stage.mcaid_claim_icdcm_header passed")
  claim_icdcm_fail <- 0
  
  # Pull out run date of stage.mcaid_claim_icdcm_header
  last_run_claim_icdcm_header <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_claim_icdcm_header")[[1]])
  
  # Pull in config files
  stage_mcaid_claim_icdcm_header_config <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_icdcm_header.yaml"))
  final_mcaid_claim_icdcm_header_config <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_icdcm_header.yaml"))
  archive_mcaid_claim_icdcm_header_config <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.mcaid_claim_icdcm_header.yaml"))
  
  
   # Track how many rows in stage
  rows_claim_icdcm_header_stage <- as.integer(odbc::dbGetQuery(
    db_claims, "SELECT COUNT (*) FROM stage.mcaid_claim_icdcm_header"))
  
  # Alter schema from final to archive
  alter_schema_f(conn = db_claims, 
                 from_schema = archive_mcaid_claim_icdcm_header_config$from_schema, 
                 to_schema = archive_mcaid_claim_icdcm_header_config$to_schema,
                 table_name = archive_mcaid_claim_icdcm_header_config$to_table,
                 rename_index = T,
                 index_name = archive_mcaid_claim_icdcm_header_config$index_name)
  
  # Alter schema from stage to final
  alter_schema_f(conn = db_claims, 
                 from_schema = final_mcaid_claim_icdcm_header_config$from_schema, 
                 to_schema = final_mcaid_claim_icdcm_header_config$to_schema,
                 table_name = final_mcaid_claim_icdcm_header_config$to_table,
                 rename_index = T,
                 index_name = final_mcaid_claim_icdcm_header_config$index_name)
  

  # QA final table
  rows_claim_icdcm_header_final <- as.integer(odbc::dbGetQuery(
    db_claims, "SELECT COUNT (*) FROM final.mcaid_claim_icdcm_header"))
  
  if (rows_claim_icdcm_header_stage == rows_claim_icdcm_header_final) {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_icdcm_header}, 
                 'final.mcaid_claim_icdcm_header',
                 'Number final rows compared to stage', 
                 'PASS', 
                 {Sys.time()}, 
                 'All rows transferred to final schema')",
                     .con = db_claims))
  } else {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_icdcm_header}, 
                 'final.mcaid_claim_icdcm_header',
                 'Number final rows compared to stage', 
                 'FAIL', 
                 {Sys.time()}, 
                 '{rows_claim_icdcm_header_final} rows in final schema (expecting {rows_claim_icdcm_header_stage})')",
                     .con = db_claims))
  }
  
  rm(last_run_claim_icdcm_header, stage_mcaid_claim_icdcm_header_config, 
     final_mcaid_claim_icdcm_header_config, rows_claim_icdcm_header_stage, rows_claim_icdcm_header_final)
}
rm(fail_tot)



#### MCAID_CLAIM_PROCEDURE ####
### Create and load table, add index
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_procedure.R")

### QA table and load to final
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_procedure.R",
                     echo = T)

if (fail_tot > 0) {
  message("One or more QA checks on stage.mcaid_claim_procedure failed. See metadata.qa_mcaid for details")
  claim_procedure_fail <- 1
} else {
  message("All QA checks on stage.mcaid_claim_procedure passed, loading to final schema")
  claim_procedure_fail <- 0
  
  # Pull out run date of stage.mcaid_claim_procedure
  last_run_claim_procedure <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_claim_procedure")[[1]])
  
  # Pull in config files
  stage_mcaid_claim_procedure_config <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_procedure.yaml"))
  final_mcaid_claim_procedure_config <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_procedure.yaml"))
  
  # Track how many rows in stage
  rows_claim_procedure_stage <- as.integer(odbc::dbGetQuery(
    db_claims, "SELECT COUNT (*) FROM stage.mcaid_claim_procedure"))
  
  # Alter schema to final table
  alter_schema_f(conn = db_claims, 
                 from_schema = final_mcaid_claim_procedure_config$from_schema, 
                 to_schema = final_mcaid_claim_procedure_config$to_schema,
                 table_name = final_mcaid_claim_procedure_config$to_table)
  
  # Rename index
  DBI::dbExecute(db_claims,
                 glue::glue_sql("EXEC sp_rename N'final.mcaid_claim_procedure.{`stage_mcaid_claim_procedure_config$index_name`}', 
                   N'{`final_mcaid_claim_procedure_config$index_name`}', N'INDEX';",
                                .con = db_claims))
  
  # QA final table
  rows_claim_procedure_final <- as.integer(odbc::dbGetQuery(
    db_claims, "SELECT COUNT (*) FROM final.mcaid_claim_procedure"))
  
  if (rows_claim_procedure_stage == rows_claim_procedure_final) {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_procedure}, 
                 'final.mcaid_claim_procedure',
                 'Number final rows compared to stage', 
                 'PASS', 
                 {Sys.time()}, 
                 'All rows transferred to final schema')",
                     .con = db_claims))
  } else {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_procedure}, 
                 'final.mcaid_claim_procedure',
                 'Number final rows compared to stage', 
                 'FAIL', 
                 {Sys.time()}, 
                 '{rows_claim_procedure_final} rows in final schema (expecting {rows_claim_procedure_stage})')",
                     .con = db_claims))
  }
  
  rm(last_run_claim_procedure, stage_mcaid_claim_procedure_config, 
     final_mcaid_claim_procedure_config, rows_claim_procedure_stage, rows_claim_procedure_final)
}
rm(fail_tot)


#### MCAID_CLAIM_PHARM ####
### Create and load table, add index
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_pharm.R")

### QA table and load to final
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_pharm.R",
                     echo = T)

if (fail_tot > 0) {
  message("One or more QA checks on stage.mcaid_claim_pharm failed. See metadata.qa_mcaid for details")
  claim_pharm_fail <- 1
} else {
  message("All QA checks on stage.mcaid_claim_pharm passed, loading to final schema")
  claim_pharm_fail <- 0
  
  # Pull out run date of stage.mcaid_claim_pharm
  last_run_claim_pharm <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_claim_pharm")[[1]])
  
  # Pull in config files
  stage_mcaid_claim_pharm_config <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_pharm.yaml"))
  final_mcaid_claim_pharm_config <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_pharm.yaml"))
  
  # Track how many rows in stage
  rows_claim_pharm_stage <- as.integer(odbc::dbGetQuery(
    db_claims, "SELECT COUNT (*) FROM stage.mcaid_claim_pharm"))
  
  # Alter schema to final table
  alter_schema_f(conn = db_claims, 
                 from_schema = final_mcaid_claim_pharm_config$from_schema, 
                 to_schema = final_mcaid_claim_pharm_config$to_schema,
                 table_name = final_mcaid_claim_pharm_config$to_table)
  
  # Rename index
  DBI::dbExecute(db_claims,
                 glue::glue_sql("EXEC sp_rename N'final.mcaid_claim_pharm.{`stage_mcaid_claim_pharm_config$index_name`}', 
                   N'{`final_mcaid_claim_pharm_config$index_name`}', N'INDEX';",
                                .con = db_claims))
  
  # QA final table
  rows_claim_pharm_final <- as.integer(odbc::dbGetQuery(
    db_claims, "SELECT COUNT (*) FROM final.mcaid_claim_pharm"))
  
  if (rows_claim_pharm_stage == rows_claim_pharm_final) {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_pharm}, 
                 'final.mcaid_claim_pharm',
                 'Number final rows compared to stage', 
                 'PASS', 
                 {Sys.time()}, 
                 'All rows transferred to final schema')",
                     .con = db_claims))
  } else {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_pharm}, 
                 'final.mcaid_claim_pharm',
                 'Number final rows compared to stage', 
                 'FAIL', 
                 {Sys.time()}, 
                 '{rows_claim_pharm_final} rows in final schema (expecting {rows_claim_pharm_stage})')",
                     .con = db_claims))
  }
  
  rm(last_run_claim_pharm, stage_mcaid_claim_pharm_config, 
     final_mcaid_claim_pharm_config, rows_claim_pharm_stage, rows_claim_pharm_final)
}
rm(fail_tot)




#### MCAID_CLAIM_HEADER ####
if (sum(claim_line_fail, claim_icdcm_fail, claim_procedure_fail, claim_pharm_fail) > 0) {
  stop("One or more claims analytic tables failed, mcaid_claim_header not created. See metadata.mcaid_qa for details")
} else {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_header.R")
  
  ### QA table and load to final
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_header.R",
                       echo = T)
  
  if (fail_tot > 0) {
    message("One or more QA checks on stage.mcaid_claim_header failed. See metadata.qa_mcaid for details")
  } else {
    message("All QA checks on stage.mcaid_claim_header passed, loading to final schema")
    # Pull out run date of stage.mcaid_claim_header
    last_run_claim_header <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_claim_header")[[1]])
    
    # Pull in config files
    stage_mcaid_claim_header_config <- yaml::yaml.load(RCurl::getURL(
      "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_header.yaml"))
    final_mcaid_claim_header_config <- yaml::yaml.load(RCurl::getURL(
      "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_header.yaml"))
    
    # Track how many rows in stage
    rows_claim_header_stage <- as.integer(odbc::dbGetQuery(
      db_claims, "SELECT COUNT (*) FROM stage.mcaid_claim_header"))
    
    # Alter schema to final table
    alter_schema_f(conn = db_claims, 
                   from_schema = final_mcaid_claim_header_config$from_schema, 
                   to_schema = final_mcaid_claim_header_config$to_schema,
                   table_name = final_mcaid_claim_header_config$to_table)
    
    # Rename index
    DBI::dbExecute(db_claims,
                   glue::glue_sql("EXEC sp_rename N'final.mcaid_claim_header.{`stage_mcaid_claim_header_config$index_name`}', 
                   N'{`final_mcaid_claim_header_config$index_name`}', N'INDEX';",
                                  .con = db_claims))
    
    # QA final table
    rows_claim_header_final <- as.integer(odbc::dbGetQuery(
      db_claims, "SELECT COUNT (*) FROM final.mcaid_claim_header"))
    
    if (rows_claim_header_stage == rows_claim_header_final) {
      DBI::dbExecute(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_header}, 
                 'final.mcaid_claim_header',
                 'Number final rows compared to stage', 
                 'PASS', 
                 {Sys.time()}, 
                 'All rows transferred to final schema')",
                       .con = db_claims))
    } else {
      DBI::dbExecute(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_header}, 
                 'final.mcaid_claim_header',
                 'Number final rows compared to stage', 
                 'FAIL', 
                 {Sys.time()}, 
                 '{rows_claim_header_final} rows in final schema (expecting {rows_claim_header_stage})')",
                       .con = db_claims))
    }
    
    rm(last_run_claim_header, stage_mcaid_claim_header_config, 
       final_mcaid_claim_header_config, rows_claim_header_stage, rows_claim_header_final)
  }
  rm(claim_line_fail, claim_icdcm_fail, claim_procedure_fail, claim_pharm_fail)
  rm(fail_tot)
}


#### MCAID_CLAIM_VALUE_SET ####




### CCW
# Load table to SQL
load_ccw(conn = db_claims, source = "mcaid")

# QA table
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_ccw.R",
                     echo = T)

# If QA passes, load to final table
if (ccw_qa_result == "PASS") {
  
  create_table_f(
    conn = db_claims, 
    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml",
    overall = T, ind_yr = F, overwrite = T)
  
  load_table_from_sql_f(
    conn = db_claims,
    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml", 
    truncate = T, truncate_date = F)
  
  # QA final table
  qa_rows_final_claim_ccw <- qa_sql_row_count_f(
    conn = db_claims,
    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml",
    overall = T, ind_yr = F)
  
  odbc::dbGetQuery(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_ccw}, 
                 'final.mcaid_claim_ccw',
                 'Number final rows compared to stage', 
                 {qa_rows_final_claim_ccw$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_claim_ccw$note})",
                   .con = db_claims))
  
  rm(qa_rows_final_claim_ccw)
} else {
  warning("CCW table failed QA and was not loaded to final schema")
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

