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


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/claim_ccw.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/mcaid/create_db_connection.R")


server <- select.list(choices = c("phclaims", "hhsaw"))
interactive_auth <- select.list(choices = c("TRUE", "FALSE"))
if (server == "hhsaw") {
  prod <- select.list(choices = c("TRUE", "FALSE"))
} else {
  prod <- F
}

db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)




#### CREATE ELIG TABLES --------------------------------------------------------
#### MCAID_ELIG_DEMO ####
### Bring in function and config file
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.R")
stage_mcaid_elig_demo_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.yaml")

# Run function
load_stage_mcaid_elig_demo_f(conn = db_claims, server = server, config = stage_mcaid_elig_demo_config)

# Pull out run date
last_run_elig_demo <- as.POSIXct(odbc::dbGetQuery(
  db_claims, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_mcaid_elig_demo_config[[server]][['to_schema']]`}.{`stage_mcaid_elig_demo_config[[server]][['to_table']]`}",
                            .con = db_claims))[[1]])

### QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_demo.R")
# Re-establish connection because it drops out faster in Azure VM
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
qa_stage_mcaid_elig_demo <- qa_mcaid_elig_demo_f(conn = db_claims, server = server, 
                                                 config = stage_mcaid_elig_demo_config, load_only = F)


# Check that things passed QA before loading final table
if (qa_stage_mcaid_elig_demo == 0) {
  # Check if the table exists and, if not, create it
  final_mcaid_elig_demo_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml")
  
  to_schema <- final_mcaid_elig_demo_config[[server]][["to_schema"]]
  to_table <- final_mcaid_elig_demo_config[[server]][["to_table"]]
  qa_schema <- final_mcaid_elig_demo_config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(final_mcaid_elig_demo_config[[server]][["qa_table"]]), '',
                     final_mcaid_elig_demo_config[[server]][["qa_table"]])
  
  if (DBI::dbExistsTable(db_claims, DBI::Id(schema = to_schema, table = to_table)) == F) {
    create_table_f(db_claims, server = server, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml")
  }
  
  #### Load final table (assumes no changes to table structure)
  load_table_from_sql_f(conn = db_claims,
                        server = server,
                        config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml", 
                        truncate = T, truncate_date = F)
  
  # QA final table
  message("QA final table")
  qa_rows_final_elig_demo <- qa_sql_row_count_f(conn = db_claims, 
                                                server = server,
                                                config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml")
  
  DBI::dbExecute(
    conn = db_claims,
    glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_elig_demo}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                 'Number final rows compared to stage', 
                 {qa_rows_final_elig_demo$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_elig_demo$note})",
                   .con = db_claims))
  
  
  rm(final_mcaid_elig_demo_config, qa_rows_final_elig_demo, to_schema, to_table, qa_schema, qa_table)
} else {
  stop(glue::glue("Something went wrong with the mcaid_elig_demo run. See {`stage_mcaid_elig_demo_config[[server]][['qa_schema']]`}.
    {DBI::SQL(stage_mcaid_elig_demo_config[[server]][['qa_table']])}qa_mcaid"))
}


### Clean up
rm(qa_stage_mcaid_elig_demo, stage_mcaid_elig_demo_config, load_stage_mcaid_elig_demo_f, 
   last_run_elig_demo)



#### MCAID_ELIG_TIMEVAR ####
### Bring in function and config file
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_timevar.R")
stage_mcaid_elig_timevar_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_timevar.yaml")

# Run function
load_stage_mcaid_elig_timevar_f(conn = db_claims, server = server, config = stage_mcaid_elig_timevar_config)

# Re-establish connection because it drops out faster in Azure VM
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)

# Pull out run date
last_run_elig_timevar <- as.POSIXct(odbc::dbGetQuery(
  db_claims, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_mcaid_elig_timevar_config[[server]][['to_schema']]`}.{`stage_mcaid_elig_timevar_config[[server]][['to_table']]`}",
                            .con = db_claims))[[1]])

### QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_timevar.R")
qa_stage_mcaid_elig_timevar <- qa_mcaid_elig_timevar_f(conn = db_claims, server = server, 
                                                 config = stage_mcaid_elig_timevar_config, load_only = F)

# Check that things passed QA before loading final table
if (qa_stage_mcaid_elig_timevar == 0) {
  # Check if the table exists and, if not, create it
  final_mcaid_elig_timevar_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml")
  
  to_schema <- final_mcaid_elig_timevar_config[[server]][["to_schema"]]
  to_table <- final_mcaid_elig_timevar_config[[server]][["to_table"]]
  qa_schema <- final_mcaid_elig_timevar_config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(final_mcaid_elig_timevar_config[[server]][["qa_table"]]), '',
                     final_mcaid_elig_timevar_config[[server]][["qa_table"]])
  
  if (DBI::dbExistsTable(db_claims, DBI::Id(schema = to_schema, table = to_table)) == F) {
    create_table_f(db_claims, server = server, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml")
  }
  
  #### Load final table (assumes no changes to table structure)
  load_table_from_sql_f(conn = db_claims,
                        server = server,
                        config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml", 
                        truncate = T, truncate_date = F)
  
  # QA final table
  qa_rows_final_elig_timevar <- qa_sql_row_count_f(conn = db_claims, 
                                                server = server,
                                                config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml")
  
  DBI::dbExecute(
    conn = db_claims,
    glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_elig_timevar}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                 'Number final rows compared to stage', 
                 {qa_rows_final_elig_timevar$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_elig_timevar$note})",
                   .con = db_claims))
  
  rm(final_mcaid_elig_timevar_config, qa_rows_final_elig_timevar, to_schema, to_table, qa_schema, qa_table)
} else {
  stop(paste0(glue::glue("Something went wrong with the mcaid_elig_timevar run. See {stage_mcaid_elig_timevar_config[[server]][['qa_schema']]}."),
    glue::glue("{DBI::SQL(ifelse(is.null(stage_mcaid_elig_timevar_config[[server]][['qa_table']]), 
               '', stage_mcaid_elig_timevar_config[[server]][['qa_table']]))}qa_mcaid")))
}

### Clean up
rm(qa_stage_mcaid_elig_timevar, stage_mcaid_elig_timevar_config, load_stage_mcaid_elig_timevar_f, 
   last_run_elig_timevar)



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

claim_load_f <- function(table = c("ccw", "icdcm_header", "header", "line", 
                                   "pharm", "procedure")) {
  
  table <- match.arg(table)
  
  ### Bring in function and config file
  # ccw script already called in above
  if (table != "ccw") {
    devtools::source_url(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_", table, ".R"))
  }
  stage_config <- yaml::read_yaml(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_", table, ".yaml"))
  
  
  # Run function, which also adds index
  if (table == "ccw") {
    load_ccw(conn = db_claims, server = server, source = "mcaid", config = stage_config)
  } else if (table == "icdcm_header") {
    load_stage_mcaid_claim_icdcm_header_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "header") {
    load_stage_mcaid_claim_header_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "line") {
    load_stage_mcaid_claim_line_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "pharm") {
    load_stage_mcaid_claim_pharm_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "procedure") {
    load_stage_mcaid_claim_procedure_f(conn = db_claims, server = server, config = stage_config)
  }
  
  # Re-establish connection because it drops out faster in Azure VM
  db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  
  # Pull out run date
  last_run_claim <- as.POSIXct(odbc::dbGetQuery(
    db_claims, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_config[[server]][['to_schema']]`}.{`stage_config[[server]][['to_table']]`}",
                              .con = db_claims))[[1]])
  
  
  ### QA table and load to final
  devtools::source_url(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_", table, ".R"))
  
  if (table == "ccw") {
    qa_stage <- qa_stage_mcaid_claim_ccw_f(conn = db_claims, server = server, config = stage_config, skip_review = T)
  } else if (table == "icdcm_header") {
    qa_stage <- qa_stage_mcaid_claim_icdcm_header_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "header") {
    qa_stage <- qa_stage_mcaid_claim_header_f(conn = db_claims, server = server, config = stage_config)
  } else if (table == "line") {
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
    final_config <- yaml::read_yaml(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_", table, ".yaml"))
    
    from_schema <- final_config[[server]][["from_schema"]]
    from_table <- final_config[[server]][["from_table"]]
    to_schema <- final_config[[server]][["to_schema"]]
    to_table <- final_config[[server]][["to_table"]]
    qa_schema <- final_config[[server]][["qa_schema"]]
    qa_table <- ifelse(is.null(final_config[[server]][["qa_table"]]), '',
                       final_config[[server]][["qa_table"]])
    
    
    # Track how many rows in stage
    rows_claim_stage <- as.integer(odbc::dbGetQuery(
      db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}",
                                .con = db_claims)))
    
    # Remove final table
    try(DBI::dbExecute(db_claims, 
                         glue::glue_sql("DROP TABLE {`to_schema`}.{`to_table`}", .con = db_claims)), 
        silent = T)
    
    # Rename to final table
    if (server == "hhsaw") {
      DBI::dbExecute(db_claims, glue::glue_sql(
        "EXEC sp_rename '{DBI::SQL(from_schema)}.{DBI::SQL(from_table)}',  {to_table}", .con = db_claims))
    } else if (server == "phclaims") {
      alter_schema_f(conn = db_claims, 
                     from_schema = from_schema, 
                     to_schema = to_schema,
                     table_name = to_table,
                     rename_index = F)
    }
    
    # QA final table
    rows_claim_final <- as.integer(odbc::dbGetQuery(
      db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}",
                                .con = db_claims)))
    
    
    if (rows_claim_stage == rows_claim_final) {
      DBI::dbExecute(
        conn = db_claims,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
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
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
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



#### MCAID_CLAIM_CCW ####
claim_ccw_fail <- claim_load_f(table = "ccw")



#### MCAID_CLAIM_VALUE_SET ####



#### PERFORMANCE MEASURES ------------------------------------------------------
#### PERF_ELIG_MEMBER_MONTH ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_perf_elig_member_month.R")
load_stage_mcaid_perf_elig_member_month_f(conn = db_claims, server = server)


#### PERF_ENROLL_DENOM ####
# Bring in config file
stage_mcaid_perf_enroll_denom_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_perf_enroll_denom.yaml")

stage_schema <- stage_mcaid_perf_enroll_denom_config[[server]][["stage_schema"]]
stage_table <- ifelse(is.null(stage_mcaid_perf_enroll_denom_config[[server]][["stage_table"]]), '',
                      stage_mcaid_perf_enroll_denom_config[[server]][["stage_table"]])
final_schema <- stage_mcaid_perf_enroll_denom_config[[server]][["final_schema"]]
final_table <- ifelse(is.null(stage_mcaid_perf_enroll_denom_config[[server]][["final_table"]]), '',
                      stage_mcaid_perf_enroll_denom_config[[server]][["final_table"]])
ref_schema <- stage_mcaid_perf_enroll_denom_config[[server]][["ref_schema"]]
ref_table <- ifelse(is.null(stage_mcaid_perf_enroll_denom_config[[server]][["ref_table"]]), '',
                   stage_mcaid_perf_enroll_denom_config[[server]][["ref_table"]])


# Need to find which months have been refreshed and run for those
# Assumes a 12-month refresh. Change manually for other options
# If the table doesn't yet exist, cover all time periods since 2012 
#  (2-yr lookback means using 201401)
max_elig_month <- odbc::dbGetQuery(
  db_claims, 
  glue::glue_sql("SELECT MAX (CLNDR_YEAR_MNTH) 
                 FROM {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_elig",
                 .con = db_claims))[[1]]

if (DBI::dbExistsTable(db_claims, name = DBI::Id(schema = stage_schema, 
                                                 table = paste0(stage_table, "mcaid_perf_enroll_denom")))) {
  min_elig_month <- odbc::dbGetQuery(
    db_claims, 
    glue::glue_sql("SELECT YEAR([12_month_prior]) * 100 + MONTH([12_month_prior])
                 FROM
                 (SELECT MAX(b.[12_month_prior]) AS [12_month_prior]
                   FROM {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_enroll_denom AS a
                  LEFT JOIN {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month AS b
                  ON a.[year_month] = b.[year_month]) AS a",
                   .con = db_claims))[[1]]
} else {
  min_elig_month <- 201401L
}

# Load and run function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_perf_enroll_denom.R")
load_stage_mcaid_perf_enroll_denom_f(conn = db_claims, server = server,
                                     start_date_int = min_elig_month,
                                     end_date_int = max_elig_month,
                                     config = stage_mcaid_perf_enroll_denom_config)

rm(max_elig_month, min_elig_month, stage_mcaid_perf_enroll_denom_config, load_stage_mcaid_perf_enroll_denom_f)


#### PERF_DISTINCT_MEMBER ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_perf_distinct_member.R")
load_stage_mcaid_perf_distinct_member_f(conn = db_claims, server = server)




#### PERF MEASURES ####
# NB. Note currently running these
# # Bring in config file
# stage_mcaid_perf_measure_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_perf_measure.yaml")
# 
# devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_perf_measure.R")
# 
# measures <- c("Acute Hospital Utilization",
#               "All-Cause ED Visits",
#               "Child and Adolescent Access to Primary Care",
#               "Follow-up ED visit for Alcohol/Drug Abuse",
#               "Follow-up ED visit for Mental Illness",
#               "Follow-up Hospitalization for Mental Illness",
#               "Mental Health Treatment Penetration",
#               "SUD Treatment Penetration",
#               "SUD Treatment Penetration (Opioid)",
#               "Plan All-Cause Readmissions (30 days)")
# 
# 
# lapply(measures, function(x) {
#   message("Loading ", x)
#   stage_mcaid_perf_measure_f(conn = db_claims, server = server,
#                              measure = x, end_month = max_elig_month,
#                              config = stage_mcaid_perf_measure_config)
# })


#### ASTHMA MEDICATION RATIO ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_perf_measure_amr.R")
stage_mcaid_perf_measure_amr_f(conn = db_claims, server = server,
                               max_month = max_elig_month,
                               return_data = F)


#### DROP TABLES NO LONGER NEEDED ####

#### CHOOSE TO DROP BACK UP ARCHIVE TABLES  ####
message("DELETE BACK UP ARCHIVE TABLES?")
bak_del <- select.list(choices = c("Yes", "No"))
if (bak_del == "Yes") {
  table_config_stage_elig <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml")) 
  table_config_stage_claim <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.yaml"))
  bak_schema <- table_config_stage_elig[[server]][["archive_schema"]]
  bak_elig <- paste0(ifelse(is.null(table_config_stage_elig[[server]][["archive_table"]]), '',
                          table_config_stage_elig[[server]][["archive_table"]]), '_bak')
  bak_claim <- paste0(ifelse(is.null(table_config_stage_claim[[server]][["archive_table"]]), '',
                            table_config_stage_claim[[server]][["archive_table"]]), '_bak')
  if (server == "hhsaw") {
    conn <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
  } else {
    conn <- db_claims
  }
  try(DBI::dbSendQuery(conn,
                     glue::glue_sql("DROP TABLE {`bak_schema`}.{`bak_elig`}",
                                    .con = conn)))
  try(DBI::dbSendQuery(conn,
                     glue::glue_sql("DROP TABLE {`bak_schema`}.{`bak_claim`}",
                                    .con = conn)))
}
rm(bak_del, table_config_stage_elig, table_config_stage_claim, bak_schema, 
   bak_elig, bak_claim, conn)

