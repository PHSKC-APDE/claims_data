#### MASTER CODE TO UPDATE MEDICAID ANALYTIC TABLES
#
# Alastair Matheson, PHSKC (APDE)
#
# 2020-01


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999, digits.secs = 3)

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


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_ccw.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/claim_bh.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/notify.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/table_duplicate.R")
#memory.limit(size = 56000) # Only necessary for R version < 4.2

server <- "hhsaw"
interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication?")$res
prod <- dlg_list(c("TRUE", "FALSE"), title = "Production Server?")$res

db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)


#### COPY NECESSARY REF TABLES ####
from_conn <- create_db_connection("hhsaw", interactive = F, prod = T)
table_copy_df <- read.csv("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/refs/heads/main/claims_db/db_loader/mcaid/McaidTableCopyList.csv")
if(prod == T) {
  stg_server <- "inthealth"
} else {
  stg_server <- "inthealth_dev"
}
table_duplicate_f(conn_from = from_conn, 
                  conn_to = dw_inthealth, 
                  server_to = stg_server, 
                  db_to = "inthealth_edw",
                  table_df = table_copy_df,
                  confirm_tables = F,
                  delete_table = T)


#### CREATE ELIG TABLES --------------------------------------------------------
#### MCAID_ELIG_DEMO ####
### Bring in function and config file
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.R")
stage_mcaid_elig_demo_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.yaml")

# Run function
load_stage_mcaid_elig_demo_f(conn = dw_inthealth, server = server, config = stage_mcaid_elig_demo_config)

# Pull out run date
last_run_elig_demo <- as.POSIXct(odbc::dbGetQuery(
  dw_inthealth, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_mcaid_elig_demo_config[[server]][['to_schema']]`}.{`stage_mcaid_elig_demo_config[[server]][['to_table']]`}",
                            .con = dw_inthealth))[[1]])

### QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_demo.R")
# Re-establish connection because it drops out faster in Azure VM
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
qa_stage_mcaid_elig_demo <- qa_mcaid_elig_demo_f(conn = dw_inthealth, conn_qa = db_claims, server = server, 
                                                 config = stage_mcaid_elig_demo_config, load_only = F)

### DISPLAY QA RESULTS
# Check that things passed QA before loading final table

 


### Clean up
rm(qa_stage_mcaid_elig_demo, stage_mcaid_elig_demo_config, load_stage_mcaid_elig_demo_f, 
   last_run_elig_demo)



#### MCAID_ELIG_TIMEVAR ####
### Bring in function and config file
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_timevar.R")
stage_mcaid_elig_timevar_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_timevar.yaml")

# Run function
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
load_stage_mcaid_elig_timevar_f(conn = dw_inthealth, server = server, config = stage_mcaid_elig_timevar_config)

# Re-establish connection because it drops out faster in Azure VM
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)

# Pull out run date
last_run_elig_timevar <- as.POSIXct(odbc::dbGetQuery(
  dw_inthealth, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_mcaid_elig_timevar_config[[server]][['to_schema']]`}.{`stage_mcaid_elig_timevar_config[[server]][['to_table']]`}",
                            .con = dw_inthealth))[[1]])

### QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_timevar.R")
qa_stage_mcaid_elig_timevar <- qa_mcaid_elig_timevar_f(conn = dw_inthealth, conn_qa = db_claims, server = server, 
                                                       config = stage_mcaid_elig_timevar_config, load_only = F)

### DISPLAY QA RESULTS
# Check that things passed QA before loading final table


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
                                   "pharm", "procedure", "bh", 
								   "moud", "naloxone", "preg_episode")) {
  time_start <- Sys.time()
  table <- match.arg(table)
  
  db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
  
  ### Bring in function and config file
  # ccw script already called in above
  if (table != "ccw" & table != "bh") {
    devtools::source_url(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_", table, ".R"))
  }
  stage_config <- yaml::read_yaml(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_", table, ".yaml"))
  
  
  # Run function, which also adds index
  if (table == "ccw") {
    load_ccw(conn = dw_inthealth, server = server, source = "mcaid", config = stage_config)
  } else if (table == "icdcm_header") {
    load_stage_mcaid_claim_icdcm_header_f(conn = dw_inthealth, server = server, config = stage_config)
  } else if (table == "header") {
    load_stage_mcaid_claim_header_f(conn = dw_inthealth, server = server, config = stage_config)
  } else if (table == "line") {
    load_stage_mcaid_claim_line_f(conn = dw_inthealth, server = server, config = stage_config)
  } else if (table == "pharm") {
    load_stage_mcaid_claim_pharm_f(conn = dw_inthealth, server = server, config = stage_config)
  } else if (table == "procedure") {
    load_stage_mcaid_claim_procedure_f(conn = dw_inthealth, server = server, config = stage_config)
  } else if (table == "moud") {
    load_stage_mcaid_claim_moud_f(conn = dw_inthealth, server = server, config = stage_config)
  } else if (table == "naloxone") {
    load_stage_mcaid_claim_naloxone_f(conn = dw_inthealth, server = server, config = stage_config)
  } else if (table == "preg_episode") {
    load_stage_mcaid_claim_preg_episode_f(conn = dw_inthealth, server = server, config = stage_config)
  } else if (table == "bh") {
    load_bh(conn = dw_inthealth, server = server, source = "mcaid", config = stage_config)
  }
  
  # Re-establish connection because it drops out faster in Azure VM
  db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
  
  # Pull out run date
  last_run_claim <- as.POSIXct(odbc::dbGetQuery(
    dw_inthealth, glue::glue_sql("SELECT MAX (last_run) FROM {`stage_config[[server]][['to_schema']]`}.{`stage_config[[server]][['to_table']]`}",
                              .con = dw_inthealth))[[1]])
  
  
  ### QA table 
 # if (table != "bh") {
    devtools::source_url(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_", table, ".R"))
  #}
  
  qa_stage <- 0
  
  if (table == "ccw") {
    qa_stage <- qa_stage_mcaid_claim_ccw_f(conn = dw_inthealth, conn_qa = db_claims, server = server, config = stage_config, skip_review = T)
  } else if (table == "icdcm_header") {
    qa_stage <- qa_stage_mcaid_claim_icdcm_header_f(conn = dw_inthealth, conn_qa = db_claims, server = server, config = stage_config)
  } else if (table == "header") {
    qa_stage <- qa_stage_mcaid_claim_header_f(conn = dw_inthealth, conn_qa = db_claims, server = server, config = stage_config)
  } else if (table == "line") {
    qa_stage <- qa_stage_mcaid_claim_line_f(conn = dw_inthealth, conn_qa = db_claims, server = server, config = stage_config)
  } else if (table == "pharm") {
    qa_stage <- qa_stage_mcaid_claim_pharm_f(conn = dw_inthealth, conn_qa = db_claims, server = server, config = stage_config)
  } else if (table == "procedure") {
    qa_stage <- qa_stage_mcaid_claim_procedure_f(conn = dw_inthealth, conn_qa = db_claims, server = server, config = stage_config)
  } else if (table == "moud") {
    qa_stage <- qa_stage_mcaid_claim_moud_f(conn = dw_inthealth, conn_qa = db_claims, server = server, config = stage_config)
  } else if (table == "naloxone") {
    qa_stage <- qa_stage_mcaid_claim_naloxone_f(conn = dw_inthealth, conn_qa = db_claims, server = server, config = stage_config)
  } else if (table == "preg_episode") {
    qa_stage <- qa_stage_mcaid_claim_preg_episode_f(conn = dw_inthealth, conn_qa = db_claims, server = server, config = stage_config)
  }
  
  conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  if (qa_stage > 0) {
    message("One or more QA checks on ", stage_config[[server]][['to_schema']], ".", stage_config[[server]][['to_table']], " failed. See ", stage_config[[server]][['qa_schema']], ".", stage_config[[server]][['qa_table']], "qa_mcaid for details")
    table_fail <- 1
  } else {
    ### Load to final
    message("All QA checks on ", stage_config[[server]][['to_schema']], ".", stage_config[[server]][['to_table']], " passed, loading to final table")
    ## SKIP QA ##
  }  
  ## END SKIP ##  
  # Bring in config file
  if(1 == 0) {
  final_config <- yaml::read_yaml(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/final/tables/load_final.mcaid_claim_", table, ".yaml"))
  
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
                 VALUES ({format(last_run_claim, usetz = FALSE)}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                 'Number final rows compared to stage', 
                 'PASS', 
                 {format(Sys.time(), usetz = FALSE)}, 
                 'All rows transferred to final table ({rows_claim_stage})')",
                     .con = db_claims))
    
    # Track success
    table_fail <- 0
  } else {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({format(last_run_claim, usetz = FALSE)}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                 'Number final rows compared to stage', 
                 'FAIL', 
                 {format(Sys.time(), usetz = FALSE)}, 
                 '{rows_claim_final} rows in final table (expecting {rows_claim_stage})')",
                     .con = db_claims))
    
    # Note failure
    table_fail <- 1
  }
  ## SKIP QA ##
  }
  ## END SKIP ##  
  time_end <- Sys.time()
  message(glue::glue("Table creation took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                     " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))
  # Export out results of load
  table_fail <- 0
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
#### MCAID_CLAIM_NALOXONE ####
claim_naloxone_fail <- claim_load_f(table = "naloxone")
#### MCAID_CLAIM_MOUD ####
claim_moud_fail <- claim_load_f(table = "moud")
#### MCAID_CLAIM_PREG_EPISODE ####
claim_preg_episode_fail <- claim_load_f(table = "preg_episode")
#### MCAID_CLAIM_CCW ####
claim_ccw_fail <- claim_load_f(table = "ccw")
#### MCAID_CLAIM_BH ####
claim_bh_fail <- claim_load_f(table = "bh")

#### UPDATING MCAID_ELIG_DEMO FOR NONCISGENDERED ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.R")
stage_mcaid_elig_demo_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.yaml")
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
#----------NEW FUNCTION----------#
### Clean up
rm(qa_stage_mcaid_elig_demo, stage_mcaid_elig_demo_config, load_stage_mcaid_elig_demo_f, 
   last_run_elig_demo)


#### STAGE TABLE TO FINAL TABLE ####
table_list <- c("elig_demo", "elig_timevar",
                "claim_line", "claim_icdcm_header",
                "claim_procedure", "claim_pharm",
                "claim_header", "claim_naloxone",
                "claim_moud", "claim_preg_episode",
                "claim_ccw", "claim_bh")

message(paste0("Beginning process to copy data from INTHEALTH_EDW to HHSAW - ", Sys.time()))

#Begin loop
for(table in table_list) {
  
  
  from_config <- yaml::read_yaml(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_", table, ".yaml"))
  from_schema <- from_config[[server]][["to_schema"]]
  from_table <- from_config[[server]][["to_table"]]
  qa_schema <- from_config[[server]][["qa_schema"]]
  if(is.null(from_config[[server]][["qa_table_pre"]])) { qa_table <- from_config[[server]][["qa_table"]] }
  else { qa_table <- from_config[[server]][["qa_table_pre"]] }
  to_config <- yaml::read_yaml(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/final/tables/load_final.mcaid_", table, ".yaml"))
  to_schema <- to_config[[server]][["to_schema"]]
  to_table <- to_config[[server]][["to_table"]]
  ext_schema <- to_config[[server]][["from_schema"]]
  ext_table <- to_config[[server]][["from_table"]]
  
  message(paste0("Working on table: ", to_table, " - ", Sys.time()))
  db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
  if(prod == T) {
    server_to <- "hhsaw"
  } else {
    server_to <- "hhsaw_dev"
  }
  suppressMessages(table_duplicate_f(conn_from = dw_inthealth, 
                    conn_to = db_claims, 
                    server_to = server_to, 
                    db_to = "hhs_analytics_workspace",
                    from_schema = from_schema,
                    from_table = from_table,
                    to_schema = to_schema,
                    to_table = to_table,
                    confirm_tables = F,
                    delete_table = T,
                    table_structure_only = T))
  last_run <- DBI::dbGetQuery(conn = dw_inthealth,
                              glue::glue_sql("SELECT MAX(last_run) FROM {`from_schema`}.{`from_table`}",
                                             .con = dw_inthealth))[[1]]
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("execute claims.usp_external_table_load @fromtable = N{ext_table}, @totable = N{to_table};",
                                .con = db_claims))
  
  #Row count comparison for all tables except PLR tables
    inthealth_row_count <- DBI::dbGetQuery(conn = db_claims,
                                           glue::glue_sql("select count(*) as row_count from {`ext_schema`}.{`ext_table`};",
                                                          .con = db_claims))
    hhsaw_row_count <- DBI::dbGetQuery(conn = db_claims,
                                       glue::glue_sql("select count(*) as row_count from {`to_schema`}.{`to_table`};",
                                                      .con = db_claims))
    
  if (inthealth_row_count$row_count == hhsaw_row_count$row_count) {
      DBI::dbExecute(
        conn = db_claims,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({format(last_run, usetz = FALSE)}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                 'Number final rows compared to stage', 
                 'PASS', 
                 {format(Sys.time(), usetz = FALSE)}, 
                 'All rows transferred to final table ({inthealth_row_count$row_count})')",
                       .con = db_claims))
      
      # Track success
      table_fail <- 0
      add_index_f(conn = db_claims, server = server, table_config = to_config)
      
    } else {
      DBI::dbExecute(
        conn = db_claims,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({format(last_run, usetz = FALSE)}, 
                 '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                 'Number final rows compared to stage', 
                 'FAIL', 
                 {format(Sys.time(), usetz = FALSE)}, 
                 '{hhsaw_row_count$row_count} rows in final table (expecting {inthealth_row_count$row_count})')",
                       .con = db_claims))
      
      # Note failure
      stop(glue::glue("Mismatching row count between inthealth_edw external table and HHSAW table."))
    }
  
  message(paste0("Done working on table: ", to_table, " - ", Sys.time()))
}



## Closing message
message(paste0("All tables have been successfully copied to inthealth_edw - ", Sys.time()))




#### DROP TABLES NO LONGER NEEDED ####
bak_check <- dlg_list(c("Yes", "No"), title = "CHECK BACKUP TABLES?")$res
if (bak_check == "Yes") {
  db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  #### QA STAGE TABLE COUNTS AND CHOOSE WHETHER TO DROP BACK UP ARCHIVE TABLES OR NOT ####
  table_config_stage_elig <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml")) 
  table_config_stage_claim <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.yaml"))
  stage_schema <- table_config_stage_elig[[server]][["to_schema"]]
  stage_elig <- ifelse(is.null(table_config_stage_elig[[server]][["to_table"]]), '',
                      table_config_stage_elig[[server]][["to_table"]])
  stage_claim <- ifelse(is.null(table_config_stage_claim[[server]][["to_table"]]), '',
                        table_config_stage_claim[[server]][["to_table"]])
  archive_schema <- table_config_stage_elig[[server]][["archive_schema"]]
  archive_elig <- ifelse(is.null(table_config_stage_elig[[server]][["archive_table"]]), '',
                        table_config_stage_elig[[server]][["archive_table"]])
  archive_claim <- ifelse(is.null(table_config_stage_claim[[server]][["archive_table"]]), '',
                          table_config_stage_claim[[server]][["archive_table"]])
  bak_schema <- table_config_stage_elig[[server]][["archive_schema"]]
  bak_elig <- paste0(ifelse(is.null(table_config_stage_elig[[server]][["archive_table"]]), '',
                            table_config_stage_elig[[server]][["archive_table"]]), '_bak')
  bak_claim <- paste0(ifelse(is.null(table_config_stage_claim[[server]][["archive_table"]]), '',
                            table_config_stage_claim[[server]][["archive_table"]]), '_bak')
  if (server == "hhsaw") {
    conn <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
  } else {
    conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  }
  ## Get row counts of each table ##
  cnt_stage_elig <- DBI::dbGetQuery(conn,
                                    glue::glue_sql("SELECT COUNT(*) FROM {`stage_schema`}.{`stage_elig`}",
                                                   .con = conn))[1,1]
  cnt_archive_elig <- DBI::dbGetQuery(conn,
                                      glue::glue_sql("SELECT COUNT(*) FROM {`archive_schema`}.{`archive_elig`}", 
                                                     .con = conn))[1,1]
  if(DBI::dbExistsTable(conn, DBI::Id( schema = bak_schema, table = bak_elig))) {
    cnt_bak_elig <- DBI::dbGetQuery(conn,
                                    glue::glue_sql("SELECT COUNT(*) FROM {`bak_schema`}.{`bak_elig`}",
                                                   .con = conn))[1,1]
  } else { cnt_bak_elig <- 0 }
  cnt_stage_claim <- DBI::dbGetQuery(conn,
                                     glue::glue_sql("SELECT COUNT(*) FROM {`stage_schema`}.{`stage_claim`}",
                                                    .con = conn))[1,1]
  cnt_archive_claim <- DBI::dbGetQuery(conn,
                                       glue::glue_sql("SELECT COUNT(*) FROM {`archive_schema`}.{`archive_claim`}", 
                                                      .con = conn))[1,1]
  if(DBI::dbExistsTable(conn, DBI::Id( schema = bak_schema, table = bak_claim))) {
    try(cnt_bak_claim <- DBI::dbGetQuery(conn,
                                     glue::glue_sql("SELECT COUNT(*) FROM {`bak_schema`}.{`bak_claim`}",
                                                    .con = conn))[1,1])
  } else { cnt_bak_claim <- 0 }
  
  ## Compare row counts between tables ##
  if (cnt_stage_elig >= cnt_archive_elig & cnt_archive_elig >= cnt_bak_elig) {
    message("No issues with Elig stage, archive and bak table counts.")
  } else {
    message("Potential issue with Elig stage, archive and bak table counts:")
  }
  message(paste0("Stage: ", cnt_stage_elig))
  message(paste0("Archive: ", cnt_archive_elig))
  message(paste0("Bak: ", cnt_bak_elig))
  if (cnt_stage_claim >= cnt_archive_claim & cnt_archive_claim >= cnt_bak_claim) {
    message("No issues with Claims stage, archive and bak table counts.")
  } else {
    message("Potential issue with Claims stage, archive and bak table counts:")
  }  
  message(paste0("Stage: ", cnt_stage_claim))
  message(paste0("Archive: ", cnt_archive_claim))
  message(paste0("Bak: ", cnt_bak_claim))
  
  ## Ask to delete backup archive tables ##
  bak_del <- dlg_list(c("Yes", "No"), title = "DELETE BACK UP ARCHIVE TABLES?")$res
  if (bak_del == "Yes") {
    try(DBI::dbSendQuery(conn,
                         glue::glue_sql("DROP TABLE {`bak_schema`}.{`bak_elig`}",
                                        .con = conn)), silent = T)
    try(DBI::dbSendQuery(conn,      
                         glue::glue_sql("DROP TABLE {`bak_schema`}.{`bak_claim`}",  
                                        .con = conn)), silent = T)
  }
  rm(conn, table_config_stage_elig, table_config_stage_claim, 
     bak_schema, bak_elig, bak_claim, bak_del,
     stage_schema, stage_claim, stage_elig, cnt_stage_claim, cnt_stage_elig,
     archive_schema, archive_claim, archive_elig, cnt_archive_claim, cnt_archive_elig,
     cnt_bak_claim, cnt_bak_elig)
}

send_email <- dlg_list(c("Yes", "No"), title = "SEND COMPLETION EMAIL?")$res
if(send_email == "Yes") {
  if (server == "phclaims") {
    schema <- "metadata"
    table <- "etl_log"
  } else if (server == "hhsaw") {
    schema <- "claims"
    table <- "metadata_etl_log"
  }
  db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  etl <- DBI::dbGetQuery(db_claims, 
                         glue::glue_sql("SELECT TOP (1) *
                                        FROM {`schema`}.{`table`}
                                        WHERE [date_load_raw] IS NOT NULL
                                        ORDER BY [date_load_raw] DESC",
                                        .con = db_claims))
  etl$server <- server
  vars <- etl
  apde_notify_f(msg_name = "claims_mcaid_update",
                vars = vars)
}


