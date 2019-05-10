#### CODE TO CREATE STAGE MCAID ELIG TABLE
# Full refresh version
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170)

library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue)

db_claims <- dbConnect(odbc(), "PHClaims51")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")


#### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
current_batch_id <- as.numeric(odbc::dbGetQuery(db_claims,
                                     "SELECT MAX(etl_batch_id) FROM load_raw.mcaid_elig"))


#### CREATE TABLE ####
# Note this is only used because this script is for a full refresh
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_elig.yaml",
               overall = T, ind_yr = F)


#### LOAD TABLE ####
# Can't use default load function because some transformation is needed

### Call in config file to get vars
table_config <- yaml::yaml.load(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_full.yaml"
))

from_schema_name <- table_config$from_schema
from_table_name <- table_config$from_table
to_schema_name <- table_config$to_schema
to_table_name <- table_config$to_table
vars <- unlist(table_config$vars)
# Need to specify which temp table the vars come from
# Can't handle this just with glue_sql
# (see https://community.rstudio.com/t/using-glue-sql-s-collapse-with-table-name-identifiers/11633)
var_names <- lapply(table_config$vars, 
                    function(nme) DBI::Id(table = "b", column = nme))
vars_dedup <- lapply(var_names, DBI::dbQuoteIdentifier, conn = db_claims)


### Set up temporary table
# This can then be used to deduplicate rows with differing end reasons
# Remove temp table if it exists
try(dbRemoveTable(db_claims, "##mcaid_elig", temporary = T))

odbc::dbGetQuery(db_claims,
                 glue::glue_sql("SELECT {`vars`*}, 
                                CASE WHEN END_REASON IS NULL THEN 1 
                                  WHEN END_REASON = 'Other' THEN 2 
                                  WHEN END_REASON = 'Other - For User Generation Only' THEN 3 
                                  WHEN END_REASON = 'Review Not Complete' THEN 4 
                                  WHEN END_REASON = 'No Eligible Household Members' THEN 5 
                                  WHEN END_REASON = 'Already Eligible for Program in Different AU' THEN 6 
                                  ELSE 7 END AS reason_score
                                INTO ##mcaid_elig 
                                FROM {`from_schema_name`}.{`from_table_name`}",
                                .con = db_claims))

### Manipulate the temporary table to deduplicate and then insert into stage
dedup_sql <- glue::glue_sql(
  "INSERT INTO {`to_schema_name`}.{`to_table_name`} WITH (TABLOCK)
  SELECT {`vars_dedup`*} FROM
    (SELECT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
      TO_DATE, SECONDARY_RAC_CODE, MAX(reason_score) AS max_score 
      FROM ##mcaid_elig
      GROUP BY CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
        TO_DATE, SECONDARY_RAC_CODE) a
  LEFT JOIN
    (SELECT * FROM ##mcaid_elig) b
  ON a.CLNDR_YEAR_MNTH = b.CLNDR_YEAR_MNTH AND 
    a.MEDICAID_RECIPIENT_ID = b.MEDICAID_RECIPIENT_ID AND 
    a.FROM_DATE = b.FROM_DATE AND a.TO_DATE = b.TO_DATE AND 
    (a.SECONDARY_RAC_CODE = b.SECONDARY_RAC_CODE OR 
      a.SECONDARY_RAC_CODE IS NULL AND b.SECONDARY_RAC_CODE IS NULL) AND 
    a.max_score = b.reason_score",
    .con = db_claims)

odbc::dbGetQuery(db_claims, dedup_sql)


#### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
# Because of deduplication, should be 42 less than load_raw table
rows_stage <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.mcaid_elig"))
rows_load_raw <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM load_raw.mcaid_elig"))

if (rows_load_raw - rows_stage != 42) {
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Rows passed from load_raw to stage', 
                                  'FAIL',
                                  {Sys.Date()},
                                  'Issue even after accoutning for the 42 people with duplicate rows. Investigate further.')",
                                  .con = db_claims))
  stop("Number of distinct rows does not match total expected")
  } else {
    odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Rows passed from load_raw to stage', 
                                  'PASS',
                                  {Sys.Date()},
                                  'Number of rows in stage matches load_raw (minus deduplicated end_reason rows)')",
                                  .con = db_claims))
    }


#### CLEAN UP ####
rm(dedup_sql)
rm(vars, var_names, vars_dedup)
rm(from_schema_name, from_table_name, to_schema_name, to_table_name)
rm(rows_stage, rows_load_raw)
rm(table_config)
rm(list = ls(pattern = "_f$"))

