#### CODE TO CREATE STAGE MCAID ELIG TABLE
# Monthly refresh version
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08

### Run from master_mcaid_full script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_monthly.R


#### CALL IN CONFIG FILE TO GET VARS ####
table_config_stage_elig <- yaml::yaml.load(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_monthly.yaml"
))

from_schema <- table_config_stage_elig$from_schema
from_table <- table_config_stage_elig$from_table
to_schema <- table_config_stage_elig$to_schema
to_table <- table_config_stage_elig$to_table
vars <- unlist(names(table_config_stage_elig$vars))
# Need to specify which temp table the vars come from
# Can't handle this just with glue_sql
# (see https://community.rstudio.com/t/using-glue-sql-s-collapse-with-table-name-identifiers/11633)
var_names <- lapply(table_config_stage_elig$vars, 
                    function(nme) DBI::Id(table = "a", column = nme))
vars_dedup <- lapply(var_names, DBI::dbQuoteIdentifier, conn = db_claims)


#### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
current_batch_id <- as.numeric(odbc::dbGetQuery(db_claims,
                                     glue::glue_sql("SELECT MAX(etl_batch_id) FROM {`from_schema`}.{`from_table`}",
                                                    .con = db_claims)))

if (is.na(current_batch_id)) {
  stop(glue::glue_sql("Missing etl_batch_id in {`from_schema`}.{`from_table`}"))
}



##### NOTE ######
# Can't use default load function because some transformation is needed
# Need to deduplicate rows (n=1) where there were two, differing, end reasons for a given month and RAC.
# This shouldn't be an issue after 2018-08 data are no longer being replaced


#### ARCHIVE EXISTING TABLE ####
# Check it exists first
if (dbExistsTable(db_claims, DBI::Id(schema = "archive", table = to_table)) == F) {
  DBI::dbCreateTable(db_claims, name = DBI::Id(schema = "archive", table = to_table), 
                     fields = table_config_stage_elig$vars)
}

sql_archive <- glue::glue_sql("INSERT INTO archive.{`to_table`} WITH (TABLOCK)  ({`vars`*}) 
                                SELECT {`vars`*} FROM 
                                {`to_schema`}.{`to_table`}", .con = db_claims,
                              archive_rows = DBI::SQL(archive_rows))

odbc::dbGetQuery(db_claims, sql_archive)


# Check that the full number of rows are in the acrhive table
archive_row_cnt <- as.numeric(odbc::dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM archive.{`to_table`}", .con = db_claims)))
stage_row_cnt <- as.numeric(odbc::dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = db_claims)))

if (archive_row_cnt != stage_row_cnt) {
  stop(glue("The number of rows differ between archive and {`to_schema`} schemas"))
}


#### TRUNCATE EXISTING TABLE ####
dbGetQuery(db_claims, glue::glue_sql("TRUNCATE TABLE {`to_schema`}.{`to_table`}", .con = db_claims))

# This code pulls out the clustered index name
index_sql <- glue::glue_sql("SELECT DISTINCT a.index_name
                                  FROM
                                  (SELECT ind.name AS index_name
                                  FROM
                                  (SELECT object_id, name, type_desc FROM sys.indexes
                                  WHERE type_desc = 'CLUSTERED') ind
                                  INNER JOIN
                                  (SELECT name, schema_id, object_id FROM sys.tables
                                  WHERE name = {`table`}) t
                                  ON ind.object_id = t.object_id
                                  INNER JOIN
                                  (SELECT name, schema_id FROM sys.schemas
                                  WHERE name = {`schema`}) s
                                  ON t.schema_id = s.schema_id
                                  ) a", .con = db_claims,
                            table = dbQuoteString(db_claims, to_table),
                            schema = dbQuoteString(db_claims, to_schema))


index_name <- dbGetQuery(db_claims, index_sql)[[1]]

if (length(index_name) != 0) {
  dbGetQuery(db_claims,
             glue::glue_sql("DROP INDEX {`index_name`} ON 
                                {`to_schema`}.{`to_table`}", .con = db_claims))
}


#### LOAD TABLE ####
# Use priority set out below (higher resaon score = higher priority)
### Set up temporary table
print("Setting up a temp table to remove duplicate rows")
# This can then be used to deduplicate rows with differing end reasons
# Remove temp table if it exists
try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_temp", temporary = T))

odbc::dbGetQuery(db_claims,
                 glue::glue_sql("SELECT {`vars`*}, 
                                CASE WHEN END_REASON IS NULL THEN 1 
                                  WHEN END_REASON = 'Other' THEN 2 
                                  WHEN END_REASON = 'Other - For User Generation Only' THEN 3 
                                  WHEN END_REASON = 'Review Not Complete' THEN 4 
                                  WHEN END_REASON = 'No Eligible Household Members' THEN 5 
                                  WHEN END_REASON = 'Already Eligible for Program in Different AU' THEN 6 
                                  ELSE 7 END AS reason_score
                                INTO ##mcaid_elig_temp
                                FROM {`from_schema`}.{`from_table`}",
                                .con = db_claims))


### Manipulate the temporary table to deduplicate
# Remove temp table if it exists
try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_dedup", temporary = T))

dedup_sql <- glue::glue_sql(
  'SELECT {`vars_dedup`*} 
  INTO ##mcaid_elig_dedup 
  FROM
    (SELECT {`vars`*}, reason_score FROM ##mcaid_elig_temp) a
  LEFT JOIN
    (SELECT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
      TO_DATE, SECONDARY_RAC_CODE, MAX(reason_score) AS max_score 
    FROM ##mcaid_elig_temp
    GROUP BY CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
    TO_DATE, SECONDARY_RAC_CODE) b
  ON a.CLNDR_YEAR_MNTH = b.CLNDR_YEAR_MNTH AND 
    a.MEDICAID_RECIPIENT_ID = b.MEDICAID_RECIPIENT_ID AND 
    (a.FROM_DATE = b.FROM_DATE OR (a.FROM_DATE IS NULL AND b.FROM_DATE IS NULL)) AND 
    (a.TO_DATE = b.TO_DATE OR (a.TO_DATE IS NULL AND b.TO_DATE IS NULL)) AND 
    (a.SECONDARY_RAC_CODE = b.SECONDARY_RAC_CODE OR 
      a.SECONDARY_RAC_CODE IS NULL AND b.SECONDARY_RAC_CODE IS NULL) 
  WHERE a.reason_score = b.max_score',
    .con = db_claims)

odbc::dbGetQuery(db_claims, dedup_sql)


### Combine relevant part of archive table and deduplicated temp data
sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                  SELECT {`vars`*} FROM 
                                  archive.{`to_table`}
                                  WHERE {`date_var`} < {date_truncate}  
                                  UNION 
                                  SELECT {`vars`*} FROM 
                                  ##mcaid_elig_dedup
                                  WHERE {`date_var`} >= {date_truncate}",
                              .con = db_claims,
                              date_var = table_config_stage_elig$date_var,
                              date_truncate = as.character(table_config_stage_elig$date_truncate))

odbc::dbGetQuery(db_claims, sql_combine)



#### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
print("Running QA checks")
# Because of deduplication, should be 1 less than load_raw table + archive before date cutoff
rows_stage <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = db_claims)))
rows_load_raw <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", .con = db_claims)))
rows_archive <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM archive.{`to_table`} 
                            WHERE {`table_config_stage_elig$date_var`} < {as.character(table_config_stage_elig$date_truncate)}", .con = db_claims)))

if ((rows_archive + rows_load_raw) - rows_stage != 1) {
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Rows passed from load_raw to stage', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Issue even after accounting for the 1 duplicate row. Investigate further.')",
                                  .con = db_claims))
  stop("Number of distinct rows does not match total expected")
  } else {
    odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Rows passed from load_raw AND archive to stage', 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of rows in stage matches load_raw and archive (minus deduplicated end_reason rows) (n = {rows_stage})')",
                                  .con = db_claims))
    }


#### QA CHECK: NULL IDs ####
null_ids <- as.numeric(dbGetQuery(db_claims, 
                                    "SELECT COUNT (*) FROM stage.mcaid_elig 
                                    WHERE MEDICAID_RECIPIENT_ID IS NULL"))

if (null_ids != 0) {
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Null Medicaid IDs', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Null IDs found. Investigate further.')",
                                  .con = db_claims))
  stop("Null Medicaid IDs found in stage.mcaid_elig")
} else {
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Null Medicaid IDs', 
                                  'PASS',
                                  {Sys.time()},
                                  'No null IDs found')",
                                  .con = db_claims))
}


#### ADD INDEX ####
print("Adding index")
if (!is.null(table_config_stage_elig$index_name)) {
  index_sql <- glue::glue_sql("CREATE CLUSTERED INDEX [{`table_config_stage_elig$index_name`}] ON 
                              {`to_schema`}.{`to_table`}({index_vars*})",
                              index_vars = dbQuoteIdentifier(db_claims, table_config_stage_elig$index),
                              .con = db_claims)
  dbGetQuery(db_claims, index_sql)
}


#### ADD VALUES TO QA_VALUES TABLE ####
odbc::dbGetQuery(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('stage.mcaid_elig',
                   'row_count', 
                   '{rows_stage}', 
                   {Sys.time()}, 
                   'Count after full refresh')",
                 .con = db_claims))


#### CLEAN UP ####
# Drop global temp table
try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_temp", temporary = T))
try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_dedup", temporary = T))
rm(dedup_sql)
rm(vars, var_names, vars_dedup)
rm(from_schema, from_table, to_schema, to_table)
rm(rows_stage, rows_load_raw, rows_archive, null_ids)
rm(table_config_stage_elig)
rm(sql_combine, sql_archive, index_sql, index_name)

