#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_BCARRIER_LINE
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_bcarrier_line_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.mcare_bcarrier_line_qa_f <- function() {
  
  #load expected counts from YAML file
  table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  
  #confirm that row counts match expected
  row_sum_union <- dbGetQuery(conn = db_claims, glue_sql(
    "select count(*) as qa from PHClaims.stage.mcare_bcarrier_line_load;",
    .con = db_claims))
  
  if(table_config$row_count_expected == row_sum_union$qa) {
    print("row sums match")
  }
  if(table_config$row_count_expected > row_sum_union$qa) {
    print(paste0("union row sum more, single-year sum: ", row_sum_single$qa, " , union sum: ", row_sum_union$qa))
  }
  if(table_config$row_count_expected < row_sum_union$qa) {
    print(paste0("union row sum less, single-year sum: ", row_sum_single$qa, " , union sum: ", row_sum_union$qa))
  }
  
  #confirm that col count matches expected
  col_count <- dbGetQuery(conn = db_claims, glue_sql(
    " select count(*) as col_cnt
      from information_schema.columns
      where table_catalog = 'PHClaims' -- the database
      and table_schema = 'stage'
      and table_name = 'mcare_' + 'bcarrier_line_load';",
    .con = db_claims))
  
  if(table_config$col_count_expected == col_count$col_cnt) {
    print("col count matches expected")
  } else {
    print("col count does not match!!")
  }
}