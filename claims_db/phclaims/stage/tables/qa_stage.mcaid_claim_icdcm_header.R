
# This code QAs table [stage].[mcaid_claim_icdcm_header]
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-12
# Alastair Matheson (building on SQL from Philip Sylling)
#
# QA checks:
# 1) IDs are all found in the elig tables
# 2) ICD-9-CM and ICD-10-CM codes are an appropriate length
# 3) icdcm_number falls in an acceptable range
# 4) (AlmosT) All dx codes are found in the ref table
# 5) Check there were as many or more diagnoses for each calendar year
# 6) [Not yet added - need a threshold for failure] Proprtion of IDs in claim header table with a dx


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded


qa_stage_mcaid_claim_icdcm_header_f <- function(conn = NULL,
                                        server = c("hhsaw", "phclaims"),
                                        config = NULL,
                                        get_config = F) {
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  final_schema <- config[[server]][["final_schema"]]
  final_table <- ifelse(is.null(config[[server]][["final_table"]]), '',
                        config[[server]][["final_table"]])
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- ifelse(is.null(config[[server]][["ref_table"]]), '',
                      config[[server]][["ref_table"]])
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                     config[[server]][["qa_table"]])
  icdcm_ref_schema <- config[[server]][["icdcm_ref_schema"]]
  icdcm_ref_table <- ifelse(is.null(config[[server]][["icdcm_ref_table"]]), '',
                     config[[server]][["icdcm_ref_table"]])
  
  
  message("Running QA on ", to_schema, ".", to_table)
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  last_run <- as.POSIXct(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}",
                         .con = conn))[[1]])
  
  
  #### Check all IDs are also found in the elig_demo and time_var tables ####
  ids_demo_chk <- as.integer(DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
                         FROM {`to_schema`}.{`to_table`} AS a
                         LEFT JOIN {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_demo AS b
                         ON a.id_mcaid = b.id_mcaid
                         WHERE b.id_mcaid IS NULL",
                         .con = conn)))
  
  ids_timevar_chk <- as.integer(DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT a.id_mcaid) AS cnt_id
                         FROM {`to_schema`}.{`to_table`} AS a
                         LEFT JOIN {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_timevar AS b
                         ON a.id_mcaid = b.id_mcaid
                         WHERE b.id_mcaid IS NULL",
                         .con = conn)))
  
  # Write findings to metadata
  if (ids_demo_chk == 0 & ids_timevar_chk == 0) {
    ids_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Distinct IDs compared to elig tables', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were the same number of IDs as in the final mcaid_elig_demo ", 
                                  "and mcaid_elig_timevar tables')",
                                  .con = conn))
  } else {
    ids_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Distinct IDs compared to elig tables', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {ids_demo_chk} {DBI::SQL(ifelse(ids_demo_chk >= 0, 'more', 'fewer'))} ",
                                  "IDs than in the final mcaid_elig_demo table and ", 
                                  "{ids_timevar_chk} {DBI::SQL(ifelse(ids_timevar_chk >= 0, 'more', 'fewer'))} ", 
                                  "IDs than in the final mcaid_elig_timevar table')",
                                  .con = conn))
  }
  
  
  #### Check length of ICD codes ####
  # ICD-9-CM should be 5
  # ICD-10-CM should be 3-7
  
  ### ICD-9-CM
  icd9_len_chk <- DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT MIN(LEN(icdcm_norm)) as min_len, MAX(LEN(icdcm_norm)) as max_len 
                         FROM {`to_schema`}.{`to_table`} WHERE icdcm_version = 9",
                         .con = conn))
  
  # Write findings to metadata
  if (icd9_len_chk$min_len == 5 & icd9_len_chk$max_len == 5) {
    icd9_len_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Length of ICD-9-CM codes', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The ICD-9-CM codes were all 5 characters in length')",
                                  .con = conn))
  } else {
    icd9_len_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Length of ICD-9-CM codes', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The ICD-9-CM codes ranged from {icd9_len_chk$min_len} to ",
                                  "{icd9_len_chk$max_len} characters in length (should be all 5)')",
                                  .con = conn))
  }
  
  ### ICD-10-CM
  icd10_len_chk <- DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT MIN(LEN(icdcm_norm)) as min_len, MAX(LEN(icdcm_norm)) as max_len 
                         FROM {`to_schema`}.{`to_table`} WHERE icdcm_version = 10",
                         .con = conn))
  
  # Write findings to metadata
  if (icd10_len_chk$min_len == 3 & icd10_len_chk$max_len == 7) {
    icd10_len_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Length of ICD-10-CM codes', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The ICD-10-CM codes ranged from {icd10_len_chk$min_len} to ",
                                  "{icd10_len_chk$max_len} characters in length, as expected')",
                                  .con = conn))
  } else {
    icd10_len_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Length of ICD-10-CM codes', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'The ICD-10-CM codes ranged from {icd10_len_chk$min_len} to ",
                                  "{icd10_len_chk$max_len} characters in length (should be 3-7)')",
                                  .con = conn))
  }
  
  
  #### Check that icdcm_number in ('01':'12','admit') ####
  icdcm_num_chk <- as.integer(
    DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT count(icdcm_number) FROM {`to_schema`}.{`to_table`}
                           WHERE icdcm_number not in 
                           ('01','02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', 'admit')",
                           .con = conn)))
  
  # Write findings to metadata
  if (icdcm_num_chk == 0) {
    icdcm_num_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'icdcm_number = 01-12 or admit', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'All icdcm_number values were 01:12 or admit')",
                                  .con = conn))
  } else {
    icdcm_num_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'icdcm_number = 01-12 or admit', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {icdcm_num_chk} icdcm_number values not 01 through 12 or admit')",
                                  .con = conn))
  }
  
  
  #### Check if any diagnosis codes do not join to ICD-CM reference table ####
  dx_chk <- as.integer(DBI::dbGetQuery(
    conn, glue::glue_sql("SELECT count(distinct 'ICD' + CAST([icdcm_version] AS VARCHAR(2)) + ' - ' + [icdcm_norm])
                         FROM {`to_schema`}.{`to_table`} as a
                         WHERE not exists
                         (SELECT 1 FROM {`icdcm_ref_schema`}.{`icdcm_ref_table`} as b
                         WHERE a.icdcm_version = b.icdcm_version and a.icdcm_norm = b.icdcm)",
                         .con = conn)))
  
  # Write findings to metadata
  if (dx_chk < 350) {
    dx_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Almost all dx codes join to ICD-CM reference table', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {dx_chk} dx values not in {DBI::SQL(icdcm_ref_schema)}.{DBI::SQL(icdcm_ref_table)} (acceptable is < 350)')",
                                  .con = conn))
  } else {
    dx_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Almost all dx codes join to ICD-CM reference table', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {dx_chk} dx values not in {DBI::SQL(icdcm_ref_schema)}.{DBI::SQL(icdcm_ref_table)} table (acceptable is < 350)')",
                                  .con = conn))
  }
  
  
  #### Compare number of dx codes in current vs. prior analytic tables ####
  if (DBI::dbExistsTable(conn,
                         DBI::Id(schema = final_schema, table = paste0(final_table, "mcaid_claim_icdcm_header")))) {
    
    num_dx_current <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS current_num_dx
                           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header
                           GROUP BY YEAR(first_service_date) ORDER BY YEAR(first_service_date)",
                           .con = conn))
    
    num_dx_new <- DBI::dbGetQuery(
      conn, glue::glue_sql("SELECT YEAR(first_service_date) AS claim_year, COUNT(*) AS new_num_dx
                         FROM {`to_schema`}.{`to_table`}
                         GROUP BY YEAR(first_service_date) ORDER by YEAR(first_service_date)", .con = conn))
    
    num_dx_overall <- left_join(num_dx_new, num_dx_current, by = "claim_year") %>%
      mutate_at(vars(new_num_dx, current_num_dx), list(~ replace_na(., 0))) %>%
      mutate(pct_change = round((new_num_dx - current_num_dx) / current_num_dx * 100, 4))
    
    # Write findings to metadata
    if (max(num_dx_overall$pct_change, na.rm = T) > 0 & 
        min(num_dx_overall$pct_change, na.rm = T) >= 0) {
      num_dx_fail <- 0
      DBI::dbExecute(conn = conn, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of diagnoses', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                 'The following years had more diagnoses than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_dx_overall$claim_year[num_dx_overall$pct_change > 0], 
                                            pct = round(abs(num_dx_overall$pct_change[num_dx_overall$pct_change > 0]), 2)),
                                 '{year} ({pct}% more)'), sep = ', ', last = ' and '))}')",
                                    .con = conn))
    } else if (min(num_dx_overall$pct_change, na.rm = T) + max(num_dx_overall$pct_change, na.rm = T) == 0) {
      num_dx_fail <- 1
      DBI::dbExecute(conn = conn, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of diagnoses', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'No change in the number of diagnoses compared to final schema table')",
                                    .con = conn))
    } else if (min(num_dx_overall$pct_change, na.rm = T) < 0) {
      num_dx_fail <- 1
      DBI::dbExecute(conn = conn, 
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Change in number of diagnoses', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                 'The following years had fewer diagnoses than in the final schema table: ", 
                                    "{DBI::SQL(glue::glue_collapse(
                 glue::glue_data(data.frame(year = num_dx_overall$claim_year[num_dx_overall$pct_change < 0], 
                                            pct = round(abs(num_dx_overall$pct_change[num_dx_overall$pct_change < 0]), 2)),
                                 '{year} ({pct}% fewer)'), sep = ', ', last = ' and '))}')",
                                    .con = conn))
    }
  } else {
    num_dx_fail <- 0
  }
  
  
  #### Check the proportion of people in the claim_header table who have a dx ####
  # Not yet implemented, need to adapt SQL code and adopt a threshold
  # --Compare number of people with claim_header table
  # set @pct_claim_header_id_with_dx = 
  #   (
  #     select
  #     cast((select count(distinct id_mcaid) as id_dcount
  #           from [stage].[mcaid_claim_icdcm_header]) as numeric) /
  #       (select count(distinct id_mcaid) as id_dcount
  #        from [stage].[mcaid_claim_header])
  #   );
  # 
  # insert into [metadata].[qa_mcaid]
  # select 
  # NULL
  # ,@last_run
  # ,'claims.stage_mcaid_claim_icdcm_header'
  # ,'Compare number of people with claim_header table'
  # ,NULL
  # ,getdate()
  # ,@pct_claim_header_id_with_dx + ' proportion of members with a claim header have a dx';
  
  
  #### SUM UP FAILURES ####
  fail_tot <- sum(ids_fail, icd9_len_fail, icd10_len_fail, icdcm_num_fail,
                  dx_fail, num_dx_fail)
  return(fail_tot)
}