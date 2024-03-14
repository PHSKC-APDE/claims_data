# This code QAs the stage mcaid bh table
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-08-12
# Alastair Matheson, adapted from Eli Kern's SQL script


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded
# skip_review = if you do not want to manually review comparison to APCD estimates
#  (set to T because it holds up automated monthly runs)


qa_stage_mcaid_claim_bh_f <- function(conn = NULL,
                                       server = c("hhsaw", "phclaims"),
                                       config = NULL,
                                       get_config = F,
                                       skip_review = T) {
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  final_schema <- config[[server]][["final_schema"]]
  final_table <- ifelse(is.null(config[[server]][["final_table"]]), '',
                        config[[server]][["final_table"]])
  final_table_pre <- ifelse(is.null(config[[server]][["final_table_pre"]]), '',
                            config[[server]][["final_table_pre"]])
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table_pre <- ifelse(is.null(config[[server]][["qa_table_pre"]]), '',
                         config[[server]][["qa_table_pre"]])
  
  
  message("Running QA on ", to_schema, ".", to_table)
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  last_run <- as.POSIXct(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}",
                         .con = conn))[[1]])
  
  #### SET UP EMPTY DATA FRAME TO TRACK RESULTS ####
  bh_qa <- data.frame(etl_batch_id = integer(),
                       last_run = as.Date(character()),
                       table_name = character(),
                       qa_item = character(),
                       qa_result = character(),
                       qa_date = as.Date(character()),
                       note = character())
  
  
  
  #### STEP 1: TABLE-WIDE CHECKS ####
  
  #### COUNT # CONDITIONS RUN ####
  distinct_cond <- as.integer(dbGetQuery(
    conn,
    glue::glue_sql("SELECT count(distinct bh_cond) as cond_count FROM {`to_schema`}.{`to_table`}",
                   .con = conn)))
  
  # See how many are in the final table
  distinct_cond_final <- as.integer(dbGetQuery(
    conn,
    glue::glue_sql("SELECT count(distinct bh_cond) as cond_count FROM {`final_schema`}.{`final_table`}",
                   .con = conn)))
  
  if (distinct_cond >= distinct_cond_final) {
    bh_qa <- rbind(bh_qa,
                    data.frame(etl_batch_id = NA_integer_,
                               last_run = last_run,
                               table_name = paste0(to_schema, ".", to_table),
                               qa_item = "# distinct conditions",
                               qa_result = "PASS",
                               qa_date = Sys.time(),
                               note = glue("There were {distinct_cond} conditions analyzed")))
  } else {
    bh_qa <- rbind(bh_qa,
                    data.frame(etl_batch_id = NA_integer_,
                               last_run = last_run,
                               table_name = paste0(to_schema, ".", to_table),
                               qa_item = "# distinct conditions",
                               qa_result = "FAIL",
                               qa_date = Sys.time(),
                               note = glue("There were {distinct_cond} conditions analyzed, but there are ",
                                           "{distinct_cond_final} conditions in the final table")))
  }
  
  
  #### COUNT NUMBER + PERCENT OF DISTINCT PEOPLE BY CONDITION ####
  distinct_id_bh <- dbGetQuery(
    conn,
    glue::glue_sql("SELECT bh_cond, count(distinct id_mcaid) as id_dcount
                 FROM {`to_schema`}.{`to_table`}
                 WHERE year(from_date) <= 2017 and year(to_date) >= 2017 
                 GROUP BY bh_cond
                 ORDER BY bh_cond",
                   .con = conn))
  
  distinct_id_pop <- as.integer(dbGetQuery(
    conn,
    glue::glue_sql("SELECT count(distinct id_mcaid) as id_dcount
                 FROM {`final_schema`}.{DBI::SQL(final_table_pre)}mcaid_elig_timevar
                 WHERE year(from_date) <= 2017 and year(to_date) >= 2017",
                   .con = conn)))
  
  
  distinct_id_chk <- distinct_id_bh %>%
    mutate(prop = id_dcount / distinct_id_pop * 100)

 
  # Show results for review
  print(distinct_id_chk)
  

  
  #### STEP 3: LOAD QA RESULTS TO SQL AND RETURN RESULT ####
  DBI::dbExecute(
    conn, 
    glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table_pre)}qa_mcaid 
                   (etl_batch_id, last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES 
                   {DBI::SQL(glue_collapse(
                     glue_data_sql(bh_qa, 
                                   '({etl_batch_id}, {last_run}, {table_name}, {qa_item}, 
                                   {qa_result}, {qa_date}, {note})', 
                                   .con = conn), 
                     sep = ', ')
                   )};",
                   .con = conn))
  
  
  
  message(glue::glue("QA of stage.mcaid_claim_bh complete. Result: {min(bh_qa$qa_result)}"))
  return(0)
}
