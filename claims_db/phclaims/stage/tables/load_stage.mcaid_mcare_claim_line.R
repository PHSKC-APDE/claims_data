#### CODE TO LOAD & TABLE-LEVEL QA STAGE.mcaid_mcare_claim_line
# Eli Kern, PHSKC and Alastair Matheson (APDE)
#
# 2019-12, major update 2021-05

### Run from master_mcaid_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcare/master_mcaid_mcare_full_union.R

# Function has the following options
# conn = ODBC connection object
# config = Already loaded YAML config file
# config_url = URL to load a YAML config file
# config_file = Path to load a YAML config file.
# mcaid = Update Medicare data. Default is TRUE.
# mcare = Update Medicare data. Default is FALSE because this data changes so infrequently.
# mcaid_date = Year and month from which to load new rows. Format is YYYY-MM-DD. Default is NULL, in which case all rows are loaded.
# mcare_date = Year from which to load new rows. Format is YYYY. Default is NULL, in which case all rows are loaded.
#
# NOTE: Until we develop a system of permanent APDE IDs, any partial load requires all IDs to be updated


#### Load script ####
load_stage.mcaid_mcare_claim_line_f <- function(conn = NULL,
                                                config = NULL,
                                                config_url = NULL,
                                                config_file = NULL,
                                                mcaid = T,
                                                mcare = F,
                                                mcaid_date = NULL,
                                                mcare_date = NULL) {
  
  
  #### ERROR CHECKS ####
  # Check if the config provided is a local object, file, or on a web page
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a local config object, config_url, or config_file but only one")
  }
  
  if (!between(mcaid_date, as.Date("2012-01-01", format = "%Y-%m-%d"), 
               as.Date("2029-12-31", format - "%Y-%m-%d"))) {
    stop("mcaid_date should have YYYY-MM-DD format and be >= 2012-01-01")
  }
  if (!between(mcare_date, 2012, 2029)) {
    stop("mcare_date should have YYYY format and be >= 2012")
  }
  
  
  #### READ IN CONFIG FILE ####
  # NOTE: The mcaid_mcare YAML files still have the older structure (no server, etc.)
  # If the format is updated, need to add in more robust code here to identify schema and table names
  
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  #### SET UP DATES ####
  if (!is.null(mcaid_date)) {
    mcaid_date_sql <- glue::glue_sql(" WHERE first_service_date >= {mcaid_date} ")
  } else  {
    mcaid_date_sql <- DBI::SQL("")
  }
  
  
  #### UPDATE EXISTING IDS ####
  # Only need to do this if we are only loading partial data
  id_update <- glue::glue_sql("UPDATE {table_config$to_schema}.{table_config$to_table} 
                              SET id_apde = y.id_apde
                              FROM 
                              {table_config$to_schema}.{table_config$to_table} a
                              LEFT JOIN
                              {table_config$xwalk_schema_old}.{table_config$xwalk_table} x
                              ON a.id_apde = x.id_apde
                              LEFT JOIN
                              {table_config$xwalk_schema_new}.{table_config$xwalk_table} y
                              ON (x.id_mcaid = y.id_mcaid AND x.id_mcare IS NULL AND y.id_mcare IS NULL) OR 
                              (x.id_mcare = y.id_mcare AND x.id_mcaid IS NULL AND y.id_mcaid IS NULL) OR
                              (x.id_mcaid = y.id_mcaid AND x.id_mcare = y.id_mcare)",
                              .con = conn)
  
  
  #### Medicaid SQL ####
  mcaid_sql <- glue::glue_sql("select
                              --top 100
                              b.id_apde
                              ,'mcaid' as source_desc
                              ,cast(a.claim_header_id as varchar(255)) --because mcare uses alpha characters
                              ,a.claim_line_id
                              ,a.first_service_date
                              ,a.last_service_date
                              ,a.rev_code as revenue_code
                              ,place_of_service_code = null
                              ,type_of_service = null
                              ,a.rac_code_line
                              ,filetype_mcare = null
                              ,getdate() as last_run
                              from PHClaims.final.mcaid_claim_line as a
                              left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
                              on a.id_mcaid = b.id_mcaid")
  
  ### Run SQL query
  odbc::dbGetQuery(conn, glue::glue_sql(
    "--Code to load data to stage.mcaid_mcare_claim_line
    --Union of mcaid and mcare claim line tables
    --Eli Kern (PHSKC-APDE)
    --2020-02
    --Run time: X min
    
    -------------------
    --STEP 1: Union mcaid and mcare tables and insert into table shell
    -------------------
    insert into PHClaims.stage.mcaid_mcare_claim_line with (tablock)
    
    --Medicaid claim ICD-CM header
    select
    --top 100
    b.id_apde
    ,'mcaid' as source_desc
    ,cast(a.claim_header_id as varchar(255)) --because mcare uses alpha characters
    ,a.claim_line_id
    ,a.first_service_date
    ,a.last_service_date
    ,a.rev_code as revenue_code
    ,place_of_service_code = null
    ,type_of_service = null
    ,a.rac_code_line
    ,filetype_mcare = null
    ,getdate() as last_run
    from PHClaims.final.mcaid_claim_line as a
    left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
    on a.id_mcaid = b.id_mcaid
    
    union
    
    --Medicare claim ICD-CM header
    select
    --top 100
    b.id_apde
    ,'mcare' as source_desc
    ,a.claim_header_id
    ,a.claim_line_id
    ,first_service_date
    ,last_service_date
    ,a.revenue_code
    ,a.place_of_service_code
    ,a.type_of_service
    ,rac_code_line = null
    ,a.filetype_mcare
    ,getdate() as last_run
    from PHClaims.final.mcare_claim_line as a
    left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
    on a.id_mcare = b.id_mcare;",
        .con = conn))
    }

#### Table-level QA script ####
qa_stage.mcaid_mcare_claim_line_qa_f <- function() {
  
  #confirm that claim row counts match as expected for union
  res1 <- dbGetQuery(conn = conn, glue_sql(
    "select 'stage.mcaid_mcare_claim_line' as 'table', 'row count, expect match with sum of mcaid and mcare' as qa_type,
    count(*) as qa
    from stage.mcaid_mcare_claim_line;",
    .con = conn))
  
  res2 <- dbGetQuery(conn = conn, glue_sql(
    "select 'stage.mcaid_claim_line' as 'table', 'row count' as qa_type,
    count(*) as qa
    from final.mcaid_claim_line;",
    .con = conn))
  
  res3 <- dbGetQuery(conn = conn, glue_sql(
    "select 'stage.mcare_claim_line' as 'table', 'row count' as qa_type,
    count(*) as qa
    from final.mcare_claim_line;",
    .con = conn))
  
res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}