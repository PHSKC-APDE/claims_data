# This code creates table ([stage].[mcaid_claim_line]) to hold DISTINCT 
# line-level claim information
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# Created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by Alastair Matheson, PHSKC (APDE), 2019-05 and 2019-12
# Modified by: Philip Sylling, 2019-06-28
# 
# Data Pull Run time: 7.68 min
# Create Index Run Time: 7.2 min


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_claim_line_f <- function(conn = NULL,
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
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- ifelse(is.null(config[[server]][["ref_table"]]), '',
                      config[[server]][["ref_table"]])
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~8 minutes to run.")
  
  
  #### DROP EXISTING TABLE TO USE SELECT INTO ####
  try(DBI::dbRemoveTable(conn, DBI::Id(schema = to_schema, table = to_table)))
  
  
  #### LOAD TABLE ####
  # NB: Changes in table structure need to altered here and the YAML file
  insert_sql <- glue::glue_sql("SELECT id_mcaid
                               ,claim_header_id
                               ,claim_line_id
                               ,first_service_date
                               ,last_service_date
                               ,rev_code
                               ,rac_code_line
                               ,last_run
                               INTO {`to_schema`}.{`to_table`}
                               FROM (
                                 SELECT DISTINCT
                                 MEDICAID_RECIPIENT_ID as id_mcaid
                                 ,TCN as claim_header_id
                                 ,CLM_LINE_TCN as claim_line_id
                                 ,FROM_SRVC_DATE as first_service_date
                                 ,TO_SRVC_DATE as last_service_date
                                 ,REVENUE_CODE as rev_code
                                 ,RAC_CODE_L as rac_code_line
                                 ,getdate() as last_run
                                 FROM {`from_schema`}.{`from_table`}) a;", 
                               .con = conn)
  
  message("Loading to ", to_schema, ".", to_table)
  time_start <- Sys.time()
  DBI::dbExecute(conn = conn, insert_sql)
  time_end <- Sys.time()
  message("Loading took ", round(difftime(time_end, time_start, units = "secs"), 2), 
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)")
  
  
  #### ADD INDEX ####
  add_index_f(conn, server = server, table_config = config)
}
