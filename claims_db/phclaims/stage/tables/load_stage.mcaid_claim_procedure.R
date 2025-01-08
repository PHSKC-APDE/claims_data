# This code creates table ([stage].[mcaid_claim_procedure]) to hold DISTINCT 
# procedure codes in long format for Medicaid claims data
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05 and 2019-12
# Modified by: Philip Sylling, 2019-06-11
# 
# Data Pull Run time: 9.66 min
# Create Index Run Time: 5.75 min
# 


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_claim_procedure_f <- function(conn = NULL,
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
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~15 minutes to run.")
  
  
  #### STEP 1: DROP EXISTING TABLE TO USE SELECT INTO ####
  try(DBI::dbRemoveTable(conn, DBI::Id(schema = to_schema, table = to_table)))
  
  
  #### STEP 2: INSERT INTO TABLE ####
  # Takes ~ 60 minutes in Azure
  # NB: Changes in table structure need to altered here and the YAML file
  insert_sql <- glue::glue_sql("SELECT DISTINCT
                               id_mcaid
                               ,claim_header_id
                               ,first_service_date
                               ,last_service_date
                               ,procedure_code
                               ,cast(procedure_code_number as varchar(4)) as procedure_code_number
                               ,modifier_1
                               ,modifier_2
                               ,modifier_3
                               ,modifier_4
                               ,getdate() as last_run
                               INTO {`to_schema`}.{`to_table`}
                               FROM 
                               (
                                 select
                                 MBR_H_SID as id_mcaid
                                 ,TCN as claim_header_id
                                 ,FROM_SRVC_DATE as first_service_date
                                 ,TO_SRVC_DATE as last_service_date
                                 ,PRCDR_CODE_1 as [01]
                                 ,PRCDR_CODE_2 as [02]
                                 ,PRCDR_CODE_3 as [03]
                                 ,PRCDR_CODE_4 as [04]
                                 ,PRCDR_CODE_5 as [05]
                                 ,PRCDR_CODE_6 as [06]
                                 ,PRCDR_CODE_7 as [07]
                                 ,PRCDR_CODE_8 as [08]
                                 ,PRCDR_CODE_9 as [09]
                                 ,PRCDR_CODE_10 as [10]
                                 ,PRCDR_CODE_11 as [11]
                                 ,PRCDR_CODE_12 as [12]
                                 ,LINE_PRCDR_CODE as [line]
                                 ,MDFR_CODE1 as [modifier_1]
                                 ,MDFR_CODE2 as [modifier_2]
                                 ,MDFR_CODE3 as [modifier_3]
                                 ,MDFR_CODE4 as [modifier_4]
                                 FROM {`from_schema`}.{`from_table`}
                               ) as a
                               
                               unpivot(procedure_code for procedure_code_number in 
                                       ([01],[02],[03],[04],[05],[06],[07],[08],[09],[10],[11],[12],[line])) as procedure_code;", 
                               .con = conn)
  
  message("Running step 2: Load to ", to_schema, ".", to_table)
  
  time_start <- Sys.time()
  DBI::dbExecute(conn = conn, insert_sql)
  time_end <- Sys.time()
  message(glue::glue("Table creation took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                     " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))
  
  
  #### STEP 3: ADD INDEX ####
  # Takes ~6 minutes in Azure
  #add_index_f(conn, server = server, table_config = config)
}
