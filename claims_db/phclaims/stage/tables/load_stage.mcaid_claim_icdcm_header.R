
# This code creates table ([stage].[mcaid_claim_icdcm_header]) to hold DISTINCT 
# procedure codes in long format for Medicaid claims data
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
# 
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05
# Modified by: Philip Sylling, 2019-06-11
# 
# Data Pull Run time: 17.36 min
# Create Index Run Time: 9.44 min
# 
# Returns
# [stage].[mcaid_claim_icdcm_header]
#  [id_mcaid]
# ,[claim_header_id]
# ,[first_service_date]
# ,[last_service_date]
# ,[icdcm_raw]
# ,[icdcm_norm]
# ,[icdcm_version]
# ,[icdcm_number]
# ,[last_run]



### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_claim_icdcm_header_f <- function(conn = NULL,
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
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~30 minutes to run.")
  
  
  #### STEP 1: DROP EXISTING TABLE TO USE SELECT INTO ####
  try(DBI::dbRemoveTable(conn, DBI::Id(schema = to_schema, table = to_table)))
  
  
  #### STEP 2: INSERT INTO TABLE ####
  # Takes ~ 90 minutes in Azure
  step2_sql <- glue::glue_sql("SELECT DISTINCT
                              id_mcaid
                              ,claim_header_id
                              ,first_service_date
                              ,last_service_date
                              --original diagnosis codes without zero right-padding
                              ,cast(diagnoses as varchar(200)) as icdcm_raw
                              
                              ,cast(
                                case
                                -- right-zero-pad ICD-9 diagnoses
                                when (diagnoses like '[0-9]%' and len(diagnoses) = 3) then diagnoses + '00'
                                when (diagnoses like '[0-9]%' and len(diagnoses) = 4) then diagnoses + '0'
                                -- Both ICD-9 and ICD-10 codes have 'V' and 'E' prefixes
                                -- Diagnoses prior to 2015-10-01 are ICD-9
                                when (diagnoses like 'V%' and last_service_date < '2015-10-01' and len(diagnoses) = 3) then diagnoses + '00'
                                when (diagnoses like 'V%' and last_service_date < '2015-10-01' and len(diagnoses) = 4) then diagnoses + '0'
                                when (diagnoses like 'E%' and last_service_date < '2015-10-01' and len(diagnoses) = 3) then diagnoses + '00'
                                when (diagnoses like 'E%' and last_service_date < '2015-10-01' and len(diagnoses) = 4) then diagnoses + '0'
                                else diagnoses 
                                end 
                                as varchar(200)) as icdcm_norm
                              
                              ,cast(
                                case
                                when (diagnoses like '[0-9]%') then 9
                                when (diagnoses like 'V%' and last_service_date < '2015-10-01') then 9
                                when (diagnoses like 'E%' and last_service_date < '2015-10-01') then 9
                                else 10 
                                end 
                                as tinyint) as icdcm_version
                              
                              ,cast(dx_number as varchar(5)) as icdcm_number
                              ,getdate() as last_run
                              INTO {`to_schema`}.{`to_table`}
                              FROM 
                              (
                                select 
                                MEDICAID_RECIPIENT_ID as id_mcaid
                                ,TCN as claim_header_id
                                --,CLM_LINE_TCN
                                ,FROM_SRVC_DATE as first_service_date
                                ,TO_SRVC_DATE as last_service_date
                                ,PRIMARY_DIAGNOSIS_CODE as [01]
                                ,DIAGNOSIS_CODE_2 as [02]
                                ,DIAGNOSIS_CODE_3 as [03]
                                ,DIAGNOSIS_CODE_4 as [04]
                                ,DIAGNOSIS_CODE_5 as [05]
                                ,DIAGNOSIS_CODE_6 as [06]
                                ,DIAGNOSIS_CODE_7 as [07]
                                ,DIAGNOSIS_CODE_8 as [08]
                                ,DIAGNOSIS_CODE_9 as [09]
                                ,DIAGNOSIS_CODE_10 as [10]
                                ,DIAGNOSIS_CODE_11 as [11]
                                ,DIAGNOSIS_CODE_12 as [12]
                                ,ADMTNG_DIAGNOSIS_CODE as [admit]
                                
                                FROM {`from_schema`}.{`from_table`}
                              ) as a
                              
                              unpivot(diagnoses for dx_number IN ([01], [02], [03], [04], [05], [06], [07], [08], [09], [10], [11], [12], [admit])) as diagnoses;", 
                              .con = conn)
                              
  message("Running step 2: Load to ", to_schema, ".", to_table)
  
  time_start <- Sys.time()
  DBI::dbExecute(conn = conn, step2_sql)
  time_end <- Sys.time()
  message(glue::glue("Step 2 took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                     " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))
  
  
  #### STEP 3: ADD INDEX ####
  # Takes ~6 minutes in Azure
  message("Running step 3: create index")
  time_start <- Sys.time()
  add_index_f(conn, server = server, table_config = config)
  time_end <- Sys.time()
  message(glue::glue("Index creation took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                     " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))
                              
}
