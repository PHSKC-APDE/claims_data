# This code creates table ([stage].[mcaid_claim_pharm]) to hold DISTINCT 
# pharmacy information
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05 and 2019-12
# Modified by: Philip Sylling, 2019-06-11
# Eli updated on 6/12/25: Added pharmacy_npi field
# 
# Data Pull Run time: 5.58 min
# Create Index Run Time: 2.17 min


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_claim_pharm_f <- function(conn = NULL,
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
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~10 minutes to run.")
  
  
  #### STEP 1: DROP EXISTING TABLE TO USE SELECT INTO ####
  try(DBI::dbRemoveTable(conn, DBI::Id(schema = to_schema, table = to_table)))
  
  
  #### LOAD TABLE ####
  # Takes ~6 minutes
  # NB: Changes in table structure need to altered here and the YAML file
  insert_sql <- glue::glue_sql("SELECT DISTINCT
                             cast(MBR_H_SID as varchar(255)) as id_mcaid
                             ,cast(TCN as bigint) as claim_header_id
                             ,cast(NDC as varchar(255)) as ndc
                             ,cast(DAYS_SUPPLY as smallint) as rx_days_supply
                             ,cast(SBMTD_DISPENSED_QUANTITY as numeric(19,3)) as rx_quantity
                             ,cast(coalesce(PRSCRPTN_FILLED_DATE, TO_SRVC_DATE) as date) as rx_fill_date
                             ,DRUG_DOSAGE as rx_dosage_form
							               ,PACKAGE_SIZE_UOM as rx_dosage_unit
                             ,cast(case when (len([PRSCRBR_ID]) = 10 and 
                                              isnumeric([PRSCRBR_ID]) = 1 and 
                                              left([PRSCRBR_ID], 1) in (1,2)) then 'NPI'
                                   when (len([PRSCRBR_ID]) = 9 and 
                                         isnumeric(substring([PRSCRBR_ID], 1, 2)) = 0 and 
                                         isnumeric(substring([PRSCRBR_ID], 3, 7)) = 1) then 'DEA'
                                   when [PRSCRBR_ID] = '5123456787' then 'WA HCA'
                                   when [PRSCRBR_ID] is not null then 'UNKNOWN' end as varchar(10)) as prescriber_id_format                           
                             ,PRSCRBR_ID as prescriber_id
                             ,cast(case 
                                when [CLAIM_STATUS] = 71 then [BLNG_NATIONAL_PRVDR_IDNTFR] 
                                when ([CLAIM_STATUS] = 83 and [NPI] is not null) then [NPI] 
                                when ([CLAIM_STATUS] = 83 and [NPI] is null) then [BLNG_NATIONAL_PRVDR_IDNTFR] 
                              end as bigint) as billing_provider_npi
                             ,getdate() as last_run
                             INTO {`to_schema`}.{`to_table`}
                             FROM {`from_schema`}.{`from_table`}
                             where ndc is not null;", 
                               .con = conn)
  
  message(glue::glue("Loading to {to_schema}.{to_table}"))
  
  time_start <- Sys.time()
  DBI::dbExecute(conn = conn, insert_sql)
  time_end <- Sys.time()
  print(paste0("Loading took ", round(difftime(time_end, time_start, units = "secs"), 2), 
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  
  #### ADD INDEX ####
  #add_index_f(conn, server = server, table_config = config)
}
