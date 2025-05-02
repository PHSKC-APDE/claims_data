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
# Update 2025-04-03 (Eli Kern): Remove procedure_code_number column and consolidate modifier codes into a single column
# Update 2025-05-02 (Eli Kern): Debugging recent code change that is dropping procedure codes without any modifier codes
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
  insert_sql <- glue::glue_sql(
    "-- Base CTE: pulled once from the source table
    WITH base_data AS (
        SELECT
            MBR_H_SID,
            TCN,
            FROM_SRVC_DATE,
            TO_SRVC_DATE,
            PRCDR_CODE_1, PRCDR_CODE_2, PRCDR_CODE_3, PRCDR_CODE_4,
            PRCDR_CODE_5, PRCDR_CODE_6, PRCDR_CODE_7, PRCDR_CODE_8,
            PRCDR_CODE_9, PRCDR_CODE_10, PRCDR_CODE_11, PRCDR_CODE_12,
            LINE_PRCDR_CODE,
            MDFR_CODE1, MDFR_CODE2, MDFR_CODE3, MDFR_CODE4
        FROM {`from_schema`}.{`from_table`}
    ),
    
    -- CTE for PRCDR_CODE_1 to 12 (ICD codes with no modifiers)
    icd_procedures AS (
        SELECT
            MBR_H_SID AS id_mcaid,
            TCN AS claim_header_id,
            FROM_SRVC_DATE AS first_service_date,
            TO_SRVC_DATE AS last_service_date,
            procedure_code,
            NULL AS modifier_code
        FROM (
            SELECT
                MBR_H_SID,
                TCN,
                FROM_SRVC_DATE,
                TO_SRVC_DATE,
                PRCDR_CODE_1 AS [01], PRCDR_CODE_2 AS [02], PRCDR_CODE_3 AS [03], PRCDR_CODE_4 AS [04],
                PRCDR_CODE_5 AS [05], PRCDR_CODE_6 AS [06], PRCDR_CODE_7 AS [07], PRCDR_CODE_8 AS [08],
                PRCDR_CODE_9 AS [09], PRCDR_CODE_10 AS [10], PRCDR_CODE_11 AS [11], PRCDR_CODE_12 AS [12]
            FROM base_data
        ) icd
        UNPIVOT (
            procedure_code FOR code_position IN
            ([01], [02], [03], [04], [05], [06], [07], [08], [09], [10], [11], [12])
        ) AS unpvt_icd
    ),
    
    -- LINE_PRCDR_CODE rows with associated modifiers
    line_procedures_with_modifiers AS (
        SELECT
            MBR_H_SID AS id_mcaid,
            TCN AS claim_header_id,
            FROM_SRVC_DATE AS first_service_date,
            TO_SRVC_DATE AS last_service_date,
            LINE_PRCDR_CODE AS procedure_code,
            modifier_code
        FROM (
            SELECT
                MBR_H_SID,
                TCN,
                FROM_SRVC_DATE,
                TO_SRVC_DATE,
                LINE_PRCDR_CODE,
                MDFR_CODE1 AS [modifier_1],
                MDFR_CODE2 AS [modifier_2],
                MDFR_CODE3 AS [modifier_3],
                MDFR_CODE4 AS [modifier_4]
            FROM base_data
        ) mod_src
        UNPIVOT (
            modifier_code FOR mod_position IN
            ([modifier_1], [modifier_2], [modifier_3], [modifier_4])
        ) AS unpvt_mod
        WHERE LINE_PRCDR_CODE IS NOT NULL
    ),
    
    -- LINE_PRCDR_CODE rows that have no modifier codes
    line_procedures_no_modifiers AS (
        SELECT
            MBR_H_SID AS id_mcaid,
            TCN AS claim_header_id,
            FROM_SRVC_DATE AS first_service_date,
            TO_SRVC_DATE AS last_service_date,
            LINE_PRCDR_CODE AS procedure_code,
            NULL AS modifier_code
        FROM base_data
        WHERE LINE_PRCDR_CODE IS NOT NULL
          AND MDFR_CODE1 IS NULL
          AND MDFR_CODE2 IS NULL
          AND MDFR_CODE3 IS NULL
          AND MDFR_CODE4 IS NULL
    )
    
    -- Final selection with deduplication via UNION
    SELECT
        id_mcaid,
        claim_header_id,
        first_service_date,
        last_service_date,
        procedure_code,
        modifier_code,
        GETDATE() AS last_run
    INTO {`to_schema`}.{`to_table`}
    FROM icd_procedures
    
    UNION
    
    SELECT
        id_mcaid,
        claim_header_id,
        first_service_date,
        last_service_date,
        procedure_code,
        modifier_code,
        GETDATE() AS last_run
    FROM line_procedures_with_modifiers
    
    UNION
    
    SELECT
        id_mcaid,
        claim_header_id,
        first_service_date,
        last_service_date,
        procedure_code,
        modifier_code,
        GETDATE() AS last_run
    FROM line_procedures_no_modifiers;", 
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
