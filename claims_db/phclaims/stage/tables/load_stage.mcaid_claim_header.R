
# This code creates table (claims.mcaid_claim_header) to hold DISTINCT 
# header-level claim information in long format for Medicaid claims data
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
# 
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05 and 2019-12
# Revised: Philip Sylling | 2019-12-12 | Added Yale ED Measure, ed_pophealth_id
# Revised: Philip Sylling | 2019-12-13 | Changed definition of [ed] column to HCA-ARM definition
# Revised: Philip Sylling | 2019-12-13 | Added [ed_perform_id] which increments [ed] column by unique [id_mcaid], [first_service_date]
# Revised: Alastair Matheson | 2020-01-27 | Added primary care flags
# Revised: Eli Kern | 2023-09-13 | Added new BH flags, new CCS flags, renamed columns, removed outdated columns, revised injury code to be consistent with CHARS
# Revised: Eli Kern | 2024-01-16 | Added ccs_superlevel_desc and ccs_midlevel_desc columns to aid with tabulating leading causes
# 
# Data Pull Run time: XX min
# Create Index Run Time: XX min
# 
# Returns
# claims.stage_mcaid_claim_header
# 
# /* Header-level columns from claims.stage_mcaid_claim */
#   [id_mcaid]
# ,[claim_header_id]
# ,[clm_type_mcaid_id]
# ,[claim_type_id]
# ,[first_service_date]
# ,[last_service_date]
# ,[patient_status]
# ,[admsn_source]
# ,[admsn_date]
# ,[admsn_time]
# ,[dschrg_date]
# ,[place_of_service_code]
# ,[type_of_bill_code]
# ,[clm_status_code]
# ,[billing_provider_npi]
# ,[drvd_drg_code]
# ,[insrnc_cvrg_code]
# ,[last_pymnt_date]
# ,[bill_date]
# ,[system_in_date]
# ,[claim_header_id_date]
# 
# /* Derived claim event flag columns */
#   
# ,[primary_diagnosis]
# ,[icdcm_version]
# ,[primary_diagnosis_poa]
# ,[ccs_superlevel_desc]
# ,[ccs_broad_desc]
# ,[ccs_broad_code]
# ,[ccs_midlevel_desc]
# ,[ccs_detail_desc]
# ,[ccs_detail_code]
# ,[mh_primary]
# ,[mh_any]
# ,[sud_primary]
# ,[sud_any]
# ,[injury_nature_narrow]
# ,[injury_nature_broad]
# ,[injury_nature_type]
# ,[injury_nature_icdcm]
# ,[injury_ecode]
# ,[injury_intent]
# ,[injury_mechanism]
# ,[ed_perform]
# ,[ed_perform_id]
# ,[ed_pophealth]
# ,[ed_pophealth_id]
# ,[inpatient]
# ,[inpatient_id]
# ,[pc_visit]
# ,[pc_visit_id] 
# ,[last_run]


##stage_mcaid_# Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_claim_header_f <- function(conn = NULL,
                                           server = c("hhsaw"),
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
  icdcm_ref_schema <- config[[server]][["icdcm_ref_schema"]]
  icdcm_ref_table <- config[[server]][["icdcm_ref_table"]]
  temp_schema <- config[[server]][["temp_schema"]]
  temp_table <- ifelse(is.null(config[[server]][["temp_table"]]), '',
                      config[[server]][["temp_table"]])
  final_schema <- config[[server]][["final_schema"]]
  final_table <- ifelse(is.null(config[[server]][["final_table"]]), '',
                       config[[server]][["final_table"]])
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~80 minutes to run.")
  
  
  #### STEP 0: SET UP TEMP TABLE ####
  message("STEP 0: SET UP TEMP TABLE")
  time_start <- Sys.time()
  ##stage_mcaid_# Remove table if it exists
  try(DBI::dbRemoveTable(conn, name = DBI::Id(schema = temp_schema, 
                                              table = paste0(temp_table, "mcaid_claim_header"))),
      silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_header", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_line", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_diag", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_procedure_code", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_pc_provider", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_temp1", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ccs", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_rda", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury9cm", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury10cm", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_temp_final", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ed_yale_step_1", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ed_yale_final", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ed_perform_id", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_inpatient_id", temporary = T), silent = T)
  
  ##stage_mcaid_# Set up temp table
  # Could turn this code into a function and add test options if desired
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT DISTINCT 
           cast([MBR_H_SID] as varchar(255)) as id_mcaid
           ,cast([TCN] as bigint) as claim_header_id
           ,cast([CLM_TYPE_CID] as varchar(20)) as clm_type_mcaid_id
           ,cast(ref.[kc_clm_type_id] as tinyint) as claim_type_id
           ,cast([FROM_SRVC_DATE] as date) as first_service_date
           ,cast([TO_SRVC_DATE] as date) as last_service_date
           ,cast([PATIENT_STATUS_LKPCD] as varchar(255)) as patient_status
           ,cast([ADMSN_SOURCE_LKPCD] as varchar(255)) as admsn_source
           ,cast([ADMSN_DATE] as date) as admsn_date
           ,cast(timefromparts([ADMSN_HOUR] / 100, [ADMSN_HOUR] % 100, 0, 0, 0) as time(0)) as admsn_time
           ,cast([DSCHRG_DATE] as date) as dschrg_date
           ,cast([FCLTY_TYPE_CODE] as varchar(255)) as place_of_service_code
           ,cast([TYPE_OF_BILL] as varchar(255)) as type_of_bill_code
           ,cast([CLAIM_STATUS] as tinyint) as clm_status_code
           ,cast(case when [CLAIM_STATUS] = 71 then [BLNG_NATIONAL_PRVDR_IDNTFR] 
                 when ([CLAIM_STATUS] = 83 and [NPI] is not null) then [NPI] 
                 when ([CLAIM_STATUS] = 83 and [NPI] is null) then [BLNG_NATIONAL_PRVDR_IDNTFR] 
                 end as bigint) as billing_provider_npi
           ,cast([DRVD_DRG_CODE] as varchar(255)) as drvd_drg_code
           ,cast([PRIMARY_DIAGNOSIS_POA_LKPCD] as varchar(255)) as primary_diagnosis_poa
           ,cast([INSRNC_CVRG_CODE] as varchar(255)) as insrnc_cvrg_code
           ,cast([LAST_PYMNT_DATE] as date) as last_pymnt_date -- Change this back to LAST_ when the external table is remade
           ,cast([BILL_DATE] as date) as bill_date
           ,cast([SYSTEM_IN_DATE] as date) as system_in_date
           ,cast([TCN_DATE] as date) as claim_header_id_date
           
           INTO {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claim_header
           from {`from_schema`}.{`from_table`} as clm
           left join {`ref_schema`}.{DBI::SQL(ref_table)}kc_claim_type_crosswalk as ref
           on cast(clm.CLM_TYPE_CID as varchar(20)) = ref.source_clm_type_id",
                                .con = conn))
  
  
  #### STEP 1: SELECT HEADER-LEVEL INFORMATION NEEDED FOR EVENT FLAGS ####
  message("STEP 1: SELECT HEADER-LEVEL INFORMATION NEEDED FOR EVENT FLAGS")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_header", temporary = F), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_header;"), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("
        IF OBJECT_ID('tempdb..##stage_mcaid_header') IS NOT NULL
          DROP TABLE ##stage_mcaid_header;
        SELECT 
           id_mcaid
           ,claim_header_id
           ,clm_type_mcaid_id
           ,claim_type_id
           ,first_service_date
           ,last_service_date
           ,patient_status
           ,admsn_source
           ,admsn_date
           ,admsn_time
           ,dschrg_date
           ,place_of_service_code
           ,type_of_bill_code
           ,clm_status_code
           ,billing_provider_npi
           ,drvd_drg_code
           ,primary_diagnosis_poa
           ,insrnc_cvrg_code
           ,last_pymnt_date
           ,bill_date
           ,system_in_date
           ,claim_header_id_date
           
           INTO ##stage_mcaid_header
           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claim_header",
                                .con = conn))
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index idx_cl_##stage_mcaid_header on ##stage_mcaid_header(claim_header_id)")
  
  
  #### STEP 2: SELECT LINE-LEVEL INFORMATION NEEDED FOR EVENT FLAGS ####
  message("STEP 2: SELECT LINE-LEVEL INFORMATION NEEDED FOR EVENT FLAGS")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_line", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_line;"), silent = T)
  DBI::dbExecute(
    conn, glue::glue_sql(
      "select 
           claim_header_id
           --ed visits sub-flags for HCA-ARM/DHSH-RDA Definition
           ,max(case when rev_code like '045[01269]' then 1 else 0 end) as 'ed_rev_code'
           into ##stage_mcaid_line
           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_line
           group by claim_header_id",
      .con = conn))
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index idx_cl_##stage_mcaid_line on ##stage_mcaid_line(claim_header_id)")
  
  
  #### STEP 3: SELECT DX CODE INFORMATION NEEDED FOR EVENT FLAGS ####
  message("STEP 3: SELECT DX CODE INFORMATION NEEDED FOR EVENT FLAGS")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_diag", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_diag;"), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT dx.claim_header_id
           --primary diagnosis code with version
           ,max(case when icdcm_number = '01' then icdcm_norm else null end) as primary_diagnosis
           ,max(case when icdcm_number = '01' then icdcm_version else null end) as icdcm_version
           --Primary care-related visits
           ,MAX(CASE WHEN dx.icdcm_number IN ('01', '02') AND dx.icdcm_version = 10 AND 
            pc_ref.pc_dxcode = 1 THEN 1 ELSE 0 END) AS 'pc_zcode' 
           INTO ##stage_mcaid_diag FROM
           (select claim_header_id, icdcm_number, icdcm_norm, icdcm_version
           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header) AS dx
           LEFT JOIN
           (SELECT code, 1 AS pc_dxcode FROM {`ref_schema`}.{DBI::SQL(ref_table)}pc_visit_oregon 
           WHERE code_system IN ('icd10cm')) pc_ref
           ON dx.icdcm_norm = pc_ref.code
           GROUP BY dx.claim_header_id",
                                .con = conn))
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index idx_cl_##stage_mcaid_diag on ##stage_mcaid_diag(claim_header_id)")
  
  
  #### STEP 4: SELECT PROCEDURE CODE INFORMATION NEEDED FOR EVENT FLAGS ####
  message("STEP 4: SELECT PROCEDURE CODE INFORMATION NEEDED FOR EVENT FLAGS")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_procedure_code", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_procedure_code;"), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT px.claim_header_id 
           --ed visits sub-flags
           ,max(case when px.procedure_code like '9928[123458]' then 1 else 0 end) as 'ed_pcode1'
           ,MAX(ISNULL(pc_ref.pc_pcode, 0)) AS pc_pcode 
           INTO ##stage_mcaid_procedure_code FROM
           (SELECT claim_header_id, procedure_code FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_procedure) AS px
           LEFT JOIN
           (SELECT code, 1 AS pc_pcode FROM {`ref_schema`}.{DBI::SQL(ref_table)}pc_visit_oregon 
           WHERE code_system IN ('cpt', 'hcpcs')) pc_ref
           ON px.procedure_code = pc_ref.code
           GROUP BY claim_header_id",
                                .con = conn))
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index idx_cl_#procedure_code on ##stage_mcaid_procedure_code(claim_header_id)")
  
  
  #### STEP 5: HEDIS INPATIENT DEFINITION ####
  message("STEP 5: HEDIS INPATIENT DEFINITION")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_hedis_inpatient_definition", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_hedis_inpatient_definition;"), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql(
                   "SELECT distinct [id_mcaid]
                 ,[claim_header_id]
                 ,[first_service_date]
                 ,1 AS [inpatient]
                 INTO ##stage_mcaid_hedis_inpatient_definition
                 FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_line AS a
                 INNER JOIN 
                 (SELECT distinct code from {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde
                  WHERE [value_set_name] IN ('Inpatient Stay') AND [code_system] = 'UBREV') AS b
                 ON a.[rev_code] = b.[code]
                  
                  EXCEPT
                  (SELECT distinct [id_mcaid]
                    ,[claim_header_id]
                    ,[first_service_date]
                    ,1 AS [inpatient]
                    FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_line AS a
                    INNER JOIN 
                    (SELECT distinct code from {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde 
                    WHERE [value_set_name] IN ('Nonacute Inpatient Stay') AND [code_system] = 'UBREV') AS b
                    ON a.[rev_code] = b.[code]
                  UNION
                  SELECT distinct [id_mcaid]
                    ,[claim_header_id]
                    ,[first_service_date]
                    ,1 AS [inpatient]
                    FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_header AS a
                    INNER JOIN 
                    (SELECT distinct code from {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde 
                    WHERE [value_set_name] IN ('Nonacute Inpatient Stay') AND [code_system] = 'UBTOB') AS b
                    ON case when len(a.[type_of_bill_code]) = 3 then '0' + a.[type_of_bill_code] else a.[type_of_bill_code] end = b.[code]
                  );", .con = conn))
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index idx_cl_##stage_mcaid_hedis_inpatient_definition on ##stage_mcaid_hedis_inpatient_definition([claim_header_id])")
  
  
  #### STEP 6: PRIMARY CARE PROVIDERS ####
  message("STEP 6: PRIMARY CARE PROVIDERS")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_pc_provider", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_pc_provider;"), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT a.billing_provider_npi, ISNULL(b.pc_provider , 0) AS pc_provider
  INTO ##stage_mcaid_pc_provider
  FROM
  (SELECT DISTINCT billing_provider_npi FROM ##stage_mcaid_header) a
  LEFT JOIN
  (SELECT DISTINCT ref_provider.npi, 1 AS pc_provider
  FROM
  (SELECT npi, primary_taxonomy, secondary_taxonomy FROM {`ref_schema`}.{DBI::SQL(ref_table)}kc_provider_master) ref_provider
  INNER JOIN
  (SELECT code FROM {`ref_schema`}.{DBI::SQL(ref_table)}pc_visit_oregon WHERE code_system = 'provider_taxonomy') ref_pc
  ON ref_provider.primary_taxonomy = ref_pc.code OR ref_provider.secondary_taxonomy = ref_pc.code) b
  ON a.billing_provider_npi = b.npi",
                                .con = conn))
  
  
  #### STEP 7: CREATE TEMP SUMMARY CLAIMS TABLE WITH EVENT-BASED FLAGS ####
  message("STEP 7: CREATE TEMP SUMMARY CLAIMS TABLE WITH EVENT-BASED FLAGS")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_temp1", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_temp1;"), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT 
           header.id_mcaid
           ,header.claim_header_id
           ,header.clm_type_mcaid_id
           ,header.claim_type_id
           ,header.first_service_date
           ,header.last_service_date
           ,header.patient_status
           ,header.admsn_source
           ,admsn_date
           ,admsn_time
           ,dschrg_date
           ,insrnc_cvrg_code
           ,last_pymnt_date
           ,bill_date
           ,system_in_date
           ,claim_header_id_date
           ,header.place_of_service_code
           ,header.type_of_bill_code
           ,header.clm_status_code
           ,header.billing_provider_npi
           ,header.drvd_drg_code
           --Inpatient stay flag
		       ,case when hedis.inpatient is null then 0 else hedis.inpatient end as [inpatient]
           --ED visit (broad definition)
           ,case when header.clm_type_mcaid_id in (3,26,34)
              and (line.ed_rev_code = 1 or procedure_code.ed_pcode1 = 1 or header.place_of_service_code = '23') then 1 else 0 end as 'ed_perform'
           --Primary diagnosis and version
           ,diag.primary_diagnosis
           ,diag.icdcm_version
           ,header.primary_diagnosis_poa
           --Primary care visit
           ,CASE WHEN (diag.pc_zcode = 1 OR procedure_code.pc_pcode = 1)
              AND pc_provider.pc_provider = 1
              AND header.clm_type_mcaid_id NOT IN (19, 31, 33) --Ambulatory surgery centers/inpatient 
            THEN 1 ELSE 0 END AS pc_visit
           
           INTO ##stage_mcaid_temp1
           FROM ##stage_mcaid_header as header
           left join ##stage_mcaid_line as line 
           on header.claim_header_id = line.claim_header_id
           left join ##stage_mcaid_diag as diag 
           on header.claim_header_id = diag.claim_header_id
           left join ##stage_mcaid_procedure_code as procedure_code 
           on header.claim_header_id = procedure_code.claim_header_id
		       left join ##stage_mcaid_hedis_inpatient_definition as hedis
		       on header.claim_header_id = hedis.claim_header_id
           LEFT JOIN ##stage_mcaid_pc_provider AS pc_provider
           ON header.billing_provider_npi = pc_provider.billing_provider_npi",
                                .con = conn))
  

  #### STEP 8: CCS GROUPINGS (SUPERLEVEL, BROAD, MIDLEVEL, DETAIL) FOR PRIMARY DIAGNOSIS ####
  message("STEP 8: CCS GROUPINGS (SUPERLEVEL, BROAD, MIDLEVEL, DETAIL) FOR PRIMARY DX")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ccs", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_ccs;"), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT 
           b.claim_header_id
           ,a.ccs_superlevel_desc
           ,a.ccs_broad_desc
           ,a.ccs_broad_code
           ,a.ccs_midlevel_desc
           ,a.ccs_detail_desc
           ,a.ccs_detail_code
           INTO ##stage_mcaid_ccs
           FROM {`icdcm_ref_schema`}.{DBI::SQL(icdcm_ref_table)} as a
           inner join (select claim_header_id, icdcm_norm, icdcm_version 
           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header where icdcm_number = '01') as b
           on (a.icdcm_version = b.icdcm_version) and (a.icdcm = b.icdcm_norm)",
                                .con = conn))
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index [idx_cl_##stage_mcaid_ccs] on ##stage_mcaid_ccs(claim_header_id)")
  
  
  #### STEP 9: RDA BEHAVIORAL HEALTH DIAGNOSIS FLAGS ####
  message("STEP 9: RDA BEHAVIORAL HEALTH DIAGNOSIS FLAGS")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_rda", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_rda;"), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT 
           b.claim_header_id
           ,max(case when b.icdcm_number = '01' and a.mh_any = 1 then 1 else 0 end) as mh_primary
           ,max(case when a.mh_any = 1 then 1 else 0 end) as mh_any
           ,max(case when b.icdcm_number = '01' and a.sud_any = 1 then 1 else 0 end) as sud_primary
           ,max(case when a.sud_any = 1 then 1 else 0 end) as sud_any
           INTO ##stage_mcaid_rda
           FROM {`icdcm_ref_schema`}.{DBI::SQL(icdcm_ref_table)} as a
           inner join {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header as b
           on (a.icdcm_version = b.icdcm_version) and (a.icdcm = b.icdcm_norm)
           group by b.claim_header_id",
                                .con = conn))
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index [idx_cl_##stage_mcaid_rda] on ##stage_mcaid_rda(claim_header_id)")
  
  
  #### STEP 10: INJURY CAUSE AND NATURE PER CDC GUIDANCE ####
  message("STEP 10: INJURY CAUSE AND NATURE PER CDC GUIDANCE")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_injury;"), silent = T)
  DBI::dbExecute(conn, glue::glue_sql("
  ----------------------------------
  --STEP 1: Create table of distinct ICD-CM codes
  ----------------------------------
  if object_id('tempdb..##stage_mcaid_icdcm_distinct') is not null drop table ##stage_mcaid_icdcm_distinct;
  select distinct icdcm_norm, icdcm_version
  into ##stage_mcaid_icdcm_distinct
  from {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header;
  ", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  ----------------------------------
  --STEP 2: Flag nature-of-injury codes per CDC injury hospitalization surveillance definition for ICD-9-CM and ICD-10-CM
  --Refer to 7/5/19 NHSR report for ICD-9-CM and ICD-10-CM surveillance case definition for injury hospitalizations
  --ICD-9-CM definition is in 2nd paragraph of introduction
  --ICD-10-CM definition is in Table C (note this is same as Table B in 2020 NHSR update to nature of injury body region classification)
  --Tip - For using SQL between operator, the second parameter must be the last value in the list we want to include or it will miss values (e.g. 9949 not 994)
  ----------------------------------
  if object_id('tempdb..##stage_mcaid_injury_nature_ref') is not null drop table ##stage_mcaid_injury_nature_ref;
  select distinct *
  into ##stage_mcaid_injury_nature_ref
  from ##stage_mcaid_icdcm_distinct
  --Apply CDC surveillance definition for ICD-9-CM codes
  where (icdcm_version = 9 and 
  	(icdcm_norm between '800%' and '9949%' or icdcm_norm like '9955%' or icdcm_norm between '99580%' and '99585%') -- inclusion
  	and icdcm_norm not like '9093%' -- exclusion
  	and icdcm_norm not like '9095%' -- exclusion
  )
  --Apply CDC surveillance definition for ICD-10-CM codes
  or (icdcm_version = 10 and (
  	(icdcm_norm like 'S%' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm between 'T07%' and 'T3499XS' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm between 'T36%' and 'T50996S' and substring(icdcm_norm,6,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm like 'T3[679]9%' and substring(icdcm_norm,5,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm like 'T414%' and substring(icdcm_norm,5,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm like 'T427%' and substring(icdcm_norm,5,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion 
  	or (icdcm_norm like 'T4[3579]9%' and substring(icdcm_norm,5,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm between 'T51%' and 'T6594XS' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm between 'T66%' and 'T7692XS' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm like 'T79%' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm between 'O9A2%' and 'O9A53' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm like 'T8404%' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	or (icdcm_norm like 'M97%' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
  	)
  );", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  ----------------------------------
  --STEP 3: Create flags for broad and narrrow injury surveillance definitions
  ----------------------------------
  if object_id('tempdb..##stage_mcaid_injury_nature') is not null drop table ##stage_mcaid_injury_nature;
  select a.*,
  case when b.icdcm_norm is not null and a.icdcm_number = '01' then 1 else 0 end as injury_narrow,
  case when b.icdcm_norm is not null then 1 else 0 end as injury_broad
  into ##stage_mcaid_injury_nature
  from {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header as a
  left join ##stage_mcaid_injury_nature_ref as b
  on (a.icdcm_norm = b.icdcm_norm) and (a.icdcm_version = b.icdcm_version);", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  ----------------------------------
  --STEP 4: Identify external cause-of-injury codes for intent and mechanism
  ----------------------------------
  
  --LIKE join distinct ICD-10-CM codes to ICD-10-CM external cause-of-injury code reference table
  IF object_id(N'tempdb..##stage_mcaid_injury_cause_icd10cm_ref') is not null drop table ##stage_mcaid_injury_cause_icd10cm_ref;
  select distinct a.icdcm_norm, a.icdcm_version, b.intent, b.mechanism
  into ##stage_mcaid_injury_cause_icd10cm_ref
  from (select * from ##stage_mcaid_icdcm_distinct where icdcm_version = 10) as a
  inner join (
  	select icdcm, icdcm + '%' as icdcm_like, icdcm_version, intent, mechanism
  	from {`icdcm_ref_schema`}.{DBI::SQL(icdcm_ref_table)}
  	where icdcm_version = 10 and intent is not null
  ) as b
  on (a.icdcm_norm like b.icdcm_like) and (a.icdcm_version = b.icdcm_version);", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  --LIKE join distinct ICD-9-CM codes to ICD-9-CM external cause-of-injury code reference table
  IF object_id(N'tempdb..##stage_mcaid_injury_cause_icd9cm_ref') is not null drop table ##stage_mcaid_injury_cause_icd9cm_ref;
  select distinct a.icdcm_norm, a.icdcm_version, b.intent, b.mechanism
  into ##stage_mcaid_injury_cause_icd9cm_ref
  from (select * from ##stage_mcaid_icdcm_distinct where icdcm_version = 9) as a
  inner join (
  	select icdcm, icdcm + '%' as icdcm_like, icdcm_version, intent, mechanism
  	from {`icdcm_ref_schema`}.{DBI::SQL(icdcm_ref_table)}
  	where icdcm_version = 9 and intent is not null
  ) as b
  on (a.icdcm_norm like b.icdcm_like) and (a.icdcm_version = b.icdcm_version);", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  --UNION ICD-10-CM and ICD-9-CM CHARS reference table
  IF object_id(N'tempdb..##stage_mcaid_injury_cause_ref') is not null drop table ##stage_mcaid_injury_cause_ref;
  select *
  into ##stage_mcaid_injury_cause_ref
  from ##stage_mcaid_injury_cause_icd9cm_ref
  union
  select *
  from ##stage_mcaid_injury_cause_icd10cm_ref;", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  --EXACT join of above table to claims data with injury flags
  IF object_id(N'tempdb..##stage_mcaid_injury_nature_cause') is not null drop table ##stage_mcaid_injury_nature_cause;
  select a.*, b.intent, b.mechanism,
  case when b.intent is not null and b.mechanism is not null then 1 else 0 end as ecode_flag
  into ##stage_mcaid_injury_nature_cause
  from ##stage_mcaid_injury_nature as a
  left join ##stage_mcaid_injury_cause_ref as b
  on (a.icdcm_norm = b.icdcm_norm) and (a.icdcm_version = b.icdcm_version);", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  --Create rank variables for valid external cause-of-injury codes and for nature-of-injury codes
  IF object_id(N'tempdb..##stage_mcaid_injury_nature_cause_ranks') is not null drop table ##stage_mcaid_injury_nature_cause_ranks;
  select *,
  case
  	when ecode_flag = 0 then null
  	else row_number() over (partition by claim_header_id, ecode_flag order by icdcm_number)
  end as ecode_rank,
  case
  	when injury_broad = 0 then null
  	else row_number() over (partition by claim_header_id, injury_broad order by icdcm_number)
  end as injury_nature_rank
  into ##stage_mcaid_injury_nature_cause_ranks
  from ##stage_mcaid_injury_nature_cause;", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  ----------------------------------
  --STEP 5: Aggregate to claim header level
  ----------------------------------
  
  --Create some aggregated fields
  IF object_id(N'tempdb..##stage_mcaid_injury_cause_header_level_tmp') is not null drop table ##stage_mcaid_injury_cause_header_level_tmp;
  select claim_header_id,
    icdcm_norm,
  	max(injury_narrow) over (partition by claim_header_id) as injury_narrow,
  	max(injury_broad) over (partition by claim_header_id) as injury_broad,
  	intent, mechanism,
  	max(ecode_flag) over (partition by claim_header_id) as ecode_flag_max,
  	ecode_rank
  into ##stage_mcaid_injury_cause_header_level_tmp
  from ##stage_mcaid_injury_nature_cause_ranks;", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  --Collapse to claim header level
  IF object_id(N'tempdb..##stage_mcaid_injury_cause_header_level_tmp2') is not null drop table ##stage_mcaid_injury_cause_header_level_tmp2;
  select distinct claim_header_id, 
    case when ecode_rank = 1 then icdcm_norm else null end as ecode,
	  injury_narrow, injury_broad, intent, mechanism
  into ##stage_mcaid_injury_cause_header_level_tmp2
  from ##stage_mcaid_injury_cause_header_level_tmp
  where (ecode_flag_max = 0) or (ecode_flag_max = 1 and ecode_rank = 1); -- subset to claim header level by selecting 1st-ranked ecode or stays having no ecodes at all", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  --Add back first-ranked diagnosis with a nature-of-injury code
  IF object_id(N'tempdb..##stage_mcaid_injury_cause_header_level_tmp3') is not null drop table ##stage_mcaid_injury_cause_header_level_tmp3;
  select a.*, b.icdcm_norm as icdcm_injury_nature, b.icdcm_version as icdcm_injury_nature_version
  into ##stage_mcaid_injury_cause_header_level_tmp3
  from ##stage_mcaid_injury_cause_header_level_tmp2 as a
  left join (select * from ##stage_mcaid_injury_nature_cause_ranks where injury_nature_rank = 1) as b
  on a.claim_header_id = b.claim_header_id;", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  ----------------------------------
  --STEP 6: Create reference table to categorize type of nature of injury
  ----------------------------------
  
  --First join to ref.icdcm_codes to grab CCS detail description, removing [initial encounter] phrase
  IF object_id(N'tempdb..##stage_mcaid_distinct_injury_nature_icdcm_tmp1') is not null drop table ##stage_mcaid_distinct_injury_nature_icdcm_tmp1;
  select distinct icdcm_injury_nature, icdcm_injury_nature_version,
  	case
  		when b.ccs_detail_desc like '%; initial encounter%' then replace(b.ccs_detail_desc, '; initial encounter', '')
  		when b.ccs_detail_desc like '%, initial encounter%' then replace(b.ccs_detail_desc, ', initial encounter', '')
  		else b.ccs_detail_desc
  	end as ccs_detail_desc
  into ##stage_mcaid_distinct_injury_nature_icdcm_tmp1
  from ##stage_mcaid_injury_cause_header_level_tmp3 as a
  left join {`icdcm_ref_schema`}.{DBI::SQL(icdcm_ref_table)} as b
  on (a.icdcm_injury_nature = b.icdcm) and (a.icdcm_injury_nature_version = b.icdcm_version)
  where a.icdcm_injury_nature is not null;", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  --Normalize type of injury categories
  IF object_id(N'tempdb..##stage_mcaid_distinct_injury_nature_icdcm_final') is not null drop table ##stage_mcaid_distinct_injury_nature_icdcm_final;
  select icdcm_injury_nature, icdcm_injury_nature_version,
  case
  	when ccs_detail_desc in ('Other specified injury', 'Other unspecified injury') then 'Other injuries'
  	when ccs_detail_desc in ('Spinal cord injury (SCI)') then 'Spinal cord injury'
  	when ccs_detail_desc in ('Effect of other external causes',
  		'External cause codes: other specified, classifiable and NEC',
  		'External cause codes: unspecified mechanism',
  		'Other injuries and conditions due to external causes')
  		then 'Other injuries and conditions due to external causes'
  	when ccs_detail_desc in ('Crushing injury', 'Crushing injury or internal injury') then 'Crushing injury or internal injury'
  	when ccs_detail_desc in ('Burns', 'Burn and corrosion') then 'Burn and corrosion'
  	else ccs_detail_desc
  end as ccs_detail_desc
  into ##stage_mcaid_distinct_injury_nature_icdcm_final
  from ##stage_mcaid_distinct_injury_nature_icdcm_tmp1;", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  ----------------------------------
  --STEP 7: Add broad type categories to nature of injury ICD-CM codes
  ----------------------------------
  select a.*, b.ccs_detail_desc as icdcm_injury_nature_type
  into ##stage_mcaid_injury
  from ##stage_mcaid_injury_cause_header_level_tmp3 as a
  left join ##stage_mcaid_distinct_injury_nature_icdcm_final as b
  on (a.icdcm_injury_nature = b.icdcm_injury_nature) and (a.icdcm_injury_nature_version = b.icdcm_injury_nature_version);
  ", .con = conn))
  
  # Clean up temp tables from this stage
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_icdcm_distinct", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_nature_ref", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_nature", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_cause_icd10cm_ref", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_cause_icd9cm_ref", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_cause_ref", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_nature_cause", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_nature_cause_ranks", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_cause_header_level_tmp", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_cause_header_level_tmp2", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury_cause_header_level_tmp3", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_distinct_injury_nature_icdcm_tmp1", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_distinct_injury_nature_icdcm_final", temporary = T), silent = T)
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index [idx_cl_##stage_mcaid_injury] on ##stage_mcaid_injury(claim_header_id)")
  
  
  #### STEP 11: CREATE ID COLUMNS FOR EVENTS THAT ARE ONLY COUNTED ONCE PER DAY OR EPISODE ####
  message("STEP 11: CREATE ID COLUMNS FOR EVENTS THAT ARE ONLY COUNTED ONCE PER DAY OR EPISODE")
  # [ed_pophealth_id] (YALE ED MEASURE)
  # [ed_perform_id]
  # [inpatient_id]
  # [pc_visit_id]
  
  # Get relevant claims for Yale-ED-Measure
  # 
  # Logic:
  #   
  #   IF [claim_type_id] = 5 (Provider/Professional) 
  # AND (([procedure_code] IN ('99281','99282','99283','99284','99285','99291') AND [place_of_service_code] = '23') OR [rev_code] IN ('0450','0451','0452','0456','0459','0981'))
  # THEN [ed_type] = 'Carrier'
  # 
  # IF [claim_type_id] IN (4, 1) (Outpatient Facility, Inpatient Facility)
  # AND ([procedure_code] IN ('99281','99282','99283','99284','99285','99291') OR [place_of_service_code] = '23' OR [rev_code] IN ('0450','0451','0452','0456','0459','0981'))
  # THEN [ed_type] = 'Facility'
  
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ed_yale_step_1", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_ed_yale_step_1;"), silent = T)
  DBI::dbExecute(
    conn, glue::glue_sql("SELECT [id_mcaid]
                        ,[claim_header_id]
                        ,[first_service_date]
                        ,[last_service_date]
                        ,'Carrier' as [ed_type]
                      INTO ##stage_mcaid_ed_yale_step_1
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_procedure
                      WHERE [procedure_code] in ('99281','99282','99283','99284','99285','99291')
                        AND [claim_header_id] in 
                          (SELECT [claim_header_id]
                           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claim_header
                           WHERE [place_of_service_code] = '23'
                           -- [claim_type_id] = 5, Provider/Professional
                           AND [claim_type_id] = 5)
                      UNION
                      SELECT [id_mcaid]
                        ,[claim_header_id]
                        ,[first_service_date]
                        ,[last_service_date]
                        ,'Carrier' as [ed_type]
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_line
                      WHERE [rev_code] in ('0450','0451','0452','0456','0459','0981')
                        AND [claim_header_id] in
                          (SELECT [claim_header_id]
                           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claim_header
                           -- [claim_type_id] = 5, Provider/Professional
                           WHERE [claim_type_id] = 5)
                      UNION
                      SELECT [id_mcaid]
                        ,[claim_header_id]
                        ,[first_service_date]
                        ,[last_service_date]
                        ,'Facility' as [ed_type]
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_procedure
                      WHERE [procedure_code] in ('99281','99282','99283','99284','99285','99291')
                        AND [claim_header_id] in 
                          (SELECT [claim_header_id]
                           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claim_header
                           -- [claim_type_id] = 1, Inpatient Facility, [claim_type_id] = 4, Outpatient Facility
                           WHERE [claim_type_id] IN (1, 4))
                      UNION
                      SELECT [id_mcaid]
                        ,[claim_header_id]
                        ,[first_service_date]
                        ,[last_service_date]
                        ,'Facility' as [ed_type]
                      FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claim_header
                      WHERE [place_of_service_code] = '23'
                        -- [claim_type_id] = 1, Inpatient Facility, [claim_type_id] = 4, Outpatient Facility
                        AND [claim_type_id] IN (1, 4)
                      UNION
                      SELECT [id_mcaid]
                        ,[claim_header_id]
                        ,[first_service_date]
                        ,[last_service_date]
                        ,'Facility' as [ed_type]
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_line
                      WHERE [rev_code] in ('0450','0451','0452','0456','0459','0981')
                        AND [claim_header_id] in 
                          (SELECT [claim_header_id]
                           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claim_header
                           -- [claim_type_id] = 1, Inpatient Facility, [claim_type_id] = 4, Outpatient Facility
                           WHERE [claim_type_id] IN (1, 4));",
                         .con = conn))
  
  # Label duplicate/adjacent visits with a single [ed_pophealth_id]
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_increment_stays_by_person", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "##stage_mcaid_increment_stays_by_person;"), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_create_within_person_stay_id", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_create_within_person_stay_id;"), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ed_yale_final", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_ed_yale_final;"), silent = T)
  DBI::dbExecute(conn, glue::glue_sql("
  SELECT [id_mcaid]
      ,[claim_header_id]
      -- If [prior_first_service_date] IS NULL, then it is the first chronological [first_service_date] for the person
      ,LAG([first_service_date]) OVER(
          PARTITION BY [id_mcaid] 
          ORDER BY [first_service_date], [last_service_date], [claim_header_id]) AS [prior_first_service_date]
      ,[first_service_date]
      ,[last_service_date]
      ,[ed_type]
      -- Number of days between consecutive rows
      ,DATEDIFF(DAY, LAG([first_service_date]) OVER(
          PARTITION BY [id_mcaid] 
          ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) AS [date_diff]
      /*
        Create a chronological (0, 1) indicator column.
      If 0, it is the first ED visit for the person OR the ED visit appears to be a duplicate
      (overlapping service dates) of the prior visit.
      If 1, the prior ED visit appears to be distinct from the following stay.
      This indicator column will be summed to create an episode_id.
      */
      ,CASE WHEN ROW_NUMBER() OVER(
        PARTITION BY [id_mcaid]
        ORDER BY [first_service_date], [last_service_date], [claim_header_id]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([first_service_date]) OVER(
        PARTITION BY [id_mcaid]
        ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) <= 1 THEN 0
      WHEN DATEDIFF(DAY, LAG(first_service_date) OVER(
        PARTITION BY [id_mcaid]
        ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) > 1 THEN 1
      END AS [increment]
	  INTO ##stage_mcaid_increment_stays_by_person
    FROM ##stage_mcaid_ed_yale_step_1
  ", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  SELECT [id_mcaid]
      ,[claim_header_id]
      ,[prior_first_service_date]
      ,[first_service_date]
      ,[last_service_date]
      ,[ed_type]
      ,[date_diff]
      ,[increment]
      ,SUM([increment]) OVER(PARTITION BY [id_mcaid] 
                             ORDER BY [first_service_date], [last_service_date], [claim_header_id] ROWS UNBOUNDED PRECEDING) + 1 AS [within_person_stay_id]
    INTO ##stage_mcaid_create_within_person_stay_id
	FROM ##stage_mcaid_increment_stays_by_person
    --ORDER BY [id_mcaid], [first_service_date], [last_service_date], [claim_header_id]
  ", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
  SELECT [id_mcaid]
      ,[claim_header_id]
      ,[prior_first_service_date]
      ,[first_service_date]
      ,[last_service_date]
      ,[ed_type]
      ,[date_diff]
      ,[increment]
      ,[within_person_stay_id]
      ,DENSE_RANK() OVER(ORDER BY [id_mcaid], [within_person_stay_id]) AS [ed_pophealth_id]
    INTO ##stage_mcaid_ed_yale_final
    FROM ##stage_mcaid_create_within_person_stay_id
    --ORDER BY [id_mcaid], [first_service_date], [last_service_date], [claim_header_id];
  ", .con = conn))
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index [idx_cl_##stage_mcaid_ed_yale_final] on ##stage_mcaid_ed_yale_final(claim_header_id)")
 
  
  # Set up ED IDs
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ed_perform_id", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_ed_perform_id;"), silent = T)
  DBI::dbExecute(
    conn, "SELECT [claim_header_id]
          ,CASE WHEN [ed_perform] = 0 THEN null
            ELSE dense_rank() OVER(ORDER BY CASE WHEN [ed_perform] = 0 THEN 2 ELSE 1 END, 
                                   [id_mcaid], [first_service_date]) end as [ed_perform_id]
            INTO ##stage_mcaid_ed_perform_id
            FROM ##stage_mcaid_temp1;")
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index [idx_cl_##stage_mcaid_ed_perform_id] on ##stage_mcaid_ed_perform_id(claim_header_id)")
  
  
  # Create [inpatient_id] column
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_inpatient_id", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_inpatient_id;"), silent = T)
  DBI::dbExecute(conn, 
                 "SELECT [id_mcaid]
                  ,[claim_header_id]
                  ,[first_service_date]
                  ,[inpatient]
                  ,DENSE_RANK() OVER(ORDER BY [id_mcaid], [first_service_date]) AS [inpatient_id]
                 INTO ##stage_mcaid_inpatient_id
                 FROM ##stage_mcaid_hedis_inpatient_definition;")
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index [idx_cl_##stage_mcaid_inpatient_id] on ##stage_mcaid_inpatient_id([claim_header_id])") 
  
  
  # Create [pc_visit_id] column
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_pc_visit_id", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_pc_visit_id;"), silent = T)
  DBI::dbExecute(conn,
                 "SELECT [id_mcaid]
                ,[claim_header_id]
                ,[first_service_date]
                ,[pc_visit]
                ,DENSE_RANK() OVER(ORDER BY [id_mcaid], [first_service_date]) AS [pc_visit_id]
               INTO ##stage_mcaid_pc_visit_id
               FROM ##stage_mcaid_temp1
               WHERE pc_visit = 1;")
  
  # Add index
  #DBI::dbExecute(conn, "create clustered index [idx_cl_##stage_mcaid_pc_visit_id] on ##stage_mcaid_pc_visit_id([claim_header_id])")
  
  
  #### STEP 12: CREATE FINAL SUMMARY TABLE WITH EVENT-BASED FLAGS (TEMP STAGE) ####
  message("STEP 12: CREATE FINAL SUMMARY TABLE WITH EVENT-BASED FLAGS (TEMP STAGE)")
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_temp_final", temporary = T), silent = T)
  try(DBI::dbExecute(conn, "DROP TABLE ##stage_mcaid_temp_final;"), silent = T)
  DBI::dbExecute(conn,
                 "SELECT 
             a.*
									  
			       --Increment [ed] column by distinct [id_mcaid], [first_service_date]
			       ,h.[ed_perform_id]
			       --Yale ED MEASURE
			       ,case when g.[ed_pophealth_id] is not null then 1 else 0 end as 'ed_pophealth'
			       ,g.[ed_pophealth_id]
             
             --Inpatient-related flags
             ,i.[inpatient_id]
             
             --Injuries
             ,f.injury_narrow as injury_nature_narrow
             ,f.injury_broad as injury_nature_broad
             ,f.icdcm_injury_nature_type as injury_nature_type
             ,f.icdcm_injury_nature as injury_nature_icdcm
             ,f.ecode as injury_ecode
             ,f.intent as injury_intent
             ,f.mechanism as injury_mechanism
             
             --CCS
             ,d.ccs_superlevel_desc
             ,d.ccs_broad_desc
             ,d.ccs_broad_code
             ,d.ccs_midlevel_desc
             ,d.ccs_detail_desc
             ,d.ccs_detail_code
             
             --RDA BH flags
             ,case when e.mh_primary = 1 then 1 else 0 end as 'mh_primary'
             ,case when e.mh_any = 1 then 1 else 0 end as 'mh_any'
             ,case when e.sud_primary = 1 then 1 else 0 end as 'sud_primary'
             ,case when e.sud_any = 1 then 1 else 0 end as 'sud_any'
             
             --Primary care
             ,j.pc_visit_id
             
             INTO ##stage_mcaid_temp_final
             FROM ##stage_mcaid_temp1 as a
             left join ##stage_mcaid_ccs as d
             on a.claim_header_id = d.claim_header_id
             left join ##stage_mcaid_rda as e
             on a.claim_header_id = e.claim_header_id
             left join ##stage_mcaid_injury as f
             on a.claim_header_id = f.claim_header_id
             left join ##stage_mcaid_ed_yale_final as g
             on a.claim_header_id = g.claim_header_id			
             left join ##stage_mcaid_ed_perform_id as h
             on a.claim_header_id = h.claim_header_id
             left join ##stage_mcaid_inpatient_id as i
             on a.claim_header_id = i.claim_header_id
             left join ##stage_mcaid_pc_visit_id AS j
             on a.claim_header_id = j.claim_header_id
           ")
  
  
  #### STEP 13: COPY FINAL TEMP TABLE INTO STAGE.MCAID_CLAIM_HEADER ####
  message("STEP 13: COPY FINAL TEMP TABLE INTO STAGE.MCAID_CLAIM_HEADER")
  message("Loading to stage table")
  
  # Delete and remake table
  create_table_f(conn = conn, server = server, config = config, overwrite = T)
  
  DBI::dbExecute(conn,
                 glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} 
                 {DBI::SQL(ifelse(server == 'phclaims', ' WITH (TABLOCK) ', ''))}
                          ({`names(config$vars)`*}) 
                          SELECT {`names(config$vars)[names(config$vars) != 'last_run']`*}
                          , getdate() AS [last_run] 
                          FROM ##stage_mcaid_temp_final", .con = conn))
  
  
  #### STEP 14: ADD INDEX ####
  message("STEP 14: ADD INDEX")
  message("Creating index on final table")
  #add_index_f(conn, server = server, table_config = config)
  
  
  
  #### STEP 15: CLEAN UP TEMP TABLES ####
  message("STEP 15: CLEAN UP TEMP TABLES")
  try(DBI::dbRemoveTable(conn, 
                         name = DBI::Id(schema = temp_schema, table = paste0(temp_table, "mcaid_claim_header"))),
      silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_header", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_line", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_diag", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_procedure_code", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_pc_provider", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_temp1", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ccs", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_rda", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury9cm", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury10cm", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_injury", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_temp_final", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ed_yale_step_1", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ed_yale_final", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_ed_perform_id", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##stage_mcaid_inpatient_id", temporary = T), silent = T)
  
  time_end <- Sys.time()
  message(glue::glue("Table creation took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                     " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))
}
