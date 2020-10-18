
# This code creates table (claims.tmp_mcaid_claims_header) to hold DISTINCT 
# header-level claim information in long format for Medicaid claims data
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/azure_migration/claims_db/db_loader/mcaid/master_mcaid_analytic.R
# 
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05 and 2019-12
# Revised: Philip Sylling | 2019-12-12 | Added Yale ED Measure, ed_pophealth_id
# Revised: Philip Sylling | 2019-12-13 | Changed definition of [ed] column to HCA-ARM definition
# Revised: Philip Sylling | 2019-12-13 | Added [ed_perform_id] which increments [ed] column by unique [id_mcaid], [first_service_date]
# Revised: Alastair Matheson| 2020-01-27 | Added primary care flags
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
# /* Derived claim event flag columns (formerly columns from [mcaid_claim_summary]) */
#   
#   ,[primary_diagnosis]
# ,[icdcm_version]
# ,[primary_diagnosis_poa]
# ,[mental_dx1]
# ,[mental_dxany]
# ,[mental_dx_rda_any]
# ,[sud_dx_rda_any]
# ,[maternal_dx1]
# ,[maternal_broad_dx1]
# ,[newborn_dx1]
# ,[ed]
# ,[ed_nohosp]
# ,[ed_bh]
# ,[ed_avoid_ca]
# ,[ed_avoid_ca_nohosp]
# ,[ed_ne_nyu]
# ,[ed_pct_nyu]
# ,[ed_pa_nyu]
# ,[ed_npa_nyu]
# ,[ed_mh_nyu]
# ,[ed_sud_nyu]
# ,[ed_alc_nyu]
# ,[ed_injury_nyu]
# ,[ed_unclass_nyu]
# ,[ed_emergent_nyu]
# ,[ed_nonemergent_nyu]
# ,[ed_intermediate_nyu]
# ,[inpatient]
# ,[ipt_medsurg]
# ,[ipt_bh]
# ,[intent]
# ,[mechanism]
# ,[sdoh_any]
# ,[ed_sdoh]
# ,[ipt_sdoh]
# ,[ccs]
# ,[ccs_description]
# ,[ccs_description_plain_lang]
# ,[ccs_mult1]
# ,[ccs_mult1_description]
# ,[ccs_mult2]
# ,[ccs_mult2_description]
# ,[ccs_mult2_plain_lang]
# ,[ccs_final_description]
# ,[ccs_final_plain_lang]
# ,[pc_visit]
# ,[pc_visit_id] 
# ,[last_run]


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_claim_header_f <- function(conn = NULL,
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
  temp_schema <- config[[server]][["temp_schema"]]
  temp_table <- ifelse(is.null(config[[server]][["temp_table"]]), '',
                      config[[server]][["temp_table"]])
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~80 minutes to run.")
  
  
  #### STEP 0: SET UP TEMP TABLE ####
  ### Remove table if it exists
  try(DBI::dbRemoveTable(conn, name = DBI::Id(schema = to_schema, 
                                              table = paste0(temp_table, "mcaid_claim_header"))))
  
  ### Set up temp table
  # Could turn this code into a function and add test options if desired
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT DISTINCT 
           cast([MEDICAID_RECIPIENT_ID] as varchar(255)) as id_mcaid
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
           
           INTO {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claims_header
           from {`from_schema`}.{`from_table`} as clm
           left join {`ref_schema`}.{DBI::SQL(ref_table)}kc_claim_type_crosswalk as ref
           on cast(clm.CLM_TYPE_CID as varchar(20)) = ref.source_clm_type_id",
                                .con = conn))
  
  
  #### STEP 1: SELECT HEADER-LEVEL INFORMATION NEEDED FOR EVENT FLAGS ####
  try(DBI::dbRemoveTable(conn, "##header", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT 
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
           
           --inpatient stay, clm_type_mcaid_id in (31,33) no longer works in 2019, Use HEDIS definition below
           --,case when clm_type_mcaid_id in (31,33) then 1 else 0 end as 'inpatient'
           --mental health-related DRG
           ,case when drvd_drg_code between '876' and '897' or 
            drvd_drg_code between '945' and '946' then 1 else 0 end as 'mh_drg'
           --newborn/liveborn infant-related DRG
           ,case when drvd_drg_code between '789' and '795' then 1 else 0 end as 'newborn_drg'
           --maternity-related DRG or type of bill
           ,case when type_of_bill_code in 
           ('840','841','842','843','844','845','847','848','84F','84G','84H',
              '84I','84J','84K','84M','84O','84X','84Y','84Z') or 
              drvd_drg_code between '765' and '782' 
           then 1 else 0 end as 'maternal_drg_tob'
           INTO ##header
           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claims_header",
                                .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index idx_cl_##header on ##header(claim_header_id)")
  
  
  #### STEP 2: SELECT LINE-LEVEL INFORMATION NEEDED FOR EVENT FLAGS ####
  try(DBI::dbRemoveTable(conn, "##line", temporary = T), silent = T)
  DBI::dbExecute(
    conn, glue::glue_sql(
      "select 
           claim_header_id
           --ed visits sub-flags
           --,max(case when rev_code like '045[01269]' or rev_code like '0981' then 1 else 0 end) as 'ed_rev_code'
           --Revised to match HCA-ARM Definition
           ,max(case when rev_code like '045[01269]' then 1 else 0 end) as 'ed_rev_code'
           --maternity revenue codes
           ,max(case when rev_code in ('0112','0122','0132','0142','0152','0720','0721','0722','0724')
                then 1 else 0 end) as 'maternal_rev_code'
           into ##line
           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_line
           group by claim_header_id",
      .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index idx_cl_##line on ##line(claim_header_id)")
  
  
  #### STEP 3: SELECT DX CODE INFORMATION NEEDED FOR EVENT FLAGS ####
  try(DBI::dbRemoveTable(conn, "##diag", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT dx.claim_header_id
           --primary diagnosis code with version
           ,max(case when icdcm_number = '01' then icdcm_norm else null end) as primary_diagnosis
           ,max(case when icdcm_number = '01' then icdcm_version else null end) as icdcm_version
           --mental health-related primary diagnosis (HEDIS 2017)
           ,max(case when icdcm_number = '01'
                and ((icdcm_norm between '290' and '316' and icdcm_version = 9)
                     or (icdcm_norm between 'F03' and 'F0391' and icdcm_version = 10)
                     or (icdcm_norm between 'F10' and 'F69' and icdcm_version = 10)
                     or (icdcm_norm between 'F80' and 'F99' and icdcm_version = 10))
                then 1 else 0 end) AS 'dx1_mental'
           --mental health-related, any diagnosis (HEDIS 2017)
           ,max(case when ((icdcm_norm between '290' and '316' and icdcm_version = 9)
                           or (icdcm_norm between 'F03' and 'F0391' and icdcm_version = 10)
                           or (icdcm_norm between 'F10' and 'F69' and icdcm_version = 10)
                           or (icdcm_norm between 'F80' and 'F99' and icdcm_version = 10))
                then 1 else 0 end) AS 'dxany_mental'
           --newborn-related primary diagnosis (HEDIS 2017)
           ,max(case when icdcm_number = '01'
                and ((icdcm_norm between 'V30' and 'V39' and icdcm_version = 9)
                     or (icdcm_norm between 'Z38' and 'Z389' and icdcm_version = 10))
                then 1 else 0 end) AS 'dx1_newborn'
           --maternity-related primary diagnosis (HEDIS 2017)
           ,max(case when icdcm_number = '01'
                and ((icdcm_norm between '630' and '679' and icdcm_version = 9)
                     or (icdcm_norm between 'V24' and 'V242' and icdcm_version = 9)
                     or (icdcm_norm between 'O00' and 'O9279' and icdcm_version = 10)
                     or (icdcm_norm between 'O98' and 'O9989' and icdcm_version = 10)
                     or (icdcm_norm between 'O9A' and 'O9A53' and icdcm_version = 10)
                     or (icdcm_norm between 'Z0371' and 'Z0379' and icdcm_version = 10)
                     or (icdcm_norm between 'Z332' and 'Z3329' and icdcm_version = 10)
                     or (icdcm_norm between 'Z39' and 'Z3909' and icdcm_version = 10))
                then 1 else 0 end) AS 'dx1_maternal'
           --maternity-related primary diagnosis (broader)
           ,max(case when icdcm_number = '01'
                and ((icdcm_norm between '630' and '679' and icdcm_version = 9)
                     or (icdcm_norm between 'V20' and 'V29' and icdcm_version = 9) /*broader*/
                       or (icdcm_norm between 'O00' and 'O9279' and icdcm_version = 10)
                     or (icdcm_norm between 'O94' and 'O9989' and icdcm_version = 10) /*broader*/
                       or (icdcm_norm between 'O9A' and 'O9A53' and icdcm_version = 10)
                     or (icdcm_norm between 'Z0371' and 'Z0379' and icdcm_version = 10)
                     or (icdcm_norm between 'Z30' and 'Z392' and icdcm_version = 10) /*broader*/
                       or (icdcm_norm between 'Z3A0' and 'Z3A49' and icdcm_version = 10)) /*broader*/
                  then 1 else 0 end) AS 'dx1_maternal_broad'
           --SDOH-related (any diagnosis)
           ,max(case when icdcm_norm between 'Z55' and 'Z659' and icdcm_version = 10
                then 1 else 0 end) AS 'sdoh_any'
           --Primary care-related visits
           ,MAX(CASE WHEN dx.icdcm_number IN ('01', '02') AND dx.icdcm_version = 10 AND 
            pc_ref.pc_dxcode = 1 THEN 1 ELSE 0 END) AS 'pc_zcode' 
           INTO ##diag FROM
           (select claim_header_id, icdcm_number, icdcm_norm, icdcm_version
           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header) AS dx
           LEFT JOIN
           (SELECT code, 1 AS pc_dxcode FROM {`ref_schema`}.{DBI::SQL(ref_table)}pc_visit_oregon 
           WHERE code_system IN ('icd10cm')) pc_ref
           ON dx.icdcm_norm = pc_ref.code
           GROUP BY dx.claim_header_id",
                                .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index idx_cl_##diag on ##diag(claim_header_id)")
  
  
  #### STEP 4: SELECT PROCEDURE CODE INFORMATION NEEDED FOR EVENT FLAGS ####
  try(DBI::dbRemoveTable(conn, "##procedure_code", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT px.claim_header_id 
           --ed visits sub-flags
           ,max(case when px.procedure_code like '9928[123458]' then 1 else 0 end) as 'ed_pcode1'
           -- Dropped to match HCA-ARM Definition
           --,max(case when px.procedure_code between '10021' and '69990' then 1 else 0 end) as 'ed_pcode2'
           ,MAX(ISNULL(pc_ref.pc_pcode, 0)) AS pc_pcode 
           INTO ##procedure_code FROM
           (SELECT claim_header_id, procedure_code FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_procedure) AS px
           LEFT JOIN
           (SELECT code, 1 AS pc_pcode FROM {`ref_schema`}.{DBI::SQL(ref_table)}pc_visit_oregon 
           WHERE code_system IN ('cpt', 'hcpcs')) pc_ref
           ON px.procedure_code = pc_ref.code
           GROUP BY claim_header_id",
                                .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index idx_cl_#procedure_code on ##procedure_code(claim_header_id)")
  
  
  #### STEP 5: HEDIS INPATIENT DEFINITION ####
  try(DBI::dbRemoveTable(conn, "##hedis_inpatient_definition", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql(
                   "SELECT [id_mcaid]
                 ,[claim_header_id]
                 ,[first_service_date]
                 ,1 AS [inpatient]
                 INTO ##hedis_inpatient_definition
                 FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_line AS a
                 INNER JOIN {`ref_schema`}.{DBI::SQL(ref_table)}hedis_code_system AS b
                 ON [value_set_name] IN ('Inpatient Stay')
                  AND [code_system] = 'UBREV'
                  AND a.[rev_code] = b.[code]
                  
                  EXCEPT
                  (SELECT [id_mcaid]
                    ,[claim_header_id]
                    ,[first_service_date]
                    ,1 AS [inpatient]
                    FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_line AS a
                    INNER JOIN {`ref_schema`}.{DBI::SQL(ref_table)}hedis_code_system AS b
                    ON [value_set_name] IN ('Nonacute Inpatient Stay')
                      AND [code_system] = 'UBREV'
                      AND a.[rev_code] = b.[code]
                  UNION
                  SELECT [id_mcaid]
                    ,[claim_header_id]
                    ,[first_service_date]
                    ,1 AS [inpatient
                    FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_header AS a
                    INNER JOIN {`ref_schema`}.{DBI::SQL(ref_table)}hedis_code_system AS b
                    ON [value_set_name] IN ('Nonacute Inpatient Stay')
                      AND [code_system] = 'UBTOB'
                      AND a.[type_of_bill_code] = b.[code]
                  );", .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index idx_cl_##hedis_inpatient_definition on ##hedis_inpatient_definition([claim_header_id])")
  
  
  #### STEP 6: PRIMARY CARE PROVIDERS ####
  try(DBI::dbRemoveTable(conn, "##pc_provider", temporary = T), silent = T)
  
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT a.billing_provider_npi, ISNULL(b.pc_provider , 0) AS pc_provider
  INTO ##pc_provider
  FROM
  (SELECT DISTINCT billing_provider_npi FROM ##header) a
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
  try(DBI::dbRemoveTable(conn, "##temp1", temporary = T), silent = T)
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
           --Mental health-related primary diagnosis
           ,case when header.mh_drg = 1 or diag.dx1_mental = 1 then 1 else 0 end as 'mental_dx1'
           --Mental health-related, any diagnosis
           ,case when header.mh_drg = 1 or diag.dxany_mental = 1 then 1 else 0 end as 'mental_dxany'
           --Maternity-related care (primary diagnosis only)
           ,case when header.maternal_drg_tob = 1 or line.maternal_rev_code = 1 or diag.dx1_maternal = 1 then 1 else 0 end as 'maternal_dx1'
           --Maternity-related care (primary diagnosis only), broader definition for diagnosis codes
           ,case when header.maternal_drg_tob = 1 or line.maternal_rev_code = 1 or diag.dx1_maternal_broad = 1 then 1 else 0 end as 'maternal_broad_dx1'
           --Newborn-related care (prim. diagnosis only)
           ,case when header.newborn_drg = 1 or diag.dx1_newborn = 1 then 1 else 0 end as 'newborn_dx1'
           --Inpatient stay flag [not using claim_type_mcaid_id any more]
           --,header.inpatient
		       ,hedis.inpatient
           --ED visit (broad definition)
           ,case when header.clm_type_mcaid_id in (3,26,34)
           and (line.ed_rev_code = 1 or procedure_code.ed_pcode1 = 1 or header.place_of_service_code = '23') then 1 else 0 end as 'ed'
           --Revised to match HCA-ARM Definition
           --and (line.ed_rev_code = 1 or procedure_code.ed_pcode1 = 1 or (header.place_of_service_code = '23' and ed_pcode2 = 1)) then 1 else 0 end as 'ed'
           --Primary diagnosis and version
           
           ,diag.primary_diagnosis
           ,diag.icdcm_version
           ,header.primary_diagnosis_poa
           --SDOH flags
           ,diag.sdoh_any
           --Primary care visit
           ,CASE WHEN (diag.pc_zcode = 1 OR procedure_code.pc_pcode = 1) AND
            pc_provider.pc_provider = 1 AND 
            header.clm_type_mcaid_id NOT IN (19, 31, 33) --Ambulatory surgery centers/inpatient 
            THEN 1 ELSE 0 END AS pc_visit
           
           INTO ##temp1
           FROM ##header as header
           left join ##line as line 
           on header.claim_header_id = line.claim_header_id
           left join ##diag as diag 
           on header.claim_header_id = diag.claim_header_id
           left join ##procedure_code as procedure_code 
           on header.claim_header_id = procedure_code.claim_header_id
		       left join ##hedis_inpatient_definition as hedis
		       on header.claim_header_id = hedis.claim_header_id
           LEFT JOIN ##pc_provider AS pc_provider
           ON header.billing_provider_npi = pc_provider.billing_provider_npi",
                                .con = conn))
  
  
  #### STEP 8: AVOIDABLE ED VISIT FLAG, CALIFORNIA ALGORITHM ####
  try(DBI::dbRemoveTable(conn, "##avoid_ca", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT 
           b.claim_header_id
           ,max(a.ed_avoid_ca) as 'ed_avoid_ca'
           INTO ##avoid_ca
           FROM (select dx, dx_ver, ed_avoid_ca from {`ref_schema`}.{DBI::SQL(ref_table)}dx_lookup where ed_avoid_ca = 1) as a
           inner join (select claim_header_id, icdcm_norm, icdcm_version FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header where icdcm_number = '01') as b
           on (a.dx_ver = b.icdcm_version) and (a.dx = b.icdcm_norm)
           group by b.claim_header_id",
                                .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##avoid_ca] on ##avoid_ca(claim_header_id)")
  
  
  #### STEP 9: ED CLASSIFICATION, NYU ALGORITHM ####
  try(DBI::dbRemoveTable(conn, "##avoid_nyu'", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("select 
           b.claim_header_id
           ,a.ed_needed_unavoid_nyu
           ,a.ed_needed_avoid_nyu
           ,a.ed_pc_treatable_nyu
           ,a.ed_nonemergent_nyu
           ,a.ed_mh_nyu
           ,a.ed_sud_nyu
           ,a.ed_alc_nyu
           ,a.ed_injury_nyu
           ,a.ed_unclass_nyu
           into ##avoid_nyu
           from {`ref_schema`}.{DBI::SQL(ref_table)}dx_lookup as a
           inner join (select claim_header_id, icdcm_norm, icdcm_version 
           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header where icdcm_number = '01') as b
           on a.dx_ver = b.icdcm_version and a.dx = b.icdcm_norm",
                                .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##avoid_nyu] on ##avoid_nyu(claim_header_id)")
  
  
  #### STEP 10: CCS GROUPINGS (CCS, CCS-LEVEL 1, CCS-LEVEL 2), PRIMARY DX, FINAL CATEGORIZATION ####
  try(DBI::dbRemoveTable(conn, "##ccs'", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT 
           b.claim_header_id
           ,a.ccs
           ,a.ccs_description
           ,a.ccs_description_plain_lang
           ,a.multiccs_lv1
           ,a.multiccs_lv1_description
           ,a.multiccs_lv2
           ,a.multiccs_lv2_description
           ,a.multiccs_lv2_plain_lang
           ,a.ccs_final_code
           ,a.ccs_final_description
           ,a.ccs_final_plain_lang
           INTO ##ccs
           FROM {`ref_schema`}.{DBI::SQL(ref_table)}dx_lookup as a
           inner join (select claim_header_id, icdcm_norm, icdcm_version 
           FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header where icdcm_number = '01') as b
           on a.dx_ver = b.icdcm_version and a.dx = b.icdcm_norm",
                                .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##ccs] on ##ccs(claim_header_id)")
  
  
  #### STEP 11: RDA MENTAL HEALTH AND SUBSTANCE USE DISORDER DX FLAGS, ANY DX ####
  try(DBI::dbRemoveTable(conn, "##rda'", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT 
           b.claim_header_id
           ,max(a.mental_dx_rda) as 'mental_dx_rda_any'
           ,max(a.sud_dx_rda) as 'sud_dx_rda_any'
           INTO ##rda
           FROM {`ref_schema`}.{DBI::SQL(ref_table)}dx_lookup as a
           inner join {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header as b
           on a.dx_ver = b.icdcm_version and a.dx = b.icdcm_norm
           group by b.claim_header_id",
                                .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##rda] on ##rda(claim_header_id)")
  
  
  #### STEP 12: INJURY INTENT AND MECHANISM, ICD9-CM ####
  try(DBI::dbRemoveTable(conn, "##injury9cm", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT 
           c.claim_header_id
           ,c.intent
           ,c.mechanism
           INTO ##injury9cm
           FROM 
           (--find external cause codes (ICD9-CM) for each TCN, then rank by diagnosis number
             SELECT 
             b.claim_header_id
             ,intent
             ,mechanism
             ,row_number() over (partition by b.claim_header_id order by b.icdcm_number) as 'diag_rank'
             FROM (SELECT 
                dx, intent, mechanism FROM {`ref_schema`}.{DBI::SQL(ref_table)}dx_lookup 
                WHERE intent is not null and dx_ver = 9) as a
             INNER JOIN 
             (SELECT claim_header_id, icdcm_norm, icdcm_number 
             FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header 
             WHERE icdcm_version = 9) as b
             on (a.dx = b.icdcm_norm)
           ) as c
           --only keep the highest ranked external cause code per claim
           where c.diag_rank = 1",
                                .con = conn))
  
  
  #### STEP 13: INJURY INTENT AND MECHANISM, ICD10-CM ####
  try(DBI::dbRemoveTable(conn, "##injury10cm", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT 
           b.claim_header_id
           INTO ##inj10_temp1
           FROM (
            SELECT dx, injury_icd10cm 
            FROM {`ref_schema`}.{DBI::SQL(ref_table)}dx_lookup 
            WHERE injury_icd10cm = 1 and dx_ver = 10) as a
          INNER JOIN 
            (SELECT claim_header_id, icdcm_norm 
            FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header 
            WHERE icdcm_number = '01' and icdcm_version = 10) as b
          ON a.dx = b.icdcm_norm;
           
           --grab the full list of diagnosis codes for these injury claims
           if object_id('tempdb..#inj10_temp2') is not null 
           drop table ##inj10_temp2;
           SELECT 
           b.claim_header_id
           ,b.icdcm_norm
           ,b.icdcm_number
           INTO ##inj10_temp2
           FROM ##inj10_temp1 as a
           INNER JOIN 
            (SELECT claim_header_id, icdcm_norm, icdcm_number 
            FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header 
            WHERE icdcm_version = 10) as b
           ON a.claim_header_id = b.claim_header_id;
           
           --grab the highest ranked external cause code for each injury claim
           if object_id('tempdb..#injury10cm') is not null 
           drop table ##injury10cm;
           SELECT 
           c.claim_header_id
           ,c.intent
           ,c.mechanism
           INTO ##injury10cm
           FROM 
            (SELECT b.claim_header_id
              ,intent
              ,mechanism
              ,row_number() over (partition by b.claim_header_id order by b.icdcm_number) as 'diag_rank'
            FROM (
              SELECT dx, dx_ver, intent, mechanism 
              FROM {`ref_schema`}.{DBI::SQL(ref_table)}dx_lookup 
              WHERE intent is not null and dx_ver = 10) as a
            INNER JOIN ##inj10_temp2 as b
            ON a.dx = b.icdcm_norm
           ) as c
           WHERE c.diag_rank = 1",
                                .con = conn))
  
  # Clean up temp tables from this stage
  try(DBI::dbRemoveTable(conn, "##inj10_temp1", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##inj10_temp2", temporary = T), silent = T)
  
  
  #### STEP 14: UNION ICD9-CM AND ICD10-CM INJURY TABLES ####
  try(DBI::dbRemoveTable(conn, "##injury", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT claim_header_id, intent, mechanism 
                          INTO ##injury 
                          FROM ##injury9cm
                          UNION
                          SELECT claim_header_id, intent, mechanism 
                          FROM ##injury10cm",
                                .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##injury] on ##injury(claim_header_id)")
  
  
  #### STEP 15: CREATE ID COLUMNS FOR EVENTS THAT ARE ONLY COUNTED ONCE PER DAY ####
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
  
  try(DBI::dbRemoveTable(conn, "##ed_yale_step_1", temporary = T), silent = T)
  DBI::dbExecute(
    conn, glue::glue_sql("SELECT [id_mcaid]
                        ,[claim_header_id]
                        ,[first_service_date]
                        ,[last_service_date]
                        ,'Carrier' as [ed_type]
                      INTO ##ed_yale_step_1
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_procedure
                      WHERE [procedure_code] in ('99281','99282','99283','99284','99285','99291')
                        AND [claim_header_id] in 
                          (SELECT [claim_header_id]
                           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claims_header
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
                           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claims_header
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
                           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claims_header
                           -- [claim_type_id] = 1, Inpatient Facility, [claim_type_id] = 4, Outpatient Facility
                           WHERE [claim_type_id] IN (1, 4))
                      UNION
                      SELECT [id_mcaid]
                        ,[claim_header_id]
                        ,[first_service_date]
                        ,[last_service_date]
                        ,'Facility' as [ed_type]
                      FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claims_header
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
                           FROM {`temp_schema`}.{DBI::SQL(temp_table)}mcaid_claims_header
                           -- [claim_type_id] = 1, Inpatient Facility, [claim_type_id] = 4, Outpatient Facility
                           WHERE [claim_type_id] IN (1, 4));",
                         .con = conn))
  
  # Label duplicate/adjacent visits with a single [ed_pophealth_id]
  try(DBI::dbRemoveTable(conn, "##ed_yale_final", temporary = T), silent = T)
  DBI::dbExecute(
    conn, glue::glue_sql(
      "WITH [increment_stays_by_person] AS
    (SELECT [id_mcaid]
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
    FROM ##ed_yale_step_1
    --ORDER BY [id_mcaid], [first_service_date], [last_service_date], [claim_header_id]
    ),
    
    /*
      Sum [increment] column (Cumulative Sum) within person to create an stay_id that
    combines duplicate/overlapping ED visits.
    */
  [create_within_person_stay_id] AS
    (SELECT [id_mcaid]
      ,[claim_header_id]
      ,[prior_first_service_date]
      ,[first_service_date]
      ,[last_service_date]
      ,[ed_type]
      ,[date_diff]
      ,[increment]
      ,SUM([increment]) OVER(PARTITION BY [id_mcaid] 
                             ORDER BY [first_service_date], [last_service_date], [claim_header_id] ROWS UNBOUNDED PRECEDING) + 1 AS [within_person_stay_id]
    FROM [increment_stays_by_person]
    --ORDER BY [id_mcaid], [first_service_date], [last_service_date], [claim_header_id]
    )
    
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
    INTO ##ed_yale_final
    FROM [create_within_person_stay_id]
    ORDER BY [id_mcaid], [first_service_date], [last_service_date], [claim_header_id];",
      .con = conn))
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##ed_yale_final] on ##ed_yale_final(claim_header_id)")
 
  
  # Set up ED IDs
  try(DBI::dbRemoveTable(conn, "##ed_perform_id", temporary = T), silent = T)
  DBI::dbExecute(
    conn, "SELECT [claim_header_id]
          ,CASE WHEN [ed] = 0 THEN null
            ELSE dense_rank() OVER(ORDER BY CASE WHEN [ed] = 0 THEN 2 ELSE 1 END, 
                                   [id_mcaid], [first_service_date]) end as [ed_perform_id]
            INTO ##ed_perform_id
            FROM ##temp1;")
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##ed_perform_id] on ##ed_perform_id(claim_header_id)")
  
  
  # Create [inpatient_id] column
  try(DBI::dbRemoveTable(conn, "##inpatient_id", temporary = T), silent = T)
  DBI::dbExecute(conn, 
                 "SELECT [id_mcaid]
                  ,[claim_header_id]
                  ,[first_service_date]
                  ,[inpatient]
                  ,DENSE_RANK() OVER(ORDER BY [id_mcaid], [first_service_date]) AS [inpatient_id]
                 INTO ##inpatient_id
                 FROM ##hedis_inpatient_definition;")
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##inpatient_id] on ##inpatient_id([claim_header_id])") 
  
  
  # Create [pc_visit_id] column
  try(DBI::dbRemoveTable(conn, "##pc_visit_id", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 "SELECT [id_mcaid]
                ,[claim_header_id]
                ,[first_service_date]
                ,[pc_visit]
                ,DENSE_RANK() OVER(ORDER BY [id_mcaid], [first_service_date]) AS [pc_visit_id]
               INTO ##pc_visit_id
               FROM ##temp1
               WHERE pc_visit = 1;")
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##pc_visit_id] on ##pc_visit_id([claim_header_id])")
  
  
  #### STEP 16: CREATE FLAGS THAT REQUIRE COMPARISON OF PREVIOUSLY CREATED EVENT-BASED FLAGS ACROSS TIME ####
  try(DBI::dbRemoveTable(conn, "##temp2", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 "SELECT temp1.*, case when ed_nohosp.ed_nohosp = 1 then 1 else 0 end as 'ed_nohosp'
           INTO ##temp2
           FROM ##temp1 as temp1
           --ED flag that rules out visits with an inpatient stay within 24hrs
           LEFT JOIN (
             SELECT y.id_mcaid, y.claim_header_id, ed_nohosp = 1
             FROM (
               --group by ID and ED visit date and take minimum difference to get closest inpatient stay
               SELECT distinct x.id_mcaid, x.claim_header_id, min(x.eh_ddiff) as 'eh_ddiff_pmin'
               FROM (
                 SELECT distinct e.id_mcaid, ed_date = e.first_service_date, hosp_date = h.first_service_date, claim_header_id,
                 --create field that calculates difference in days between each ED visit and following inpatient stay
                 --set to null when comparison is between ED visits and PRIOR inpatient stays
                 case when datediff(dd, e.first_service_date, h.first_service_date) >=0 
                    then datediff(dd, e.first_service_date, h.first_service_date)
                  else null
                  end as 'eh_ddiff'
                 FROM ##temp1 as e
                 LEFT JOIN (
                   SELECT distinct id_mcaid, first_service_date
                   FROM ##temp1
                   WHERE inpatient = 1
                 ) as h
                 ON e.id_mcaid = h.id_mcaid
                 WHERE e.ed = 1
               ) as x
               GROUP BY x.id_mcaid, x.claim_header_id
             ) as y
             WHERE y.eh_ddiff_pmin > 1 or y.eh_ddiff_pmin is null
           ) ed_nohosp
           on temp1.claim_header_id = ed_nohosp.claim_header_id")
  
  
  # Add index
  DBI::dbExecute(conn, "create clustered index [idx_cl_##temp2] on ##temp2(claim_header_id)")
  
  
  #### STEP 17: CREATE FINAL TABLE STRUCTURE ####
  create_table_f(conn = conn, 
                 config = config,
                 server = server,
                 overwrite = T)
  
  
  #### STEP 18: CREATE FINAL SUMMARY TABLE WITH EVENT-BASED FLAGS (TEMP STAGE) ####
  try(DBI::dbRemoveTable(conn, "##temp_final", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 "SELECT 
             a.*
               --ED-related flags
             ,case when a.ed = 1 and a.mental_dxany = 1 then 1 else 0 end as 'ed_bh'
             ,case when a.ed = 1 and b.ed_avoid_ca = 1 then 1 else 0 end as 'ed_avoid_ca'
             ,case when a.ed_nohosp = 1 and b.ed_avoid_ca = 1 then 1 else 0 end as 'ed_avoid_ca_nohosp'
             
             --original nine categories of NYU ED algorithm
             ,case when a.ed = 1 and c.ed_nonemergent_nyu > 0.50 then 1 else 0 end as 'ed_ne_nyu'
             ,case when a.ed = 1 and c.ed_pc_treatable_nyu > 0.50 then 1 else 0 end as 'ed_pct_nyu'
             ,case when a.ed = 1 and c.ed_needed_avoid_nyu > 0.50 then 1 else 0 end as 'ed_pa_nyu'
             ,case when a.ed = 1 and c.ed_needed_unavoid_nyu > 0.50 then 1 else 0 end as 'ed_npa_nyu'
             ,case when a.ed = 1 and c.ed_mh_nyu > 0.50 then 1 else 0 end as 'ed_mh_nyu'
             ,case when a.ed = 1 and c.ed_sud_nyu > 0.50 then 1 else 0 end as 'ed_sud_nyu'
             ,case when a.ed = 1 and c.ed_alc_nyu > 0.50 then 1 else 0 end as 'ed_alc_nyu'
             ,case when a.ed = 1 and c.ed_injury_nyu > 0.50 then 1 else 0 end as 'ed_injury_nyu'
             
             ,case when a.ed = 1 and ((c.ed_unclass_nyu > 0.50)  or 
                                      (c.ed_nonemergent_nyu <= 0.50 and c.ed_pc_treatable_nyu <= 0.50
                                        and c.ed_needed_avoid_nyu <= 0.50 and c.ed_needed_unavoid_nyu <= 0.50 
                                        and c.ed_mh_nyu <= 0.50 and c.ed_sud_nyu <= 0.50
                                        and c.ed_alc_nyu <= 0.50 and c.ed_injury_nyu <= 0.50 and c.ed_unclass_nyu <= 0.50))
             then 1 else 0 end as 'ed_unclass_nyu'
             
             --collapsed 3 categories of NYU ED algorithm based on Ghandi et al.
             ,case when a.ed = 1 and (c.ed_needed_unavoid_nyu + c.ed_needed_avoid_nyu) > 0.50 then 1 else 0 end as 'ed_emergent_nyu'
             ,case when a.ed = 1 and (c.ed_pc_treatable_nyu + c.ed_nonemergent_nyu) > 0.50 then 1 else 0 end as 'ed_nonemergent_nyu'
             ,case when a.ed = 1 and (((c.ed_needed_unavoid_nyu + c.ed_needed_avoid_nyu) = 0.50) or 
                                      ((c.ed_pc_treatable_nyu + c.ed_nonemergent_nyu) = 0.50)) then 1 else 0 end as 'ed_intermediate_nyu'
									  
			       --Increment [ed] column by distinct [id_mcaid], [first_service_date]
			       ,h.[ed_perform_id]
			       --Yale ED MEASURE
			       ,g.[ed_pophealth_id]
             
             --Inpatient-related flags
             ,case when a.inpatient = 1 and a.mental_dx1 = 0 and a.newborn_dx1 = 0 and a.maternal_dx1 = 0 then 1 else 0 end as 'ipt_medsurg'
             ,case when a.inpatient = 1 and a.mental_dxany = 1 then 1 else 0 end as 'ipt_bh'
             ,i.[inpatient_id]
             
             --Injuries
             ,f.intent
             ,f.mechanism
             
             --CCS
             ,d.ccs
             ,d.ccs_description
             ,d.ccs_description_plain_lang
             ,d.multiccs_lv1 as 'ccs_mult1'
             ,d.multiccs_lv1_description as 'ccs_mult1_description'
             ,d.multiccs_lv2 as 'ccs_mult2'
             ,d.multiccs_lv2_description as 'ccs_mult2_description'
             ,d.multiccs_lv2_plain_lang as 'ccs_mult2_plain_lang'
             ,d.ccs_final_description
             ,d.ccs_final_plain_lang
             
             --RDA MH and SUD flags
             ,case when e.mental_dx_rda_any = 1 then 1 else 0 end as 'mental_dx_rda_any'
             ,case when e.sud_dx_rda_any = 1 then 1 else 0 end as 'sud_dx_rda_any'
             
             --SDOH ED and IPT flags
             ,case when a.ed = 1 and a.sdoh_any = 1 then 1 else 0 end as 'ed_sdoh'
             ,case when a.inpatient = 1 and a.sdoh_any = 1 then 1 else 0 end as 'ipt_sdoh'
             
             --Primary care
             ,j.pc_visit_id
             
             INTO ##temp_final
             FROM ##temp2 as a
             left join ##avoid_ca as b
             on a.claim_header_id = b.claim_header_id
             left join ##avoid_nyu as c
             on a.claim_header_id = c.claim_header_id
             left join ##ccs as d
             on a.claim_header_id = d.claim_header_id
             left join ##rda as e
             on a.claim_header_id = e.claim_header_id
             left join ##injury as f
             on a.claim_header_id = f.claim_header_id
             left join ##ed_yale_final as g
             on a.claim_header_id = g.claim_header_id			
             left join ##ed_perform_id as h
             on a.claim_header_id = h.claim_header_id
             left join ##inpatient_id as i
             on a.claim_header_id = i.claim_header_id
             left join ##pc_visit_id AS j
             on a.claim_header_id = j.claim_header_id
           ")
  
  
  #### STEP 19: COPY FINAL TEMP TABLE INTO STAGE.MCAID_CLAIM_HEADER ####
  message("Loading to final table")
  DBI::dbExecute(conn,
                 glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} 
                          ({`names(config$vars)`*}) 
                          SELECT {`names(config$vars)[names(config$vars) != 'last_run']`*}
                          , getdate() AS [last_run] 
                          FROM ##temp_final", .con = conn))
  
  
  #### STEP 20: ADD INDEX ####
  message("Creating index on final table")
  time_start <- Sys.time()
  add_index_f(conn, server = server, table_config = config)
  time_end <- Sys.time()
  message(glue::glue("Index creation took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                     " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))
  
  
  
  #### STEP 21: CLEAN UP TEMP TABLES ####
  try(DBI::dbRemoveTable(conn, name = DBI::Id(schema = "tmp", table = "mcaid_claim_header")), silent = T)
  try(DBI::dbRemoveTable(conn, "##header", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##line", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##diag", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##procedure_code", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##pc_provider", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##temp1", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##avoid_ca", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##avoid_nyu'", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##ccs'", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##rda'", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##injury9cm", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##injury10cm", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##injury", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##temp2", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##temp_final", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##ed_yale_step_1", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##ed_yale_final", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##ed_perform_id", temporary = T), silent = T)
  try(DBI::dbRemoveTable(conn, "##inpatient_id", temporary = T), silent = T)
}
