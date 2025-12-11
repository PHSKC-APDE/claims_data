##Eli's adaptation of claim_bh script to work for WA-APCD data##
## 2025-11

## Code to create stage.SOURCE_claim_bh table
## Person-level behavior health condition status by time period
## Minh Phan (DCSH-PME)
## Adapted code from Eli Kern and Alastair Matheson (PHSKC-APDE) for claim_ccw
## 2021-10
## Run time: ~3h (Medicaid/HHSAW_prod)
## Eli 9/15/24 update: Modify to use new RDA value sets reference table
## Eli 6/13/24 update: Added inthealth as server option
## Eli 6/13/24 update: Add branching logic for Rx fill date based on data source
## Eli 11/6/24 update: Simplify and revise logic such that from_date (now first_encounter_date) is earliest condition-defining encounter
  ## and to_date (now last_encounter_date) is last condition-defining encounter, removing need for rolling time window
## Eli 6/3/25 update: Revise code to create Opioid Use Disorder flag using definition developed through OD2A: LOCAL grant
  
### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# source = which CCW table is being built
# config = config file already in memory
# config_url = URL location of YAML config file (should be blank if using config_file)
# config_file = path + file name of YAML config file (should be blank if using config_url)
# test_rows = number of rows to load if testing function (integer)

load_bh <- function(conn = NULL,
                     server = c("phclaims", "hhsaw", "inthealth"),
                     source = c("apcd", "mcaid", "mcare", "mcaid_mcare"),
                     config = NULL,
                     config_url = NULL,
                     config_file = NULL,
                     test_rows = NULL) {
  
  
  #### ERROR CHECKS ####
  # Make sure a connection to the DB exists
  if (is.null(conn)) {stop("No ODBC connection supplied")}
  
  
  #### SET UP PARAMETERS ETC. ####
  # Figure out which server is being used and which CCW table is being made
  server <- match.arg(server)
  source <- match.arg(source)
  
  
  # Check libraries are called in and load if not
  packages <- list("dplyr", "ggplot2", "tidyr", "readr", "purrr", "tibble", 
                   "stringr", "forcats", "odbc", "yaml","tidyverse","glue")
  
  lapply(packages, function (x) {
    if (!x %in% (.packages())) {
      suppressPackageStartupMessages(library(x, character.only = T))
    }
  })
  
  
  # Specify id variable name based on data source
  if (source == "mcaid_mcare") {
    id_source <- "id_apde"
  } else {
    id_source <- paste0("id_", source)
  }
  
  # Specify Rx filled date variable name based on data source
  # Adapted for WA-APCD
  if (source %in% c("mcaid_mcare", "mcaid")) {
    rx_fill_date <- "rx_fill_date"
  } else if (source %in% c("apcd")) {
    rx_fill_date <- "prescription_filled_dt"
  } else {
    rx_fill_date <- "last_service_date"
  }
  
  # Specify NDC variable based on data source (needed for WA-APCD)
  if (source %in% c("apcd")) {
    ndc <- "national_drug_code"
  } else {
    ndc <- "ndc"
  }
  
  # Set up test number of rows if needed
  if (is.null(test_rows)) {
    top_rows = DBI::SQL('')
  } else {
    if (!is.integer(test_rows)) {
      stop("Select an integer number of test_rows")
    } else {
      top_rows = glue_sql(" TOP {test_rows} ", .con = conn)
    }
  }
  
  
  #### STEP 1: LOAD CONFIG FILE AND PARAMETERS ####
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else if (!is.null(config_file)) {
    table_config <- yaml::read_yaml(config_file)
  } else {
    table_config <- yaml::yaml.load(httr::GET(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.", source, "_claim_bh.yaml")))
  }
  
  #conditions <- names(table_config[str_detect(names(table_config), "cond_")])
  vars <- table_config$vars
  
  if (server %in% names(table_config)) {
    schema <- table_config[[server]][["to_schema"]][[1]]
    to_table <- table_config[[server]][["to_table"]][[1]]
    claim_header_from_schema <- table_config[[server]][["claim_header_from_schema"]][[1]]
    claim_header_from_table <- table_config[[server]][["claim_header_from_table"]][[1]]
    claim_pharm_from_schema <- table_config[[server]][["claim_pharm_from_schema"]][[1]]
    claim_pharm_from_table <- table_config[[server]][["claim_pharm_from_table"]][[1]]
    claim_procedure_from_schema <- table_config[[server]][["claim_procedure_from_schema"]][[1]]
    claim_procedure_from_table <- table_config[[server]][["claim_procedure_from_table"]][[1]]
    icdcm_from_schema <- table_config[[server]][["icdcm_from_schema"]][[1]]
    icdcm_from_table <- table_config[[server]][["icdcm_from_table"]][[1]]
    ref_schema <- table_config[[server]][["ref_schema"]][[1]]
    ref_table <- table_config[[server]][["ref_table"]][[1]]
    icdcm_ref_schema <- table_config[[server]][["icdcm_ref_schema"]][[1]]
    icdcm_ref_table <- table_config[[server]][["icdcm_ref_table"]][[1]]
  } else {
    schema <- table_config[["schema"]][[1]]
    to_table <- table_config[["to_table"]][[1]]
    claim_header_from_schema <- table_config[["claim_header_from_schema"]][[1]]
    claim_header_from_table <- table_config[["claim_header_from_table"]][[1]]
    claim_pharm_from_schema <- table_config[["claim_pharm_from_schema"]][[1]]
    claim_pharm_from_table <- table_config[["claim_pharm_from_table"]][[1]]
    claim_procedure_from_schema <- table_config[["claim_procedure_from_schema"]][[1]]
    claim_procedure_from_table <- table_config[["claim_procedure_from_table"]][[1]]
    icdcm_from_schema <- table_config[["icdcm_from_schema"]][[1]]
    icdcm_from_table <- table_config[["icdcm_from_table"]][[1]]
    # Assumes working in PHClaims for ref data if using older YAML format
    ref_schema <- "ref"
    ref_table <- ""
    icdcm_ref_schema <- table_config[["icdcm_ref_schema"]][[1]]
    icdcm_ref_table <- table_config[["icdcm_ref_table"]][[1]]
  }
  
  
  
  #### STEP 2: CREATE TABLE ####
  # Set up table name
  tbl_name <- DBI::Id(schema = schema, table = to_table)
  
  # Remove table if it exists
  try(dbRemoveTable(conn, tbl_name), silent = T)
  
  # Create table
  DBI::dbCreateTable(conn, tbl_name, fields = table_config$vars)

  #### STEP 3: CREATE TEMP TABLE TO HOLD CONDITION-SPECIFIC CLAIMS AND DATES ####
  #Excluding Opioid Use Disorder (OUD), which is flagged using condition-specific logic
  #conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  message("STEP 1: CREATE TEMP TABLE TO HOLD CONDITION-SPECIFIC CLAIMS AND DATES")
  time_start <- Sys.time()
  
    # Build SQL query
    sql1 <- glue_sql("
     SELECT DISTINCT  
      {`id_source`}
   ,svc_date
   ,bh_cond
   INTO {`schema`}.tmp_header_bh
   --BASED ON DIAGNOSIS
   FROM (SELECT 
      {`id_source`}
	    ,svc_date
      ,b.sub_group_condition as 'bh_cond'
        FROM  (SELECT DISTINCT {`id_source`}, icdcm_norm, icdcm_version, first_service_date as 'svc_date'
                FROM {`icdcm_from_schema`}.{`icdcm_from_table`} 
                ) as a
        INNER JOIN (SELECT sub_group_condition, code_set, code, icdcm_version, value_set_name
    		            FROM  {`ref_schema`}.{`ref_table`}
    		            WHERE code_set in ('ICD9CM', 'ICD10CM') and sub_group_condition not in ('sud_opioid')
    		            ) as b
    		ON (a.icdcm_norm = b.code) and (a.icdcm_version = b.icdcm_version) 
   ) diag
   
   UNION
   
   SELECT DISTINCT  
    {`id_source`}
   ,svc_date
   ,bh_cond
   -- BASED ON PRESCRIPTIONS
   FROM (SELECT DISTINCT a.{`id_source`}
			,a.{`rx_fill_date`} as 'svc_date'
	    ,b.sub_group_condition as 'bh_cond'
    FROM {`claim_pharm_from_schema`}.{`claim_pharm_from_table`} a
    INNER JOIN (SELECT sub_group_condition, code_set, code
    	          FROM {`ref_schema`}.{`ref_table`}
    	          WHERE code_set in ('NDC') and sub_group_condition not in ('sud_opioid')) as b
    ON a.{`ndc`} = b.code
          ) rx
      ",.con = conn)
    
    #Run SQL query
    try(DBI::dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_header_bh")), silent = T)
    DBI::dbGetQuery(conn = conn, sql1)
    
    #### STEP 4: COLLAPSE TO FIRST AND LAST ENCOUNTER DATE FOR EACH PERSON-CONDITION ####
    #conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    message("STEP 2: COLLAPSE TO FIRST AND LAST ENCOUNTER DATE FOR EACH PERSON-CONDITION")
    # Build SQL query
    sql2 <- glue_sql("
      SELECT
        {`id_source`}
        ,min(svc_date) as 'first_encounter_date'
        ,max(svc_date) as 'last_encounter_date'
        ,bh_cond
      INTO {`schema`}.tmp_collapse_bh
      FROM {`schema`}.tmp_header_bh
      GROUP BY {`id_source`}, bh_cond
      ",.con = conn)
    
    #Run SQL query
    try(DBI::dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_collapse_bh")), silent = T)
    DBI::dbGetQuery(conn = conn, sql2)
    
    #### STEP 5: CREATE TEMP TABLE TO HOLD PERSON-MONTHS WITH OUD DIAGNOSES #### 
    
    message("STEP 3: CREATE TEMP TABLE TO HOLD PERSON-MONTHS WITH OUD DIAGNOSES")
    # Build SQL query
    sql3 <- glue_sql("
    --Identify all OUD-relevant claims using diagnosis, drug and procedure codes
    
    --to prep for procedure code step, extract all claims with primary OUD diagnosis
    with oud_dx1 as (
    	SELECT DISTINCT
    	claim_header_id
    	,1 as oud_dx1
    	FROM  (
    		SELECT claim_header_id, primary_diagnosis, icdcm_version
    		FROM {`claim_header_from_schema`}.{`claim_header_from_table`}
    	) as a
    	INNER JOIN (
    		SELECT sub_group_condition, code_set, code, icdcm_version, value_set_name
    	FROM  {`ref_schema`}.{`ref_table`}
    	WHERE code_set in ('ICD9CM', 'ICD10CM') and sub_group_condition = 'sud_opioid'
    	) as b
    	ON (a.primary_diagnosis = b.code) and (a.icdcm_version = b.icdcm_version)
    ),

    oud_claims as (
    	SELECT  
    	coalesce(diag.{`id_source`}, rx.{`id_source`}, pcode.{`id_source`}) as {`id_source`}
    	,coalesce(diag.claim_header_id, rx.claim_header_id, pcode.claim_header_id) as claim_header_id
    	,coalesce(diag.svc_date, rx.svc_date, pcode.svc_date) as svc_date
    	,diag.icdcm_flag
    	,rx.rx_flag
    	,pcode.pcode_flag
    	--BASED ON DIAGNOSIS
    	FROM (
    		SELECT DISTINCT
    		{`id_source`}
    		,claim_header_id
    		,svc_date
    		,1 as icdcm_flag
    		FROM  (
    			SELECT DISTINCT {`id_source`}, claim_header_id, icdcm_norm, icdcm_version, first_service_date as 'svc_date'
    			FROM {`icdcm_from_schema`}.{`icdcm_from_table`}
    		) as a
    		INNER JOIN (
    			SELECT sub_group_condition, code_set, code, icdcm_version, value_set_name
    		FROM  {`ref_schema`}.{`ref_table`}
    		WHERE code_set in ('ICD9CM', 'ICD10CM') and sub_group_condition = 'sud_opioid'
    		) as b
    		ON (a.icdcm_norm = b.code) and (a.icdcm_version = b.icdcm_version) 
    	) as diag
       
    	FULL JOIN (
    
    		SELECT DISTINCT  
    		{`id_source`}
    		,claim_header_id
    		,svc_date
    		,1 as rx_flag
    		-- BASED ON PRESCRIPTIONS
    		FROM (
    			SELECT DISTINCT
    			a.{`id_source`}, a.claim_header_id, a.{`rx_fill_date`} as 'svc_date'
    			FROM {`claim_pharm_from_schema`}.{`claim_pharm_from_table`} as a
    			INNER JOIN (
    				SELECT sub_group_condition, code_set, code
    				FROM {`ref_schema`}.{`ref_table`}
    				WHERE code_set in ('NDC') and sub_group_condition = 'sud_opioid'
    			) as b
    			ON a.{`ndc`} = b.code
    		) as c
    	) as rx
    	on diag.claim_header_id = rx.claim_header_id
    
    	FULL JOIN (
    
    		SELECT DISTINCT
    		{`id_source`}
    		,claim_header_id
    		,svc_date
    		,1 as pcode_flag
    		--BASED ON PROCEDURE CODES
    		FROM (
    			SELECT DISTINCT a.{`id_source`}
    			,a.claim_header_id
    			,a.first_service_date as 'svc_date'
    			,case
				    when b.oud_dx1_flag = 0 then 1
				    when b.oud_dx1_flag = 1 and c.oud_dx1 = 1 then 1
				    else 0
			    end as oud_dx1_flag	
    			FROM {`claim_procedure_from_schema`}.{`claim_procedure_from_table`} as a
    			INNER JOIN (
    				SELECT sub_group_condition, code_set, code, oud_dx1_flag
    				FROM {`ref_schema`}.{`ref_table`}
    				WHERE value_set_name in ('apde-moud-procedure') and sub_group_condition = 'sud_opioid'
    			) as b
    			ON a.procedure_code = b.code
    			LEFT JOIN oud_dx1 as c
			    on a.claim_header_id = c.claim_header_id
    		) as c
    		where c.oud_dx1_flag = 1 -- ensure restriction for procedure codes requiring dx1 = OUD
    	) as pcode
    	on diag.claim_header_id = pcode.claim_header_id
    ),
    
    --Identify all person-months where people have an OUD diagnosis
    oud_diag_month as (
    	select distinct a.{`id_source`},
    	b.first_day_month as first_encounter_date,
    	b.last_day_month as last_encounter_date
    	from oud_claims as a
    	inner join {`schema`}.ref_date as b
    	on a.svc_date = b.[date]
    	where a.icdcm_flag = 1
    ),
    
    --Identify earliest OUD diagnosis month for each person
    oud_diag_month_min as (
    	select {`id_source`},
    	min(first_encounter_date) as oud_diag_min
    	from oud_diag_month
    	group by {`id_source`}
    ),
    
    --For MOUD headers not associated with an OUD diagnosis on the claim, flag those where an OUD diagnosis occurred anytime before the claim
    moud_oud_ever as (
    	select distinct
    	a.*,
    	case when b.oud_diag_min <= a.svc_date then 1 else 0 end as moud_include
    	from (
    		select *
    		from oud_claims
    		where icdcm_flag is null
    	) as a
    	inner join (
    		select *
    		from oud_diag_month_min
    	) as b
    	on a.{`id_source`} = b.{`id_source`}
    ),
    
    --Identify all months with MOUD ever claims
    moud_oud_ever_month as (
    	select distinct {`id_source`},
    	b.first_day_month as first_encounter_date,
    	b.last_day_month as last_encounter_date
    	from moud_oud_ever as a
    	inner join {`schema`}.ref_date as b
    	on a.svc_date = b.[date]
    	where moud_include = 1
    )
    
    --Union both sets of OUD-relevant person-months
    select *, 'sud_opioid' as bh_cond
    into {`schema`}.tmp_header_oud
    from oud_diag_month
    union select *, 'sud_opioid' as bh_cond
    from moud_oud_ever_month;

      ",.con = conn)
    
    #Run SQL query
    try(DBI::dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_header_oud")), silent = T)
    DBI::dbGetQuery(conn = conn, sql3)
    
    
    #### STEP 6: INSERT ALL CONDITION TABLES INTO FINAL STAGE TABLE #### 
    #conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    message("STEP 4: INSERT ALL CONDITIONS INTO FINAL STAGE TABLE")
    # Build SQL query
    sql4 <- glue_sql(
      "INSERT INTO {`schema`}.{`to_table`}
      SELECT
      {`id_source`}, first_encounter_date, last_encounter_date, bh_cond, 
      getdate() as last_run
      FROM {`schema`}.tmp_collapse_bh
      
      UNION
      
      SELECT
      {`id_source`}, first_encounter_date, last_encounter_date, bh_cond, 
      getdate() as last_run
      FROM {`schema`}.tmp_header_oud;",
      .con = conn)
    
    #Run SQL query
    dbGetQuery(conn = conn, sql4)  
    
    try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_header_bh")), silent = T)
    try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_collapse_bh")), silent = T)
    try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_header_oud")), silent = T)
  
  #Run time of all steps
    time_end <- Sys.time()
    message(glue::glue("Table creation took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                       " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))
}

### TEST RUN
#hhsaw_prod <- DBI::dbConnect(odbc::odbc(), dsn = 'HHSAW_prod')
#test_mcaid <- load_bh(conn=hhsaw_prod, server='hhsaw',source="mcaid")