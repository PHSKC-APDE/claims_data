## Code to create stage.SOURCE_claim_bh table
## Person-level behavior health condition status by time period
## Minh Phan (DCSH-PME)
## Adapted code from Eli Kern and Alastair Matheson (PHSKC-APDE) for claim_ccw
## 2021-10
## Run time: ~3h (Medicaid/HHSAW_prod)
## Eli 9/15/24 update: Modify to use new RDA value sets reference table
## Eli 6/13/24 update: Added inthealth as server option
## Eli 6/13/24 update: Add branching logic for Rx fill date based on data source

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
  if (source %in% c("mcaid_mcare", "mcaid")) {
    rx_fill_date <- "rx_fill_date"
  } else {
    rx_fill_date <- "last_service_date"
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
    icdcm_from_schema <- table_config[[server]][["icdcm_from_schema"]][[1]]
    icdcm_from_table <- table_config[[server]][["icdcm_from_table"]][[1]]
    ref_schema <- table_config[[server]][["ref_schema"]][[1]]
    ref_table <- table_config[[server]][["ref_table"]][[1]]
    icdcm_ref_schema <- table_config[[server]][["icdcm_ref_schema"]][[1]]
    icdcm_ref_table <- table_config[[server]][["icdcm_ref_table"]][[1]]
    rolling_schema <- table_config[[server]][["rolling_schema"]][[1]] 
    rolling_table <- table_config[[server]][["rolling_table"]][[1]] 
  } else {
    schema <- table_config[["schema"]][[1]]
    to_table <- table_config[["to_table"]][[1]]
    claim_header_from_schema <- table_config[["claim_header_from_schema"]][[1]]
    claim_header_from_table <- table_config[["claim_header_from_table"]][[1]]
    claim_pharm_from_schema <- table_config[["claim_pharm_from_schema"]][[1]]
    claim_pharm_from_table <- table_config[["claim_pharm_from_table"]][[1]]
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
  conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  message("STEP 1: CREATE TEMP TABLE TO HOLD CONDITION-SPECIFIC CLAIMS AND DATES")
  time_start <- Sys.time()
  
    # Build SQL query
    sql1 <- glue_sql("
     SELECT DISTINCT  
      {`id_source`}
   ,svc_date
   ,bh_cond
   ,link = 1 
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
    		            WHERE code_set in ('ICD9CM', 'ICD10CM') 
    		            ) as b
    		ON (a.icdcm_norm = b.code) and (a.icdcm_version = b.icdcm_version) 
   ) diag
   
   UNION
   
   SELECT DISTINCT  
    {`id_source`}
   ,svc_date
   ,bh_cond
   ,'link' = 1 
   -- BASED ON PRESCRIPTIONS
   FROM (SELECT DISTINCT a.{`id_source`}
			,a.{`rx_fill_date`} as 'svc_date'
	    ,b.sub_group_condition as 'bh_cond'
    FROM {`claim_pharm_from_schema`}.{`claim_pharm_from_table`} a
    INNER JOIN (SELECT sub_group_condition, code_set, code
    	          FROM {`ref_schema`}.{`ref_table`}
    	          WHERE code_set in ('NDC')) as b
    ON a.ndc = b.code
          ) rx
      ",.con = conn)
    
    #Run SQL query
    try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_header_bh")), silent = T)
    dbGetQuery(conn = conn, sql1)
    
    #### STEP 4: JOIN WITH ROLLING 24MONTH  ####
    conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    message("STEP 2: JOIN WITH ROLLING 24MONTH")
    # Build SQL query
    sql2 <- glue_sql("
    SELECT
      {`id_source`}
      ,svc_date
      ,bh_cond
      ,start_window
      ,end_window
    INTO {`schema`}.tmp_matrix
    FROM {`schema`}.tmp_header_bh as header
    LEFT JOIN
      (SELECT cast(start_window as date) as 'start_window'
		          ,cast(end_window as date) as 'end_window'
		          ,1 as link 
		    FROM {`rolling_schema`}.{`rolling_table`} 
		  ) as rolling
    ON header.link=rolling.link
    WHERE header.svc_date between rolling.[start_window] and rolling.[end_window]
    ",.con = conn)
    
    #Run SQL query
    try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_matrix")), silent = T)
    dbGetQuery(conn = conn, sql2)
    
    #### STEP 5: CREATE TEMP TABLE TO HOLD ID AND ROLLING TIME MATRIX ####
    conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    message("STEP 3: CREATE TEMP TABLE TO HOLD ID AND ROLLING TIME MATRIX")
    # Build SQL query
    sql3 <- glue_sql("
    SELECT  * 
      ,CASE
          WHEN datediff(month,
				      lag(b.end_window) over (partition by b.{`id_source`}, b.bh_cond order by b.{`id_source`}, b.bh_cond, b.start_window, b.svc_date),
				      b.start_window) <= 1 
			        then null
          WHEN b.start_window < 
				      lag(b.end_window) over (partition by b.{`id_source`}, b.bh_cond order by b.{`id_source`},  b.bh_cond, b.start_window, b.svc_date) 
			        then null
          WHEN row_number() over (partition by b.{`id_source`}, b.bh_cond order by b.{`id_source`}, b.bh_cond,  b.start_window, b.svc_date) = 1 
			        then null
          ELSE row_number() over (partition by b.{`id_source`}, b.bh_cond order by b.{`id_source`}, b.bh_cond, b.start_window, b.svc_date)
          END AS 'discont'
      ,row_number() over (partition by b.{`id_source`}, b.bh_cond order by b.{`id_source`}, b.bh_cond, b.start_window, b.svc_date) as row_no
      --,lag(b.end_window) over (partition by b.{`id_source`}, b.bh_cond order by b.{`id_source`}, b.bh_cond, b.start_window) as lag_end
    INTO {`schema`}.tmp_rolling_matrix
    FROM {`schema`}.tmp_matrix b
    --order by {`id_source`}, bh_cond, start_window
        ",.con = conn)
    
    #Run SQL query
    try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_rolling_matrix")), silent = T)
    dbGetQuery(conn = conn, sql3)  
    
    #### STEP 6: ID CONDITION STATUS OVER TIME AND COLLAPSE TO CONTIGUOUS PERIODS ####
    conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    message("STEP 4: ID CONDITION STATUS OVER TIME AND COLLAPSE TO CONTIGUOUS PERIODS")
    # Build SQL query
    sql4 <- glue_sql("
      SELECT --DISTINCT
        d.{`id_source`}
        ,bh_cond
        ,min(d.start_window) as 'from_date'
        ,max(d.end_window) as 'to_date' 
      INTO {`schema`}.tmp_rolling_tmp_bh
      FROM
        (SELECT c.{`id_source`}, c.start_window, c.end_window,bh_cond
              --, c.discont, c.row_no 
              ,sum(case when c.discont is null then 0 else 1 end) over
                  (order by c.{`id_source`}, bh_cond, c.row_no) as 'grp'
        FROM {`schema`}.tmp_rolling_matrix c) d
      GROUP BY d.{`id_source`},d.grp,d.bh_cond
      ",.con = conn)
    
    #Run SQL query
    try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_rolling_tmp_bh")), silent = T)
    dbGetQuery(conn = conn, sql4)
    
    #### STEP 7: INSERT ALL CONDITION TABLES INTO FINAL STAGE TABLE #### 
    conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    message("STEP 5: INSERT ALL CONDITION TABLES INTO FINAL STAGE TABLE")
    # Build SQL query
    sql5 <- glue_sql(
      "INSERT INTO {`schema`}.{`to_table`}
      SELECT
      {`id_source`}, from_date, to_date, bh_cond, 
      getdate() as last_run
      FROM {`schema`}.tmp_rolling_tmp_bh;",
      .con = conn)
    
    #Run SQL query
    dbGetQuery(conn = conn, sql5)
    
    #try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_header_bh")), silent = T)
    #try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_matrix")), silent = T)
    #try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_rolling_matrix")), silent = T)
    #try(dbRemoveTable(conn, tbl_name <- DBI::Id(schema = schema, table = "tmp_rolling_tmp_bh")), silent = T)
  
  #Run time of all steps
    time_end <- Sys.time()
    message(glue::glue("Table creation took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                       " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))
}

### TEST RUN
#hhsaw_prod <- DBI::dbConnect(odbc::odbc(), dsn = 'HHSAW_prod')
#test_mcaid <- load_bh(conn=hhsaw_prod, server='hhsaw',source="mcaid")