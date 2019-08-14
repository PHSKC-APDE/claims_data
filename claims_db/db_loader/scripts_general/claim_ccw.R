## Code to create stage.SOURCE_claim_ccw table
## Person-level CCW condition status by time period
## Eli Kern and Alastair Matheson (PHSKC-APDE)
## 2019-08-13
## Run time: 20 mins (Medicaid) to 2h 30min (APCD) 

load_ccw <- function(conn = NULL,
                     source = c("apcd", "mcaid", "mcare"),
                     test_rows = NULL) {
  
  # Check libraries are called in and load if not
  packages <- list("dplyr", "ggplot2", "tidyr", "readr", "purrr", "tibble", 
                   "stringr", "forcats", "odbc", "yaml")
  
  lapply(packages, function (x) {
    if (!x %in% (.packages())) {
      suppressPackageStartupMessages(library(x, character.only = T))
    }
  })
  
  # Make sure a connection to the DB exists, using a default if not provided
  if (is.null(conn)) {
    conn <- tryCatch(
      {
        dbConnect(odbc(), "PHClaims51")
      },
      error = function(result) {
        stop(glue("The default ODBC connection (PHClaims51) failed. ",
                     "Supply your own using 'conn = <odbc_conn>'."))
      },
      finally = {
        message("Used default connection of 'PHClaims51'")
      })
  }
  
  # Figure out which CCW table is being made
  source <- match.arg(source)
  
  # Select config file for desired data source
  if (source == "apcd") {
    id_source <- "id_apcd"
    config_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_claim_ccw.yaml" 
  } else if (source == "mcaid") {
    id_source <- "id_mcaid"
    config_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_ccw.yaml" 
  } else if (source == "mcare") {
    id_source <- "id_mcare"
    config_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_ccw.yaml"
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
  
  # ### ### ### ### ### ### ###
  #### Step 1: Load parameters from config file #### 
  # ### ### ### ### ### ### ###
  
  table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  conditions <- names(table_config[str_detect(names(table_config), "cond_")])
  schema <- table_config[["schema"]][[1]]
  to_table <- table_config[["to_table"]][[1]]
  claim_header_from_schema <- table_config[["claim_header_from_schema"]][[1]]
  claim_header_from_table <- table_config[["claim_header_from_table"]][[1]]
  icdcm_from_schema <- table_config[["icdcm_from_schema"]][[1]]
  icdcm_from_table <- table_config[["icdcm_from_table"]][[1]]
  vars <- table_config$vars
  
  
  # ### ### ### ### ### ### ###
  #### Step 2: Run the create table script first
  # ### ### ### ### ### ### ###
  
  # Set up table name
  tbl_name <- DBI::Id(schema = schema, table = to_table)
  
  # Remove table if it exists
  try(dbRemoveTable(conn, tbl_name))
  
  # Create table
  DBI::dbCreateTable(conn, tbl_name, fields = table_config$vars)
  
  
  # ### ### ### ### ### ### ###
  #### Step 3: Create branching code segments for type 1 versus type 2 conditions #### 
  # ### ### ### ### ### ### ###
  
  ptm01 <- proc.time() # Times how long this query takes
  
  ## Begin loop over conditions - loop continues across all SQL queries
  lapply(conditions, function(x){
    
    message(glue("Working on {x}"))
    
    ccw_code <- table_config[[x]][["ccw_code"]]
    ccw_desc <- table_config[[x]][["ccw_desc"]]
    ccw_abbrev <- paste0("ccw_", table_config[[x]][["ccw_abbrev"]])
    dx_fields <- table_config[[x]][["dx_fields"]]
    dx_exclude1 <- table_config[[x]][["dx_exclude1"]]
    dx_exclude2 <- table_config[[x]][["dx_exclude2"]]
    dx_exclude1_fields <- table_config[[x]][["dx_exclude1_fields"]]
    dx_exclude2_fields <- table_config[[x]][["dx_exclude2_fields"]]
    condition_type <- table_config[[x]][["condition_type"]]
    
    if (is.null(table_config[[x]][["claim_type1"]])) {
      claim1 <- ""
    } else {
      claim1 <- glue_sql('{as.character(table_config[[x]][["claim_type1"]])*}',
                         .con = conn)
    }
    
    if (is.null(table_config[[x]][["claim_type2"]])) {
      claim2 <- ""
    } else {
      claim2 <- glue_sql('{as.character(table_config[[x]][["claim_type2"]])*}',
                         .con = conn)
    }

    ## Construct where statement for claim count requirements
    if (condition_type == 1) {
      claim_count_condition <- glue_sql("where (b.condition_1_cnt >= 1)", 
                                        .con = conn)
    }
    if (condition_type == 2) {
      claim_count_condition <- 
        glue_sql("where (b.condition_1_cnt >= 1) or (b.condition_2_cnt >=2 and ",
             "abs(datediff(day, b.condition_2_min_date, b.condition_2_max_date)) >=1)",
             .con = conn)
    }
    
    ## Construct where statement for diagnosis field numbers
    if (dx_fields == "1-2") {
      dx_fields_condition <- glue_sql(" where icdcm_number in ('01','02')",
                                      .con = conn)
    }
    if (dx_fields == "1") {
      dx_fields_condition <- glue_sql(" where icdcm_number = '01'" ,
                                      .con = conn)
    }
    if (dx_fields == "any") {
      dx_fields_condition <- DBI::SQL('')
    }
    
    ## Construct diagnosis field number code for exclusion code
    if (!is.null(dx_exclude1_fields)) {
      if (dx_exclude1_fields == "1-2") {
        dx_exclude1_fields_condition <- glue_sql(" and diag.icdcm_number in ('01','02')",
                                                 .con = conn)
      }
      if (dx_exclude1_fields == "1") {
        dx_exclude1_fields_condition <- glue_sql(" and diag.icdcm_number = '01'",
                                                 .con = conn)
      }
      if (dx_exclude1_fields == "any") {
        dx_exclude1_fields_condition <- DBI::SQL('')
      }
    } else {
      dx_exclude1_fields_condition <- DBI::SQL('')
    }
    
    if (!is.null(dx_exclude2_fields)) {
      if (dx_exclude2_fields == "1-2") {
        dx_exclude2_fields_condition <- glue_sql(" and diag.icdcm_number in ('01','02')",
                                                 .con = conn)
      }
      if (dx_exclude2_fields == "1") {
        dx_exclude2_fields_condition <- glue_sql(" and diag.icdcm_number = '01'",
                                                 .con = conn)
      }
      if (dx_exclude2_fields == "any") {
        dx_exclude2_fields_condition <- DBI::SQL('')
      }
    } else {
      dx_exclude2_fields_condition <- DBI::SQL('')
    }
    
    ## Construct diagnosis-based exclusion code
    if(is.null(dx_exclude1) & is.null(dx_exclude2)){
      dx_exclude_condition <- DBI::SQL('')
    } else if (!is.null(dx_exclude1) & is.null(dx_exclude2)){
      dx_exclude1 <- paste0("ccw_", table_config[[x]][["dx_exclude1"]])
      
      dx_exclude_condition <- glue_sql(
        "--left join diagnoses to claim-level exclude flag if specified
          left join(
          select diag.claim_header_id, max(ref.{`dx_exclude1`}) as exclude1 
      
          --pull out claim and diagnosis fields
            from (
            select {top_rows} {`id_source`}, claim_header_id, icdcm_norm, icdcm_version, icdcm_number
            from PHClaims.{`icdcm_from_schema`}.{`icdcm_from_table`}) diag
      
          --join to diagnosis reference table, subset to those with CCW exclusion flag
            inner join (
            select {top_rows} dx, dx_ver, {`dx_exclude1`} 
            from PHClaims.ref.dx_lookup
            where {`dx_exclude1`} = 1
            ) ref
      
            on (diag.icdcm_norm = ref.dx) and (diag.icdcm_version = ref.dx_ver)
            where (ref.{`dx_exclude1`} = 1 {dx_exclude1_fields_condition})
            group by diag.claim_header_id
            ) as exclude
      
            on diag_lookup.claim_header_id = exclude.claim_header_id
            where exclude.exclude1 is null",
          .con = conn)
      } else if (!is.null(dx_exclude1) & !is.null(dx_exclude2)){
        dx_exclude1 <- paste0("ccw_", table_config[[x]][["dx_exclude1"]])
        dx_exclude2 <- paste0("ccw_", table_config[[x]][["dx_exclude2"]])
        
        dx_exclude_condition <- glue_sql(
        "--left join diagnoses to claim-level exclude flag if specified
            left join (
            select diag.claim_header_id, max(ref.{`dx_exclude1`}) as exclude1, 
              max(ref.{`dx_exclude2`}) as exclude2 
            
          --pull out claim and diagnosis fields
            from (
            select {top_rows} {`id_source`}, claim_header_id, icdcm_norm, icdcm_version, icdcm_number
            from PHClaims.{`icdcm_from_schema`}.{`icdcm_from_table`}) diag
            
          --join to diagnosis reference table, subset to those with CCW exclusion flag
            inner join (
            select {top_rows} dx, dx_ver, {`dx_exclude1`}, {`dx_exclude2`} 
            from PHClaims.ref.dx_lookup
            where {`dx_exclude1`} = 1 or {`dx_exclude2`} = 1
            ) ref
            
            on (diag.icdcm_norm = ref.dx) and (diag.icdcm_version = ref.dx_ver)
            where (ref.{`dx_exclude1`} = 1 {dx_exclude1_fields_condition}) or 
              (ref.{`dx_exclude2`} = 1 {dx_exclude2_fields_condition})
            group by diag.claim_header_id
            ) as exclude
            on diag_lookup.claim_header_id = exclude.claim_header_id
            where exclude.exclude1 is null and exclude.exclude2 is null",
        .con = conn)
        }
    
    
    # ### ### ### ### ### ### ###
    #### Step 4: create temp table to hold condition-specific claims and dates #### 
    # ### ### ### ### ### ### ###
    
    # Build SQL query
    sql1 <- glue_sql(
    "--#drop temp table if it exists
    if object_id('tempdb..##header') IS NOT NULL drop table ##header;
    
    --apply CCW claim type criteria to define conditions 1 and 2
    select header.{`id_source`}, header.claim_header_id, header.claim_type_id, 
      header.first_service_date, diag_lookup.{`ccw_abbrev`},
      diag_lookup.{`id_source`} as id_source_tmp,  -- zero rows returned without this, unclear why
      case when header.claim_type_id in ({claim1}) 
        then 1 else 0 end as 'condition1',
      case when header.claim_type_id in ({claim2}) then 1 else 0 end as 'condition2',
      case when header.claim_type_id in ({claim1}) 
        then header.first_service_date else null end as 'condition_1_from_date',
      case when header.claim_type_id in ({claim2})
        then header.first_service_date else null end as 'condition_2_from_date'

    into ##header
    
    --pull out claim type and service dates
    from (
      select {`id_source`}, claim_header_id, claim_type_id, first_service_date
      from PHClaims.{`claim_header_from_schema`}.{`claim_header_from_table`}) header
  
    --right join to claims containing a diagnosis in the CCW condition definition
    right join (
      select diag.{`id_source`}, diag.claim_header_id, ref.{`ccw_abbrev`} 
    
    --pull out claim and diagnosis fields
    from (
      select {top_rows} {`id_source`}, claim_header_id, icdcm_norm, icdcm_version
      from PHClaims.{`icdcm_from_schema`}.{`icdcm_from_table`} {dx_fields_condition}) diag

    --join to diagnosis reference table, subset to those with CCW condition
    inner join (
      select {top_rows} dx, dx_ver, {`ccw_abbrev`}
      from PHClaims.ref.dx_lookup
      where {`ccw_abbrev`} = 1) ref
      
      on (diag.icdcm_norm = ref.dx) and (diag.icdcm_version = ref.dx_ver)
      ) as diag_lookup
  
      on header.claim_header_id = diag_lookup.claim_header_id 
    {dx_exclude_condition}",
    .con = conn)

    #Run SQL query
    dbGetQuery(conn = conn, sql1)
    
    
    # ### ### ### ### ### ### ###
    #### Step 5: create temp table to hold ID and rolling time period matrix #### 
    # ### ### ### ### ### ### ###
    
    #Build SQL query
    sql2 <- glue_sql(
    
      "if object_id('tempdb..##rolling_tmp') IS NOT NULL drop table ##rolling_tmp;
      
      --join rolling time table to person ids
      select id.{`id_source`}, rolling.start_window, rolling.end_window
      into ##rolling_tmp
      
      from (
        select distinct {`id_source`}, 'link' = 1 from ##header
        ) as id
        
      right join (
        select cast(start_window as date) as 'start_window', 
          cast(end_window as date) as 'end_window',
          'link' = 1
        from PHClaims.ref.rolling_time_{table_config[[x]][['lookback_months']]}mo_2012_2020
        ) as rolling
        
      on id.link = rolling.link
      order by id.{`id_source`}, rolling.start_window",
      .con = conn)
    
    #Run SQL query
    dbGetQuery(conn = conn, sql2)
    
    # ### ### ### ### ### ### ###
    #### Step 6: identify condition status over time and collapse to contiguous time periods  #### 
    # ### ### ### ### ### ### ###
    
    #Build SQL query
    sql3 <- glue_sql(
      "--#drop temp table if it exists
        if object_id('tempdb..{`ccw_abbrev_table`}') IS NOT NULL drop table {`ccw_abbrev_table`};
      
      --collapse to single row per ID and contiguous time period
        select distinct d.{`id_source`}, min(d.start_window) as 'from_date', 
          max(d.end_window) as 'to_date', {ccw_code} as 'ccw_code',
          {ccw_abbrev} as 'ccw_desc'
      
      into {`ccw_abbrev_table`}
      
      from (
      --set up groups where there is contiguous time
      select c.{`id_source`}, c.start_window, c.end_window, c.discont, c.temp_row,
      
      sum(case when c.discont is null then 0 else 1 end) over
      (order by c.{`id_source`}, c.temp_row rows between unbounded preceding and current row) as 'grp'
  
    from (
      --pull out ID and time periods that contain appropriate claim counts
      select b.{`id_source`}, b.start_window, b.end_window, b.condition_1_cnt, 
        b.condition_2_min_date, b.condition_2_max_date,
    
      --create a flag for a discontinuity in a person's disease status
      case
        when datediff(month, lag(b.end_window) over 
          (partition by b.{`id_source`} order by b.{`id_source`}, b.start_window), b.start_window) <= 1 then null
        when b.start_window < lag(b.end_window) over 
          (partition by b.{`id_source`} order by b.{`id_source`}, b.start_window) then null
        when row_number() over (partition by b.{`id_source`} 
          order by b.{`id_source`}, b.start_window) = 1 then null
        else row_number() over (partition by b.{`id_source`} 
          order by b.{`id_source`}, b.start_window)
      end as 'discont',
  
    row_number() over (partition by b.{`id_source`} order by b.{`id_source`}, b.start_window) as 'temp_row'
  
    from (
    --sum condition1 and condition2 claims by ID and period, take min and max service date for each condition2 claim by ID and period
      select a.{`id_source`}, a.start_window, a.end_window, sum(a.condition1) as 'condition_1_cnt', 
      sum(a.condition2) as 'condition_2_cnt', min(a.condition_2_from_date) as 'condition_2_min_date', 
      max(a.condition_2_from_date) as 'condition_2_max_date'
    
      from (
      --pull ID, time period and claim information, subset to ID x time period rows containing a relevant claim
        select matrix.{`id_source`}, matrix.start_window, matrix.end_window, cond.first_service_date, cond.condition1,
          cond.condition2, condition_2_from_date
      
      --pull in ID x time period matrix
        from (
          select {`id_source`}, start_window, end_window
          from ##rolling_tmp
        ) as matrix
      
      --join to condition temp table
        left join (
          select {`id_source`}, first_service_date, condition1, condition2, condition_2_from_date
          from ##header
        ) as cond
      
        on matrix.{`id_source`} = cond.{`id_source`}
        where cond.first_service_date between matrix.start_window and matrix.end_window
        ) as a
        group by a.{`id_source`}, a.start_window, a.end_window
      ) as b 
      {claim_count_condition}) as c
    ) as d
    group by d.{`id_source`}, d.grp
    order by d.{`id_source`}, from_date",
      .con = conn,
      ccw_abbrev_table = glue("##{ccw_abbrev}"))
    #Run SQL query
    dbGetQuery(conn = conn, sql3)
    
    
    # ### ### ### ### ### ### ###
    #### Step 7: Insert all condition tables into final stage table  #### 
    # ### ### ### ### ### ### ###
    
    #Build SQL query
    sql4 <- glue_sql(
      "insert into PHClaims.{`schema`}.{`to_table`} with (tablock)
      select
      {`id_source`}, from_date, to_date, ccw_code, ccw_desc, 
      {Sys.Date()}
      from {`ccw_abbrev_table`}",
      .con = conn,
      ccw_abbrev_table = glue("##{ccw_abbrev}"))

    #Run SQL query
    dbGetQuery(conn = conn, sql4)
    
  })
  
  #Run time of all steps
  proc.time() - ptm01
  
}
