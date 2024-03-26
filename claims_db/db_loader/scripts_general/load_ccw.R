## Code to create stage.SOURCE_claim_ccw table
## Person-level CCW condition status by time period
## Eli Kern and Alastair Matheson (PHSKC-APDE)
## 2019-08-13
## Run time: 20 mins (Medicaid) to 2h 30min (APCD) 

### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# source = which CCW table is being built
# ccw_list_name = vector of CCW conditions to load, by ccw_abbrev, e.g., c("hypothyroid", "copd", "depression"). 
#   See ref_ccw_lookup table for names. Default is 'all'. Will be ignored if a config file has any conditions.
# drop_table = create blank destination table. Set to FALSE if you are only running a subset of 
#   CCW conditions and do not want overwrite other existing ones. If FALSE, any existing data for conditions
#   being run will be removed from the destination table. Default is TRUE.
# config = config file already in memory (should be blank if supplying config_url or config_file)
# config_url = URL location of YAML config file (should be blank if supplying config or config_file)
# config_file = path + file name of YAML config file (should be blank if supplying config or config_url)
# test_rows = number of rows to load if testing function (integer)
# print_query = print the config files and SQL queries used to make tables. Default is FALSE.

### Expected YAML config components:
## Common to all CCW conditions (required)
# to_schema: schema for the table that is to be created
# to_table: name of table to be created
# claim_header_from_schema: schema the source claims header table is in
# claim_header_from_table: name of the source claims header table
# icdcm_from_schema: schema the source ICD-CM table is in
# icdcm_from_table: name of the source ICD-CM table
# ref_schema: schema the ref tables are in (usually 'ref')
# ref_table_pre: prefix for ref table names (may be null, depending on the server)
# icdcm_ref_schema: schema the ICD-CM reference table is in (e.g., ref)
# icdcm_ref_table: name of ICD-CM reference table (e.g., icdcm_codes)
#
## Specific to each CCW condition (optional)
# ccw_code: numeric code for this CCW condition (see relevant ref table, e.g., claims.ref_ccw_lookup)
# ccw_desc: name of CCW condition
# ccw_abbrev: shortened name of CCW condition
# lookback_months_9: number of months to look back (for ICD-9-CM records)
# dx_9_fields: which dx fields to look in (1, 1-2, or any) for ICD-9-CM records
# dx_9_exclude1: name of column 1 (excluding ccw_ prefix) that flags if an ICD-9-CM code counts as an exclusion
# dx_9_exclude2: name of column 2 (excluding ccw_ prefix) that flags if an ICD-9-CM code counts as an exclusion
# dx_9_exclude1_fields: which dx fields to look in (1, 1-2, or any) for exclusion 1 (for ICD-9-CM records)
# dx_9_exclude2_fields: which dx fields to look in (1, 1-2, or any) for exclusion 2 (for ICD-9-CM records)
# claim_type1_9: claim types that need 1 event to qualify (for ICD-9-CM records)
# claim_type2_9: claim types that need 2 events to qualify (for ICD-9-CM records)
# condition_type_9: whether there are types of claims that only need 1 event to qualify (condition_type = 1) 
#                 or if some claim types require 2 events to qualify (condition_type = 2) (for ICD-9-CM records)
# lookback_months_10: number of months to look back (for ICD-10-CM records)
# dx_10_fields: which dx fields to look in (1, 1-2, or any) for ICD-10-CM records
# dx_10_exclude1: name of column 1 (excluding ccw_ prefix) that flags if an ICD-10-CM code counts as an exclusion
# dx_10_exclude2: name of column 2 (excluding ccw_ prefix) that flags if an ICD-10-CM code counts as an exclusion
# dx_10_exclude1_fields: which dx fields to look in (1, 1-2, or any) for exclusion 1 (for ICD-10-CM records)
# dx_10_exclude2_fields: which dx fields to look in (1, 1-2, or any) for exclusion 2 (for ICD-10-CM records)
# claim_type1_10: claim types that need 1 event to qualify (for ICD-10-CM records)
# claim_type2_10: claim types that need 2 events to qualify (for ICD-10-CM records)
# condition_type_10: whether there are types of claims that only need 1 event to qualify (condition_type = 1) 
#                 or if some claim types require 2 events to qualify (condition_type = 2) (for ICD-10-CM records)


load_ccw <- function(conn = NULL,
                     server = c("phclaims", "hhsaw"),
                     source = c("apcd", "mcaid", "mcare", "mcaid_mcare"),
                     ccw_list_name = "all",
                     drop_table = T,
                     config = NULL,
                     config_url = NULL,
                     config_file = NULL,
                     test_rows = NULL,
                     print_query = F) {
  
  
  # ERROR CHECKS ----
  # Make sure a connection to the DB exists
  if (is.null(conn)) {stop("No ODBC connection supplied")}
  
  
  # SET UP PARAMETERS ETC. ----
  # Figure out which server is being used and which CCW table is being made
  server <- match.arg(server)
  source <- match.arg(source)
  
  # Check libraries are called in and load if not
  if (!require("pacman")) {install.packages("pacman")}
  pacman::p_load(dplyr, ggplot2, tidyr, readr, purrr, tibble, stringr, forcats, odbc, yaml)
  
  # Select id name for desired data source
  if (source == "mcaid_mcare") {
    id_source <- "id_apde"
  } else {
    id_source <- paste0("id_", source)
  }
  
  # Set up test number of rows if needed
  if (is.null(test_rows)) {
    top_rows <- DBI::SQL('')
  } else {
    if (!is.integer(test_rows)) {
      stop("Select an integer number of test_rows")
    } else {
      top_rows <- glue::glue_sql(" TOP {test_rows} ", .con = conn)
    }
  }
  
  # STEP 1: LOAD CONFIG FILE AND PARAMETERS ----
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else if (!is.null(config_file)) {
    table_config <- yaml::read_yaml(config_file)
  } else {
    message("No config supplied, attempting to use one from the claims_data repo")
    table_config <- yaml::yaml.load(httr::GET(paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.", source, "_claim_ccw.yaml")))
  }
  
  if ("404" %in% names(table_config)) {
    stop("Invalid URL for config file")
  }
  
  if (server %in% names(table_config)) {
    to_schema <- table_config[[server]][["to_schema"]][[1]]
    to_table <- table_config[[server]][["to_table"]][[1]]
    claim_header_from_schema <- table_config[[server]][["claim_header_from_schema"]][[1]]
    claim_header_from_table <- table_config[[server]][["claim_header_from_table"]][[1]]
    icdcm_from_schema <- table_config[[server]][["icdcm_from_schema"]][[1]]
    icdcm_from_table <- table_config[[server]][["icdcm_from_table"]][[1]]
    icdcm_ref_schema <- table_config[[server]][["icdcm_ref_schema"]][[1]]
    icdcm_ref_table <- table_config[[server]][["icdcm_ref_table"]][[1]]
    ref_schema <- table_config[[server]][["ref_schema"]][[1]]
    ref_table_pre <- ifelse(is.null(table_config[[server]][["ref_table_pre"]]), '',
                        table_config[[server]][["ref_table_pre"]])
  } else {
    to_schema <- table_config[["to_schema"]][[1]]
    to_table <- table_config[["to_table"]][[1]]
    claim_header_from_schema <- table_config[["claim_header_from_schema"]][[1]]
    claim_header_from_table <- table_config[["claim_header_from_table"]][[1]]
    icdcm_from_schema <- table_config[["icdcm_from_schema"]][[1]]
    icdcm_from_table <- table_config[["icdcm_from_table"]][[1]]
    icdcm_ref_schema <- table_config[["icdcm_ref_schema"]][[1]]
    icdcm_ref_table <- table_config[["icdcm_ref_table"]][[1]]
    # Assumes working in PHClaims for ref data if using older YAML format
    ref_schema <- "ref"
    ref_table_pre <- ""
  }
  
  vars <- table_config$vars
  
  
  # STEP 2: SET UP CONDITIONS ----
  # See if the config file has any conditions in it
  conditions <- names(table_config[str_detect(names(table_config), "cond_")])
  
  # If not, use the default list from the ref table
  if (length(conditions) == 0) {
    conditions_ref <- dbGetQuery(conn, 
                                 glue::glue_sql("SELECT * FROM {`ref_schema`}.{DBI::SQL(ref_table_pre)}ccw_lookup",
                                                .con = conn))
    
    # Get list of conditions to run
    if ("all" %in% ccw_list_name) {
      conditions <- conditions_ref %>% 
        distinct(ccw_abbrev)%>%
        filter(str_detect(ccw_abbrev, "exclude", negate = T)) %>%
        unlist()
    } else if (!is.null(ccw_list_name) & !"all" %in% ccw_list_name) {
      conditions <- conditions_ref %>%
        filter(ccw_abbrev %in% ccw_list_name) %>%
        distinct(ccw_abbrev) %>% unlist()
    }
    
    # Set up flag for how the CCW qualifications will be sources
    ccw_source <- "ref"
  } else {
    ccw_source <- "yaml"
  }

  # If still no conditions, stop code
  if (length(conditions) == 0) {
    stop(paste0("No conditions chosen to run. Possible errors include misconfigured YAML file ",
                "or ccw_list_name values don't align with the ref.ccw_lookup table"))
  }
  
  
  # STEP 3: CREATE TABLE ----
  # Set up table name
  tbl_name <- DBI::Id(schema = to_schema, table = to_table)
  
  # See if destination table exists
  to_tbl_exist <- DBI::dbExistsTable(conn, tbl_name)
  
  # Issue warning if a person is at risk of overwriting a final table with a stage
  # table that only has a subset of CCW conditions
  if ((drop_table == T | to_tbl_exist == F) & !is.null(ccw_list_name) & !"all" %in% ccw_list_name) {
    warning(paste0("Destination table did not exist so was created but only a susbet of ",
                   "CCW conditions was selected. There may be a discrepancy between the ",
                   "destination table and a final table"))
  }
  
  
  if (drop_table == T | to_tbl_exist == F) {
    # Remove table if it exists
    try(dbRemoveTable(conn, tbl_name), silent = T)
    
    # Create table
    DBI::dbCreateTable(conn, tbl_name, fields = table_config$vars)
  }
  
  # If only some conditions are being run and a new blank destination table was not made,
  #  remove rows for this condition in the destination table
  # Probably best to load to a new table, truncate, then load back
  if (drop_table == F & to_tbl_exist == T) {
    # Get ccw_abbrev values.
    if (ccw_source == "yaml") {
      ccw_drop <- map_chr(conditions2, ~ table_config2[[.x]][["ccw_abbrev"]])
    } else if (ccw_source == "ref") {
      ccw_drop <- paste0("ccw_", conditions)
    }
    
    # Load to temp table
    tmp_tbl <- paste0(to_table, "_tmp")
    
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("SELECT * INTO {`to_schema`}.{`tmp_tbl`}
                            FROM {`to_schema`}.{`to_table`}
                            WHERE ccw_desc NOT IN ({ccw_drop*})",
                            .con = conn))
    
    # Drop existing table
    DBI::dbExecute(conn = conn, 
                   glue::glue_sql("DROP TABLE {`to_schema`}.{`to_table`}", .con = conn))
    
    # Insert rows back
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("SELECT * INTO {`to_schema`}.{`to_table`}
                            FROM {`to_schema`}.{`tmp_tbl`}",
                            .con = conn))
    
    # Drop temp table
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("DROP TABLE {`to_schema`}.{`tmp_tbl`}", .con = conn))
  }
  
  
  
  # STEP 4: SET UP FUNCTIONS ----
  # Create functions to handle either ICD-9 or ICD-10
  # Note: these functions need to be run sequentially for a given condition and ICD version
  #     Otherwise temp tables will be overwritten.
  
  ## Standardized config ----
  # Arrange config to produce standard values for a condition, regardless of config source and ICD version
  config_cond_gen <- function(cond, icd = c(9, 10), 
                              ccw_source = c("ref", "yaml"),
                              table_config = table_config) {
    ccw_source <- match.arg(ccw_source)
    
    ### Set up config structure ----
    if (ccw_source == "yaml") {
      # Convert YAML code into data frame to match ref
      if (max(str_detect(names(table_config[[cond]]), "_9")) == 1) {
        # Reconfigure so there are separate rows for each ICD version
        config <- bind_rows(tibble(ccw_icd_version = 9,
                                   map_df(table_config[[cond]], ~ ifelse(is.null(.x), NA_character_, glue_collapse(.x, sep = ";"))) %>%
                                     select(ccw_code, ccw_desc, ccw_abbrev, contains("_9")) %>%
                                     rename_with(~ str_remove(.x, "_9"))),
                            tibble(ccw_icd_version = 10,
                                   map_df(table_config[[cond]], ~ ifelse(is.null(.x), NA_character_, glue_collapse(.x, sep = ";"))) %>%
                                     select(ccw_code, ccw_desc, ccw_abbrev, contains("_10")) %>%
                                     rename_with(~ str_remove(.x, "_10")))
        )
      } else {
        # If no distinction between ICD-9 and ICD-10, repeat rows
        config <- bind_rows(tibble(ccw_icd_version = 9,
                                   map_df(table_config[[cond]], 
                                          ~ ifelse(is.null(.x), NA_character_, glue_collapse(.x, sep = ";")))),
                            tibble(ccw_icd_version = 10, 
                                   map_df(table_config[[cond]], 
                                          ~ ifelse(is.null(.x), NA_character_, glue_collapse(.x, sep = ";"))))
        )
      }
      # Align column names with ref
      config <- config %>%
        rename_with(~ str_replace(.x, "_type1", "_type_1")) %>%
        rename_with(~ str_replace(.x, "_type2", "_type_2"))
    } else if (ccw_source == "ref") {
      # Use str_detect to capture all rows, including exclusions
      config <- conditions_ref %>% filter(str_detect(ccw_abbrev, cond)) %>%
        # Reformat so exclusions are on the same line as main condition
        mutate(dx_exclude1 = ifelse(str_detect(ccw_abbrev, "exclude1|exclude_1"), ccw_abbrev, NA_character_),
               dx_exclude1_fields = ifelse(str_detect(ccw_abbrev, "exclude1|exclude_1"), dx_fields, NA_character_),
               dx_exclude2 = ifelse(str_detect(ccw_abbrev, "exclude2|exclude_2"), ccw_abbrev, NA_character_),
               dx_exclude2_fields = ifelse(str_detect(ccw_abbrev, "exclude2|exclude_2"), dx_fields, NA_character_)) %>%
        # Get everything onto the top line and just keep that
        group_by(ccw_icd_version) %>%
        fill(contains("exclude"), .direction = "downup") %>%
        slice(1) %>%
        ungroup()
    }
    
    
    ### Set up claim types ----
    claim1_chk <- filter(config, ccw_icd_version == icd) %>% select(claim_type_1) %>% pull()
    
    if (purrr::is_empty(claim1_chk)) {
      dx_claim1 <- ""
    }
    else if (is.na(claim1_chk)) {
      dx_claim1 <- ""
    } else {
      dx_claim1 <- glue::glue_sql('{str_split(filter(config, ccw_icd_version == icd) %>% select(claim_type_1), ";",
                              simplify = T)*}', .con = conn)
    }
    
    
    claim2_chk <- filter(config, ccw_icd_version == icd) %>% select(claim_type_2) %>% pull()
    if (purrr::is_empty(claim2_chk)) {
      dx_claim2 <- ""
    }
    else if (is.na(claim2_chk)) {
      dx_claim2 <- ""
    } else {
      dx_claim2 <- glue::glue_sql('{str_split(filter(config, ccw_icd_version == icd) %>% select(claim_type_2), ";",
                              simplify = T)*}', .con = conn)
    }
    
    
    ### Set up claim count requirements code ----
    condition_type <- config %>% filter(ccw_icd_version == icd) %>% select(condition_type) %>% pull()
    
    if (purrr::is_empty(condition_type)) {
      claim_count_condition <- DBI::SQL('')
    } else if (condition_type == 1) {
      claim_count_condition <- glue::glue_sql("where (b.condition_1_cnt >= 1)", 
                                        .con = conn)
    } else if (condition_type == 2) {
      claim_count_condition <- 
        glue::glue_sql("where (b.condition_1_cnt >= 1) or (b.condition_2_cnt >=2 and ",
                 "abs(datediff(day, b.condition_2_min_date, b.condition_2_max_date)) >=1)",
                 .con = conn)
    }
    
    
    ### Set up dx field number code ----
    dx_fields = config %>% filter(ccw_icd_version == icd) %>% select(dx_fields) %>% pull()
    
    if (purrr::is_empty(dx_fields)) {
      dx_fields_condition <- DBI::SQL('')
    } else if (dx_fields %in% c("1-2", "1;2")) {
      dx_fields_condition <- glue::glue_sql(" where icdcm_number in ('01','02')",
                                      .con = conn)
    } else if (dx_fields == "1") {
      dx_fields_condition <- glue::glue_sql(" where icdcm_number = '01'" ,
                                      .con = conn)
    } else if (dx_fields == "any") {
      # Always need the where so the ICD filter later works
      dx_fields_condition <- DBI::SQL(' where 1 = 1 ')
    }
    
    
    ### Set up dx field number code for exclusion code ----
    dx_exclude1_fields <- config %>% filter(ccw_icd_version == icd) %>% select(dx_exclude1_fields) %>% pull()
    dx_exclude2_fields <- config %>% filter(ccw_icd_version == icd) %>% select(dx_exclude2_fields) %>% pull()
    
    if (purrr::is_empty(dx_exclude1_fields)) {
      dx_exclude1_fields_condition <- DBI::SQL('')
    } else if (!is.na(dx_exclude1_fields)) {
      if (dx_exclude1_fields %in% c("1-2", "1;2")) {
        dx_exclude1_fields_condition <- glue::glue_sql(" and diag.icdcm_number in ('01','02')",
                                                 .con = conn)
      } else if (dx_exclude1_fields == "1") {
        dx_exclude1_fields_condition <- glue::glue_sql(" and diag.icdcm_number = '01'",
                                                 .con = conn)
      } else if (dx_exclude1_fields == "any") {
        dx_exclude1_fields_condition <- DBI::SQL('')
      }
    } else {
      dx_exclude1_fields_condition <- DBI::SQL('')
    }
    
    if (purrr::is_empty(dx_exclude2_fields)) {
      dx_exclude2_fields_condition <- DBI::SQL('')
    } else if (!is.na(dx_exclude2_fields)) {
      if (dx_exclude2_fields %in% c("1-2", "1;2")) {
        dx_exclude2_fields_condition <- glue::glue_sql(" and diag.icdcm_number in ('01','02')",
                                                 .con = conn)
      } else if (dx_exclude2_fields == "1") {
        dx_exclude2_fields_condition <- glue::glue_sql(" and diag.icdcm_number = '01'",
                                                 .con = conn)
      } else if (dx_exclude2_fields == "any") {
        dx_exclude2_fields_condition <- DBI::SQL('')
      }
    } else {
      dx_exclude2_fields_condition <- DBI::SQL('')
    }
    
    
    ### Set up dx-based exclusion code ----
    dx_exclude1 <- config %>% filter(ccw_icd_version == icd) %>% select(dx_exclude1) %>% pull()
    dx_exclude2 <- config %>% filter(ccw_icd_version == icd) %>% select(dx_exclude2) %>% pull()
    
    if (purrr::is_empty(dx_exclude1)) {
      dx_exclude_condition <- DBI::SQL('')
    } else if (is.na(dx_exclude1) & is.na(dx_exclude2)){
      # Always need the where so the claim type where later works
      dx_exclude_condition <- DBI::SQL('WHERE 1 = 1 ')
    } else if (!is.na(dx_exclude1) & is.na(dx_exclude2)){
      dx_exclude1 <- paste0("ccw_", dx_exclude1)
      
      dx_exclude_condition <- glue::glue_sql(
        "--left join diagnoses to claim-level exclude flag if specified
          left join(
          SELECT diag.claim_header_id, max(ref.{`dx_exclude1`}) as exclude1 
      
          --pull out claim and diagnosis fields
            FROM (
            select {top_rows} {`id_source`}, claim_header_id, icdcm_norm, icdcm_version, icdcm_number
            FROM {`icdcm_from_schema`}.{`icdcm_from_table`}) diag
      
          --join to diagnosis reference table, subset to those with CCW exclusion flag
            inner join (
            SELECT icdcm, icdcm_version, {`dx_exclude1`} 
            FROM {`icdcm_ref_schema`}.{`icdcm_ref_table`}
            where {`dx_exclude1`} = 1
            ) ref
      
            on (diag.icdcm_norm = ref.icdcm) and (diag.icdcm_version = ref.icdcm_version)
            where (ref.{`dx_exclude1`} = 1 {dx_exclude1_fields_condition})
            group by diag.claim_header_id
            ) as exclude
      
            on diag_lookup.claim_header_id = exclude.claim_header_id
            where exclude.exclude1 is null",
        .con = conn)
    } else if (!is.na(dx_exclude1) & !is.na(dx_exclude2)){
      dx_exclude1 <- paste0("ccw_", dx_exclude1)
      dx_exclude2 <- paste0("ccw_", dx_exclude2)
      
      dx_exclude_condition <- glue::glue_sql(
        "--left join diagnoses to claim-level exclude flag if specified
            left join (
            SELECT diag.claim_header_id, max(ref.{`dx_exclude1`}) as exclude1, 
              max(ref.{`dx_exclude2`}) as exclude2 

          --pull out claim and diagnosis fields
            FROM (
              SELECT {top_rows} {`id_source`}, claim_header_id, icdcm_norm, icdcm_version, icdcm_number
              FROM {`icdcm_from_schema`}.{`icdcm_from_table`}) diag

          --join to diagnosis reference table, subset to those with CCW exclusion flag
            inner join (
            SELECT icdcm, icdcm_version, {`dx_exclude1`}, {`dx_exclude2`} 
            FROM {`icdcm_ref_schema`}.{`icdcm_ref_table`}
            where {`dx_exclude1`} = 1 or {`dx_exclude2`} = 1
            ) ref
            
            on (diag.icdcm_norm = ref.icdcm) and (diag.icdcm_version = ref.icdcm_version)
            where (ref.{`dx_exclude1`} = 1 {dx_exclude1_fields_condition}) or 
              (ref.{`dx_exclude2`} = 1 {dx_exclude2_fields_condition})
            group by diag.claim_header_id
            ) as exclude
            on diag_lookup.claim_header_id = exclude.claim_header_id
            where exclude.exclude1 is null and exclude.exclude2 is null",
        .con = conn)
    }
    
    
    ## Set up if this ICD version should be run ----
    # Some conditions don't have ICD-9 codes so need to flag that
    if (nrow(config %>% filter(ccw_icd_version == icd)) == 0) {
      icd_run <- F
    } else {
      icd_run <- T
    }
    
    ### Set up output ----
    output <- list(ccw_code = unique(config$ccw_code),
                   ccw_desc = unique(config$ccw_desc),
                   ccw_abbrev = paste0("ccw_", unique(config$ccw_abbrev)),
                   lookback_months = config %>% filter(ccw_icd_version == icd) %>% select(lookback_months) %>% pull(),
                   dx_fields_condition = dx_fields_condition,
                   dx_exclude_condition = dx_exclude_condition,
                   claim_count_condition = claim_count_condition,
                   dx_claim1 = dx_claim1,
                   dx_claim2 = dx_claim2,
                   icd_run = icd_run)
    
    output
  }
  
  
  ## Temp table to hold condition-specific claims and dates ----
  header_load <- function(conn = conn, config_cond, icd = c(9, 10), print_query = F) {
    if (config_cond$icd_run == F) {
      return()
    }
    
    header_tbl <- DBI::SQL(paste0("##header_dx", icd))
    
    # Build SQL query
    # Split out table drop from table creation so it works in Synapse Analytics
    try(DBI::dbRemoveTable(conn, header_tbl, temporary = T), silent = T)
    
    sql1 <- glue::glue_sql(
      "--apply CCW claim type criteria to define conditions 1 and 2
    SELECT header.{`id_source`}, header.claim_header_id, header.claim_type_id, 
      header.first_service_date, diag_lookup.{`config_cond$ccw_abbrev`},
      diag_lookup.{`id_source`} as id_source_tmp,  -- zero rows returned without this, unclear why
      CASE WHEN header.claim_type_id in ({config_cond$dx_claim1}) THEN 1 ELSE 0 END AS 'condition1',
      CASE WHEN header.claim_type_id in ({config_cond$dx_claim2}) THEN 1 ELSE 0 END AS 'condition2',
      CASE WHEN header.claim_type_id in ({config_cond$dx_claim1}) THEN header.first_service_date ELSE null END AS 'condition_1_from_date',
      CASE WHEN header.claim_type_id in ({config_cond$dx_claim2}) THEN header.first_service_date ELSE null END AS 'condition_2_from_date'

    INTO {header_tbl}
    
    --pull out claim type and service dates
    FROM (
      SELECT {`id_source`}, claim_header_id, claim_type_id, first_service_date
      FROM {`claim_header_from_schema`}.{`claim_header_from_table`}) header
  
    --right join to claims containing a diagnosis in the CCW condition definition
    right join (
      SELECT diag.{`id_source`}, diag.claim_header_id, ref.{`config_cond$ccw_abbrev`} 
    
    --pull out claim and diagnosis fields
    FROM (
      SELECT {top_rows} {`id_source`}, claim_header_id, icdcm_norm, icdcm_version
      FROM {`icdcm_from_schema`}.{`icdcm_from_table`} 
      {config_cond$dx_fields_condition} AND icdcm_version = {icd}) diag

    --join to diagnosis reference table, subset to those with CCW condition
    inner join (
      SELECT icdcm, icdcm_version, {`config_cond$ccw_abbrev`}
      FROM {`icdcm_ref_schema`}.{`icdcm_ref_table`}
      where {`config_cond$ccw_abbrev`} = 1 AND icdcm_version = {icd}) ref
      
      on (diag.icdcm_norm = ref.icdcm) and (diag.icdcm_version = ref.icdcm_version)
      ) as diag_lookup
  
      on header.claim_header_id = diag_lookup.claim_header_id 
    {config_cond$dx_exclude_condition}
      
      -- Ensure only records with relevant claim types are kept
      -- the dx_exclude_condition code should have the start of the WHERE statement
      AND (header.claim_type_id in ({config_cond$dx_claim1}) OR header.claim_type_id in ({config_cond$dx_claim2}))",
      .con = conn)
    
    if (print_query == T) {
      print(sql1)
    }
    
    #Run SQL query
    dbGetQuery(conn = conn, sql1)
  }
  
  
  ## Temp table to hold ID and rolling time matrix ----
  rolling_load <- function(conn = conn, config_cond, icd = c(9, 10), print_query = F) {
    if (config_cond$icd_run == F) {
      return()
    }
    
    header_tbl <- DBI::SQL(paste0("##header_dx", icd))
    rolling_tbl <- DBI::SQL(paste0("##rolling_tmp_", icd))
    
    if (icd == 9) {
      rolling_break <- glue::glue_sql(" WHERE start_window < 
                                      {format(as.Date('2015-09-01') + 
                                      lubridate::dmonths(as.numeric(config_cond$lookback_months)) + 
                                      lubridate::ddays(1), usetz = FALSE)} ",
                                      .con = conn)
    } else if (icd == 10) {
      rolling_break <- glue::glue_sql(" WHERE start_window >= 
                                      {format(as.Date('2015-10-01') - 
                                      lubridate::dmonths(as.numeric(config_cond$lookback_months)) + 
                                      lubridate::ddays(1), usetz = FALSE)} ",
                                      .con = conn)
    }
    
    
    # Again, split table drop from creation
    try(DBI::dbRemoveTable(conn, rolling_tbl, temporary = T), silent = T)
    
    sql2 <- glue::glue_sql(
      "--join rolling time table to person ids
      SELECT id.{`id_source`}, rolling.start_window, rolling.end_window
      INTO {rolling_tbl}
      
      FROM 
        (SELECT distinct {`id_source`}, 'link' = 1 from {header_tbl}) as id
        
      RIGHT JOIN 
        (SELECT cast(start_window as date) as 'start_window', 
                cast(end_window as date) as 'end_window',
                'link' = 1
            FROM {`ref_schema`}.{DBI::SQL(ref_table_pre)}rolling_time_{DBI::SQL(config_cond$lookback_months)}mo_2012_2020
            {rolling_break}
        ) as rolling
        
      on id.link = rolling.link
      order by id.{`id_source`}, rolling.start_window",
      .con = conn)
    
    if (print_query == T) {
      print(sql2)
    }
    
    #Run SQL query
    dbGetQuery(conn = conn, sql2)
  }
  
  
  ## Collapse dates across both ICD versions ----
  ccw_load <- function(conn = conn, config_cond_9, config_cond_10, print_query = F) {
    ccw_tbl <- DBI::SQL(paste0("##", config_cond_10$ccw_abbrev))
    
    ### Determine code based on if both ICD version headers were made ----
    if (config_cond_9$icd_run == T) {
      icd9_code <- glue::glue_sql(
        "SELECT * FROM
            (SELECT matrix.{`id_source`}, matrix.start_window, matrix.end_window, cond.first_service_date, 
              cond.condition1, cond.condition2, condition_2_from_date  
              FROM (SELECT {`id_source`}, start_window, end_window FROM ##rolling_tmp_9) as matrix
            --join to condition temp table
            LEFT JOIN
            (SELECT {`id_source`}, first_service_date, condition1, condition2, condition_2_from_date
              FROM ##header_dx9) as cond
            ON matrix.{`id_source`} = cond.{`id_source`}
            WHERE cond.first_service_date between matrix.start_window and matrix.end_window) as a9",
            .con = conn)
    } else {
      icd9_code <- DBI::SQL('')
    }
    
    if (config_cond_10$icd_run == T) {
      icd10_code <- glue::glue_sql(
        "SELECT * FROM
            (SELECT matrix.{`id_source`}, matrix.start_window, matrix.end_window, cond.first_service_date, 
              cond.condition1, cond.condition2, condition_2_from_date  
              FROM (SELECT {`id_source`}, start_window, end_window FROM ##rolling_tmp_10) as matrix
            --join to condition temp table
            LEFT JOIN
            (SELECT {`id_source`}, first_service_date, condition1, condition2, condition_2_from_date
              FROM ##header_dx10) as cond
            ON matrix.{`id_source`} = cond.{`id_source`}
            WHERE cond.first_service_date between matrix.start_window and matrix.end_window) as a10",
        .con = conn)
    } else {
      icd10_code <- DBI::SQL('')
    }
    
    if (config_cond_9$icd_run == T & config_cond_10$icd_run == T) {
      union_code <- DBI::SQL(' UNION ')
    } else {
      union_code <- DBI::SQL('')
    }
    
    
    ### Set up code ----
    # Split out table drop from creation
    try(DBI::dbRemoveTable(conn, ccw_tbl, temporary = T), silent = T)
    
    sql3 <- glue::glue_sql(
      "--collapse to single row per ID and contiguous time period
        SELECT distinct d.{`id_source`}, min(d.start_window) as 'from_date', 
          max(d.end_window) as 'to_date', {config_cond_10$ccw_code} as 'ccw_code',
          {config_cond_10$ccw_abbrev} as 'ccw_desc'
      
      INTO {ccw_tbl}
      
      FROM (
      --set up groups where there is contiguous time
      SELECT c.{`id_source`}, c.start_window, c.end_window, c.discont, c.temp_row,
      
      sum(case when c.discont is null then 0 else 1 end) over
      (order by c.{`id_source`}, c.temp_row rows between unbounded preceding and current row) as 'grp'
  
    FROM (
      --pull out ID and time periods that contain appropriate claim counts
      SELECT b.{`id_source`}, b.start_window, b.end_window, b.condition_1_cnt, 
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
  
    FROM (
    --sum condition1 and condition2 claims by ID and period, take min and max service date for each condition2 claim by ID and period
      SELECT a.{`id_source`}, a.start_window, a.end_window, sum(a.condition1) as 'condition_1_cnt', 
      sum(a.condition2) as 'condition_2_cnt', min(a.condition_2_from_date) as 'condition_2_min_date', 
      max(a.condition_2_from_date) as 'condition_2_max_date'
    
        FROM 
        --pull ID, time period and claim information, subset to ID x time period rows containing a relevant claim
          (
          -- ICD-9
          {icd9_code}
          
          {union_code}
          
          -- ICD-10
          {icd10_code}
        ) a
        group by a.{`id_source`}, a.start_window, a.end_window
    
      ) as b 
      {config_cond_9$claim_count_condition}) as c
    ) as d
    group by d.{`id_source`}, d.grp
    order by d.{`id_source`}, from_date",
      .con = conn)
    
    if (print_query == T) {
      print(sql3)
    }
    
    #Run SQL query
    dbGetQuery(conn = conn, sql3)
  }
  
  
  ## Insert condition table into stage combined CCW table ----
  stage_load <- function(conn = conn, config_cond, print_query = F) {
    ccw_tbl <- DBI::SQL(paste0("##", config_cond$ccw_abbrev))
    
    sql4 <- glue::glue_sql(
      "INSERT INTO {`to_schema`}.{`to_table`} with (tablock)
      SELECT
      {`id_source`}, from_date, to_date, ccw_code, ccw_desc, getdate() as last_run
      FROM {`ccw_tbl`};

      -- drop temp tables to free up space on tempdb
      if object_id('tempdb..##header_10') IS NOT NULL drop table ##header_10;
      if object_id('tempdb..##rolling_tmp_9') IS NOT NULL drop table ##rolling_tmp_9;
      if object_id('tempdb..##rolling_tmp_10') IS NOT NULL drop table ##rolling_tmp_10;
      if object_id('tempdb..{`ccw_tbl`}') IS NOT NULL drop table {`ccw_tbl`};",
      .con = conn)
    
    if (print_query == T) {
      print(sql4)
    }
    
    #Run SQL query
    dbGetQuery(conn = conn, sql4)
  }
  
  
  
  
  # ## TEST ----
  # test <- config_cond_gen(cond = "cond_stroke2", icd = 9, ccw_source = "yaml", table_config = table_config2)
  # header_load(conn = conn, config = test, icd = 9, print_query = T)
  # rolling_load(conn = conn, config = test, icd = 9, print_query = T)
  # 
  # 
  # test2 <- config_cond_gen(cond = "cond_stroke2", icd = 10, ccw_source = "yaml", table_config = table_config2)
  # header_load(conn = conn, config = test2, icd = 10, print_query = T)
  # rolling_load(conn = conn, config = test2, icd = 10, print_query = T)
  # 
  # 
  # ccw_load(conn = conn, config_cond_9 = test, config_cond_10 = test2, print_query = T)
  # stage_load(conn = conn, config_cond = test2, print_query = T)
  # 
  # 
  # test3 <- config_cond_gen(cond = "asthma", icd = 9, ccw_source = "ref", table_config = conditions_ref)
  # header_load(conn = conn, config = test3, icd = 9, print_query = T)
  # rolling_load(conn = conn, config = test3, icd = 9, print_query = T)
  #   
  # test4 <- config_cond_gen(cond = "asthma", icd = 10, ccw_source = "ref", table_config = conditions_ref)
  # header_load(conn = conn, config = test4, icd = 10, print_query = T)
  # rolling_load(conn = conn, config = test4, icd = 10, print_query = T)
  # 
  # ccw_load(conn = conn, config_cond_9 = test3, config_cond_10 = test4, print_query = T)
  # stage_load(conn = conn, config_cond = test4, print_query = T)
  # 
  # ## END TEST ----
  
  
  
  # STEP 5: RUN CODE FOR EACH FUNCTION ----
  ptm01 <- proc.time() # Times how long this query takes
  
  ## Begin loop over conditions - loop continues across all SQL queries
  lapply(conditions, function(x) {
    
    message("Working on ", x)
    
    ## ICD-9 ----
    # Set up config
    config_9 <- config_cond_gen(cond = x, icd = 9, ccw_source = ccw_source, table_config = table_config)
    
    if (print_query == T) {
      print(config_9)
    }
    
    # Make header and rolling time tables
    header_load(conn = conn, config = config_9, icd = 9, print_query = print_query)
    rolling_load(conn = conn, config = config_9, icd = 9, print_query = print_query)
    
    
    ## ICD-10 ----
    # Set up config
    config_10 <- config_cond_gen(cond = x, icd = 10, ccw_source = ccw_source, table_config = table_config)
    
    if (print_query == T) {
      print(config_10)
    }
    
    # Make header and rolling time tables
    header_load(conn = conn, config = config_10, icd = 10, print_query = print_query)
    rolling_load(conn = conn, config = config_10, icd = 10, print_query = print_query)
    
    
    ## Combine ----
    ccw_load(conn = conn, config_cond_9 = config_9, config_cond_10 = config_10, print_query = F)
    stage_load(conn = conn, config_cond = config_10, print_query = F)
  })
  
  #Run time of all steps
  proc.time() - ptm01
  
}
