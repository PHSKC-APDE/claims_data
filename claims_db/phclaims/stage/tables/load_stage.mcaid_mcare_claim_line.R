#### CODE TO LOAD & TABLE-LEVEL QA STAGE.mcaid_mcare_claim_line
# Eli Kern, PHSKC and Alastair Matheson (APDE)
#
# 2019-12, major update 2021-05

### Run from master_mcaid_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/master_mcaid_mcare_full_union.R

# Function has the following options
# conn = ODBC connection object
# config = Already loaded YAML config file
# config_url = URL to load a YAML config file
# config_file = Path to load a YAML config file.
# mcaid_date = Year and month from which to load new rows. Format is YYYY-MM-DD. Default is NULL, in which case all rows are loaded.
# mcare_date = Year from which to load new rows. Format is YYYY. Default is NULL, in which case all rows are loaded.
#
# NOTE: Until we develop a system of permanent APDE IDs, any partial load requires all IDs to be updated

# General approach is as follows:
# 1) Maintain both stage and final versions of the table.
# 2) Only drop rows from the stage table that meet the date criteria. Then 
#     only insert rows from the final source tables that also meet the date criteria.
# 3) If stage table passes QA (number of rows expected matches), repeat the same on final.
# This should be more efficient than moving the entire tables around each time.

# NEED TO ADD IN CHECKS THAT STAGE AND FINAL TABLE EXIST AND ARE POPULATED
# THEN CHECK ROWS IN EACH STAGE AND FINAL, IF THEY ARE MISMATCHED DO NOT RUN
# 


load_stage.mcaid_mcare_claim_line_f <- function(conn = NULL,
                                                config = NULL,
                                                config_url = NULL,
                                                config_file = NULL,
                                                to_schema = "stage",
                                                to_table = "mcaid_mcare_claim_line",
                                                from_schema = "final",
                                                from_table_mcaid = "mcaid_claim_line",
                                                from_table_mcare = "mcare_claim_line",
                                                xwalk_schema_old = "archive",
                                                xwalk_table_old = "xwalk_apde_mcaid_mcaid_pha",
                                                xwalk_schema_new = "final",
                                                xwalk_table_new = "xwalk_apde_mcaid_mcaid_pha",
                                                mcaid_date = NULL,
                                                mcare_date = NULL) {
  
  
  # ERROR CHECKS ----
  # Check if the config provided is a local object, file, or on a web page
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a local config object, config_url, or config_file but only one")
  }
  
  if (is.na(as.Date(mcaid_date, format = "%Y-%m-%d")) | 
      !between(as.Date(mcaid_date, format = "%Y-%m-%d"), 
               as.Date("2012-01-01", format = "%Y-%m-%d"), 
               as.Date("2099-12-31", format = "%Y-%m-%d"))) {
    stop("mcaid_date should have YYYY-MM-DD format and be >= 2012-01-01")
  }
  if (!between(mcare_date, 2012, 2099)) {
    stop("mcare_date should have YYYY format and be >= 2012")
  }
  
  
  # READ IN CONFIG FILE ----
  # NOTE: The mcaid_mcare YAML files still have the older structure (no server, etc.)
  # If the format is updated, need to add in more robust code here to identify schema and table names
  
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  # VARIABLES ----
  ## to_schema ----
  if (is.null(to_schema)) {
    if (!is.null(table_config[[server]][["to_schema"]])) {
      to_schema <- table_config[[server]][["to_schema"]]
    } else if (!is.null(table_config$to_schema)) {
      to_schema <- table_config$to_schema
    }
  }
  
  ## to_table ----
  if (is.null(to_table)) {
    if (!is.null(table_config[[server]][["to_table"]])) {
      to_table <- table_config[[server]][["to_table"]]
    } else if (!is.null(table_config$to_table)) {
      to_table <- table_config$to_table
    }
  }
  
  ## from_schema ----
  if (is.null(from_schema)) {
    if (!is.null(table_config[[server]][["from_schema"]])) {
      from_schema <- table_config[[server]][["from_schema"]]
    } else if (!is.null(table_config$from_schema)) {
      from_schema <- table_config$from_schema
    }
  }
  
  ## from_table_mcaid ----
  if (is.null(from_table_mcaid)) {
    if (!is.null(table_config[[server]][["from_table_mcaid"]])) {
      from_table_mcaid <- table_config[[server]][["from_table_mcaid"]]
    } else if (!is.null(table_config$from_table_mcaid)) {
      from_table_mcaid <- table_config$from_table_mcaid
    }
  }
  
  ## from_table_mcare ----
  if (is.null(from_table_mcare)) {
    if (!is.null(table_config[[server]][["from_table_mcare"]])) {
      from_table_mcare <- table_config[[server]][["from_table_mcare"]]
    } else if (!is.null(table_config$from_table_mcare)) {
      from_table_mcare <- table_config$from_table_mcare
    }
  }
  
  ## xwalk_schema_old ----
  if (is.null(xwalk_schema_old)) {
    if (!is.null(table_config[[server]][["xwalk_schema_old"]])) {
      xwalk_schema_old <- table_config[[server]][["xwalk_schema_old"]]
    } else if (!is.null(table_config$xwalk_schema_old)) {
      xwalk_schema_old <- table_config$xwalk_schema_old
    }
  }
  
  ## xwalk_table_old ----
  if (is.null(xwalk_table_old)) {
    if (!is.null(table_config[[server]][["xwalk_table_old"]])) {
      xwalk_table_old <- table_config[[server]][["xwalk_table_old"]]
    } else if (!is.null(table_config$xwalk_table_old)) {
      xwalk_table_old <- table_config$xwalk_table_old
    }
  }
  
  ## xwalk_schema_new ----
  if (is.null(xwalk_schema_new)) {
    if (!is.null(table_config[[server]][["xwalk_schema_new"]])) {
      xwalk_schema_new <- table_config[[server]][["xwalk_schema_new"]]
    } else if (!is.null(table_config$xwalk_schema_new)) {
      xwalk_schema_new <- table_config$xwalk_schema_new
    }
  }
  
  ## xwalk_table_new ----
  if (is.null(xwalk_table_new)) {
    if (!is.null(table_config[[server]][["xwalk_table_new"]])) {
      xwalk_table_new <- table_config[[server]][["xwalk_table_new"]]
    } else if (!is.null(table_config$xwalk_table_new)) {
      xwalk_table_new <- table_config$xwalk_table_new
    }
  }
  
  ## mcaid_date ----
  if (!is.null(mcaid_date)) {
    mcaid_delete_sql <- glue::glue_sql(" WHERE (first_service_date >= {mcaid_date} AND source_desc = 'mcaid') ",
                                     .con = conn)
    mcaid_add_sql <- glue::glue_sql(" WHERE first_service_date >= {mcaid_date} ",
                                    .con = conn)
  } else  {
    mcaid_delete_sql <- DBI::SQL("")
    mcaid_add_sql <- DBI::SQL("")
  }
  
  ## mcare_date ----
  if (!is.null(mcare_date) & !is.null(mcaid_date)) {
    mcare_delete_sql <- glue::glue_sql(" OR (YEAR(first_service_date) >= {mcare_date} AND source_desc = 'mcare') ",
                                     .con = conn)
    mcare_add_sql <- glue::glue_sql(" WHERE YEAR(first_service_date) >= {mcare_date} ", .con = conn)
  } else if (!is.null(mcare_date) & is.null(mcaid_date)) {
    mcare_delete_sql <- glue::glue_sql(" WHERE (YEAR(first_service_date) >= {mcare_date} AND source_desc = 'mcare') ",
                                     .con = conn)
    mcare_add_sql <- glue::glue_sql(" WHERE YEAR(first_service_date) >= {mcare_date} ", .con = conn)
  } else  {
    mcare_delete_sql <- DBI::SQL("")
    mcare_add_sql <- DBI::SQL("")
  }
  
  
  # UPDATE EXISTING IDS ----
  # Only need to do this if we are only loading partial data
  
  ## THIS NEEDS TESTING!!!!! ----
  id_update <- glue::glue_sql("UPDATE {`to_schema`}.{`to_table`} 
                              SET id_apde = y.id_apde
                              FROM 
                              {`to_schema`}.{`to_table`} a
                              LEFT JOIN
                              {`xwalk_schema_old`}.{`xwalk_table_old`} x
                              ON a.id_apde = x.id_apde
                              LEFT JOIN
                              {`xwalk_schema_new`}.{`xwalk_table_new`} y
                              ON (x.id_mcaid = y.id_mcaid AND x.id_mcare IS NULL AND y.id_mcare IS NULL) OR 
                              (x.id_mcare = y.id_mcare AND x.id_mcaid IS NULL AND y.id_mcaid IS NULL) OR
                              (x.id_mcaid = y.id_mcaid AND x.id_mcare = y.id_mcare)",
                              .con = conn)
  
  # Run SQL query
  DBI::dbExecute(conn, id_update)
  
  
  # TRUNCATE EXISTING STAGE ----
  if (!is.na(mcaid_date) | !is.na(mcare_date)) {
    copy_sql <- glue::glue_sql(
      "DELETE {`to_schema`}.{`to_table`}
      {mcaid_delete_sql} {mcare_delete_sql}",
      .con = conn)
  }
  
  
  # SET UP AND RUN SQL ----
  insert_sql <- glue::glue_sql(
    "SELECT
    --top 100
    b.id_apde
    ,'mcaid' as source_desc
    ,cast(a.claim_header_id as varchar(255)) collate SQL_Latin1_General_Cp1_CS_AS
    ,cast(a.claim_line_id as varchar(255))
    ,a.first_service_date
    ,a.last_service_date
    ,a.rev_code as revenue_code
    ,place_of_service_code = null
    ,type_of_service = null
    ,a.rac_code_line
    ,filetype_mcare = null
    ,getdate() as last_run
    FROM {`from_schema`}.{`from_table_mcaid`} 
    {mcaid_add_sql} AS a
    LEFT JOIN {`xwalk_schema_new`}.{`xwalk_table_new`} AS b
    ON a.id_mcaid = b.id_mcaid
    
    UNION
    
    SELECT
    b.id_apde
    ,'mcare' as source_desc
    ,a.claim_header_id
    ,a.claim_line_id
    ,first_service_date
    ,last_service_date
    ,a.revenue_code
    ,a.place_of_service_code
    ,a.type_of_service
    ,rac_code_line = null
    ,a.filetype_mcare
    ,getdate() as last_run
    FROM {`from_schema`}.{`from_table_mcare`}
    {mcare_add_sql} AS a
    LEFT JOIN {`xwalk_schema_new`}.{`xwalk_table_new`} AS b
    on a.id_mcare = b.id_mcare",
    .con = conn)
  
  # Run SQL query
  DBI::dbExecute(conn, insert_sql)
  }


# Table-level QA script ----
## THIS NEEDS REWORKING!!!!!!!! ----
qa_stage.mcaid_mcare_claim_line_qa_f <- function() {
  
  #confirm that claim row counts match as expected for union
  res1 <- dbGetQuery(conn = conn, glue_sql(
    "select 'stage.mcaid_mcare_claim_line' as 'table', 'row count, expect match with sum of mcaid and mcare' as qa_type,
    count(*) as qa
    from stage.mcaid_mcare_claim_line;",
    .con = conn))
  
  res2 <- dbGetQuery(conn = conn, glue_sql(
    "select 'stage.mcaid_claim_line' as 'table', 'row count' as qa_type,
    count(*) as qa
    from final.mcaid_claim_line;",
    .con = conn))
  
  res3 <- dbGetQuery(conn = conn, glue_sql(
    "select 'stage.mcare_claim_line' as 'table', 'row count' as qa_type,
    count(*) as qa
    from final.mcare_claim_line;",
    .con = conn))
  
res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}