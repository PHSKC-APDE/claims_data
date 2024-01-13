#' @title Top N conditions seen among Mediciad patients
#' 
#' @description \code{top_causes} identifies the top N causes for a given set of visits.
#' 
#' @details This function builds a temp table with the IDs of a cohort of interest.
#' It then creates a SQL query to find claims for that cohort made in a given time frame
#' The top N categories are selected and a count of the claims in each category given.
#' There are optional flags for the following:
#' 1) To limit categories to just the primary dx vs. all dx.
#' 2) To restrict to certain visit types (e.g., ED visits, hospitalizations)
#' 
#' @param conn SQL server connection created using \code{odbc} package.
#' @param server Which server do you want to run the query against? NB. Currently only
#' Medicaid data is available on HHSAW.
#' @param source Which claims data source do you want to pull from?
#' @param cohort The group of individuals of interest. Note: it is possible to generate a cohort on the fly
#' using \code{\link{claims_elig}}.
#' @param cohort_id The field that contains the ID in the cohort data. Defaults to id_apde.
#' @param renew_ids Option to avoid reloading ID fields to temp table.
#' @param from_date Begin date for claims period, "YYYY-MM-DD", defaults to start of 
#' the previous calendar year.
#' @param to_date End date for claims period, "YYYY-MM-DD", defaults to end of the previous calendar year
#' or 6 months prior to today's date, whichever is earlier.
#' @param ind_dates Flag to indicate that individualized dates are used to narrow
#' the default date window.
#' @param ind_from_date Field in the cohort data that contains an individual from date.
#' @param ind_to_date Field in the cohort data that contains an individual to date.
#' @param top The maximum number of condition groups that will be returned, default is 15.
#' @param catch_all Determines whether or not catch_all codes are included in the list,
#' default is no.
#' @param primary_dx Whether or not to only look at the primary diagnosis field, default is TRUE.
#' @param type Which types of visits to include. Choose from the following:
#' ed (any ED visit), 
#' inpatient (any inpatient visit)
#' all (all claims, must be paired with override_all option)
#' @param override_all Override the warning message about pulling all claims, default is FALSE.
#'
#' @examples
#' \dontrun{
#' db_hhsaw <- create_db_connection("hhsaw", interactive = F, prod = T)
#' system.time(mcaid_only <- claims_elig(conn = db_hhsaw, source = "mcaid", server = "hhsaw",
#'     from_date = "2014-01-01", to_date = "2015-02-25",
#'     geo_zip = c("98104", "98133", "98155"),
#'     cov_type = "FFS", race_asian = 1, 
#'     show_query = T))
#' top_15_dynamic <- top_causes(
#'     cohort = mcaid_only, top = 3, conn = db_hhsaw, source = "mcaid",
#'     server="hhsaw"
#' )
#' }
#' 
#' @export

top_causes <- function(conn,
                       server = c("phclaims", "hhsaw"),
                       source = c("apcd", "mcaid", "mcaid_mcare", "mcare"),
                       cohort,
                       cohort_id = NULL,
                       renew_ids = T,
                       from_date = NULL,
                       to_date = NULL,
                       ind_dates = F,
                       ind_from_date = NULL,
                       ind_to_date = NULL,
                       top = 15,
                       catch_all = F,
                       primary_dx = T,
                       type = c("ed", "inpatient", "all"),
                       override_all = F) {
  
  
  #### ERROR CHECKS ####
  # ODBC check
  if(missing(conn)) {
    stop("please provide a SQL connection")
  }
  
  # Source check
  source <- match.arg(source)
  
  if (server == "hhsaw" & source != "mcaid") {
    stop("Currently only Medicaid data is available on HHSAW")
  }
  
  if (server == "phclaims") {
    header_schema <- "final"
    header_tbl_prefix <- ""
    ref_schema <- "ref"
  } else { # HHSAW organizes under claims schema
    header_schema <- "claims"
    header_tbl_prefix <- "final_"
    ref_schema <- "ref"
  }
  
  # Source
  if (source == "apcd") {
    id_name <- glue::glue_sql("id_apcd", .con = conn)
  } else if (source == "mcaid") {
    id_name <- glue::glue_sql("id_mcaid", .con = conn)
  } else if (source == "mcaid_mcare") {
    id_name <- glue::glue_sql("id_apde", .con = conn)
  } else if (source == "mcare") {
    id_name <- glue::glue_sql("id_mcare", .con = conn)
  } else {
    stop("Something went wrong when selecting a source")
  }
  
  # ID var name
  if (!missing(cohort_id)) {
    id_quo <- enquo(cohort_id)
  } else if ("id_apde" %in% names(cohort)) {
    id_quo <- quo(id_apde)
  } else if ("id_apcd" %in% names(cohort)) {
    id_quo <- quo(id_apcd)
  } else if ("id_mcaid" %in% names(cohort)) {
    id_quo <- quo(id_mcaid)
  } else if ("id_mcare" %in% names(cohort)) {
    id_quo <- quo(id_mcare)
  } else {
    stop("No valid ID field found")
  }
  
  # Assume that an individualized date fields are from_date and to_date
  if (ind_dates == T) {
    if (!missing(ind_from_date)) {
      ind_from_date_quo <- enquo(ind_from_date)
    } else if("from_date" %in% names(cohort)) {
      ind_from_date_quo <- quo(from_date)
    } else{
      stop("No valid individualized from date found")
    }
    
    if (!missing(ind_to_date)) {
      ind_to_date_quo <- enquo(ind_to_date)
    } else if("to_date" %in% names(cohort)) {
      ind_to_date_quo <- quo(to_date)
    } else{
      stop("No valid individualized to date found")
    }
  }
  
  # Set common dates (default to cover last calendar year)
  if (!is.null(from_date)) {
    if (str_detect(from_date, "[0-9]{4}-[0-9]{2}-[0-9]{2}") == F) {
      stop("Invalid from_date. Use YYYY-MM-DD format")
    } else {
      from_date <- as.Date(from_date)
      message(glue::glue("Looking at claims starting from {from_date}"))
    }
  } else if (is.null(from_date)) {
    from_date <- as.Date(paste0(year(Sys.Date()) - 1, "-01-01"))
  }
  
  if (!is.null(to_date)) {
    if (str_detect(to_date, "[0-9]{4}-[0-9]{2}-[0-9]{2}") == F) {
      stop("Invalid to_date. Use YYYY-MM-DD format")
    } else {
      to_date <- as.Date(to_date)
      message(glue::glue("Looking at claims through to {to_date}"))
    }
  } else if (is.null(to_date)) {
    to_date <- as.Date(as.numeric(min(
      paste0(year(Sys.Date()) - 1, "-12-31"),
      Sys.Date() %m-% months(6))),
      origin = "1970-01-01")
  }
  
  # Visit type
  type <- match.arg(type)
  
  
  #### SET UP SQL ####
  # Process dx type flag
  if (primary_dx == T) {
    dx_num <- glue::glue_sql("WHERE d.icdcm_number IN ('01', 'admit') ", .con = conn)
  } else {
    dx_num <- DBI::SQL('')
  }
  
  # Date range
  if (ind_dates == T) {
    extra_ind_cols <- glue::glue_sql("a.from_date_ind, a.to_date_ind, ")
    from_to_date <- glue::glue_sql("WHERE b.from_date >= a.from_date_ind AND b.from_date <= a.to_date_ind) ", .con = conn)
  } else {
    extra_ind_cols <- DBI::SQL('')
    from_to_date <- DBI::SQL('')
  }
  
  # Select visit type
  if (type == "ed") {
    flags <- glue::glue_sql(" (ed_pophealth_id IS NOT NULL) AND ", .con = conn)
  } else if (type == "inpatient") {
    flags <- glue::glue_sql(" (inpatient_id IS NOT NULL) AND ", .con = conn)
  } else if (type == "all" & override_all == T) {
    flags <- DBI::SQL('')
  } else {
    stop("Warning: no flags selected so all visits will be pulled (slow). 
           Use override_all = T to confirm")
  }
  
  
  
  #### SET UP IDS ####
  ### Extract list of unique IDs (and dates) and set up for writing to SQL
  if (ind_dates == T) {
    ids <- cohort %>% mutate(id = !!id_quo,
                             from_date_ind = !!ind_from_date_quo,
                             to_date_ind = !!ind_to_date_quo) %>%
      select(id, from_date_ind, to_date_ind)
    
    ids <- data.table::setDT(ids)
    ids <- unique(ids)
    ids <- ids[!(to_date_ind < from_date | from_date_ind > to_date)]
    ids[, from_date_ind := pmax(from_date_ind, from_date, na.rm = T)]
    ids[, to_date_ind := pmax(to_date_ind, to_date, na.rm = T)]
    ids <- unique(ids)
    
  } else {
    ids <- cohort %>% mutate(id = !!id_quo) %>% select(id)
    ids <- data.table::setDT(ids)
    ids <- unique(ids)
  }
  
  # Can only write 1000 values at a time so may need to do multiple rounds
  num_ids <- nrow(ids)
  n_rounds <- ceiling(num_ids/1000)
  id_lists <- as.list(glue::glue("id_list_{1:n_rounds}"))
  
  ### Compose SQL query
  # 1) Add IDs to local temp table (if new IDs are needed)
  if (renew_ids == T) {
    message("Setting up IDs in temp table")
    list_start <- 1
    list_end <- min(1000, num_ids)
    
    if (ind_dates == T) {
      id_vars_create <- glue::glue_sql("(id VARCHAR(20), from_date_ind DATE, to_date_ind DATE) ",
                                       .con = conn)
      id_vars <- glue::glue_sql("(id, from_date_ind, to_date_ind) ", .con = conn)
    } else {
      id_vars_create <- glue::glue_sql("(id VARCHAR(20)) ", .con = conn)
      id_vars <- glue::glue_sql("(id) ", .con = conn)
    }
    
    # Make progress bar
    print(glue("Loading {n_rounds} ID sets"))
    pb <- txtProgressBar(min = 0, max = n_rounds, style = 3)
    
    for (i in 1:n_rounds) {
      
      if (ind_dates == T) {
        id_lists[[i]] <- paste0("('", paste(paste(ids$id[list_start:list_end], 
                                                  ids$from_date_ind[list_start:list_end], 
                                                  ids$to_date_ind[list_start:list_end], 
                                                  sep = "', '"), 
                                            collapse = "'), ('"), "')")
      } else {
        id_lists[[i]] <- paste0("('", paste(ids$id[list_start:list_end], collapse = "'), ('"), "')")
      }
      
      
      if (i == 1) {
        # Clear temp table with standalone command
        # (otherwise switching between just ID and individual dates causes an error)
        DBI::dbExecute(conn, "IF object_id('tempdb..##temp_ids') IS NOT NULL DROP TABLE ##temp_ids;")
        id_load <- paste0("CREATE TABLE ##temp_ids ", id_vars_create,
                          "INSERT INTO ##temp_ids ", id_vars,
                          "VALUES ", id_lists[[i]], ";")
        DBI::dbExecute(conn, id_load)
      } else {
        id_load <- paste0("INSERT INTO ##temp_ids ", id_vars,
                          "VALUES ", id_lists[[i]], ";")
        DBI::dbExecute(conn, id_load)
      }
      
      list_start <- list_start + 1000
      list_end <- min(list_end + 1000, num_ids)
      
      # Update progress bar
      setTxtProgressBar(pb, i)
    }
    
    # Add index to id and from_date for faster join
    # Think about only using this if n_rounds is >2-3
    if (ind_dates == T) {
      DBI::dbExecute(conn,
                     "CREATE NONCLUSTERED INDEX temp_ids_id ON ##temp_ids (id) 
                    CREATE NONCLUSTERED INDEX temp_ids_from_date ON ##temp_ids (from_date_ind)")
    } else {
      DBI::dbExecute(conn,
                     "CREATE NONCLUSTERED INDEX temp_ids_id ON ##temp_ids (id)")
    }
  }
  
  
  #### JOIN DXS TO DX LOOKUP ####
  claim_query <- glue::glue_sql(
    "SELECT DISTINCT c.id, c.claim_header_id, c.from_date, c.ed_pophealth_id, c.inpatient_id, 
    e.ccs_detail_desc, e.ccs_catch_all
  FROM 
    (SELECT a.id, {extra_ind_cols} b.from_date, b.claim_header_id,
    b.ed_pophealth_id, b.inpatient_id 
    FROM ##temp_ids AS a
    LEFT JOIN 
    (SELECT {id_name}, first_service_date AS from_date, claim_header_id, ed_pophealth_id, inpatient_id, primary_diagnosis
    FROM {`header_schema`}.{`paste0(header_tbl_prefix, source, '_claim_header')`}
    WHERE first_service_date >= {from_date} AND first_service_date <= {to_date} AND 
      {flags} primary_diagnosis IS NOT NULL) AS b
    ON a.id = b.{id_name} {from_to_date}) AS c
    LEFT JOIN {`header_schema`}.{`paste0(header_tbl_prefix, source, '_claim_icdcm_header')`} AS d
    ON c.claim_header_id = d.claim_header_id
    LEFT JOIN {`ref_schema`}.{`'icdcm_codes'`} AS e
    ON d.icdcm_version = e.icdcm_version AND d.icdcm_norm = e.icdcm {dx_num};",
    .con = conn)

  
  claims <- DBI::dbGetQuery(conn, claim_query)
  claims <- claims[!is.na(claims$ccs_detail_desc),]
  
  #### PROCESS DATA IN R ####
  ### Decide whether or not to include catch-all categories
  if (catch_all == F) {
    claims <- claims %>% filter(is.na(ccs_catch_all) | ccs_catch_all == 0)
  }
  
  ### Take top N causes
  if (type == "ed") {
    claims <- claims %>%
      group_by(ccs_detail_desc) %>%
      summarise(claim_cnt = n_distinct(ed_pophealth_id)) %>%
      ungroup()
  } else if (type == "inpatient") {
    claims <- claims %>%
      group_by(ccs_detail_desc) %>%
      summarise(claim_cnt = n_distinct(inpatient_id)) %>%
      ungroup()
  } else {
    claims <- claims %>%
      group_by(ccs_detail_desc) %>%
      summarise(claim_cnt = n_distinct(claim_header_id)) %>%
      ungroup()
  }


  final_n <- min(n_distinct(claims$ccs_detail_desc), top)
  if (final_n < top) {
    print(paste0("Warning: Only ", final_n, " categories were found"))
  }

  claims <- top_n(claims, final_n, wt = claim_cnt) %>%
    arrange(-claim_cnt, ccs_detail_desc)

  return(claims)
}
