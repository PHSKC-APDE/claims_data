#' @title Top N conditions seen among Mediciad patients
#' 
#' @description \code{top_causes_f} identifies the top N causes for a given set of visits.
#' 
#' @details This function builds a temp table with the IDs of a cohort of interest.
#' It then creates a SQL query to find claims for that cohort made in a given time frame
#' The top N categories are selected and a count of the claims in each category given.
#' There are optional flags for the following:
#' 1) To limit categories to just the primary dx vs. all dx.
#' 2) To restrict to certain visit types (e.g., ED visits, hospitalizations)
#' 
#' @param cohort The group of individuals of interest. Note: it is possible to generate a cohort on the fly
#' using \code{\link{mcaid_elig_f}}.
#' @param cohort_id The field that contains the Medicaid ID in the cohort data. Defaults to id.
#' @param server SQL server connection created using \code{odbc} package.
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
#' @param ed_all Will include any ED visit
#' @param ed_avoid_ny Will include any avoidable ED visit (based on NYU classification)
#' @param ed_avoid_ca Will include any avoidable ED visit (based on CA classification)
#' @param inpatient Will include any inpatient visit
#'
#' @examples
#' \dontrun{
#' top_15 <- top_causes_f(cohort = focus_pop, cohort_id = id, server = db.claims51)
#' top_15_dynamic <- top_causes_f(cohort = mcaid_elig_f(server = db.claims51, 
#' from_date = "2017-01-01", to_date = "2017-12-31", korean = 1, zip = "98103"), top = 3)
#' }
#' 
#' @export
top_causes_f <- function(cohort,
                         cohort_id = NULL,
                         server = db.claims51,
                         renew_ids = T,
                         from_date = NULL,
                         to_date = NULL,
                         ind_dates = F,
                         ind_from_date = NULL,
                         ind_to_date = NULL,
                         top = 15,
                         catch_all = F,
                         primary_dx = T,
                         ed_all = T,
                         ed_avoid_ny = T,
                         ed_avoid_ca = T,
                         inpatient = T) {
  
  ### Set up quosures and other vars
  # Assume that id is the variable
  if (!missing(cohort_id)) {
    id_quo <- enquo(cohort_id)
  } else if("id" %in% names(cohort)) {
    id_quo <- quo(id)
  } else{
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
      print(paste0("Looking at claims starting from ", from_date))
    }
  } else if (is.null(from_date)) {
    from_date <- as.date(paste0(year(Sys.Date()) - 1, "-01-01"))
  }
  
  if (!is.null(to_date)) {
    if (str_detect(to_date, "[0-9]{4}-[0-9]{2}-[0-9]{2}") == F) {
      stop("Invalid to_date. Use YYYY-MM-DD format")
    } else {
      to_date <- as.Date(to_date)
      print(paste0("Looking at claims through to ", to_date))
    }
  } else if (is.null(to_date)) {
    to_date <- as.date(min(paste0(year(Sys.Date()) - 1, "-12-31"), Sys.Date() - months(6)))
  }
  
  # Process dx type flag
  if (primary_dx == T) {
    dx_num <- "WHERE d.dx_number = 1"
  } else {
    dx_num <- NULL
  }
  
  # Combine visit type flags together
  if (ed_avoid_ny == F & ed_avoid_ny == F & ed_avoid_ca == F & inpatient == F) {
    flags <- NULL
  } else {
    flags <- " ("
    
    if (ed_all == T) {
      flags <- paste0(flags, "ed = 1")
    }
    
    if (ed_avoid_ny == T) {
      if(nchar(flags) > 2) {
        flags <- paste0(flags, " OR ", "ed_nonemergent_nyu = 1")
      } else {
        flags <- paste0(flags, "ed_nonemergent_nyu = 1")
      }
    }
    
    if (ed_avoid_ca == T) {
      if(nchar(flags) > 2) {
        flags <- paste0(flags, " OR ", "ed_avoid_ca = 1")
      } else {
        flags <- paste0(flags, "ed_avoid_ca = 1")
      }
    }
    
    if (inpatient == T) {
      if(nchar(flags) > 2) {
        flags <- paste0(flags, " OR ", "inpatient = 1")
      } else {
        flags <- paste0(flags, "inpatient = 1")
      }
    }
    
    flags <- paste0(flags, ") AND ")
  }
  
  
  ### Extract list of unique IDs (and dates) and set up for writing to SQL
  if (ind_dates == T) {
    ids <- cohort %>% distinct(!!id_quo, !!ind_from_date_quo, !!ind_to_date_quo) %>% 
      rename(id = !!id_quo,
             from_date_ind = !!ind_from_date_quo,
             to_date_ind = !!ind_to_date_quo) %>%
      # Constrain dates to between common range
      filter(!(to_date_ind < from_date | from_date_ind > to_date)) %>%
      mutate(from_date_ind = pmax(from_date_ind, from_date, na.rm = T),
             to_date_ind = pmin(to_date_ind, to_date, na.rm = T)) %>%
      distinct(id, from_date_ind, to_date_ind)
    
  } else {
    ids <- cohort %>% distinct(!!id_quo) %>% rename(id = !!id_quo)
  }
  
  # Can only write 1000 values at a time so may need to do multiple rounds
  num_ids <- nrow(ids)
  n_rounds <- ceiling(num_ids/1000)
  id_lists <- as.list(paste0("id_list_", 1:n_rounds))
  
  ### Compose SQL query
  # 1) Add IDs to local temp table (if new IDs are needed)
  if (renew_ids == T) {
    list_start <- 1
    list_end <- min(1000, num_ids)
    
    if (ind_dates == T) {
      id_vars_create <- "(id VARCHAR(20), from_date_ind DATE, to_date_ind DATE) "
      id_vars <- "(id, from_date_ind, to_date_ind) "
    } else {
      id_vars_create <- "(id VARCHAR(20)) "
      id_vars <- "(id) "
    }
    
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
        print(paste0("Loading ID set 1 of ", n_rounds))
        id_load <- paste0("IF object_id('tempdb..##temp_ids') IS NOT NULL DROP TABLE ##temp_ids;
                        CREATE TABLE ##temp_ids ", id_vars_create,
                          "INSERT INTO ##temp_ids ", id_vars,
                          "VALUES ", id_lists[[i]], ";")
        DBI::dbExecute(server, id_load)
      } else {
        print(paste0("Loading ID set ", i, " of ", n_rounds))
        id_load <- paste0("INSERT INTO ##temp_ids ", id_vars,
                          "VALUES ", id_lists[[i]], ";")
        DBI::dbExecute(server, id_load)
      }
      
      list_start <- list_start + 1000
      list_end <- min(list_end + 1000, num_ids)
    }
  }
  

  
  # 2) Pull claims from date range into temp table
  
  claim_load <- paste0("IF object_id('tempdb..##claims_temp') IS NOT NULL DROP TABLE ##claims_temp;
                       SELECT id, from_date, tcn, ed, inpatient, ccs_description
                       INTO ##claims_temp
                       FROM PHClaims.dbo.mcaid_claim_summary
                       WHERE from_date >= '", from_date, "' AND from_date <= '", to_date, "' AND ",
                       flags,
                       "ccs_description IS NOT NULL")
  
  DBI::dbExecute(server, claim_load)
  
  # 3) Join IDs to claims that fall in desired date range
  # 4) Obtain DXs from claims
  # 5) Join DXs to DX lookup
  if (ind_dates == T) {
    claim_query <- paste0("SELECT DISTINCT c.id, c.from_date, e.ccs_final_plain_lang, e.ccs_catch_all
                          FROM (SELECT a.id, a.from_date_ind, a.to_date_ind, b.from_date, b.tcn
                          FROM ##temp_ids AS a
                          LEFT JOIN 
                          ##claims_temp AS b
                          ON a.id = b.id
                          WHERE b.from_date >= a.from_date_ind AND b.from_date <= a.to_date_ind) AS c
                          LEFT JOIN PHClaims.dbo.mcaid_claim_dx AS d
                          ON c.tcn = d.tcn
                          LEFT JOIN PHClaims.dbo.ref_dx_lookup AS e
                          ON d.dx_norm = e.dx and d.dx_ver = e.dx_ver ",
                          dx_num,
                          " ORDER BY c.id, c.from_date, e.ccs_final_plain_lang;")
  } else {
    claim_query <- paste0("SELECT DISTINCT c.id, c.from_date, e.ccs_final_plain_lang, e.ccs_catch_all
                          FROM (SELECT a.id, b.from_date, b.tcn
                          FROM ##temp_ids AS a
                          LEFT JOIN 
                          ##claims_temp AS b
                          ON a.id = b.id) AS c
                          LEFT JOIN PHClaims.dbo.mcaid_claim_dx AS d
                          ON c.tcn = d.tcn
                          LEFT JOIN PHClaims.dbo.ref_dx_lookup AS e
                          ON d.dx_norm = e.dx and d.dx_ver = e.dx_ver ",
                          dx_num,
                          " ORDER BY c.id, c.from_date, e.ccs_final_plain_lang;")
  }
  
  claims <- DBI::dbGetQuery(server, claim_query)
  
  
  ### Decide whether or not to include catch-all categories
  if (catch_all == F) {
    claims <- claims %>% filter(is.na(ccs_catch_all))
  }
  
  
  ### Take top N causes
  claims <- claims %>%
    group_by(ccs_final_plain_lang) %>%
    summarise(claim_cnt = n()) %>%
    ungroup()
  
  final_n <- min(n_distinct(claims$ccs_final_plain_lang), top)
  if (final_n < top) {
    print(paste0("Warning: Only ", final_n, " categories were found"))
  }
  
  claims <- top_n(claims, final_n, wt = claim_cnt) %>%
    arrange(-claim_cnt, ccs_final_plain_lang)
  
  return(claims)
}
