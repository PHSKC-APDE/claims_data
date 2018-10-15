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
#' @param cohort_d The field that contains the Medicaid ID in the cohort data. Defaults to id.
#' @param server SQL server connection created using \code{odbc} package.
#' @param from_date Begin date for claims period, "YYYY-MM-DD", defaults to start of 
#' the previous calendar year.
#' @param to_date End date for claims period, "YYYY-MM-DD", defaults to end of the previous calendar year
#' or 6 months prior to today's date, whichever is earlier.
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
                         from_date = NULL,
                         to_date = NULL,
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
  
  # Set default dates
  if (!is.null(from_date)) {
    if (str_detect(from_date, "[0-9]{4}-[0-9]{2}-[0-9]{2}") == F) {
      stop("Invalid from_date. Use YYYY-MM-DD format")
    } else {
      print(paste0("Looking at claims starting from ", from_date))
    }
  } else if (is.null(from_date)) {
    from_date <- paste0(year(Sys.Date()) - 1, "-01-01")
  }
  
  if (!is.null(to_date)) {
    if (str_detect(to_date, "[0-9]{4}-[0-9]{2}-[0-9]{2}") == F) {
      stop("Invalid to_date. Use YYYY-MM-DD format")
    } else {
      print(paste0("Looking at claims starting to ", to_date))
    }
  } else if (is.null(to_date)) {
    to_date <- paste0(year(Sys.Date()) - 1, "-12-31")
  }
  
  # Process flags
  if (primary_dx == T) {
    dx_num <- "WHERE d.dx_number = 1"
  } else {
    dx_num <- NULL
  }
  
  if (ed_all == T) {
    ed_all_code <- "b.ed = 1"
  } else {
    ed_all_code <- NULL
  }
  
  if (ed_avoid_ny == T) {
    ed_avoid_ny_code <- "b.ed_nonemergent_nyu = 1"
  } else {
    ed_avoid_ny_code <- NULL
  }
  
  if (ed_avoid_ca == T) {
    ed_avoid_ca_code <- "b.ed_avoid_ca = 1"
  } else {
    ed_avoid_ca_code <- NULL
  }
  
  if (inpatient == T) {
    inpatient_code <- "b.inpatient = 1 "
  } else {
    inpatient_code <- NULL
  }
  
  # Combine flags together
  if (is.null(ed_all_code) & is.null(ed_avoid_ny) & 
      is.null(ed_avoid_ca) & is.null(inpatient)) {
    flags <- NULL
  } else {
    flags <- " ("
    
    if (!is.null(ed_all_code)) {
      flags <- paste0(flags, ed_all_code)
    }
    
    if (!is.null(ed_avoid_ny_code)) {
      if(nchar(flags) > 2) {
        flags <- paste0(flags, " OR ", ed_avoid_ny_code)
      } else {
        flags <- paste0(flags, ed_avoid_ny_code)
      }
    }
    
    if (!is.null(ed_avoid_ca_code)) {
      if(nchar(flags) > 2) {
        flags <- paste0(flags, " OR ", ed_avoid_ca_code)
      } else {
        flags <- paste0(flags, ed_avoid_ca_code)
      }
    }
    
    if (!is.null(inpatient_code)) {
      if(nchar(flags) > 2) {
        flags <- paste0(flags, " OR ", inpatient_code)
      } else {
        flags <- paste0(flags, inpatient_code)
      }
    }
    
    flags <- paste0(flags, ") AND ")
  }
  

  ### Extract list of unique IDs and set up for writing to SQL
  ids <- cohort %>% distinct(!!id_quo) %>% rename(id = !!id_quo)
  # Can only write 1000 values at a time so may need to do multiple rounds
  num_ids <- nrow(ids)
  n_rounds <- ceiling(num_ids/1000)
  id_lists <- as.list(paste0("id_list_", 1:n_rounds))
  
  ### Compose SQL query
  # 1) Add IDs to local temp table
  list_start <- 1
  list_end <- min(1000, num_ids)
  
  for (i in 1:n_rounds) {
    
    id_lists[[i]] <- paste0("('", paste(ids$id[list_start:list_end], collapse = "'), ('"), "')")
    
    list_start <- list_start + 1000
    list_end <- min(list_start + 1000, num_ids)
    
    if (i == 1) {
      id_load <- paste0("IF object_id('tempdb..##temp_ids') IS NOT NULL DROP TABLE ##temp_ids;
                        CREATE TABLE ##temp_ids (id VARCHAR(20))
                        INSERT INTO ##temp_ids (id)
                        VALUES ", id_lists[[i]], ";")
      DBI::dbExecute(server, id_load)
    } else {
      id_load <- paste0("INSERT INTO ##temp_ids (id)
                        VALUES ", id_lists[[i]], ";")
      DBI::dbExecute(server, id_load)
    }
  }
  
  
  # 2) Join IDs to claims that fall in desired date range
  # 3) Obtain DXs from claims
  # 4) Join DXs to DX lookup
  claim_query <- paste0("SELECT DISTINCT c.id, c.from_date, e.ccs_final_description, 
                          e.ccs_final_plain_lang, e.ccs_catch_all
                        FROM (SELECT a.id, b.from_date, b.tcn
                        FROM ##temp_ids AS a
                        LEFT JOIN PHClaims.dbo.mcaid_claim_summary AS b
                        ON a.id = b.id
                        WHERE b.from_date >= '", from_date, "' AND 
                        b.from_date <= '", to_date, "' AND ",
                        flags,
                        "b.ccs_description IS NOT NULL) AS c
                        LEFT JOIN PHClaims.dbo.mcaid_claim_dx AS d
                        ON c.tcn = d.tcn
                        LEFT JOIN PHClaims.dbo.ref_dx_lookup AS e
                        ON d.dx_norm = e.dx and d.dx_ver = e.dx_ver ",
                        dx_num,
                        " ORDER BY c.id, c.from_date, e.ccs_final_plain_lang;")
  
  claims <- DBI::dbGetQuery(server, claim_query)
  
  ### Decide whether or not to include catch-all categories
  if (catch_all == F) {
    claims <- claims %>% filter(is.na(ccs_catch_all))
  }


  ### Take top N causes
  claims <- claims %>%
    group_by(ccs_final_description, ccs_final_plain_lang) %>%
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
