#' @title Find chronic health condition status for claims IDs
#' 
#' @description \code{claims_condition} builds a SQL query to return chronic health condition information.
#' 
#' @details LARGELY FOR INTERNAL USE
#' This function builds and sends a SQL query to return a Medicaid member cohort with a specified chronic health condition.
#' If requested, the function will also join the returned data to a specified data frame in R, joining on Medicaid member ID. 
#' Users can specify the join type (left, right, inner). By default "ever" status is returned - for example a request for diabetic
#' members will return members with any history of diabetes in the Medicaid claims database, using the Chronic Conditions Warehouse
#' definition from CDC. If a date range is supplied, the function will only return members who were identified as having the
#' condition during the date range (a function of both Medicaid coverage and health care diagnostic information).
#' 
#' @param conn SQL server connection created using \code{odbc} package
#' @param source Which claims data source do you want to pull from?
#' @param condition The chronic health condition requested from SQL Server. 
#' Can select multiple using format c("<condition1>", "<condition2>").
#' @param from_date Begin date for coverage period, "YYYY-MM-DD", 
#' defaults to 18 months prior to today's date.
#' @param to_date End date for coverage period, "YYYY-MM-DD", 
#' defaults to 6 months prior to today's date.
#' @param id List of IDs to look up. Use format c("<id1>", "<id2>") or point
#' to a vector of IDs. Leave blank for all IDs in the specified date range.
#'
#' @examples
#' \dontrun{
#' condition <- claims_condition(con = db_claims, source = "mcaid", condition = "ccw_anemia")
#' condition <- claims_condition(con = db_claims, source = "apcd",
#'                               from_date = "2020-01-01", to_date = "2020-12-31", 
#'                               condition = c("ccw_diabetes", "ccw_arthritis", "ccw_cataract", "ccw_hypertension")
#' condition <- claims_condition(con = db_claims, source = "mcaid_mcare", condition = "ccw_depression", id = c(1, 6, 2:8))
#' 
#' @export
claims_condition <- function(conn, 
                               source = c("apcd", "mcaid", "mcaid_mcare", "mcare"),
                               condition = c("ccw_alzheimer", "ccw_alzheimer_related",
                                             "ccw_anemia", "ccw_arthritis",
                                             "ccw_asthma", "ccw_atrial_fib",
                                             "ccw_bph", "ccw_cancer_breast",
                                             "ccw_cancer_colorectal", "ccw_cancer_endometrial",
                                             "ccw_cancer_lung", "ccw_cancer_prostate",
                                             "ccw_cancer_urologic", "ccw_cataract",
                                             "ccw_chr_kidney_dis", "ccw_copd",
                                             "ccw_depression", "ccw_diabetes",
                                             "ccw_glaucoma", "ccw_heart_failure",
                                             "ccw_hip_fracture", "ccw_hyperlipid",
                                             "ccw_hypertension", "ccw_hypothyroid",
                                             "ccw_ischemic_heart_dis", "ccw_mi",
                                             "ccw_non_alzheimer_dementia", "ccw_osteoporosis",
                                             "ccw_parkinsons", "ccw_pneumonia",
                                             "ccw_stroke"), 
                               from_date = Sys.Date() %m-% months(18),
                               to_date = Sys.Date() %m-% months(6),
                               id = NULL) {
  
  
  #### ERROR CHECKS ####
  # ODBC check
  if(missing(conn)) {
    stop("please provide a SQL connection")
  }
  
  # Date checks
  if(from_date > to_date & !missing(from_date) & !missing(to_date)) {
    stop("from_date date must be <= to_date date")
  }
  
  if(missing(from_date) & missing(to_date)) {
    message("Default from/to dates used: - 18 and 6 months prior to today's date, respectively")
  }
  
  
  #### SET UP VARIABLES ####
  source <- match.arg(source)
  tbl <- glue::glue("{source}_claim_ccw")
  
  condition <- match.arg(condition, several.ok = T)
  
  # ID var name
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
  
  
  #### PROCESS PARAMETERS FOR SQL QUERY ####
  # ID
  ifelse(!is.null(id), 
         id_sql <- glue::glue_sql(" AND {id_name} IN ({id*}) ", .con = conn),
         id_sql <- DBI::SQL(''))
  
  
  # Condtion
  cond_sql <- glue::glue_sql(" AND ccw_desc IN ({condition*}) ", .con = conn)
  
  
  #### BUILD AND RUN SQL QUERY ####
  sql_call <- glue::glue_sql(
    "SELECT {id_name}, ccw_desc, from_date, to_date
    FROM final.{`tbl`} 
    WHERE from_date <= {to_date} AND to_date >= {from_date} 
    {cond_sql} {id_sql} 
    ORDER BY {id_name}, ccw_desc, from_date",
    .con = conn)
  
  #Execute SQL query
  result <- DBI::dbGetQuery(conn, sql_call)
  
  return(result)
}
