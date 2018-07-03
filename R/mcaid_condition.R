#' @title Medicaid member chronic health condition status
#' 
#' @description \code{mcaid_condition_f} builds a SQL query to return Medicaid member chronic health condition information.
#' 
#' @details LARGELY FOR INTERNAL USE
#' This function builds and sends a SQL query to return a Medicaid member cohort with a specified chronic health condition.
#' If requested, the function will also join the returned data to a specified data frame in R, joining on Medicaid member ID. 
#' Users can specify the join type (left, right, inner). By default "ever" status is returned - for example a request for diabetic
#' members will return members with any history of diabetes in the Medicaid claims database, using the Chronic Conditions Warehouse
#' definition from CDC. If a date range is supplied, the function will only return members who were identified as having the
#' condition during the date range (a function of both Medicaid coverage and health care diagnostic information).
#' 
#' @param server SQL server connection created using \code{odbc} package
#' @param condition The chronic health condition requested from SQL Server, current valid values include "asthma", "chr_kidney_dis", 
#' "copd", "depression", "diabetes", "heart_failure", "hypertension", and "ischemic_heart_dis"
#' @param from_date Begin date for Medicaid coverage period, "YYYY-MM-DD", defaults to 12 months prior to today's date
#' @param to_date End date for Medicaid coverage period, "YYYY-MM-DD", defaults to 6 months prior to today's date
#' @param join_type Type of requested join - left, right or inner, defaults to no join.
#' @df_join_name The name of the existing data frame to which the coverage group information should be joined, provided as unquoted text.
#'
#' @examples
#' \dontrun{
#' condition <- mcaid_condition_f(server = db.claims51, condition = "chr_kidney_dis")
#' condition <- mcaid_condition_f(server = db.claims51, condition = "asthma", join_type = "inner", df_join_name = elig_test)
#' condition <- mcaid_condition_f(server = db.claims51, condition = "depression", from_date = "2017-01-01", to_date = "2017-01-31")
#' }
#' 
#' @export
mcaid_condition_f <- function(server, condition, from_date, to_date, join_type = "none", df_join_name) {
  
  #Error checks
  if(missing(server)) {
    stop("please provide a SQL server where data resides")
  }
  
  if(!missing(join_type) & missing(df_join_name)) {
    stop("you asked to join result to another data frame, please provide the name of this data frame")
  }
  
  if((missing(from_date) & !missing(to_date)) | (!missing(from_date) & missing(to_date))) {
    stop("If from_date date provided, to_date date must also be provided. And vice versa.")
  }
  
  if(!join_type %in% c("left", "right", "inner", "none")) {
    stop("Not a valid join type - either omit parameter or select left, right or inner")
  }
  
  if(!condition %in% c("asthma", "chr_kidney_dis", "copd", "depression", "diabetes", "heart_failure", "hypertension",
                       "ischemic_heart_dis")) {
    stop("Not a valid condition, consult function help file to see valid values")
  }
  
  #Build SQL query
  
  if (!missing(from_date) & !missing(to_date)) {
    from_date_t <- paste("\'", from_date, "\'", sep = "")
    to_date_t <- paste("\'", to_date, "\'", sep = "")
    date_sql <- paste0("\n where from_date <= ", to_date_t, " and to_date >= ", from_date_t)
  } else {
    date_sql <- ""
  }
  
  condition_t <- paste0(condition, "_ccw")
  
  sql <- paste0("select distinct id, ", condition_t,
                  " from PHClaims.dbo.mcaid_claim_", condition, "_person", date_sql)
  
  #Execute SQL query
  result <- odbc::dbGetQuery(server, sql)
  
  #Join to existing data frame if desired
  if (join_type == "left") {
    result2 <- left_join(df_join_name, result, by = "id")
  } else if (join_type == "inner") {
    result2 <- inner_join(df_join_name, result, by = "id")
  } else if (join_type == "right") {
    result2 <- right_join(df_join_name, result, by = "id")
  } else {
    return(result)
  }
}