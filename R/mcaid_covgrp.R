#' @title Medicaid member coverage group information
#' 
#' @description \code{mcaid_covgrp_f} builds a SQL query to return Medicaid member coverage group information.
#' 
#' @details LARGELY FOR INTERNAL USE
#' This function builds and sends a SQL query to return a Medicaid member cohort with coverage group information
#' for a specified coverage date range. If requested, the function will also join the returned data to a specified data
#' frame in R, joining on Medicaid member ID. Given that members can have multiple values for a coverage group variable
#' in a given date range (e.g. in new adult coverage for 1st 6 months and then older adults coverage for next 6 months),
#' the function will return a 0/1 value overall for the requested date range. For example, if a member had new adult 
#' coverage at any point during the date range a "1" will be returned.
#' 
#' @param server SQL server connection created using \code{odbc} package
#' @param from_date Begin date for Medicaid coverage period, "YYYY-MM-DD", defaults to 12 months prior to today's date
#' @param to_date End date for Medicaid coverage period, "YYYY-MM-DD", defaults to 6 months prior to today's date
#' @param join True/false value to denote whether returned data should be joined to an existing data frame, defaults to FALSE
#' @df_join_name The name of the existing data frame to which the coverage group information should be joined, provided as unquoted text.
#'
#' @examples
#' \dontrun{
#' elig_test_covgrp <- mcaid_covgrp_f(server = db.claims51, from_date = "2016-04-01", to_date = "2017-03-31")
#' elig_test_covgrp <- mcaid_covgrp_f(server = db.claims51, from_date = "2016-04-01", to_date = "2017-03-31", join = T,
#' df_join_name = elig_test)
#' }
#' 
#' @export
mcaid_covgrp_f <- function(server, from_date = Sys.Date() - months(12), to_date = Sys.Date() - months(6), join = FALSE,
                           df_join_name) {
  
  #Error checks
  if(missing(server)) {
    stop("please provide a SQL server where data resides")
  }
  
  if(join == T & missing(df_join_name)) {
    stop("you asked to join result to another data frame, please provide the name of this data frame")
  }
  
  if(from_date > to_date & !missing(from_date) & !missing(to_date)) {
    stop("from_date date must be <= to_date date")
  }
  
  if(missing(from_date) & missing(to_date)) {
    print("Default from_date and to_date dates used - 12 and 6 months prior to today's date, respectively")
  }
  
  if((missing(from_date) & !missing(to_date)) | (!missing(from_date) & missing(to_date))) {
    stop("If from_date date provided, to_date date must also be provided. And vice versa.")
  }
  
  #Build SQL query
  from_date_t <- paste("\'", from_date, "\'", sep = "")
  to_date_t <- paste("\'", to_date, "\'", sep = "")
  sql <-   paste0("select id, max(new_adult) as 'new_adult', max(apple_kids) as 'apple_kids', max(older_adults) as 'older_adults',
                  max(family_med) as 'family_med', max(family_planning) as 'family_planning', max(former_foster) as 'former_foster',
                  max(foster) as 'foster', max(caretaker_adults) as 'caretaker_adults', max(partial_duals) as 'partial_duals',
                  max(disabled) as 'disabled', max(pregnancy) as 'pregnancy'
                  from PHClaims.dbo.mcaid_elig_covgrp
                  where from_date <= ", to_date_t, " and to_date >= ", from_date_t,
                  "\n group by id")
  
  #Execute SQL query
  result <- odbc::dbGetQuery(server, sql)
  
  #Join to existing data frame if desired
  if (join == T) {
    result2 <- left_join(df_join_name, result, by = "id")
  } else {
    return(result)
  }
}