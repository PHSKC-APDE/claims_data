#' @title Broad use helper functions
#' 
#' @description Various functions used to support processing and analysis
#' of Medicaid claims data.
#' 
#' \code{list_var} accepts a list of unquoted variable names and returns a list of quosures
#' for passing to any function.
#' 
#' \code{sqlbatch_f} prepares and sends batched SQL statements to SQL Server with the final statement
#' returning a result set.
#' 
#' @param ... Variables that will be passed as a list to another function
#' @param server SQL server connection created using \code{odbc} package
#' @param sqlbatch Any number of SQL queries in list format
#' @param df Data frame
#' @param lower Lower bound for small number suppression, defaults to 1
#' @param upper Upper bound for small number suppression, defaults to 9
#' @param varlist Vector of variable names to suppress in quotes (e.g. c = ("count", "mean"))
#'
#' @name helper
#'  
#' @export
#' @rdname helper
list_var <- function(...) {
  list <- quos(...)
  return(list)
}

#' @export
#' @rdname helper
sqlbatch_f <- function(server, sqlbatch) {
  
  #Split list of sql statements into those with and without a return reseult set
  query_only <- head(sqlbatch, -1)
  query_return <- as.character(tail(sqlbatch, 1))
  
  #Run statements with no returned result set
  lapply(query_only, function(x) {
    temp <- odbc::dbSendQuery(server, x)
    dbClearResult(temp)
  })
  
  #Run final statement with returned result set
  odbc::dbGetQuery(server, query_return)
} 

#' @export
#' @rdname helper
suppress_f <- function(df, lower = 1, upper = 9, varlist) {
  
  df <- df %>%
    mutate_at(
      vars(varlist),
      funs(case_when(
        between(., lower, upper) ~ NA_real_,
        TRUE ~ .
      )))
  return(df)
}