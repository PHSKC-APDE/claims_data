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
#' @param df Data frame on which to perform small number suppression
#' @param suppress_var Specifies which variables to base suppression on
#' @param lower Lower cell count for suppression (inclusive), defaults to 1
#' @param upper Upper cell count for suppression (inclusive), defaults to 10
#' @col_wise Whether to perform total result set or column-wise suppression, defaults to TRUE
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
suppress_f <- function(df, suppress_var, lower = 1, upper = 10, col_wise = TRUE) {

#Prepare data frame of column to use for total result set suppression
suppress_var_vector <- sapply(suppress_var, function(y) {
  suppress_var <- y
  suppress_var <- enquo(suppress_var)
  suppress_var <- quo_name(suppress_var)
  return(suppress_var)
})
result_suppress <- select(df, !!! suppress_var_vector)  

ifelse(col_wise == TRUE,
       
       #Apply column-wise suppression
       df <- df %>%
         mutate_at(
           vars(!!! suppress_var_vector),
           funs(case_when(
             between(., lower, upper) ~ NA_real_,
             TRUE ~ .))),
       
       #Apply full result set suppression
       df <- df %>%
         mutate_if(
           is.numeric,
           funs(ifelse(rowSums(result_suppress >= lower & result_suppress <= upper) > 0, NA_real_, .)))
)
return(df)
}