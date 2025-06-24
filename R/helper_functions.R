#' @title Create a list of quosures from unquoted variable names
#'
#' @description Accepts a list of unquoted variable names and returns a list of quosures
#' for passing to any function that expects quosures.
#'
#' @details This is a utility function commonly used in conjunction with dplyr and 
#' other tidyverse functions that expect quosures as input. The function uses 
#' `rlang::quos()` internally to capture the expressions.
#'
#' @param ... Variables that will be passed as a list to another function
#'
#' @note This function uses a local variable named 'list' which shadows R's built-in 
#' list() function. This may not cause functional issues, but it is a non-standard 
#' coding practice and may make debugging more annoying.
#'
#' @return A list of quosures created from the input variables
#'
#' @importFrom rlang quos
#'
#' @keywords utilities
#'
#' @examples
#' \dontrun{
#' # Create quosures for use with dplyr functions
#' vars_to_select <- list_var(name, age, income)
#' 
#' # Use with select functions
#' selected_data <- dplyr::select(data, !!!vars_to_select)
#' }
#'
#' @export
list_var <- function(...) {
  list <- quos(...)
  return(list)
}

#' @title Execute batched SQL statements with final result return
#'
#' @description Prepares and sends batched SQL statements to SQL Server with all but the 
#' final statement executed without returning results, and the final statement 
#' returning a result set.
#'
#' @details This function is useful for executing setup queries (like creating temporary 
#' tables or updating records) followed by a final SELECT statement. All queries except 
#' the last are executed without returning data to improve performance. The connection 
#' should be established using the `odbc` package.
#'
#' @param server SQL server connection created using `odbc` package
#' @param sqlbatch A list of SQL queries where all but the last are executed 
#'   without returning results. The final query should return the desired result set.
#'
#' @return A data frame containing the results of the final SQL statement
#'
#' @importFrom odbc dbSendQuery dbClearResult dbGetQuery
#'
#' @keywords database utilities
#'
#' @examples
#' \dontrun{
#' # Establish connection
#' conn <- odbc::dbConnect(odbc::odbc(), "MyServer")
#' 
#' # Execute setup queries followed by a SELECT statement
#' queries <- list(
#'   "CREATE TEMP TABLE #temp AS SELECT * FROM main_table WHERE year = 2025",
#'   "UPDATE #temp SET status = 'processed'",
#'   "SELECT * FROM #temp"
#' )
#' result <- sqlbatch_f(conn, queries)
#' }
#'
#' @export
sqlbatch_f <- function(server, 
                       sqlbatch) {
  
  #Split list of sql statements into those with and without a return reseult set
  query_only <- utils::head(sqlbatch, -1)
  query_return <- as.character(utils::tail(sqlbatch, 1))
  
  #Run statements with no returned result set
  lapply(query_only, function(x) {
    temp <- odbc::dbSendQuery(server, x)
    dbClearResult(temp)
  })
  
  #Run final statement with returned result set
  odbc::dbGetQuery(server, query_return)
}

#' @title Apply suppression to data frame (total result set or column-wise)
#'
#' @description Applies suppression to data frame by replacing values within 
#' specified ranges with NA. Can perform either column-wise suppression 
#' (suppress individual cells) or total result set suppression (suppress entire 
#' rows when any suppression variable meets criteria).
#' 
#' @details Column-wise suppression replaces individual cells that fall within 
#' the suppression range, while total result set suppression removes entire rows 
#' when any of the specified suppression variables meet the criteria.
#'
#' @param df Data frame on which to perform small number suppression
#' @param suppress_var Character vector specifying which variables to base suppression on.
#'   These should be numeric columns containing count data.
#' @param lower Lower bound for suppression range (inclusive). Defaults to 1.
#' @param upper Upper cell count for suppression (inclusive). Values less than or 
#'   equal to this number will be considered for suppression. Defaults to 10.
#' @param col_wise Logical. Whether to perform column-wise suppression (`TRUE`) or 
#'   total result set suppression (`FALSE`). Defaults to `TRUE`.
#'
#' @return A data frame with suppressed values replaced by `NA`
#'
#' @importFrom dplyr select mutate_at mutate_if vars between case_when
#' @importFrom rlang enquo quo_name
#'
#' @keywords data privacy utilities
#'
#' @examples
#' \dontrun{
#' # Create sample data
#' sample_data <- data.frame(
#'   category = c("A", "B", "C"),
#'   member_count = c(5, 15, 8),
#'   claim_count = c(3, 25, 12)
#' )
#' 
#' # Apply column-wise suppression to count variables
#' suppressed_data <- suppress_f(
#'   df = sample_data, 
#'   suppress_var = c("member_count", "claim_count"),
#'   lower = 1, 
#'   upper = 10
#' )
#' 
#' # Apply total result set suppression
#' suppressed_data <- suppress_f(
#'   df = sample_data,
#'   suppress_var = "member_count", 
#'   col_wise = FALSE
#' )
#' }
#'
#' @export
suppress_f <- function(df, 
                       suppress_var, 
                       lower = 1, 
                       upper = 10, 
                       col_wise = TRUE) {

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