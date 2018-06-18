#' @title Broad use helper functions
#' 
#' @description Various functions used to support processing and analysis
#' of Medicaid claims data.
#' 
#' \code{list_var} accepts a list of unquoted variable names and returns a list of quosures
#' for passing to any function.
#' 
#' @param ... Variables that will be passed as a list to another function
#'
#' @name helper
#'  
#' @export
#' @rdname helper
list_var <- function(...) {
  list <- quos(...)
  return(list)
}