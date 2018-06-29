#' @title Tabulation loop
#' 
#' @description \code{tabloop_f} tabulates a data frame (e.g. summarize ndistinct) over fixed and 
#' looped by variables, binding all output as a single data frame.
#' 
#' @details This function tabulates a single data frame over fixed and looped by variables, and binds
#' all output as a single data frame. Fixed by variables are variables by which the data frame will be disaggregated
#' for all loop variables, whereas loop variables will only be disaggregated separately. For example, a combination of
#' region for fixed and age group and sex for loop would produce counts by age group and sex for each region, but not
#' counts for each sex by age group. 
#' 
#' The function accepts a row ID variable and summarizes distinct counts of this variable.
#' The function will produce zero counts for all by variable values that exist in the full join of the fixed
#' and loop by variable matrix.
#' 
#' @param df A data frame in tidy format
#' @param unit A variable that identifies the unit of tabulation (e.g.person, row, etc.)
#' @param loop A list of the loop by variables, requires use of \code{list_var}, required
#' @param fixed A list of the fixed by variables, requires use of \code{list_var}, defaults to null
#'
#' @examples
#' \dontrun{
#' tabloop_f(df = mcaid_cohort, unit = id, loop = list_var(gender, race), fixed = list_var(region))
#' tabloop_f(df = mcaid_cohort, unit = id, loop = list_var(gender, race, zip_code, cov_grp, language))
#' }
#' 
#' @export
tabloop_f <- function(df, unit, loop, fixed = NULL) {
  
  #Error checks
  if(missing(loop)) {
    stop("Loop variable(s) must be provided. If tabulation by fixed variables only is desired, simply use count(df, var1, var2, etc.)")
  }
  
  if(missing(df)) {
    stop("Data frame has not been provided or does not exist")
  }
  
  if(missing(unit)) {
    stop("Unit parameter has not been provided or is not valid column name in data frame; unit must be provided for tabulation")
  }
  
  #### Step 1: Create matrix of fixed and loop by variables to allow padding for zero counts #### 
  
  #Create permutation matrix for fixed by variables
  
  if(!is.null(fixed)) {
    fix_matrix <- lapply(fixed, function(x) {
      
      #Process quosures
      fix_matrix_var <-  enquo(x)
      
      #Distinct values of each by variable, create dummy linking variable
      df <- df %>%
        select(., !! fix_matrix_var) %>%
        distinct(., !! fix_matrix_var) %>%
        mutate(link = 1)
      return(df)
    }) %>%
      
      #Full join of resulting matrices for each fixed by variable
      as.list(df) %>% 
      Reduce(function(dtf1, dtf2) full_join(dtf1, dtf2, by = "link"), .)
  }
  
  #Create stacked group_cat and group matrix for loop variables
  loop_matrix <- lapply(loop, function(x) {
    
    loop_matrix_var <-  enquo(x)
    
    #Process names of loop variables
    loop_var <- x 
    group_name <- quo_name(loop_var)
    group_name <- enquo(group_name)
    
    #Process data frame
    df <- df %>%
      select(., !! loop_matrix_var) %>%
      distinct(., !! loop_matrix_var) %>%
      mutate(link = 1,
             group_cat = quo_name(loop_var)
      ) %>%
      rename(., group = !! group_name) %>%
      mutate(
        group = as.character(group)
      )
    return(df)
  }) %>%
    bind_rows() %>%
    select(., group_cat, group, link)
  
  
  #Join fixed and loop var matrices
  ifelse(!is.null(fixed),
         
         full_matrix <- full_join(fix_matrix, loop_matrix, by = "link") %>%
           select(., -link),
         
         full_matrix <- select(loop_matrix, -link)
  )
  
  
  #### Step 2: Create results grouping by fixed and loop by variables #### 
  
  #Process function arguments
  unit <- enquo(unit)
  
  #Tabulate data frame by fixed vars, looping over loop vars
  result <- lapply(loop, function(x) {
    
    #Process names of loop variables
    loop_var <- x 
    group_name <- quo_name(loop_var)
    group_name <- enquo(group_name)
    
    #Tabulate data frame by fixed and each loop variable
    df <- df %>%
      group_by(!!! fixed, (!! loop_var)) %>%
      summarise(count = n_distinct(!! unit)) %>%
      
      #Create variable to hold name of loop variable
      mutate(
        group_cat = quo_name(loop_var)
      ) %>%
      ungroup() %>%
      
      #Create variable to hold name of loop variable values
      rename(., group = !! group_name) %>%
      mutate(
        group = as.character(group)
      ) %>%
      
      #Order columns
      select(., !!! fixed, group_cat, group, count)
    
    #Return one data frame for each loop variable provided
    return(df)
  }) %>%
    #Bind results of lapply function and return result
    bind_rows()
  
  
  #### Step 3: Join by variable matrix with tabulate results to add zero counts #### 
  
  #Process names of fixed by variables
  fixed_name <- str_replace_all(as.character(fixed), "~", "")
  
  ifelse(!is.null(fixed),
         
         merge_list <- c(fixed_name, "group_cat", "group"),
         
         merge_list <- c("group_cat", "group")
  )
  
  #Join
  df <- left_join(full_matrix, result, by = merge_list) %>%
    mutate(
      count = case_when(
        is.na(count) ~ as.integer(0),
        TRUE ~ count
      )
    )
}