###############################################################################
# Eli Kern
# 2018-5-14
# APDE
# Function to tabulate R data frame (e.g. summarize ndistinct) over fixed and looped by variables, binding all output as a single data frame
# Version 1.0
###############################################################################

#### Define helper functions #####
fixed_var <- function(...) {
  fixed <- quos(...)
  return(fixed)
}

loop_var <- function(...) {
  loop <- quos(...)
  return(loop)
}

#### Define main function #####
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
  
  #Process function arguments
  unit <- enquo(unit)
  loop_len <- length(loop)
  
  #Tabulate data frame by fixed vars, looping over loop vars
  df_list <- list()
  for (i in 1:loop_len){
    
    #Process names of loop variables
    loop_var <- loop[i] 
    loop_var <- rlang::eval_tidy(enquo(loop_var))
    
    #Iterate names of data frames for list of data frames
    df_name <- paste0("df", i)
    
    #Process name of group variable for later rename step
    group_name <- paste0("(", str_replace(as.character(loop[i]), "~", ""), ")")
    group_name <- enquo(group_name)
    
    #Tabulate data frame by fixed and each loop variable
    df_name <- df %>%
      group_by(!!! fixed, (!!! loop_var)) %>%
      summarise(count = n_distinct(!! unit)) %>%
      #Create variable to hold name of loop variable
      mutate(
        group_cat = str_replace(as.character(loop_var), "~", "")
      ) %>%
      ungroup() %>%
      #Create variable to hold name of loop variable values
      rename(., group = !! group_name) %>%
      mutate(group = as.character(group)) %>%
      #Order columns
      select(., !!! fixed, group_cat, group, count)
    
    #Return and build list of resulting data frames
    df_list[[i]] <- df_name
  }
  
  #Return list of data frames as single bound data frame
  df_complete <- as.data.frame(bind_rows(df_list))
  return(df_complete)
}