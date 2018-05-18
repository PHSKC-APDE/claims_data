###############################################################################
# Eli Kern
# 2018-5-14
# APDE
# Function to tabulate R data frame (e.g. summarize ndistinct) over fixed and looped by variables, binding all output as a single data frame
# Version 1.0

#Example of running function:
#tabloop_f(df = mydata, unit = id, loop = loop_var(age, race), fixed = fixed_var(year, region))
#This would produce a single data frame using data in "mydata" tabulating distinct counts of "id" by year and region, separately by age and race
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
  
  #Tabulate data frame by fixed vars, looping over loop vars
  lapply(loop, function(x) {
    
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
}