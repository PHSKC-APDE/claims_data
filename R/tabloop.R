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
#' @param count A variable that identifies the unit of tabulation for non-distinct counts
#' @param dcount A variable that identifies the unit of tabulation for distinct counts
#' @param sum A variable that identifies the unit of tabulation for sums
#' @param mean A variable that identifies the unit of tabulation for calculating means
#' @param median A variable that identifies the unit of tabulation for calculating medians
#' @param loop A list of the loop by variables, requires use of \code{list_var}, required
#' @param fixed A list of the fixed by variables, requires use of \code{list_var}, defaults to null
#' @param filter Specifies whether results should be filtered to positive values only for binary variables, defaults to false
#' @param rename Specifies whether results group categories should be renamed according to APDE defauts, defaults to false
#' @param suppress Specifies whether suppression should be applied
#' @param rounding Specifies how many decimal places to round mean and median to
#' @param ... Additional arguments passed to \code{suppress_f} when \code{suppress = TRUE}
#'
#' @examples
#' \dontrun{
#' tabloop_f(df = mcaid_cohort, count = id, loop = list_var(gender, race), fixed = list_var(region))
#' tabloop_f(df = mcaid_cohort, count = id, loop = list_var(gender, race, zip_code, cov_grp, language))
#' tabloop_f_test(df = depression, 
#'                dcount = list_var(id), 
#'                count = list_var(hra_id),
#'                sum = list_var(ed_cnt, inpatient_cnt, depression_ccw), 
#'                mean = list_var(age), median = list_var(age),
#'                loop = list_var(gender_mx), 
#'                filter = T, 
#'                rename = T, 
#'                suppress = T, 
#'                suppress_var = list_var(id_dcount), round = 3)
#' }
#' 
#' @export
tabloop_f <- function(df, count, dcount, sum, mean, median, loop, fixed = NULL, filter = FALSE, 
                      rename = FALSE, suppress = FALSE, rounding = 1,
                      ...) {
  
  
  #### Error checks ####
  if(missing(loop)) {
    stop("Loop variable(s) must be provided. If tabulation by fixed variables only is desired, simply use count(df, var1, var2, etc.)")
  }
  
  if(missing(df)) {
    stop("Data frame has not been provided or does not exist")
  }
  
  if(missing(dcount) & missing(count) & missing(sum) & missing(mean) & missing(median)) {
    stop("Column to tabulate has not been provided or is not valid column name in data frame")
  }
  
  #### Step 1: Create matrix of fixed and loop by variables to allow padding for zero counts #### 
  
  if(!is.null(fixed)) {
    
    #Create list of vectorized fixed var names, counting down to 1 var
    series <- rev(1:length(fixed))
    fixed_name_cntdwn_vector <- sapply(series, function(x) {
      fixed_i <- fixed[1:x]
      fixed_name_vector <- sapply(fixed_i, function(y) {
        fixed_temp <- y
        fixed_temp <- enquo(fixed_temp)
        fixed_name <- quo_name(fixed_temp)
        return(fixed_name)
      })
      return(fixed_name_vector)
    })
    
    #Create matrix
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
  
  if(!is.null(fixed)) {
    full_matrix <- full_join(fix_matrix, loop_matrix, by = "link") %>%
      select(., -link)
  } else {
    full_matrix <- select(loop_matrix, -link)
  }
  
  
  #### Step 2: Calculate distinct counts by fixed and loop by variables, if requested #### 
  
  if(!missing(dcount)) {
    
    ##Create list of variables to be counted for later use
    dcount_name_list <- sapply(dcount, function(y) {
      dcount <- y
      dcount <- enquo(dcount)
      dcount_name <- paste0(quo_name(dcount), "_dcount")
      return(as.list(dcount_name))
    })
    
    ##Create vectors of variables to be counted for later use
    dcount_name_vector <- sapply(dcount, function(y) {
      dcount <- y
      dcount <- enquo(dcount)
      dcount_name <- paste0(quo_name(dcount), "_dcount")
      return(dcount_name)
    })
    
    #Initialize counter variable
    dcount_counter <- 0
    
    ##Loop over variables to be counted, counting by fixed and loop by variables
    result_dcount <- lapply(dcount, function(y) {
      dcount <- y
      dcount <- enquo(dcount)
      dcount_name <- paste0(quo_name(dcount), "_dcount")
      
      dcount_counter <<- dcount_counter + 1
      
      #Tabulate data frame by fixed vars, looping over loop vars
      result_dcount <- lapply(loop, function(x) {
        
        #Process names of loop variables
        loop_var <- x 
        group_name <- quo_name(loop_var)
        group_name <- enquo(group_name)
        
        #Tabulate data frame by fixed and each loop variable
        df <- df %>%
          group_by(!!! fixed, (!! loop_var)) %>%
          summarise(!! dcount_name := n_distinct(!! dcount, na.rm = TRUE)) %>%
          
          #Create variable to hold name of loop variable
          mutate(
            group_cat = quo_name(loop_var)
          ) %>%
          ungroup() %>%
          
          #Create variable to hold name of loop variable values
          rename(., group = !! group_name) %>%
          mutate(
            group = as.character(group)
          )
        
        #Select final columns based on number of variables to be tabulated
        if (dcount_counter == 1) {
          df <- select(df, !!! fixed, group_cat, group, !! dcount_name)
        } else {
          df <- select(df, !! dcount_name)
        }
        
        #Return one data frame for each loop variable provided
        return(df)
      }) %>%
        #Bind results of lapply function and return result
        bind_rows()
      return(result_dcount)
    }) %>%
      #Bind results across variables to be summed
      bind_cols()
    #Keep only one set of by variable columns
    result_dcount <- select(result_dcount, !!! fixed, group_cat, group, !!! dcount_name_list)
  }  
  
  
  #### Step 3: Calculate non-distinct counts by fixed and loop by variables, if requested #### 
  
  if(!missing(count)) {
    
    #Create list of variables to be counted for later use
    count_name_list <- sapply(count, function(y) {
      count <- y
      count <- enquo(count)
      count_name <- paste0(quo_name(count), "_count")
      return(as.list(count_name))
    })
    
    #Create vectors of variables to be counted for later use
    count_name_vector <- sapply(count, function(y) {
      count <- y
      count <- enquo(count)
      count_name <- paste0(quo_name(count), "_count")
      return(count_name)
    })
    
    #Initialize counter variable
    count_counter <- 0
    
    #Loop over variables to be counted, counting by fixed and loop by variables
    result_count <- lapply(count, function(y) {
      count <- y
      count <- enquo(count)
      count_name <- paste0(quo_name(count), "_count")
      
      count_counter <<- count_counter + 1
      
      #Tabulate data frame by fixed vars, looping over loop vars
      result_count <- lapply(loop, function(x) {
        
        #Process names of loop variables
        loop_var <- x 
        group_name <- quo_name(loop_var)
        group_name <- enquo(group_name)
        
        #Tabulate data frame by fixed and each loop variable
        df <- df %>%
          group_by(!!! fixed, (!! loop_var)) %>%
          summarise(!! count_name := length(which(!is.na(!! count)))) %>% # code needed to appropriately ignore NAs
          
          #Create variable to hold name of loop variable
          mutate(
            group_cat = quo_name(loop_var)
          ) %>%
          ungroup() %>%
          
          #Create variable to hold name of loop variable values
          rename(., group = !! group_name) %>%
          mutate(
            group = as.character(group)
          )
        
        #Select final columns based on number of variables to be tabulated
        if (count_counter == 1) {
          df <- select(df, !!! fixed, group_cat, group, !! count_name)
        } else {
          df <- select(df, !! count_name)
        }
        
        #Return one data frame for each loop variable provided
        return(df)
      }) %>%
        #Bind results of lapply function and return result
        bind_rows()
      return(result_count)
    }) %>%
      #Bind results across variables to be summed
      bind_cols()
    #Keep only one set of by variable columns
    result_count <- select(result_count, !!! fixed, group_cat, group, !!! count_name_list)
  }
  
  
  #### Step 4: Calculate sums by fixed and loop by variables, if requested #### 
  
  if(!missing(sum)) {
    
    
    #Create list of variables to be summed for later use
    sum_name_list <- sapply(sum, function(y) {
      sum <- y
      sum <- enquo(sum)
      sum_name <- paste0(quo_name(sum), "_sum")
      return(as.list(sum_name))
    })
    
    #Create vectors of variables to be summed for later use
    sum_name_vector <- sapply(sum, function(y) {
      sum <- y
      sum <- enquo(sum)
      sum_name <- paste0(quo_name(sum), "_sum")
      return(sum_name)
    })
    
    #Initialize counter variable
    sum_counter <- 0
    
    #Loop over variables to be summed, summing by fixed and loop by variables
    result_sum <- lapply(sum, function(y) {
      sum <- y
      sum <- enquo(sum)
      sum_name <- paste0(quo_name(sum), "_sum")
      
      sum_counter <<- sum_counter + 1
      
      #Tabulate data frame by fixed vars, looping over loop vars
      result_sum <- lapply(loop, function(x) {
        
        #Process names of loop variables
        loop_var <- x 
        group_name <- quo_name(loop_var)
        group_name <- enquo(group_name)
        
        #Tabulate data frame by fixed and each loop variable
        df <- df %>%
          group_by(!!! fixed, (!! loop_var)) %>%
          summarise(!! sum_name := sum(!! sum, na.rm = TRUE)) %>%
          
          #Create variable to hold name of loop variable
          mutate(
            group_cat = quo_name(loop_var)
          ) %>%
          ungroup() %>%
          
          #Create variable to hold name of loop variable values
          rename(., group = !! group_name) %>%
          mutate(
            group = as.character(group)
          )
        
        #Select final columns based on number of variables to be tabulated
        if (sum_counter == 1) {
          df <- select(df, !!! fixed, group_cat, group, !! sum_name)
        } else {
          df <- select(df, !! sum_name)
        }
        
        #Return data frame for each loop variable provided
        return(df)
        
      }) %>%
        #Bind results across by variables
        bind_rows()
      return(result_sum)
    }) %>%
      #Bind results across variables to be summed
      bind_cols()
    #Keep only one set of by variable columns
    result_sum <- select(result_sum, !!! fixed, group_cat, group, !!! sum_name_list)
  }  
  
  
  #### Step 5: Calculate means by fixed and loop by variables, if requested #### 
  
  if(!missing(mean)) {
    
    
    #Create list of variables to be averaged for later use
    mean_name_list <- sapply(mean, function(y) {
      mean <- y
      mean <- enquo(mean)
      mean_name <- paste0(quo_name(mean), "_mean")
      return(as.list(mean_name))
    })
    
    #Create vectors of variables to be averaged for later use
    mean_name_vector <- sapply(mean, function(y) {
      mean <- y
      mean <- enquo(mean)
      mean_name <- paste0(quo_name(mean), "_mean")
      return(mean_name)
    })
    
    #Initialize counter variable
    mean_counter <- 0
    
    #Loop over variables to be averaged, calculating by fixed and loop by variables
    result_mean <- lapply(mean, function(y) {
      mean <- y
      mean <- enquo(mean)
      mean_name <- paste0(quo_name(mean), "_mean")
      
      mean_counter <<- mean_counter + 1
      
      #Tabulate data frame by fixed vars, looping over loop vars
      result_mean <- lapply(loop, function(x) {
        
        #Process names of loop variables
        loop_var <- x 
        group_name <- quo_name(loop_var)
        group_name <- enquo(group_name)
        
        #Tabulate data frame by fixed and each loop variable
        df <- df %>%
          group_by(!!! fixed, (!! loop_var)) %>%
          summarise(!! mean_name := round(mean(!! mean, na.rm = TRUE), rounding)) %>%
          
          #Create variable to hold name of loop variable
          mutate(
            group_cat = quo_name(loop_var)
          ) %>%
          ungroup() %>%
          
          #Create variable to hold name of loop variable values
          rename(., group = !! group_name) %>%
          mutate(
            group = as.character(group)
          )
        
        #Select final columns based on number of variables to be tabulated
        if (mean_counter == 1) {
          df <- select(df, !!! fixed, group_cat, group, !! mean_name)
        } else {
          df <- select(df, !! mean_name)
        }
        
        #Return one data frame for each loop variable provided
        return(df)
      }) %>%
        #Bind results across by variables
        bind_rows()
      return(result_mean)
    }) %>%
      #Bind results across variables to be averaged
      bind_cols()
    #Keep only one set of by variable columns
    result_mean <- select(result_mean, !!! fixed, group_cat, group, !!! mean_name_list)
  }  
  
  
  #### Step 6: Calculate medians by fixed and loop by variables, if requested #### 
  
  if(!missing(median)) {
    
    
    #Create list of variables to be averaged for later use
    median_name_list <- sapply(median, function(y) {
      median <- y
      median <- enquo(median)
      median_name <- paste0(quo_name(median), "_median")
      return(as.list(median_name))
    })
    
    #Create vectors of variables to be averaged for later use
    median_name_vector <- sapply(median, function(y) {
      median <- y
      median <- enquo(median)
      median_name <- paste0(quo_name(median), "_median")
      return(median_name)
    })
    
    #Initialize counter variable
    median_counter <- 0
    
    #Loop over variables to be averaged, calculating by fixed and loop by variables
    result_median <- lapply(median, function(y) {
      median <- y
      median <- enquo(median)
      median_name <- paste0(quo_name(median), "_median")
      
      median_counter <<- median_counter + 1
      
      #Tabulate data frame by fixed vars, looping over loop vars
      result_median <- lapply(loop, function(x) {
        
        #Process names of loop variables
        loop_var <- x 
        group_name <- quo_name(loop_var)
        group_name <- enquo(group_name)
        
        #Tabulate data frame by fixed and each loop variable
        df <- df %>%
          group_by(!!! fixed, (!! loop_var)) %>%
          summarise(!! median_name := round(median(!! median, na.rm = TRUE), rounding)) %>%
          
          #Create variable to hold name of loop variable
          mutate(
            group_cat = quo_name(loop_var)
          ) %>%
          ungroup() %>%
          
          #Create variable to hold name of loop variable values
          rename(., group = !! group_name) %>%
          mutate(
            group = as.character(group)
          )
        
        #Select final columns based on number of variables to be tabulated
        if (median_counter == 1) {
          df <- select(df, !!! fixed, group_cat, group, !! median_name)
        } else {
          df <- select(df, !! median_name)
        }
        
        #Return one data frame for each loop variable provided
        return(df)
      }) %>%
        #Bind results across by variables
        bind_rows()
      return(result_median)
    }) %>%
      #Bind results across variables to be averaged
      bind_cols()
    #Keep only one set of by variable columns
    result_median <- select(result_median, !!! fixed, group_cat, group, !!! median_name_list)
  } 
  
  
  #### Step 7: Join individual result sets if more than one #### 
  
  #Process names of fixed by variables
  fixed_name <- str_replace_all(as.character(fixed), "~", "")
  
  ifelse(!is.null(fixed),
         merge_list <- c(fixed_name, "group_cat", "group"),
         merge_list <- c("group_cat", "group")
  )
  
  #Create list of result sets that have been created
  df_list <- list("result_dcount", "result_count", "result_sum", "result_mean", "result_median")
  df_list <- Filter(function(x) exists(x), df_list)
  df_list <- lapply(df_list, function(x) {
    y <- eval(parse(text = x))
  })
  
  #Join all result sets
  result <- df_list %>% 
    Reduce(function(dtf1, dtf2) inner_join(dtf1, dtf2, by = merge_list), .)
  
  
  #### Step 8: Join by variable matrix with tabulate results to add zero counts #### 
  
  #Create list of variables for which NA will be replaced with zero
  varlist <- list("dcount_name_vector", "count_name_vector", "sum_name_vector", "mean_name_vector", "median_name_vector")
  varlist <- Filter(function(x) exists(x), varlist)
  varlist <- lapply(varlist, function(x) {
    y <- eval(parse(text = x))
  })
  
  #Join with variable matrix to add zero counts
  result <- left_join(full_matrix, result, by = merge_list) %>%
    mutate_at(
      vars(!!! varlist),
      funs(case_when(
        is.na(.) ~ as.numeric(0),
        TRUE ~ as.numeric(.)
      )))
  
  
  #### Step 9: Filter results and/or rename columns if requested #### 
  
  #Filter to keep only relevant rows
  if(filter == T) {
    result <- filter(result, group_cat %in% c("cov_cohort", "cov_cohort3", "age_grp7", "gender_mx", "race_eth_mx", "race_mx", "tractce10", "zip_new", 
                                              "hra_id", "hra", "region_id", "region", "maxlang", "overall", "year") | group == 1)
  }
  
  #Rename group names with meaningful values
  if(rename == T) {
    
    result <- result %>%
      mutate(
        group = case_when(
          group_cat %in% c("male", "female", "gender_unk","aian", "asian", "black", "nhpi", "white", "latino", "race_unk",
                           "english", "spanish", "vietnamese", "chinese", "somali", "russian", "arabic", "korean",
                           "ukrainian", "amharic", "lang_unk","new_adult", "apple_kids", "older_adults", "family_med", 
                           "family_planning", "former_foster", "foster", "caretaker_adults", "partial_duals", "disabled", 
                           "pregnancy", "dual_flag") ~ tools::toTitleCase(group_cat),
          group_cat %in% c("overall", "Overall", "OVERALL") ~ "_Overall",
          TRUE ~ group
        ),
        
        group_cat = case_when(
          group_cat == "age_grp7" ~ "Age",
          group_cat %in% c("male", "female", "gender_unk") ~ "Gender, inclusive",
          group_cat == "gender_mx" ~ "Gender, exclusive",
          group_cat %in% c("aian", "asian", "black", "nhpi", "white", "latino", "race_unk") ~ "Race/ethnicity, inclusive",
          group_cat == "race_eth_mx" ~ "Race/ethnicity, Latino included, exclusive",
          group_cat == "race_mx" ~ "Race/ethnicity, Latino excluded, exclusive",
          group_cat %in% c("english", "spanish", "vietnamese", "chinese", "somali", "russian", "arabic", "korean",
                           "ukrainian", "amharic", "lang_unk") ~ "Language",
          group_cat %in% c("new_adult", "apple_kids", "older_adults", "family_med", "family_planning", "former_foster",
                           "foster", "caretaker_adults", "partial_duals", "disabled", "pregnancy") ~ "Coverage group",
          group_cat == "dual_flag" ~ "Coverage group",
          group_cat %in% c("cov_cohort", "cov_cohort3") ~ "Coverage cohort",
          group_cat == "zip_new" ~ "ZIP code",
          group_cat == "hra" ~ "HRA",
          group_cat == "tractce10" ~ "Census tract",
          group_cat == "region" ~ "Region",
          group_cat == "maxlang" ~ "Preferred language",
          group_cat %in% c("overall", "Overall", "OVERALL") ~ "_Overall",
          group_cat == "year" ~ "Year",
          TRUE ~ group_cat
        )
      )
  }
  
  #### Step 10: Apply suppression if requested #### 
  
  if(suppress == T) {
    result <- suppress_f(df = result, ...)
  }
  
  #Return final result data frame
  result <- arrange(result, !!! fixed, group_cat, group)
  return(result)
}