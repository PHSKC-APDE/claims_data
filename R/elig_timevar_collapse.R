#### HELPER FUNCTION TO COLLAPSE THE ELIG_TIMEVAR TABLE BASED ON DESIRED DATA ELEMENTS
# Alastair Matheson, PHSKC
#
# 2019-07-31

# Assumes odbc and glue packages are loaded and ODBC connections made

### Areas for improvement
# 1) Add the contiguous column back in
# 2) Have a Shiny interface to just check the boxes of desired columns
# 3) Format this function to fit into a package

# Most of the options in the function just specify if that column should be 
# included in the collapsed data frame
# Exceptions:
# - geocode_vars = will bring in desired geocded data elements 
#    (geo_zip_centroid, geo_street_centroid, geo_countyfp10, geo_tractce10,
#     geo_hra_id, geo_school_geoid10)
# - cov_time_day = recalculate coverage time in the new period
# - last_run = bring in the last run date

elig_timevar_collapse <- function(conn,
                              source = c("mcaid", "mcare", "apcd"),
                              dual = F,
                              tpl = F,
                              rac_code_1 = F,
                              rac_code_2 = F,
                              rac_code_3 = F,
                              rac_code_4 = F,
                              rac_code_5 = F,
                              rac_code_6 = F,
                              rac_code_7 = F,
                              rac_code_8 = F,
                              #cov_type = F, # Not yet in elig_timevar
                              mco_id = F,
                              geo_add1_clean = F,
                              geo_add2_clean = F,
                              geo_city_clean = F,
                              geo_state_clean = F,
                              geo_zip_clean = F,
                              geocode_vars = list("geo_zip_centroid", 
                                                  "geo_street_centroid", 
                                                  "geo_countyfp10", 
                                                  "geo_tractce10",
                                                  "geo_hra_id", 
                                                  "geo_school_geoid10"),
                              cov_time_day = T,
                              last_run = F) {
  
  #### ERROR CHECKS ####
  cols <- sum(dual, tpl, rac_code_1, rac_code_2, rac_code_3, rac_code_4, 
              rac_code_5, rac_code_6, rac_code_7, rac_code_8, # cov_type, 
              mco_id, geo_add1_clean, geo_add2_clean, geo_city_clean,
              geo_state_clean, geo_zip_clean)
  
  # Make sure something is being selected
  if (cols == 0) {
    stop("Choose at least one column to collapse over")
  }
  
  if (cols == 16) { # Change this once cov_type added in
    stop("You have selected every time-varying column. Just use the elig_timevar table")
  }
  
  # Make sure geocode_vars selected are legit
  if (min(geocode_vars %in% list("geo_zip_centroid", "geo_street_centroid", 
                             "geo_countyfp10", "geo_tractce10", "geo_hra_id", 
                             "geo_school_geoid10")) == 0) {
    stop("You have chosen a geocode_var that does not exist. Check spelling.")
  }
  
  
  #### SET UP VARIABLES ####
  source <- match.arg(source)
  tbl <- glue("final.{source}_elig_timevar")
  
  id_name <- glue("id_{source}")
  
  
  
  
  if (source == "mcaid") {
    vars_to_check <- list("dual" = dual, "tpl" = tpl, "rac_code_1" = rac_code_1, 
                          "rac_code_2" = rac_code_2, "rac_code_3" = rac_code_3, 
                          "rac_code_4" = rac_code_4, "rac_code_5" = rac_code_5, 
                          "rac_code_6" = rac_code_6, "rac_code_7" = rac_code_7, 
                          "rac_code_8" = rac_code_8, # "cov_type" = cov_type, 
                          "mco_id" = mco_id, "geo_add1_clean" = geo_add1_clean, 
                          "geo_add2_clean" = geo_add2_clean, 
                          "geo_city_clean" = geo_city_clean,
                          "geo_state_clean" = geo_state_clean,
                          "geo_zip_clean" = geo_zip_clean)
  } else if (source == "mcare") {
      
  } else if (source == "apcd") {
      
    }

  vars <- vector()
  
  lapply(seq_along(vars_to_check), n = names(vars_to_check), function(x, n) {
    if (vars_to_check[x] == T) {
      vars <<- c(vars, n[x])
    }
  })
  
  message("Printing vars")
  print(vars)
  
  
  message("adding in geocode variables")
  if (source == "mcaid" & length(geocode_vars) > 0) {
    vars_geo <- unlist(geocode_vars)
  } else {
    vars_geo <- vector()
  }

  if (last_run == T) {
    vars_date <- "last_run"
  } else {
    vars_date <- vector()
  }
  
  vars_combined <- c(vars, vars_geo, vars_date)
  
  sql_call <- glue_sql(
    "SELECT DISTINCT e.{`id_name`}, e.min_from AS from_date, e.max_to AS to_date,
    {`vars_to_quote_e`*}
      FROM
      (SELECT d.*,
        MIN(from_date) OVER 
        (PARTITION BY {`id_name`}, group_num3 
          ORDER BY {`id_name`}, from_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS [min_from],
        MAX(to_date) OVER 
        (PARTITION BY {`id_name`}, group_num3 
          ORDER BY {`id_name`}, from_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS [max_to]
        FROM
        (SELECT c.*,
          group_num3 = max(group_num2) OVER 
          (PARTITION BY {`id_name`}, {`vars`*} ORDER BY from_date)
          FROM
          (SELECT b.*, 
            CASE 
            WHEN b.group_num > 1  OR b.group_num IS NULL THEN ROW_NUMBER() OVER (PARTITION BY b.{`id_name`} ORDER BY b.from_date) + 1
            WHEN b.group_num = 1 OR b.group_num = 0 THEN NULL
            END AS group_num2
            FROM
            (SELECT a.{`id_name`}, a.from_date, a.to_date, {`vars_to_quote_a`*},
              datediff(day, lag(a.to_date) OVER (
                PARTITION BY a.{`id_name`}, {`vars_to_quote_a`*}
                ORDER by from_date), a.from_date) as group_num 
              FROM 
              (SELECT TOP (100) {`id_name`}, from_date, to_date, {`vars_combined`*} 
              FROM {tbl}) a) b) c) d) e
      ORDER BY {`id_name`}, from_date",
    vars_to_quote_a = lapply(vars_combined, function(nme) DBI::Id(table = "a", column = nme)),
    vars_to_quote_e = lapply(vars_combined, function(nme) DBI::Id(table = "e", column = nme)),
    .con = conn)
  
  result <- dbGetQuery(conn, sql_call)
  
  return(result)
}


#### TESTS #####

elig_timevar_collapse(conn = db_claims, source = "mcaid",
                      dual = T, rac_code_4 = T,
                      geocode_vars = list("geo_hra_id"),
                      last_run = F)


test_sql2 <- elig_timevar_collapse(conn = db_claims, source = "mcaid",
                                  dual = T, rac_code_4 = T,
                                  geocode_vars = list("geo_hra_id"),
                                  last_run = F)
