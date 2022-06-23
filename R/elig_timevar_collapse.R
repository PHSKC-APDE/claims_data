#' @title Collapse elig_timevar tables
#' 
#' @description \code{elig_timevar_collapse} collapses elig_timevar tables
#' 
#' @details Standard time-varying eligibility (elig_timevar) tables have new lines 
#' (i.e., new from_date and to_date values) for a change in any time-varying element. 
#' This function allows users to generate a new time-varying eligibility
#' table (elig_timevar) that creates a new line only for desired data elements. 
#' All fields that define what the new table is collapsed over are set to FALSE 
#' by default. NB. Function does not yet support Medicare or combined Medicaid/Medicare
#' tables.
#' 
#' @param conn SQL server connection created using \code{odbc} package.
#' @param server Which server do you want to run the query against? NB. Currently only
#' Medicaid data is available on HHSAW.
#' @param source Which claims data source do you want to pull from?
#' @param dual Collapse over the dual eligiblity flag.
#' @param cov_time_day Recalculate coverage time in the new period. Default is TRUE.
#' @param last_run Bring in the last run date.
#' @param ids Restrict to specified IDs. Use format c("<id1>", "<id2>") or pass a vector.
#' @param tpl Collapse over the third party liability flag (Medicaid only).
#' @param bsp_group_name Collapse over the bsp_group_name field (Medicaid only)
#' @param full_benefit Collapse over the full_benefit field (Medicaid only).
#' @param cov_type Collapse over the cov_type field (Medicaid only).
#' @param mco_id Collapse over the mco_id field (Medicaid only)
#' @param med_covgrp Collapse over the med_covgrp field (APCD only).
#' @param pharm_covgrp Collapse over the pharm_covgrp field (APCD only).
#' @param med_medicaid Collapse over the med_medicaid field (APCD only).
#' @param med_medicare Collapse over the med_medicare field (APCD only).
#' @param med_commercial Collapse over the med_commercial field (APCD only).
#' @param pharm_medicaid Collapse over the pharm_medicaid field (APCD only).
#' @param pharm_medicare Collapse over the pharm_medicare field (APCD only).
#' @param pharm_commercial Collapse over the pharm_commercial field (APCD only).
#' @param geo_add1 Collapse over the geo_add1 field (Medicaid only).
#' @param geo_add2 Collapse over the geo_add2 field (Medicaid only).
#' @param geo_city Collapse over the geo_city field (Medicaid only).
#' @param geo_state Collapse over the geo_state field (Medicaid only).
#' @param geo_zip Collapse over the geo_zip field (Medicaid only).
#' @param geocode_vars Bring in all geocded data elements (geo_zip_centroid, 
#' geo_street_centroid, geo_county_code, geo_tractce10, geo_hra_code, 
#' geo_school_code). Default is FALSE.
#' @param geo_zip_code Collapse over the geo_zip_code field (APCD only).
#' @param geo_county Collapse over the geo_countyfield (APCD only).
#' @param geo_ach Collapse over the geo_ach field (APCD only).
#'
#' @examples
#' \dontrun{
#' new_timevar <- elig_timevar_collapse(conn = db_claims51, source = "mcaid",
#' full_benefit = T, geo_add1 = T, geo_city = T, geo_zip = T, geocode_vars = T)
#' new_timevar2 <- elig_timevar_collapse(conn = db_claims51, source = "apcd",
#' ids = c("123", "456", "789"), med_covgrp = T, geo_county = T)
#' }
#' 
#' @export

elig_timevar_collapse <- function(conn,
                                  server = c("phclaims", "hhsaw"),
                                  source = c("mcaid", "apcd"),
                                  #all-source columns
                                  dual = F,
                                  cov_time_day = T,
                                  last_run = F,
                                  ids = NULL, 
                                  
                                  #mcaid columns
                                  tpl = F,
                                  bsp_group_name = F,
                                  full_benefit = F,
                                  cov_type = F,
                                  mco_id = F,
                                  
                                  #apcd columns
                                  med_covgrp = F,
                                  pharm_covgrp = F,
                                  med_medicaid = F,
                                  med_medicare = F,
                                  med_commercial = F,
                                  pharm_medicaid = F,
                                  pharm_medicare = F,
                                  pharm_commercial = F,
                                  
                                  #mcaid geo columns
                                  geo_add1 = F,
                                  geo_add2 = F,
                                  geo_city = F,
                                  geo_state = F,
                                  geo_zip = F,
                                  geocode_vars = F,
                                  
                                  #apcd geo columns
                                  geo_zip_code = F,
                                  geo_county = F,
                                  geo_ach = F) {
  
  #### ERROR CHECKS ####
  cols <- sum(dual, tpl, bsp_group_name, full_benefit, cov_type, 
              mco_id, geo_add1, geo_add2, geo_city,
              geo_state, geo_zip, geocode_vars, 
              med_covgrp, pharm_covgrp, med_medicaid, med_medicare, 
              med_commercial, pharm_medicaid, pharm_medicare, pharm_commercial,
              geo_zip_code, geo_county, geo_ach)
  
  # Make sure something is being selected
  if (cols == 0) {
    stop("Choose at least one column to collapse over")
  }
  
  if (source == "mcaid" & cols == 12) {
    stop("You have selected every Medicaid time-varying column. Just use the mcaid.elig_timevar table")
  }
  
  if (source == "apcd" & cols == 12) { 
    stop("You have selected every APCD time-varying column. Just use the apcd.elig_timevar table")
  }
  
  
  #### SET UP VARIABLES ####
  server <- match.arg(server)
  source <- match.arg(source)
  
  if (server == "hhsaw" & source != "mcaid") {
    stop("Currently only Medicaid data is available on HHSAW")
  }
  
  if (server == "phclaims") {
    schema <- "final"
    tbl <- glue::glue("{source}_elig_timevar")
  } else {
    schema <- "claims"
    tbl <- "final_mcaid_elig_timevar"
  }
  
  
  id_name <- glue::glue("id_{source}")
  
  
  if (source == "mcaid") {
    vars_to_check <- list("dual" = dual, 
                          "tpl" = tpl, 
                          "bsp_group_name" = bsp_group_name, 
                          "full_benefit" = full_benefit, 
                          "cov_type" = cov_type, 
                          "mco_id" = mco_id, 
                          "geo_add1" = geo_add1, 
                          "geo_add2" = geo_add2, 
                          "geo_city" = geo_city,
                          "geo_state" = geo_state,
                          "geo_zip" = geo_zip)
  } else if (source == "mcare") {
    
  } else if (source == "apcd") {
    vars_to_check <- list("dual" = dual, 
                          "med_covgrp" = med_covgrp, 
                          "pharm_covgrp" = pharm_covgrp, 
                          "med_medicaid" = med_medicaid, 
                          "med_medicare" = med_medicare, 
                          "med_commercial" = med_commercial, 
                          "pharm_medicaid" = pharm_medicaid, 
                          "pharm_medicare" = pharm_medicare, 
                          "pharm_commercial" = pharm_commercial, 
                          "geo_zip_code" = geo_zip_code, 
                          "geo_county" = geo_county, 
                          "geo_ach" = geo_ach)
  }
  
  vars <- vector()
  
  lapply(seq_along(vars_to_check), n = names(vars_to_check), function(x, n) {
    if (vars_to_check[x] == T) {
      vars <<- c(vars, n[x])
    }
  })
  
  message(glue::glue('Collapsing over the following vars: {glue::glue_collapse(vars, sep = ", ")}'))
  
  # Add in other variables as desired
  if (source == "mcaid" & geocode_vars == T) {
    vars_geo <- c("geo_zip_centroid", 
                  "geo_street_centroid", 
                  "geo_county_code", 
                  "geo_tract_code",
                  "geo_hra_code", 
                  "geo_school_code")
    
    message(glue::glue('Adding in geocode variables: {glue_collapse(vars_geo, sep = ", ")}'))
  } else {
    vars_geo <- vector()
  }
  
  if (last_run == T) {
    vars_date <- "last_run"
  } else {
    vars_date <- vector()
  }
  
  vars_combined <- c(vars, vars_geo, vars_date)
  
  # Set up cov_time code if needed
  if (cov_time_day == T) {
    cov_time_sql <- glue::glue_sql(", DATEDIFF(dd, e.min_from, e.max_to) + 1 AS cov_time_day ",
                                   .con = conn)
  } else {
    cov_time_sql <- DBI::SQL('')
  }
  
  
  #### RESTRICT TO SPECIFIC IDS IF DESIRED ####
  if (!missing(ids)) {
    ids <- unique(ids)
    num_ids <- length(ids)
    
    # If there are lots of IDs, set them up in a temp table and join
    if (num_ids > 1000) {
      message("Large number of IDs detected, setting up IDs in temp table")
      temp_ids <- T
      
      try(dbExecute(db_hhsaw, "drop table ##temp_ids"), silent = T)
      DBI::dbWriteTable(db_hhsaw,
                        name = "##temp_ids",
                        value = data.frame("id" = ids),
                        overwrite = T, append = F)
      
      # Add index to id and from_date for faster join
      # Think about only using this if n_rounds is >2-3
      DBI::dbExecute(conn, "CREATE NONCLUSTERED INDEX temp_ids_id ON ##temp_ids (id)")
      
      id_sql <- glue::glue_sql(") a 
                                INNER JOIN ##temp_ids x
                                ON a.{`id_name`} = x.id ",
                               .con = conn)
      message("Temp IDs table created")
    } else {
      temp_ids <- F
      id_sql <- glue::glue_sql(" WHERE {`id_name`} IN ({ids*}) ) a", .con = conn)
    }
  } else {
    temp_ids <- F
    id_sql <- DBI::SQL(' ) a')
  }
  
  
  #### SET UP AND RUN SQL CODE ####
  # Set up components of SQL that need a prefix
  if (length(vars_combined) > 1) {
    vars_to_quote_a <- lapply(vars_combined, function(nme) DBI::Id(table = "a", column = nme))
    vars_to_quote_e <- lapply(vars_combined, function(nme) DBI::Id(table = "e", column = nme))
  } else {
    vars_to_quote_a <- glue::glue_sql("a.{`vars_combined`}", .con = conn)
    vars_to_quote_e <- glue::glue_sql("e.{`vars_combined`}", .con = conn)
  }
  
  message("Running collapse code")
  sql_call <- glue::glue_sql(
    "SELECT DISTINCT e.{`id_name`}, e.min_from AS from_date, e.max_to AS to_date,
    {`vars_to_quote_e`*} {cov_time_sql} 
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
              (SELECT {`id_name`}, from_date, to_date, {`vars_combined`*} 
              FROM {`schema`}.{`tbl`} 
              {id_sql}
              ) b) c) d) e
      ORDER BY {`id_name`}, from_date",
    .con = conn)
  
  print(sql_call)
  result <- DBI::dbGetQuery(conn, sql_call)


  #### CLEAN UP ####
  if (temp_ids == T) {
    DBI::dbExecute(conn, "IF object_id('tempdb..##temp_ids') IS NOT NULL DROP TABLE ##temp_ids;")
  }

  return(result)
}
