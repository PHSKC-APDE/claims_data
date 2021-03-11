# This code QAs the stage mcaid CCW table
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# 2019-08-12
# Alastair Matheson, adapted from Eli Kern's SQL script


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded


qa_stage_mcaid_claim_ccw_f <- function(conn = NULL,
                                          server = c("hhsaw", "phclaims"),
                                          config = NULL,
                                          get_config = F) {
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  final_schema <- config[[server]][["final_schema"]]
  final_table <- ifelse(is.null(config[[server]][["final_table"]]), '',
                        config[[server]][["final_table"]])
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                     config[[server]][["qa_table"]])
  
  
  message("Running QA on ", to_schema, ".", to_table)
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  last_run <- as.POSIXct(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}",
                         .con = conn))[[1]])
  
  #### SET UP EMPTY DATA FRAME TO TRACK RESULTS ####
  ccw_qa <- data.frame(etl_batch_id = integer(),
                       last_run = as.Date(character()),
                       table_name = character(),
                       qa_item = character(),
                       qa_result = character(),
                       qa_date = as.Date(character()),
                       note = character())
  
  
  
  #### STEP 1: TABLE-WIDE CHECKS ####
  
  #### COUNT # CONDITIONS RUN ####
  distinct_cond <- as.integer(dbGetQuery(
    conn,
    glue::glue_sql("SELECT count(distinct ccw_code) as cond_count FROM {`to_schema`}.{`to_table`}",
                   .con = conn)))
  
  # See how many were in the YAML file
  conditions <- names(config[str_detect(names(config), "cond_")])
  
  if (distinct_cond == length(conditions)) {
    ccw_qa <- rbind(ccw_qa,
                    data.frame(etl_batch_id = NA_integer_,
                               last_run = last_run,
                               table_name = paste0(to_schema, ".", to_table),
                               qa_item = "# distinct conditions",
                               qa_result = "PASS",
                               qa_date = Sys.time(),
                               note = glue("There were {length(conditions)} conditions analyzed as expected")))
  } else {
    ccw_qa <- rbind(ccw_qa,
                    data.frame(etl_batch_id = NA_integer_,
                               last_run = last_run,
                               table_name = paste0(to_schema, ".", to_table),
                               qa_item = "# distinct conditions",
                               qa_result = "FAIL",
                               qa_date = Sys.time(),
                               note = glue("There were {length(conditions)} conditions analyzed instead of ",
                                           "the {distinct_cond} expected")))
  }
  
  
  #### COUNT NUMBER + PERCENT OF DISTINCT PEOPLE BY CONDITION ####
  distinct_id_ccw <- dbGetQuery(
    conn,
    glue::glue_sql("SELECT ccw_code, ccw_desc, count(distinct id_mcaid) as id_dcount
                 FROM {`to_schema`}.{`to_table`}
                 WHERE year(from_date) <= 2017 and year(to_date) >= 2017 
                 GROUP BY ccw_code, ccw_desc
                 ORDER BY ccw_code",
                   .con = conn))
  
  distinct_id_pop <- as.integer(dbGetQuery(
    conn,
    glue::glue_sql("SELECT count(distinct id_mcaid) as id_dcount
                 FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_timevar
                 WHERE year(from_date) <= 2017 and year(to_date) >= 2017",
                   .con = conn)))
  
  
  distinct_id_chk <- distinct_id_ccw %>%
    mutate(prop = id_dcount / distinct_id_pop * 100)
  
  # Compare to APCD-derived data
  apcd_prop <- data.frame(
    ccw_desc = c("ccw_anemia", "ccw_asthma", "ccw_cancer_breast", "ccw_copd", 
                 "ccw_depression", "ccw_diabetes", "ccw_hypertension", 
                 "ccw_hypothyroid", "ccw_mi"),
    apcd_2017_all = c(8.6, 5.3, 0.5, 2.6, 14.5, 6.5, 12.4, 2.7, 0.3),
    apcd_2017_7mth = c(6.9, 5.3, 0.3, 1.5, 12.5, 3.8, 7.3, 1.6, 0.2))
  
  
  distinct_id_chk <- left_join(distinct_id_chk, apcd_prop, by = "ccw_desc")
  
  distinct_id_chk <- distinct_id_chk %>%
    mutate(abs_diff = prop - apcd_2017_all,
           per_diff = abs_diff / prop * 100)
  
  # Show results for review
  print(distinct_id_chk %>% filter(!is.na(abs_diff)))
  
  prop_chk <- askYesNo(msg = glue("Are the deviations from the APCD estimates ", 
                                  "within acceptable parameters? Ideally a small ",
                                  "percentage difference (<10%) but for small estimates, ",
                                  "a small absolute difference is ok (<0.5)."))
  
  if (is.na(prop_chk)) {
    stop("QA process aborted at proportion checking step")
  } else if (prop_chk == T) {
    ccw_qa <- rbind(ccw_qa,
                    data.frame(etl_batch_id = NA_integer_,
                               last_run = last_run,
                               table_name = paste0(to_schema, ".", to_table),
                               qa_item = "Overall proportion with each condition (compared to APCD)",
                               qa_result = "PASS",
                               qa_date = Sys.time(),
                               note = glue("Most conditions are close to APCD-derived estimates")))
  } else if (prop_chk == F) {
    ccw_qa <- rbind(ccw_qa,
                    data.frame(etl_batch_id = NA_integer_,
                               last_run = last_run,
                               table_name = paste0(to_schema, ".", to_table),
                               qa_item = "Overall proportion with each condition (compared to APCD)",
                               qa_result = "FAIL",
                               qa_date = Sys.time(),
                               note = glue("One or more conditions deviate from expected proportions")))
  }
  
  
  #### CHECK AGE DISTRIBUTION BY CONDITION FOR A GIVEN YEAR ####
  age_dist_cond_f <- function(year = 2017) {
    
    if (lubridate::leap_year(year)) {
      pt <- 366
    } else {
      pt <- 365
    }
    
    sql_call <- glue_sql(
      "SELECT c.ccw_code, c.ccw_desc, c.age_grp7, count(distinct id_mcaid) as id_dcount
    FROM (
      SELECT a.id_mcaid, a.ccw_code, a.ccw_desc, 
      case
        when b.age >= 0 and b.age < 5 then '00-04'
        when b.age >= 5 and b.age < 12 then '05-11'
        when b.age >= 12 and b.age < 18 then '12-17'
        when b.age >= 18 and b.age < 25 then '18-24'
        when b.age >= 25 and b.age < 45 then '25-44'
        when b.age >= 45 and b.age < 65 then '45-64'
        when b.age >= 65 then '65 and over'
      end as age_grp7
      FROM (
        SELECT distinct id_mcaid, ccw_code, ccw_desc
        FROM {`to_schema`}.{`to_table`}
        where year(from_date) <= {year} and year(to_date) >= {year}
      ) as a
      left join (
        SELECT id_mcaid,
        case
          when datediff(day, dob, '{year}-12-31') >= 0 then floor((datediff(day, dob, '{year}-12-31') + 1) / {pt})
          when datediff(day, dob, '{year}-12-31') < 0 then NULL
        end as age
        FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_demo
      ) as b
      on a.id_mcaid = b.id_mcaid
    ) as c
    where c.age_grp7 is not null
    group by c.ccw_code, c.ccw_desc, c.age_grp7
    order by c.ccw_code, c.age_grp7",
      .con = conn
    )
    
    output <- dbGetQuery(conn, sql_call)
    return(output)
  }
  
  age_dist_pop_f <- function(year = 2017) {
    
    if (lubridate::leap_year(year)) {
      pt <- 366
    } else {
      pt <- 365
    }
    
    sql_call <- glue_sql(
      "SELECT age_grp7, count(distinct id_mcaid) as pop
    FROM (
      SELECT a.id_mcaid, 
      case
        when b.age >= 0 and b.age < 5 then '00-04'
        when b.age >= 5 and b.age < 12 then '05-11'
        when b.age >= 12 and b.age < 18 then '12-17'
        when b.age >= 18 and b.age < 25 then '18-24'
        when b.age >= 25 and b.age < 45 then '25-44'
        when b.age >= 45 and b.age < 65 then '45-64'
        when b.age >= 65 then '65 and over'
      end as age_grp7
      FROM (
	      SELECT id_mcaid
	      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_timevar
	      where year(from_date) <= {year} and year(to_date) >= {year}
	      ) as a
	    left join (
	      SELECT id_mcaid,
          case
            when datediff(day, dob, '{year}-12-31') >= 0 then floor((datediff(day, dob, '{year}-12-31') + 1) / {pt})
            when datediff(day, dob, '{year}-12-31') < 0 then NULL
          end as age
        FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_demo
      ) as b
      on a.id_mcaid = b.id_mcaid
    ) as c
    where c.age_grp7 is not null
    group by c.age_grp7
    order by c.age_grp7",
      .con = conn
    )
    
    output <- dbGetQuery(conn, sql_call)
    return(output)
  }
  
  
  age_dist_cond_chk <- age_dist_cond_f(year = 2017)
  age_dist_pop_chk <- age_dist_pop_f(year = 2017)
  
  age_dist_cond_chk <- left_join(age_dist_cond_chk, age_dist_pop_chk,
                                 by = "age_grp7") %>%
    mutate(prev = id_dcount / pop * 100)
  
  # Plot results for visual inspection
  win.graph(width = 16, height = 10)
  ggplot(data = age_dist_cond_chk, 
         aes(x = age_grp7, y = prev, group = ccw_desc)) +
    geom_line() +
    geom_point() + 
    facet_wrap( ~ ccw_desc, ncol = 4, scales = "free")
  
  # Seek user input on whether or not patterns match what is expected
  # NB. It would be nice to quantify this but human inspection will do for now
  
  age_dist_chk <- askYesNo(
    msg = glue("Do the age distributions look to be what is expected ",
               "(generally increasing with age but drop offs after 65 not unusual)?")
  )
  
  if (is.na(age_dist_chk)) {
    stop("QA process aborted at age distribution step")
  } else if (age_dist_chk == T) {
    ccw_qa <- rbind(ccw_qa,
                    data.frame(etl_batch_id = NA_integer_,
                               last_run = last_run,
                               table_name = paste0(to_schema, ".", to_table),
                               qa_item = "Patterns by age group",
                               qa_result = "PASS",
                               qa_date = Sys.time(),
                               note = glue("Most conditions increased with age as expected")))
  } else if (age_dist_chk == F) {
    ccw_qa <- rbind(ccw_qa,
                    data.frame(etl_batch_id = NA_integer_,
                               last_run = last_run,
                               table_name = paste0(to_schema, ".", to_table),
                               qa_item = "Patterns by age group",
                               qa_result = "FAIL",
                               qa_date = Sys.time(),
                               note = glue("One or more conditions had unusual age patterns")))
  }
  
  
  #### STEP 2: VALIDATE STATUS OF ONE PERSON PER CONDITION WITH 2+ TIME PERIODS ####
  ### Only run this when checking manually because end dates in the csv file get 
  # out of date.
  
  # # Bring in csv file with specific individuals
  # ids_csv <- read.csv(file = "//dchs-shares01/dchsdata/DCHSPHClaimsData/Data/QA_specific/stage.mcaid_claim_ccw_qa_ind.csv",
  #                     stringsAsFactors = F)
  # 
  # 
  # # Restrict to relevant columns
  # ids_csv <- ids_csv %>% select(id_mcaid, ccw_desc, from_date, to_date) %>%
  #   arrange(id_mcaid, from_date)
  # 
  # # Pull relevant people from ccw table
  # # Note, need to use glue instead of glue_sql to get quotes to work in collapse
  # ids_ccw <- dbGetQuery(
  #   conn,
  #   glue_sql("SELECT id_mcaid, ccw_desc, from_date, to_date 
  #          FROM {`to_schema`}.{`to_table`}
  #          WHERE {DBI::SQL(
  #             glue_collapse(
  #               glue_data_sql(
  #                 ids_csv, 
  #                 '(id_mcaid = {id_mcaid} and ccw_desc = {ccw_desc})', .con = conn), 
  #               sep = ' OR '))} 
  #          ORDER BY id_mcaid, from_date",
  #            .con = conn))
  # 
  # 
  # if (isTRUE(all_equal(ids_csv, ids_ccw))) {
  #   ccw_qa <- rbind(ccw_qa,
  #                   data.frame(etl_batch_id = NA_integer_,
  #                              last_run = last_run,
  #                              table_name = paste0(to_schema, ".", to_table),
  #                              qa_item = "Specific individuals",
  #                              qa_result = "PASS",
  #                              qa_date = Sys.time(),
  #                              note = glue("From/to dates matched what was expected")))
  # } else {
  #   ccw_qa <- rbind(ccw_qa,
  #                   data.frame(etl_batch_id = NA_integer_,
  #                              last_run = last_run,
  #                              table_name = paste0(to_schema, ".", to_table),
  #                              qa_item = "Specific individuals",
  #                              qa_result = "FAIL",
  #                              qa_date = Sys.time(),
  #                              note = glue("From/to dates DID NOT match what was expected")))
  # }
  
  
  #### STEP 3: LOAD QA RESULTS TO SQL AND RETURN RESULT ####
  DBI::dbExecute(
    conn, 
    glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid 
                   (etl_batch_id, last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES 
                   {DBI::SQL(glue_collapse(
                     glue_data_sql(ccw_qa, 
                                   '({etl_batch_id}, {last_run}, {table_name}, {qa_item}, 
                                   {qa_result}, {qa_date}, {note})', 
                                   .con = conn), 
                     sep = ', ')
                   )};",
                   .con = conn))
  
  
  if (max(str_detect(ccw_qa$qa_result, "FAIL")) == 0) {
    ccw_qa_fail <- 0L
  } else {
    ccw_qa_fail <- 1L
  }
  
  message(glue::glue("QA of stage.mcaid_claim_ccw complete. Result: {min(ccw_qa$qa_result)}"))
  return(ccw_qa_fail)
}
