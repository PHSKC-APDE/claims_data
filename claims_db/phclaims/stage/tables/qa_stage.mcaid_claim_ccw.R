## QA of stage.mcaid_claim_ccw table
## 2019-08-12
## Alastair Matheson, adapted from Eli Kern's SQL script


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code


db_claims <- dbConnect(odbc(), "PHClaims51")

# Bring in YAML file used to make CCW table
table_config <- yaml::yaml.load(
  RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_ccw.yaml" ))

##Pull out run date of stage.mcaid_elig_demo
last_run_claim_ccw <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_claim_ccw")[[1]])


#### SET UP EMPTY DATA FRAME TO TRACK RESULTS ####
ccw_qa <- data.frame(etl_batch_id = integer(),
                     last_run = as.Date(character()),
                     table_name = character(),
                     qa_item = character(),
                     qa_result = character(),
                     qa_date = as.Date(character()),
                     note = character())



########################
## STEP 1: Table-wide checks
########################

#### COUNT # CONDITIONS RUN ####
distinct_cond <- as.integer(dbGetQuery(
  db_claims,
  "select count(distinct ccw_code) as cond_count
  from PHClaims.stage.mcaid_claim_ccw"))

# See how many were in the YAML file
conditions <- names(table_config[str_detect(names(table_config), "cond_")])

if (distinct_cond == length(conditions)) {
  ccw_qa <- rbind(ccw_qa,
                  data.frame(etl_batch_id = NA_integer_,
                             last_run = last_run_claim_ccw,
                             table_name = "stage.mcaid_claim_ccw",
                             qa_item = "# distinct conditions",
                             qa_result = "PASS",
                             qa_date = Sys.time(),
                             note = glue("There were {length(conditions)} conditions analyzed as expected")))
} else {
  ccw_qa <- rbind(ccw_qa,
                  data.frame(etl_batch_id = NA_integer_,
                             last_run = last_run_claim_ccw,
                             table_name = "stage.mcaid_claim_ccw",
                             qa_item = "# distinct conditions",
                             qa_result = "FAIL",
                             qa_date = Sys.time(),
                             note = glue("There were {length(conditions)} conditions analyzed instead of ",
                                         "the {distinct_cond} expected")))
  }


#### COUNT NUMBER + PERCENT OF DISTINCT PEOPLE BY CONDITION ####
distinct_id_ccw <- dbGetQuery(
  db_claims,
  "select ccw_code, ccw_desc, count(distinct id_mcaid) as id_dcount
  from PHClaims.stage.mcaid_claim_ccw
  where year(from_date) <= 2017 and year(to_date) >= 2017 
  group by ccw_code, ccw_desc
  order by ccw_code")

distinct_id_pop <- as.integer(dbGetQuery(
  db_claims,
  "select count(distinct id_mcaid) as id_dcount
  from PHClaims.final.mcaid_elig_timevar
  where year(from_date) <= 2017 and year(to_date) >= 2017 "))


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
distinct_id_chk %>% filter(!is.na(abs_diff))

prop_chk <- askYesNo(msg = glue("Are the deviations from the APCD estimates ", 
                                "within acceptable parameters? Ideally a small ",
                                "percentage difference (<10%) but for small estimates, ",
                                "a small absolute difference is ok (<0.5)."))

if (is.na(prop_chk)) {
  stop("QA process aborted at proportion checking step")
} else if (prop_chk == T) {
  ccw_qa <- rbind(ccw_qa,
                  data.frame(etl_batch_id = NA_integer_,
                             last_run = last_run_claim_ccw,
                             table_name = "stage.mcaid_claim_ccw",
                             qa_item = "Overall proportion with each condition (compared to APCD)",
                             qa_result = "PASS",
                             qa_date = Sys.time(),
                             note = glue("Most conditions are close to APCD-derived estimates")))
} else if (prop_chk == F) {
  ccw_qa <- rbind(ccw_qa,
                  data.frame(etl_batch_id = NA_integer_,
                             last_run = last_run_claim_ccw,
                             table_name = "stage.mcaid_claim_ccw",
                             qa_item = "Overall proportion with each condition (compared to APCD)",
                             qa_result = "FAIL",
                             qa_date = Sys.time(),
                             note = glue("One or more conditions deviate from expected proportions")))
}


#### CHECK AGE DISTRIBUTION BY CONDITION FOR A GIVEN YEAR ####
age_dist_cond_f <- function(year = 2017) {
  
  if (leap_year(year)) {
    pt <- 366
  } else {
    pt <- 365
  }
  
  sql_call <- glue_sql(
    "select c.ccw_code, c.ccw_desc, c.age_grp7, count(distinct id_mcaid) as id_dcount
    from (
      select a.id_mcaid, a.ccw_code, a.ccw_desc, 
      case
        when b.age >= 0 and b.age < 5 then '00-04'
        when b.age >= 5 and b.age < 12 then '05-11'
        when b.age >= 12 and b.age < 18 then '12-17'
        when b.age >= 18 and b.age < 25 then '18-24'
        when b.age >= 25 and b.age < 45 then '25-44'
        when b.age >= 45 and b.age < 65 then '45-64'
        when b.age >= 65 then '65 and over'
      end as age_grp7
      from (
        select distinct id_mcaid, ccw_code, ccw_desc
        from PHClaims.stage.mcaid_claim_ccw
        where year(from_date) <= {year} and year(to_date) >= {year}
      ) as a
      left join (
        select id_mcaid,
        case
          when datediff(day, dob, '{year}-12-31') >= 0 then floor((datediff(day, dob, '{year}-12-31') + 1) / {pt})
          when datediff(day, dob, '{year}-12-31') < 0 then NULL
        end as age
        from PHClaims.final.mcaid_elig_demo
      ) as b
      on a.id_mcaid = b.id_mcaid
    ) as c
    where c.age_grp7 is not null
    group by c.ccw_code, c.ccw_desc, c.age_grp7
    order by c.ccw_code, c.age_grp7",
    .con = db_claims
  )
  
  output <- dbGetQuery(db_claims, sql_call)
  return(output)
}

age_dist_pop_f <- function(year = 2017) {
  
  if (leap_year(year)) {
    pt <- 366
  } else {
    pt <- 365
  }
  
  sql_call <- glue_sql(
    "select age_grp7, count(distinct id_mcaid) as pop
    from (
      select a.id_mcaid, 
      case
        when b.age >= 0 and b.age < 5 then '00-04'
        when b.age >= 5 and b.age < 12 then '05-11'
        when b.age >= 12 and b.age < 18 then '12-17'
        when b.age >= 18 and b.age < 25 then '18-24'
        when b.age >= 25 and b.age < 45 then '25-44'
        when b.age >= 45 and b.age < 65 then '45-64'
        when b.age >= 65 then '65 and over'
      end as age_grp7
      from (
	      select id_mcaid
	      from PHClaims.final.mcaid_elig_timevar
	      where year(from_date) <= {year} and year(to_date) >= {year}
	      ) as a
	    left join (
	      select id_mcaid,
          case
            when datediff(day, dob, '{year}-12-31') >= 0 then floor((datediff(day, dob, '{year}-12-31') + 1) / {pt})
            when datediff(day, dob, '{year}-12-31') < 0 then NULL
          end as age
        from PHClaims.final.mcaid_elig_demo
      ) as b
      on a.id_mcaid = b.id_mcaid
    ) as c
    where c.age_grp7 is not null
    group by c.age_grp7
    order by c.age_grp7",
    .con = db_claims
  )
  
  output <- dbGetQuery(db_claims, sql_call)
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
                             last_run = last_run_claim_ccw,
                             table_name = "stage.mcaid_claim_ccw",
                             qa_item = "Patterns by age group",
                             qa_result = "PASS",
                             qa_date = Sys.time(),
                             note = glue("Most conditions increased with age as expected")))
} else if (age_dist_chk == F) {
  ccw_qa <- rbind(ccw_qa,
                  data.frame(etl_batch_id = NA_integer_,
                             last_run = last_run_claim_ccw,
                             table_name = "stage.mcaid_claim_ccw",
                             qa_item = "Patterns by age group",
                             qa_result = "FAIL",
                             qa_date = Sys.time(),
                             note = glue("One or more conditions had unusual age patterns")))
}



########################
## STEP 2: Validate status of one person per condition with two or more time periods
########################

# Bring in csv file with specific individuals
ids_csv <- read.csv(file = "//dchs-shares01/dchsdata/DCHSPHClaimsData/Data/QA_specific/stage.mcaid_claim_ccw_qa_ind.csv",
                      stringsAsFactors = F)


# Restrict to relevant columns
ids_csv <- ids_csv %>% select(id_mcaid, ccw_desc, from_date, to_date)

# Pull relevant people from ccw table
# Note, need to use glue instead of glue_sql to get quotes to work in collapse
ids_ccw <- dbGetQuery(
  db_claims,
  glue("SELECT id_mcaid, ccw_desc, from_date, to_date 
        FROM stage.mcaid_claim_ccw 
        WHERE {glue_collapse(glue_data_sql(
             ids_csv, '(id_mcaid = {id_mcaid} and ccw_desc = {ccw_desc})', 
             .con = db_claims), sep = ' OR ')}"
  ))


if (all_equal(ids_csv, ids_ccw)) {
  ccw_qa <- rbind(ccw_qa,
                  data.frame(etl_batch_id = NA_integer_,
                             last_run = last_run_claim_ccw,
                             table_name = "stage.mcaid_claim_ccw",
                             qa_item = "Specific individuals",
                             qa_result = "PASS",
                             qa_date = Sys.time(),
                             note = glue("From/to dates matched what was expected")))
} else {
  ccw_qa <- rbind(ccw_qa,
                  data.frame(etl_batch_id = NA_integer_,
                             last_run = last_run_claim_ccw,
                             table_name = "stage.mcaid_claim_ccw",
                             qa_item = "Specific individuals",
                             qa_result = "FAIL",
                             qa_date = Sys.time(),
                             note = glue("From/to dates DID NOT match what was expected")))
}


########################
## STEP 3: Load QA results to SQL and return result
########################

load_sql <- glue::glue_sql(
  "INSERT INTO metadata.qa_mcaid 
  (etl_batch_id, last_run, table_name, qa_item, qa_result, qa_date, note) 
  VALUES 
  {glue_data_sql(ccw_qa, '({etl_batch_id}, {last_run}, {table_name}, {qa_item}, {qa_result}, {qa_date}, {note}) ', .con = db_claims)*} ",
  .con = db_claims)

odbc::dbGetQuery(conn = db_claims, load_sql)


if (max(str_detect(ccw_qa$qa_result, "FAIL")) == 0) {
  ccw_qa_result <- "PASS"
} else {
  ccw_qa_result <- "FAIL"
}

# Remove objects
rm(table_config, ccw_qa, distinct_cond, conditions, 
   distinct_id_ccw, distinct_id_pop, apcd_prop, distinct_id_chk, prop_chk, 
   age_dist_cond_f, age_dist_pop_f, age_dist_cond_chk, age_dist_pop_chk, age_dist_chk, 
   ids_csv, ids_ccw, load_sql)

message("QA of stage.mcaid_claim_ccw complete")
