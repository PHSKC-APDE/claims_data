#devtools::install_github("PHSKC-APDE/claims_data")
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
Sys.setenv(TZ="America/Los_Angeles") # Set Time Zone
library(tidyverse) # Manipulate data
library(data.table) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(RecordLinkage)
library(claims)
library(keyring)

#Enter credentials for HHSAW
key_set("hhsaw", username = "eli.kern@kingcounty.gov")

#Establish connection to HHSAW
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

#Query Medicaid eligibility data using claims_elig function
mcaid_denom_raw <- claims::claims_elig(conn = db_hhsaw,
                                source = "mcaid",
                                server = "hhsaw",
                                from_date = "2016-01-01",
                                to_date = "2021-12-31",
                                age_min = 18,
                                age_max = 64)

#Restrict to King County (eventually I will use geo_kc_pct for this one it is coded correctly)

#Pull in ZIP reference table for KC
ref.geo_kc_zip <- dbGetQuery(conn = db_hhsaw, 
                             "select geo_zip, geo_kc as geo_kc_new from claims.ref_geo_kc_zip;")

#Join to elig data
mcaid_denom <- left_join(mcaid_denom_raw, ref.geo_kc_zip, by = "geo_zip")

#Create King County residence flags using APDE-approved method
mcaid_denom <- mcaid_denom %>%
  mutate(
    
    #Create new KC flag
    geo_kc_flag = case_when(
      !is.na(geo_county_code) & geo_county_code == "53033" ~ 1,
      is.na(geo_county_code) & geo_kc_new == 1 ~ 1,
      TRUE ~ 0),
    
    #Create variable for days spent in KC
    geo_kc_days = case_when(
      !is.na(geo_county_code) & geo_county_code == "53033" ~ geo_county_code_days,
      is.na(geo_county_code) & geo_kc_new == 1 ~ geo_zip_days,
      TRUE ~ 0L),
    
    #Create variable for percentage of duration spent in KC
    geo_kc_pct = rads::round2((geo_kc_days/duration*100), 1))

#Subset to KC residents
mcaid_denom_kc <-filter(mcaid_denom, geo_kc_flag == 1)

#QA
view(filter(mcaid_denom, geo_kc_flag == 0) %>% select(id_mcaid, geo_zip:geo_kc_pct) %>% slice(1:100))

filter(mcaid_denom, geo_kc_flag == 0 & !is.na(geo_county_code)) %>% count(geo_county_code) #Should not see 53033
filter(mcaid_denom, geo_kc_flag == 0 & is.na(geo_county_code)) %>% count(geo_zip) #Should not find ZIPs in ref_geo_kc_zip

#Create coverage cohorts commonly used in Medicaid Transformation projects
mcaid_denom_kc <- mcaid_denom_kc %>%
  
  mutate(
    
    #7-month coverage cohort
    cov_cohort_7mo = case_when(
      full_benefit_pct >= rads::round2((7/12*100), 1) &
        dual_pct < rads::round2((5/12*100), 1) &
        tpl_pct < rads::round2((5/12*100), 1) &
        geo_kc_pct >= rads::round2((7/12*100), 1)
      ~ 1, TRUE ~ 0),
    
    #11-month coverage cohort
    cov_cohort_11mo = case_when(
      full_benefit_pct >= rads::round2((11/12*100), 1) &
        dual_pct < rads::round2((1/12*100), 1) &
        tpl_pct < rads::round2((1/12*100), 1) &
        geo_kc_pct >= rads::round2((11/12*100), 1)
      ~ 1, TRUE ~ 0))

#QA/check data:
#view(filter(mcaid_denom_kc, geo_kc_pct < rads::round2((7/12*100), 1)) %>%
#       select(id_mcaid, full_benefit_pct, dual_pct, tpl_pct, geo_zip:geo_kc_pct) %>% slice(1:100))

count(mcaid_denom_kc, cov_cohort_7mo)
count(mcaid_denom_kc, cov_cohort_11mo)

view(filter(mcaid_denom_kc, cov_cohort_7mo == 0) %>%
       select(id_mcaid, cov_cohort_7mo, cov_cohort_11mo, full_benefit_pct, dual_pct, tpl_pct, geo_zip:geo_kc_pct) %>%
       slice(1:100))


#Next steps:

#Characterize demographics of 3 cohorts, as well as people in overall cohort excluded from 7-month cohort
  #and people in 7-month cohort excluded from 11-month cohort