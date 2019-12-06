#' @title Claims data member eligibility and demographics
#' 
#' @description \code{claims_elig} returns member eligibility and demographics.
#' 
#' @details LARGELY FOR INTERNAL USE
#' This function builds and sends a SQL query to return a member cohort with 
#' specified parameters, including coverage time period, coverage characteristics 
#' (e.g., Medicare dual eligibility), and member demographics. Can be used on three
#' data sources: APCD, Medicaid, and Medicare (also Medicaid/Medicare combined).
#' Parameters below have the following key to indicator which data source they 
#' can be used against: A = All sources, AP = APCD, MD = Medicaid, ME = Medicare
#' Most parameters default to NULL, which means that all values are included for
#' that field.
#' 
#' @param conn SQL server connection created using \code{odbc} package
#' @param source Which claims data source do you want to pull from?
#' @param from_date Begin date for coverage period, "YYYY-MM-DD", 
#' defaults to 18 months prior to today's date (A)
#' @param to_date End date for coverage period, "YYYY-MM-DD", 
#' defaults to 6 months prior to today's date (A)
#' @param cov_min Minimum coverage required during requested date range (percent scale), defaults to 0 (A)
#' @param covgap_max Maximum gap in continuous coverage allowed during requested date range (days) (A)
#' @param mcaid_min Minimum Medicaid coverage allowed during requested date
#' range (percent scale) (MD/ME)
#' @param mcaid_max Maximum Medicaid coverage allowed during requested date
#' range (percent scale) (MD/ME)
#' @param mcare_min Minimum Medicare coverage allowed during requested date
#' range (percent scale) (MD/ME)
#' @param mcare_max Maximum Medicare coverage allowed during requested date
#' range (percent scale) (MD/ME)
#' @param dual_min Minimum Medicare-Medicaid dual eligibility coverage allowed 
#' during requested date range (percent scale) (A)
#' @param dual_max Maximum Medicare-Medicaid dual eligibility coverage allowed 
#' during requested date range (percent scale) (A)
#' @param med_covgrp Medical coverage group type in APCD data (AP)
#' @param pharm_covgrp Pharmacy coverage group type in APCD data (AP)
#' @param bsp_group_name Most frequently reported BSP group during requested date
#' range, case insensitive, can take multiple values (MD)
#' @param full_benefit_min Minimum time with full benefits during the requested
#' time period (percent scale) (MD)
#' @param cov_type Medicaid coverage type (FFS or MC) (MD)
#' @param mco_id Managed care organization ID (MD)
#' @param part_a_min Minimum time enrolled in Part A of Medicare during the requested 
#' time period (percent scale) (ME)
#' @param part_a_min Maximum time enrolled in Part A of Medicare during the requested 
#' time period (percent scale) (ME)
#' @param part_b_min Minimum time enrolled in Part B of Medicare during the requested 
#' time period (percent scale) (ME)
#' @param part_b_min Maximum time enrolled in Part B of Medicare during the requested 
#' time period (percent scale) (ME)
#' @param part_c_min Minimum time enrolled in Part C of Medicare during the requested 
#' time period (percent scale) (ME)
#' @param part_c_min Maximum time enrolled in Part C of Medicare during the requested 
#' time period (percent scale) (ME)
#' @param buy_in_min Minimum time with state buy in during the requested time
#' period (percent scale) (ME)
#' @param buy_in_max Maximum time with state buy in during the requested time
#' period (percent scale) (ME)
#' @param id Restrict to these specific APCD/Medicaid/Medicare IDs (A)
#' @param age_min Minimum age for cohort (integer), age is calculated as of 
#' last day of requested date range, defaults to 0 (A)
#' @param age_max Maximum age for cohort (integer), age is calculated as of 
#' last day of requested date range, defaults to 200 (A)
#' @param female Alone or in combination female gender over entire member history (A)
#' @param male Alone or in combination female gender over entire member history (A)
#' @param gender_me Most commonly reported gender, by time enrolled, case insensitive, 
#' can take multiple values (e.g., c("female", "multiple")) (A)
#' @param gender_recent Most recently reported gender, , case insensitive, 
#' can take multiple values (e.g., c("female", "multiple")) (AP/MD)
#' @param race_aian Alone or in combination American Indian/Alaska Native race over entire member history (MD/ME)
#' @param race_asian Alone or in combination Asian race over entire member history (MD/ME)
#' @param race_asian_pi Alone or in combination Asian/Pacific Islanfer race over entire member history (ME)
#' @param race_black Alone or in combination Black race over entire member history (MD/ME)
#' @param race_latino Alone or in combination Latino race over entire member history (MD/ME)
#' @param race_nhpi Alone or in combination Native Hawaiian/Pacific Islander race over entire member history (MD/ME)
#' @param race_white Alone or in combination white race over entire member history (MD/ME)
#' @param race_unk No recorded race over entire member history (MD/ME)
#' @param race_me Most frequently recorded race (excluding Latino) over entire member history,
#' case insensitive, can take multiple values (e.g., c("aian", "black")) (MD/ME)
#' @param race_eth_me Most frequently recorded race (including Latino) over entire member history,
#' case insensitive, can take multiple values (e.g., c("latino", "black")) (MD/ME)
#' @param race_recent Most recently recorded race (excluding Latino),
#' case insensitive, can take multiple values (e.g., c("aian", "black")) (MD/ME)
#' @param race_eth_recent Most recently recorded race (including Latino),
#' case insensitive, can take multiple values (e.g., c("latino", "black")) (MD/ME)
#' @param lang_amharic Alone or in combination Amharic written or spoken language over entire member history (MD)
#' @param lang_arabic Alone or in combination Arabic written or spoken language over entire member history (MD)
#' @param lang_chinese Alone or in combination Chinese written or spoken language over entire member history (MD)
#' @param lang_english Alone or in combination English written or spoken language over entire member history (MD)
#' @param lang_korean Alone or in combination Korean written or spoken language over entire member history (MD)
#' @param lang_russian Alone or in combination Russian written or spoken language over entire member history (MD)
#' @param lang_somali Alone or in combination Somali written or spoken language over entire member history (MD)
#' @param lang_spanish Alone or in combination Spanish written or spoken language over entire member history (MD)
#' @param lang_ukrainian Alone or in combination Ukrainian written or spoken language over entire member history (MD)
#' @param lang_vietnamese Alone or in combination Vietnamese written or spoken language over entire member history (MD)
#' @param lang_me Most frequently recorded spoken/written language over entire member history, 
#' case insensitive, can take multiple values (e.g., c("chinese", "english")) (MD)
#' @param lang_recent Most recently recorded spoken/written language, 
#' case insensitive, can take multiple values (e.g., c("chinese", "english")) (MD)
#' @param geo_zip Most frequently reported ZIP code during requested date range,
#' can take multiple values (e.g., c("98104", "98105")) (A)
#' @param geo_hra_code Most frequently reported health reporting area code during 
#' requested date range, can take multiple values (e.g., c("2100", "9000")) (MD)
#' @param geo_region Most frequently mapped HRA-based region during requested date range, 
#' (choose from east, north, seattle, south), case insensitive, can take multiple values (MD)
#' @param geo_school_code Most frequently reported school district code during 
#' requested date range, can take multiple values (e.g., c("5307710", "5303540")) (MD)
#' @param geo_county_code Most frequently reported county during requested date range 
#' (use FIPS codes), can take multiple values (AP/MD)
#' @param geo_ach_code Most recently reported accountable community of health 
#' during requested data range (use ACH codes), can take multiple values (AP)
#' @param geo_kc_ever Ever resided in King County (ME)
#' @param geo_kc_min Minimum amount of requested date range a person needs to have 
#' resided in King County to be included (AP/ME)
#' @param timevar_denom Which denominator is used to calculate the percentages
#' for time-varying parameters (e.g., dual_min, geo_kc_min). Choose from
#' duration (number of days in selected period, i.e., between from_date and to_date)
#' or cov_days (the number of days within selected period the person was actually
#' enrolled). Default is duration.
#' @param show_query Print the SQL query that is being run. Useful for debugging.
#' Default is TRUE
#'
#' @examples
#' \dontrun{
#' claims_elig(server = db.claims51, source = "apcd", 
#'   from_date = "2017-01-01", to_date = "2017-06-30")
#' claims_elig(server = db.claims51, source = "mcaid", 
#'   from_date = "2017-01-01", to_date = "2017-06-30", age_min = 18, 
#'   age_max = 64, lang_me = c("ARABIC", "SOMALI"), zip = c("98103", "98105"))
#' }
#' 
#' @export
#' 
#' 



#### Create function ####
claims_elig <- function(conn,
                       source = c("apcd", "mcaid", "mcaid_mcare", "mcare"),
                       # Coverage time and type
                       from_date = Sys.Date() - months(18),
                       to_date = Sys.Date() - months(6),
                       cov_min = 0,
                       covgap_max = NULL,
                       mcaid_min = NULL,
                       mcaid_max = NULL,
                       mcare_min = NULL,
                       mcare_max = NULL,
                       dual_min = NULL,
                       dual_max = NULL,
                       med_covgrp = NULL,
                       pharm_covgrp = NULL,
                       bsp_group_name = NULL,
                       full_benefit_min = NULL,
                       cov_type = NULL,
                       mco_id = NULL,
                       part_a_min = NULL,
                       part_a_max = NULL,
                       part_b_min = NULL,
                       part_b_max = NULL,
                       part_c_min = NULL,
                       part_c_max = NULL,
                       buy_in_min = NULL,
                       buy_in_max = NULL,
                       # Demographics
                       id = NULL,
                       age_min = NULL,
                       age_max = NULL, 
                       female = NULL,
                       male = NULL,
                       gender_me = NULL,
                       gender_recent = NULL,
                       race_aian = NULL,
                       race_asian = NULL,
                       race_asian_pi = NULL,
                       race_black = NULL,
                       race_nhpi = NULL,
                       race_latino = NULL,
                       race_white = NULL,
                       race_unk = NULL,
                       race_me = NULL,
                       race_eth_me = NULL,
                       race_recent = NULL,
                       race_eth_recent = NULL,
                       lang_amharic = NULL,
                       lang_arabic = NULL,
                       lang_chinese = NULL,
                       lang_english = NULL,
                       lang_korean = NULL,
                       lang_russian = NULL,
                       lang_somali = NULL,
                       lang_spanish = NULL,
                       lang_ukrainian = NULL,
                       lang_vietnamese = NULL,
                       lang_me = NULL,
                       lang_recent = NULL,
                       # Geography
                       geo_zip = NULL,
                       geo_hra_code = NULL,
                       geo_school_code = NULL,
                       geo_region = NULL,
                       geo_county_code = NULL,
                       geo_ach_code = NULL,
                       geo_kc_ever = NULL,
                       geo_kc_min = NULL,
                       timevar_denom = c("duration", "cov_days"),
                       show_query = T) {
  
  #### ERROR CHECKS ####
  # ODBC check
  if(missing(conn)) {
    stop("please provide a SQL connection")
  }
  
  # Source check
  source <- match.arg(source)
  
  # Date checks
  if(from_date > to_date & !missing(from_date) & !missing(to_date)) {
    stop("from_date date must be <= to_date date")
  }
  
  if(missing(from_date) & missing(to_date)) {
    message("Default from_date and to_date dates used: - 18 and 6 months prior to today's date, respectively")
  }
  
  # Coverage checks
  if(!is.numeric(cov_min) | cov_min < 0 | cov_min > 100){
    stop("Coverage requirement must be numeric between 0 and 100")
  }
  
  if(!is.null(covgap_max)){
    if (!is.numeric(covgap_max) | covgap_max < 0) {
      stop("Maximum continuous coverage gap must be a positive number")  
    }
  }
  
  # Mcaid and Mcare min checks
  if (!is.null(mcaid_min)) {
    if(!is.numeric(mcaid_min) | mcaid_min < 0 | mcaid_min > 100){
      stop("Minimum time on Medicaid  must be numeric between 0 and 100")
    }
  }
  
  if (!is.null(mcare_min)) {
    if(!is.numeric(mcare_min) | mcare_min < 0 | mcare_min > 100){
      stop("Minimum time on Medicare  must be numeric between 0 and 100")
    }
  }

  # Dual checks
  if (!is.null(dual_min)) {
    if(!is.numeric(dual_min) | dual_min < 0 | dual_min > 100){
      stop("Dual eligibility must be numeric between 0 and 100")
    }
  }

  if (!is.null(dual_max)) {
    if(!is.numeric(dual_max) | dual_max < 0 | dual_max > 100){
      stop("Dual eligibility must be numeric between 0 and 100")
    }
  }
  
  if (!is.null(dual_min) & !is.null(dual_max)) {
    if (dual_min > dual_max) {
      stop("dual_min must be <= dual_max (or one should be NULL)")  
    }
  }
  
  # Add in med_covgrp and pharm_covgrp checks
  
  # Add in part_a/b/c checks
  
  # Age checks
  if (!is.null(age_min)) {
    if(!is.numeric(age_min)) {
      stop("age_min must be numeric")
    }
  }
  
  if (!is.null(age_max)) {
    if(!is.numeric(age_max)) {
      stop("age_max must be numeric")
    }
  }
  
  if (!is.null(age_min) & !is.null(age_max)) {
    if (age_min > age_max) {
      stop("age_min must be <= age_max (or one should be NULL)")  
    }
  }
  
  # Gender checks
  lapply(list(female, male), function(x) {
    if (!is.null(x)) {
      if (!(x %in% c(1, 0))) {
        stop("Binary gender options must be NULL, 0, or 1")}
    }
  })
  
  if (!is.null(gender_me)) {
    if (!is.null(gender_me) & !gender_me %in% c("Female", "Male", "Multiple")) {
      stop("gender_me must be NULL, 'Female', 'Male', or 'Multiple' (case insensitive)")
    }
  }
  if (!is.null(gender_recent)) {
    if (!is.null(gender_recent) & !gender_recent %in% c("Female", "Male", "Multiple")) {
      stop("gender_recent must be NULL, 'Female', 'Male', or 'Multiple' (case insensitive)")
    }
  }
  
  # Race/ethnicity checks
  lapply(list(race_aian, race_asian, race_asian_pi, race_black, race_latino, 
              race_nhpi, race_white, race_unk), function(x) {
                if (!is.null(x)) {if (!(x %in% c(1, 0))) {
                  stop("Binary race options must be NULL, 0, or 1")}
                }
              })
  
  # Could make this more complicated to check for race_asian_pi in Mcaid but not worth it
  if (source %in% c("apcd")) {
    lapply(list(race_aian, race_asian, race_asian_pi, race_black, race_latino, 
                race_nhpi, race_white, race_unk), function(x) {
                  if (!is.null(x)) {
                    warning("Race options not available for APCD data")
                  }
                })
    }

  
  # Language checks
  lapply(list(lang_amharic, lang_arabic, lang_chinese, lang_english, lang_korean, 
              lang_russian, lang_somali, lang_spanish, lang_ukrainian, lang_vietnamese), 
         function(x) {
                if (!is.null(x)) {if (!(x %in% c(1, 0))) {
                  stop("Binary language options must be NULL, 0, or 1")}
                }
              })
  
  if (source %in% c("apcd", "mcare")) {
    lapply(list(lang_amharic, lang_arabic, lang_chinese, lang_english, lang_korean, 
                lang_russian, lang_somali, lang_spanish, lang_ukrainian, lang_vietnamese), 
           function(x) {
             if (!is.null(x)) {
               warning("Language options only available for Medicaid data")
             }
           })
  }

  # Geography checks
  if (!is.null(geo_region)) {
    if (!tolower(geo_region) %in% c("east", "north", "seattle", "south")) {
      stop("Region must be one of 'east', 'north', 'seattle', 'south' (case insensitive)")   
    }
  }
  
  # timevar_denom check
  timevar_denom <- match.arg(timevar_denom)
  
  
  #### ID SETUP ####
  # ID var name
  if (source == "apcd") {
    id_name <- glue::glue_sql("id_apcd", .con = conn)
  } else if (source == "mcaid") {
    id_name <- glue::glue_sql("id_mcaid", .con = conn)
  } else if (source == "mcaid_mcare") {
    id_name <- glue::glue_sql("id_apde", .con = conn)
  } else if (source == "mcare") {
    id_name <- glue::glue_sql("id_mcare", .con = conn)
  } else {
    stop("Something went wrong when selecting a source")
  }
  
  
  #### PROCESS NON-TIME-VARYING PARAMETERS FOR SQL QUERY ####
  # ID
  ifelse(!is.null(id), 
         id_sql <- glue::glue_sql(" AND {id_name} IN ({id*}) ", .con = conn),
         id_sql <- DBI::SQL(''))
  
  # Age
  ifelse(!is.null(age_min), 
         age_min_sql <- glue::glue_sql(" AND age_min >= {age_min} ", .con = conn),
         age_min_sql <- DBI::SQL(''))
  
  ifelse(!is.null(age_max), 
         age_max_sql <- glue::glue_sql(" AND age_max >= {age_max} ", .con = conn),
         age_max_sql <- DBI::SQL(''))
  
  # Gender
  ifelse(!is.null(female), 
         female_sql <- glue::glue_sql(" AND female = {female} ", .con = conn),
         female_sql <- DBI::SQL(''))
  ifelse(!is.null(male), 
         male_sql <- glue::glue_sql(" AND male = {male} ", .con = conn),
         male_sql <- DBI::SQL(''))
  ifelse(!is.null(gender_me), 
         gender_me_sql <- glue::glue_sql(" AND LOWER(gender_me) IN ({tolower(gender_me)*}) ", .con = conn),
         gender_me_sql <- DBI::SQL(''))
  ifelse(!is.null(gender_recent), 
         gender_recent_sql <- glue::glue_sql(" AND LOWER(gender_recent) IN ({tolower(gender_recent)*}) ", .con = conn),
         gender_recent_sql <- DBI::SQL(''))
  
  # Race
  if (source %in% c("mcaid", "mcaid_mcare", "mcare")) {
    ifelse(!is.null(race_aian), 
           race_aian_sql <- glue::glue_sql(" AND race_aian = {race_aian} ", .con = conn),
           race_aian_sql <- DBI::SQL(''))
    ifelse(!is.null(race_asian), 
           race_asian_sql <- glue::glue_sql(" AND race_asian = {race_asian} ", .con = conn),
           race_asian_sql <- DBI::SQL(''))
    if (source %in% c("mcaid_mcare", "mcare")) {
      ifelse(!is.null(race_asian_pi), 
             race_asian_pi_sql <- glue::glue_sql(" AND race_asian_pi = {race_asian_pi} ", .con = conn),
             race_asian_pi_sql <- DBI::SQL(''))
    } else {
      race_asian_pi_sql <- DBI::SQL('')
    }
    ifelse(!is.null(race_black), 
           race_black_sql <- glue::glue_sql(" AND race_black = {race_black} ", .con = conn),
           race_black_sql <- DBI::SQL(''))
    ifelse(!is.null(race_latino), 
           race_latino_sql <- glue::glue_sql(" AND race_latino = {race_latino} ", .con = conn),
           race_latino_sql <- DBI::SQL(''))
    ifelse(!is.null(race_nhpi), 
           race_nhpi_sql <- glue::glue_sql(" AND race_nhpi = {race_nhpi} ", .con = conn),
           race_nhpi_sql <- DBI::SQL(''))
    ifelse(!is.null(race_white), 
           race_white_sql <- glue::glue_sql(" AND race_white = {race_white} ", .con = conn),
           race_white_sql <- DBI::SQL(''))
    ifelse(!is.null(race_unk), 
           race_unk_sql <- glue::glue_sql(" AND race_unk = {race_unk} ", .con = conn),
           race_unk_sql <- DBI::SQL(''))
    ifelse(!is.null(race_me), 
           race_me_sql <- glue::glue_sql(" AND LOWER(race_me) IN ({tolower(race_me)*}) ", .con = conn),
           race_me_sql <- DBI::SQL(''))
    ifelse(!is.null(race_eth_me), 
           race_eth_me_sql <- glue::glue_sql(" AND LOWER(race_eth_me) IN ({tolower(race_eth_me)*}) ", .con = conn),
           race_eth_me_sql <- DBI::SQL(''))
    ifelse(!is.null(race_recent), 
           race_recent_sql <- glue::glue_sql(" AND LOWER(race_recent) IN ({tolower(race_recent)*}) ", .con = conn),
           race_recent_sql <- DBI::SQL(''))
    ifelse(!is.null(race_eth_recent), 
           race_eth_recent_sql <- glue::glue_sql(" AND LOWER(race_eth_recent) IN ({tolower(race_eth_recent)*}) ", .con = conn),
           race_eth_recent_sql <- DBI::SQL(''))
  } else {
    race_aian_sql <- DBI::SQL('')
    race_asian_sql <- DBI::SQL('')
    race_asian_pi_sql <- DBI::SQL('')
    race_black_sql <- DBI::SQL('')
    race_latino_sql <- DBI::SQL('')
    race_nhpi_sql <- DBI::SQL('')
    race_white_sql <- DBI::SQL('')
    race_unk_sql <- DBI::SQL('')
    race_me_sql <- DBI::SQL('')
    race_eth_me_sql <- DBI::SQL('')
    race_recent_sql <- DBI::SQL('')
    race_eth_recent_sql <- DBI::SQL('')
  }

  
  # Language
  if (source %in% c("mcaid", "mcaid_mcare")) {
    ifelse(!is.null(lang_amharic), 
           lang_amharic_sql <- glue::glue_sql(" AND lang_amharic = {lang_amharic} ", .con = conn),
           lang_amharic_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_arabic), 
           lang_arabic_sql <- glue::glue_sql(" AND lang_arabic = {lang_arabic} ", .con = conn),
           lang_arabic_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_chinese), 
           lang_chinese_sql <- glue::glue_sql(" AND lang_chinese = {lang_chinese} ", .con = conn),
           lang_chinese_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_english), 
           lang_english_sql <- glue::glue_sql(" AND lang_english = {lang_english} ", .con = conn),
           lang_english_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_korean), 
           lang_korean_sql <- glue::glue_sql(" AND lang_korean = {lang_korean} ", .con = conn),
           lang_korean_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_russian), 
           lang_russian_sql <- glue::glue_sql(" AND lang_russian = {lang_russian} ", .con = conn),
           lang_russian_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_somali), 
           lang_somali_sql <- glue::glue_sql(" AND lang_somali = {lang_somali} ", .con = conn),
           lang_somali_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_spanish), 
           lang_spanish_sql <- glue::glue_sql(" AND lang_spanish = {lang_spanish} ", .con = conn),
           lang_spanish_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_ukrainian), 
           lang_ukrainian_sql <- glue::glue_sql(" AND lang_ukrainian = {lang_ukrainian} ", .con = conn),
           lang_ukrainian_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_vietnamese), 
           lang_vietnamese_sql <- glue::glue_sql(" AND lang_vietnamese = {lang_vietnamese} ", .con = conn),
           lang_vietnamese_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_me), 
           lang_me_sql <- glue::glue_sql(" AND lang_me IN ({lang_me*}) ", .con = conn),
           lang_me_sql <- DBI::SQL(''))
    ifelse(!is.null(lang_recent), 
           lang_recent_sql <- glue::glue_sql(" AND LOWER(lang_recent) IN ({tolower(lang_recent)*}) ", .con = conn),
           lang_recent_sql <- DBI::SQL(''))
  } else {
    lang_amharic_sql <- DBI::SQL('')
    lang_arabic_sql <- DBI::SQL('')
    lang_chinese_sql <- DBI::SQL('')
    lang_english_sql <- DBI::SQL('')
    lang_korean_sql <- DBI::SQL('')
    lang_russian_sql <- DBI::SQL('')
    lang_somali_sql <- DBI::SQL('')
    lang_spanish_sql <- DBI::SQL('')
    lang_ukrainian_sql <- DBI::SQL('')
    lang_vietnamese_sql <- DBI::SQL('')
    lang_me_sql <- DBI::SQL('')
    lang_recent_sql <- DBI::SQL('')
  }

  
  # Geography
  if (source %in% c("mcaid_mcare", "mcaid")) {
    ifelse(!is.null(geo_kc_ever),
           geo_kc_ever_sql <- glue::glue_sql(" AND geo_kc_ever = {geo_kc_ever} ", .con = conn),
           geo_kc_ever_sql <- DBI::SQL(''))
  } else {
    geo_kc_ever_sql <- DBI::SQL('')
  }


  #### ELIG_DEMO VARS ####
  if (source == "apcd") {
    demo_vars <- glue::glue_sql(
      "{id_name}, dob, ninety_only, 
      CASE 
        WHEN (datediff(day, dob, {to_date}) + 1) >= 0 THEN 
          FLOOR((datediff(day, dob, {to_date}) + 1) / 365.25)
        WHEN datediff(day, dob, {to_date}) < 0 then NULL
        END as 'age',
      gender_female, gender_male, gender_me, gender_recent",
      .con = conn)
  } else if (source == "mcaid") {
    demo_vars <- glue::glue_sql(
      "{id_name}, 
      -- age vars
      dob, 
      CASE 
        WHEN (datediff(day, dob, {to_date}) + 1) >= 0 THEN 
          FLOOR((datediff(day, dob, {to_date}) + 1) / 365.25)
        WHEN datediff(day, dob, {to_date}) < 0 then NULL
        END as 'age',
      --gender vars
      gender_me, gender_recent, gender_male, gender_female, 
      -- gender_unk, -- This still needs to go back in the table
      gender_male_t, gender_female_t,
      --race vars
      race_eth_me, race_me, race_recent, race_eth_recent, 
      race_aian, race_asian, race_black, race_latino, race_nhpi, race_white, 
      race_aian_t, race_asian_t, race_black_t, race_latino_t, race_nhpi_t, race_white_t,
      -- race_unk, -- This still needs to go back in the table
      --language vars
      lang_max, lang_amharic, lang_arabic, lang_chinese, lang_english, lang_korean, 
      lang_russian, lang_somali, lang_spanish, lang_ukrainian, lang_vietnamese,
      lang_amharic_t, lang_arabic_t, lang_chinese_t, lang_english_t, lang_korean_t, 
      lang_russian_t, lang_somali_t, lang_spanish_t, lang_ukrainian_t, lang_vietnamese_t
      -- lang_unk -- this still needs to go back in the table"
      , .con = conn)
  } else if (source == "mcaid_mcare") {
    demo_vars <- glue::glue_sql(
      "{id_name}, dob, 
      CASE 
        WHEN (datediff(day, dob, {to_date}) + 1) >= 0 THEN 
          FLOOR((datediff(day, dob, {to_date}) + 1) / 365.25)
        WHEN datediff(day, dob, {to_date}) < 0 then NULL
        END as 'age', death_dt, 
      gender_me, gender_recent, gender_female, gender_male, 
      race_me, race_eth_me, race_recent, race_eth_recent, 
      race_aian, race_asian, race_asian_pi, race_black, race_latino, 
      race_nhpi, race_other, race_white, race_unk,
      --dual AS dual_ever -- do we want this to come in?
      geo_kc_ever ",
      .con = conn)
  } else if (source == "mcare") {
    demo_vars <- glue::glue_sql(
      "{id_name}, dob, 
      CASE 
        WHEN (datediff(day, dob, {to_date}) + 1) >= 0 THEN 
          FLOOR((datediff(day, dob, {to_date}) + 1) / 365.25)
        WHEN datediff(day, dob, {to_date}) < 0 then NULL
        END as 'age', death_dt, 
      gender_me, gender_recent, gender_female, gender_male, gender_female_t, gender_male_t, 
      race_me, race_eth_me, race_recent, race_eth_recent, 
      race_aian, race_asian, race_asian_pi, race_black, race_latino, 
      race_nhpi, race_other, race_white, race_unk,
      race_aian_t, race_asian_t, race_asian_pi_t, race_black_t, race_nhpi_t, 
      race_white_t, race_latino_t, race_other_t, race_unk_t, geo_kc_ever",
      .con = conn)
  }
  
  
  #### TIMEVAR SETUP ####
  ### Determine length of requested period
  duration <- lubridate::interval(from_date, to_date) / lubridate::ddays(1) + 1
  
  ### Set up coverage time restrictions
  ifelse(!is.null(cov_min),
         cov_min_sql <- glue::glue_sql(" AND c.cov_pct >= {cov_min} ", .con = conn),
         cov_min_sql <- DBI::SQL(''))
  
  ifelse(!is.null(covgap_max),
         covgap_max_sql <- glue::glue_sql(" AND c.covgap_max >= {covgap_max} ", .con = conn),
         covgap_max_sql <- DBI::SQL(''))
  
  ### Set up the denominator to be used in timevar percents
  if (timevar_denom == "duration") {
    denom_sql <- glue::glue_sql("duration")
  } else if (timevar_denom == "cov_days") {
    denom_sql <- glue::glue_sql("cov_days")
  } else {
    stop("timevar_denom must be one of 'duration' or 'cov_days'")
  }
  
  ### Helpful to make a couple of temp tables that store coverage time as it 
  # is used in several sub-queries
  
  ### Make part-way table to avoid calculating cov_days repeatedly in sub-queries
  timevar_part_sql <- glue::glue_sql(
    "SELECT a.{id_name}, a.from_date, a.to_date, a.contiguous, 
          CASE 
            WHEN a.from_date <= {from_date} AND a.to_date >= {to_date} 
              THEN datediff(day, {from_date}, {to_date}) + 1
            WHEN a.from_date <= {from_date} AND a.to_date < {to_date} 
              THEN datediff(day, {from_date}, a.to_date) + 1
            WHEN a.from_date > {from_date} AND a.to_date >= {to_date} 
              THEN datediff(day, a.from_date, {to_date}) + 1
            WHEN a.from_date > {from_date} AND a.to_date < {to_date} 
              THEN datediff(day, a.from_date, a.to_date) + 1
            ELSE NULL END AS cov_days,
          CASE
            WHEN a.from_date <= {from_date} THEN 0
            WHEN LAG(a.to_date, 1) OVER 
              (PARTITION BY a.{id_name} ORDER BY a.to_date) IS NULL
              THEN datediff(day, {from_date}, a.from_date)
            ELSE datediff(day, 
                          LAG(a.to_date, 1) OVER 
                          (PARTITION BY a.{id_name} ORDER BY a.to_date), 
                          a.from_date) - 1
            END AS pre_gap,
          CASE
            WHEN a.to_date >= {to_date} THEN 0 
            WHEN LEAD(a.to_date, 1) OVER 
              (PARTITION BY a.{id_name} ORDER BY a.to_date) IS NULL 
              THEN datediff(day, a.to_date, {to_date})
            ELSE datediff(day, a.to_date, 
                          LEAD(a.from_date, 1) 
                          OVER (PARTITION BY a.{id_name} ORDER BY a.from_date)) - 1
            END AS post_gap 
          INTO ##cov_time_part
          FROM 
          (SELECT {id_name}, from_date, to_date, contiguous FROM PHClaims.final.{`paste0(source, '_elig_timevar')`}
          WHERE from_date <= {to_date} AND to_date >= {from_date}) a",
    .con = conn)
  
  
  # Get rid of the temp table if it already exists
  try(odbc::dbRemoveTable(conn = conn, name = "##cov_time_part", temporary = T), 
      silent = T)
  
  odbc::dbGetQuery(conn = conn, timevar_part_sql)
  
  # Add index for faster joins
  odbc::dbGetQuery(conn = conn,
                   glue::glue_sql(
                     "CREATE CLUSTERED INDEX idx_cl_id_date ON ##cov_time_part ({id_name}, from_date, to_date);",
                     .con = conn))
  
  
  ### Now make final table that is used for calculating percentages etc.
  timevar_tot_sql <- glue::glue_sql(
    "SELECT c.* INTO ##cov_time_tot
      FROM
      (SELECT b.{id_name}, MAX(b.cov_days) AS cov_days, MAX(duration) AS duration,
        CAST((MAX(b.cov_days) * 1.0) / MAX(duration) * 100 AS decimal(4, 1)) AS cov_pct,  
        (SELECT MAX(v) FROM (VALUES (MAX(b.pre_gap)), (MAX(b.post_gap))) AS VALUE(v)) AS covgap_max
      FROM
        (SELECT a.{id_name}, SUM(a.cov_days) AS cov_days, {duration} AS duration, 
          MAX(a.pre_gap) AS pre_gap, MAX(a.post_gap) AS post_gap 
        FROM 
          (SELECT * FROM ##cov_time_part) a
        GROUP BY {id_name}) b
      GROUP BY {id_name}) c
    WHERE 1 = 1 {cov_min_sql} {covgap_max_sql}",
    .con = conn)
  
  # Get rid of the temp table if it already exists
  try(odbc::dbRemoveTable(conn = conn, name = "##cov_time_tot", temporary = T), 
      silent = T)
  
  odbc::dbGetQuery(conn = conn, timevar_tot_sql)
  
  # Add index for faster joins
  odbc::dbGetQuery(conn = conn,
                   glue::glue_sql(
                     "CREATE CLUSTERED INDEX idx_cl_id ON ##cov_time_tot ({id_name});",
                     .con = conn))
  
  
  #### SET UP GENERIC TIMEVAR CODE GENERATOR ####
  # Generates code that is used for joining each time-varying element below
  # var = name of the variable
  # pct = choose whether or not to add a percent column or just a days one
  timevar_gen_sql <- function(var, conn_inner = conn, source_inner = source,
                              pct = F) {
    
    # Currently id_name and denom_sql seem to be being drawn from the parent environment
    # works ok but could also define in this function
    
    if (pct == T) {
      # Table names
      pt1_a <- glue::glue("{var}_pt1_a")
      pt1_b <- glue::glue("{var}_pt1_b")
      pt2_a <- glue::glue("{var}_pt2_a")
      pt2_b <- glue::glue("{var}_pt2_b")
      pt2_c <- glue::glue("{var}_pt2_c")
      pt2_d <- glue::glue("{var}_pt2_d")
      tbl_final <- glue::glue("{var}_final")
      # Var names
      var_pct_num <- glue::glue("{var}_pct_num")
      var_pct <- glue::glue("{var}_pct")
      
      output_sql <- glue::glue_sql(
        "LEFT JOIN
        (SELECT {`pt1_b`}.{id_name}, {`pt1_b`}.{`var`}, {`pt2_d`}.{`var_pct`}
          FROM
          (SELECT {`pt1_a`}.{id_name}, {`pt1_a`}.{`var`}, 
            ROW_NUMBER() OVER(PARTITION BY {`pt1_a`}.{id_name} 
                              ORDER BY SUM(cov_time_part.cov_days) DESC, {`pt1_a`}.{`var`}) AS rk
            FROM 
            (SELECT {id_name}, {`var`}, from_date, to_date 
              FROM PHClaims.final.{`paste0(source_inner, '_elig_timevar')`}) {`pt1_a`}
            INNER JOIN
            (SELECT {id_name}, from_date, to_date, cov_days FROM ##cov_time_part) cov_time_part
            ON {`pt1_a`}.{id_name} = cov_time_part.{id_name} AND 
               {`pt1_a`}.from_date = cov_time_part.from_date AND
               {`pt1_a`}.to_date = cov_time_part.to_date 
            GROUP BY {`pt1_a`}.{id_name}, {`pt1_a`}.{`var`}) {`pt1_b`}
          
            INNER JOIN
            (SELECT {`pt2_c`}.{id_name}, 
              CAST({`pt2_c`}.{`var_pct_num`} * 1.0 / cov_tot.{denom_sql} * 100 AS decimal(4,1)) AS {`var_pct`}
              FROM
              (SELECT {`pt2_b`}.{id_name}, MAX({`var_pct_num`}) AS {`var_pct_num`}
                FROM
                (SELECT {`pt2_a`}.{id_name}, SUM(cov_time_part.cov_days * {`pt2_a`}.{`var`}) AS {`var_pct_num`}
                FROM 
                  (SELECT {id_name}, {`var`}, from_date, to_date FROM 
                    PHClaims.final.{`paste0(source_inner, '_elig_timevar')`}) {`pt2_a`}
                INNER JOIN
                  (SELECT {id_name}, from_date, to_date, cov_days FROM ##cov_time_part) cov_time_part
                    ON {`pt2_a`}.{id_name} = cov_time_part.{id_name} AND 
                      {`pt2_a`}.from_date = cov_time_part.from_date AND
                      {`pt2_a`}.to_date = cov_time_part.to_date 
                    GROUP BY {`pt2_a`}.{id_name}, {`pt2_a`}.{`var`}) {`pt2_b`}
                GROUP BY {`pt2_b`}.{id_name}) {`pt2_c`}
              INNER JOIN
                (SELECT {id_name}, cov_days, duration FROM ##cov_time_tot) cov_tot
              ON {`pt2_c`}.{id_name} = cov_tot.{id_name}) {`pt2_d`}
            ON {`pt1_b`}.{id_name} = {`pt2_d`}.{id_name}
            WHERE rk = 1) {`tbl_final`}
          ON demo.{id_name} = {`tbl_final`}.{id_name} ",
        .con = conn_inner)

    } else {
      # Table names
      tbl_a <- glue::glue("{var}_tbl_a")
      tbl_b <- glue::glue("{var}_tbl_b")
      tbl_final <- glue::glue("{var}_final") # Use final because of joins in core code below
      # Var names
      var_days <- glue::glue("{var}_days")
      
      output_sql <- glue::glue_sql(
        "LEFT JOIN
        (SELECT {`tbl_b`}.{id_name}, {`tbl_b`}.{`var`}, 
          {`tbl_b`}.{`var_days`} 
        FROM
          (SELECT {`tbl_a`}.{id_name}, {`tbl_a`}.{`var`}, 
            SUM(cov_time_part.cov_days) AS {var_days}, 
            ROW_NUMBER() OVER(PARTITION BY {`tbl_a`}.{id_name} 
                              ORDER BY SUM(cov_time_part.cov_days) DESC, {`tbl_a`}.{`var`}) AS rk
          FROM 
            (SELECT {id_name}, {`var`}, from_date, to_date 
              FROM PHClaims.final.{`paste0(source_inner, '_elig_timevar')`}) {`tbl_a`}
            INNER JOIN
            (SELECT {id_name}, from_date, to_date, cov_days
              FROM ##cov_time_part) cov_time_part
            ON {`tbl_a`}.{id_name} = cov_time_part.{id_name} AND 
              {`tbl_a`}.from_date = cov_time_part.from_date AND
              {`tbl_a`}.to_date = cov_time_part.to_date 
            GROUP BY {`tbl_a`}.{id_name}, {`tbl_a`}.{`var`}) {`tbl_b`}
        WHERE rk = 1) {`tbl_final`}
      ON demo.{id_name} = {`tbl_final`}.{id_name} ",
        .con = conn_inner)
    }
    return(output_sql)
  }
  
  
  #### SET UP COVERAGE TYPE (MCAID/MCARE COMBINED) ####
  if (source == "mcaid_mcare") {
    mcaid_cov_sql <- timevar_gen_sql(var = "mcaid", pct = T)
    
    if (!is.null(mcaid_min) | !is.null(mcaid_max)) {
      ifelse(!is.null(mcaid_min),
             mcaid_min_sql <- glue::glue_sql(" AND mcaid_final.mcaid_pct >= {mcaid_min} ", 
                                             .con = conn),
             mcaid_min_sql <- DBI::SQL(''))
      ifelse(!is.null(mcaid_max),
             mcaid_max_sql <- glue::glue_sql(" AND mcaid_final.mcaid_pct <= {mcaid_max} ", 
                                             .con = conn),
             mcaid_max_sql <- DBI::SQL(''))
      
      mcaid_cov_where_sql <- glue::glue_sql(" {mcaid_min_sql} {mcaid_max_sql}", .con = conn)

    } else {
      mcaid_cov_where_sql <- DBI::SQL('')
    }
    
  } else {
    mcaid_cov_sql <- DBI::SQL('')
    mcaid_cov_where_sql <- DBI::SQL('')
  }
  
  if (source == "mcaid_mcare") {
    mcare_cov_sql <- timevar_gen_sql(var = "mcare", pct = T)
    
    if (!is.null(mcare_min) | !is.null(mcare_max)) {
      ifelse(!is.null(mcare_min),
             mcare_min_sql <- glue::glue_sql(" AND mcare_final.mcare_pct >= {mcare_min} ", 
                                             .con = conn),
             mcare_min_sql <- DBI::SQL(''))
      ifelse(!is.null(mcare_max),
             mcare_max_sql <- glue::glue_sql(" AND mcare_final.mcare_pct <= {mcare_max} ", 
                                             .con = conn),
             mcare_max_sql <- DBI::SQL(''))
      
      mcare_cov_where_sql <- glue::glue_sql(" {mcare_min_sql} {mcare_max_sql}", .con = conn)
    } else {
      mcare_cov_where_sql <- DBI::SQL('')
    }
    
  } else {
    mcare_cov_sql <- DBI::SQL('')
    mcare_cov_where_sql <- DBI::SQL('')
  }
  
  
  
  #### SET UP DUAL CODE (ALL) ####
  if (source == "mcaid") {
    dual_sql <- timevar_gen_sql(var = "dual", pct = T)
    
    if (!is.null(dual_min) | !is.null(dual_max)) {
      ifelse(!is.null(dual_min),
             dual_min_sql <- glue::glue_sql(" AND dual_final.dual_pct >= {dual_min} ", 
                                            .con = conn),
             dual_min_sql <- DBI::SQL(''))
      ifelse(!is.null(dual_max),
             dual_max_sql <- glue::glue_sql(" AND dual_final.dual_pct <= {dual_max} ", 
                                            .con = conn),
             dual_max_sql <- DBI::SQL(''))
      
      dual_where_sql <- glue::glue_sql(" {dual_min_sql} {dual_max_sql}", .con = conn)
    } else {
      dual_where_sql <- DBI::SQL('')
    }
  } else if (source == "mcaid_mcare") {
    dual_sql <- timevar_gen_sql(var = "apde_dual", pct = T)
    
    if (!is.null(dual_min) | !is.null(dual_max)) {
      ifelse(!is.null(dual_min),
             dual_min_sql <- glue::glue_sql(" AND apde_dual_final.apde_dual_pct >= {dual_min} ", 
                                            .con = conn),
             dual_min_sql <- DBI::SQL(''))
      ifelse(!is.null(dual_max),
             dual_max_sql <- glue::glue_sql(" AND apde_dual_final.apde_dual_pct <= {dual_max} ", 
                                            .con = conn),
             dual_max_sql <- DBI::SQL(''))
      
      dual_where_sql <- glue::glue_sql(" {dual_min_sql} {dual_max_sql}", .con = conn)
    } else {
      dual_where_sql <- DBI::SQL('')
    }
  }
  
  
  
  #### SET UP COVERAGE GROUP TYPES CODE (APCD) ####
  # To come, add in code for med_covgrp and pharm_covgrp
  
  
  #### SET UP MEDICAID COVERAGE TYPES AND MCO ID CODE (MCAID) ####
  if (source %in% c("mcaid", "mcaid_mcare")) {
    # BSP group name
    bsp_group_name_sql <- timevar_gen_sql(var = "bsp_group_name", pct = F)
    
    if (!is.null(bsp_group_name)) {
      bsp_group_name_where_sql <- glue::glue_sql(
        " AND LOWER(bsp_group_name_final.bsp_group_name) IN ({tolower(bsp_group_name)*})",
        .con = conn)
    } else {
      bsp_group_name_where_sql <- DBI::SQL('')
    }
    
    # Full benefits
    full_benefit_sql <- timevar_gen_sql(var = "full_benefit", pct = T)
    
    if (!is.null(full_benefit_min)) {
      full_benefit_where_sql <- glue::glue_sql(
        " AND full_benefit_final.full_benefit_pct >= {full_benefit_min} ",
        .con = conn)
    } else {
      full_benefit_where_sql <- DBI::SQL('')
    }
    
    # Coverage type
    cov_type_sql <- timevar_gen_sql(var = "cov_type", pct = F)
    
    if (!is.null(cov_type)) {
      cov_type_where_sql <- glue::glue_sql(
        " AND LOWER(cov_type_final.cov_type) IN ({tolower(cov_type)*})",
        .con = conn)
    } else {
      cov_type_where_sql <- DBI::SQL('')
    }
    
    # MCO ID
    mco_id_sql <- timevar_gen_sql(var = "mco_id", pct = F)
    
    if (!is.null(mco_id)) {
      mco_id_where_sql <- glue::glue_sql(
        " AND mco_id_final.mco_id IN ({mco_id*})",
        .con = conn)
    } else {
      mco_id_where_sql <- DBI::SQL('')
    }
    
  } else {
    bsp_group_name_sql <- DBI::SQL('')
    bsp_group_name_where_sql <- DBI::SQL('')
    full_benefit_sql <- DBI::SQL('')
    full_benefit_where_sql <- DBI::SQL('')
    cov_type_sql <- DBI::SQL('')
    cov_type_where_sql <- DBI::SQL('')
    mco_id_sql <- DBI::SQL('')
    mco_id_where_sql <- DBI::SQL('')
  }
  
  
  #### SET UP MEDICARE COVERAGE TYPES CODE (MCARE) ####
  if (source %in% c("mcaid_mcare", "mcare")) {
    # Part A
    part_a_sql <- timevar_gen_sql(var = "part_a", pct = T)
    
    if (!is.null(part_a_min) | !is.null(part_a_max)) {
      ifelse(!is.null(part_a_min),
             part_a_min_sql <- glue::glue_sql(" AND part_a_final.part_a_pct >= {part_a_min} ", .con = conn),
             part_a_min_sql <- DBI::SQL(''))
      ifelse(!is.null(part_a_max),
             part_a_max_sql <- glue::glue_sql(" AND part_a_final.part_a_pct <= {part_a_max} ", .con = conn),
             part_a_max_sql <- DBI::SQL(''))
      
      part_a_where_sql <- glue::glue_sql(" {part_a_min_sql} {part_a_max_sql}", .con = conn)
    } else {
      part_a_where_sql <- DBI::SQL('')
    }
    
    # Part B
    part_b_sql <- timevar_gen_sql(var = "part_b", pct = T)
    
    if (!is.null(part_b_min) | !is.null(part_b_max)) {
      ifelse(!is.null(part_b_min),
             part_b_min_sql <- glue::glue_sql(" AND part_b_final.part_b_pct >= {part_b_min} ", .con = conn),
             part_b_min_sql <- DBI::SQL(''))
      ifelse(!is.null(part_b_max),
             part_b_max_sql <- glue::glue_sql(" AND part_b_final.part_b_pct <= {part_b_max} ", .con = conn),
             part_b_max_sql <- DBI::SQL(''))
      
      part_b_where_sql <- glue::glue_sql(" {part_b_min_sql} {part_b_max_sql}", .con = conn)
    } else {
      part_b_where_sql <- DBI::SQL('')
    }
    
    # Part C
    part_c_sql <- timevar_gen_sql(var = "part_c", pct = T)
    
    if (!is.null(part_c_min) | !is.null(part_c_max)) {
      ifelse(!is.null(part_c_min),
             part_c_min_sql <- glue::glue_sql(" AND part_c_final.part_c_pct >= {part_c_min} ", .con = conn),
             part_c_min_sql <- DBI::SQL(''))
      ifelse(!is.null(part_c_max),
             part_c_max_sql <- glue::glue_sql(" AND part_c_final.part_c_pct <= {part_c_max} ", .con = conn),
             part_c_max_sql <- DBI::SQL(''))
      
      part_c_where_sql <- glue::glue_sql(" {part_c_min_sql} {part_c_max_sql}", .con = conn)
    } else {
      part_c_where_sql <- DBI::SQL('')
    }
  } else {
    part_a_sql <- DBI::SQL('')
    part_a_where_sql <- DBI::SQL('')
    part_b_sql <- DBI::SQL('')
    part_b_where_sql <- DBI::SQL('')
    part_c_sql <- DBI::SQL('')
    part_c_where_sql <- DBI::SQL('')
  }

  #### SET UP BUY_IN CODE (MCARE) ####
  if (source %in% c("mcaid_mcare", "mcare")) {
    buy_in_sql <- timevar_gen_sql(var = "buy_in", pct = T)
    
    if (!is.null(buy_in_min) | !is.null(buy_in_max)) {
      ifelse(!is.null(buy_in_min),
             buy_in_min_sql <- glue::glue_sql(" AND buy_in_final.buy_in_pct >= {buy_in_min} ", .con = conn),
             buy_in_min_sql <- DBI::SQL(''))
      ifelse(!is.null(buy_in_max),
             buy_in_max_sql <- glue::glue_sql(" AND buy_in_final.buy_in_pct <= {buy_in_max} ", .con = conn),
             buy_in_max_sql <- DBI::SQL(''))
      
      buy_in_where_sql <- glue::glue_sql(" {buy_in_min_sql} {buy_in_max_sql}", .con = conn)
    } else {
      buy_in_where_sql <- DBI::SQL('')
    }
  } else {
    buy_in_sql <- DBI::SQL('')
    buy_in_where_sql <- DBI::SQL('')
  }

  
  #### SET UP GEOGRAPHY CODE ####
  # ZIP (ALL)
  geo_zip_sql <- timevar_gen_sql(var = "geo_zip", pct = F)
  
  if (!is.null(geo_zip)) {
    geo_zip_where_sql <- glue::glue_sql(" AND geo_zip_final.geo_zip IN ({geo_zip*})", .con = conn)
  } else {
    geo_zip_where_sql <- DBI::SQL('')
  }
  
  # HRA (MCAID)
  if (source %in% c("mcaid", "mcaid_mcare")) {
    geo_hra_code_sql <- timevar_gen_sql(var = "geo_hra_code", pct = F)
    
    if (!is.null(geo_hra_code)) {
      geo_hra_code_where_sql <- glue::glue_sql(
        " AND geo_hra_code_final.geo_hra_code IN ({geo_hra_code*})",
        .con = conn)
    } else {
      geo_hra_code_where_sql <- DBI::SQL('')
    }
  } else {
    geo_hra_code_sql <- DBI::SQL('')
    geo_hra_code_where_sql <- DBI::SQL('')
  }
  
  # School district
  if (source %in% c("mcaid", "mcaid_mcare")) {
    geo_school_code_sql <- timevar_gen_sql(var = "geo_school_code", pct = F)
    
    if (!is.null(geo_school_code)) {
      geo_school_code_where_sql <- glue::glue_sql(
        " AND geo_school_code_final.geo_school_code IN ({geo_school_code*})",
        .con = conn)
    } else {
      geo_school_code_where_sql <- DBI::SQL('')
    }
  } else {
    geo_school_code_sql <- DBI::SQL('')
    geo_school_code_where_sql <- DBI::SQL('')
  }

  # Region (MCAID)
  # Need to join with another table, not yet implemented
  
  
  # County (APCD/MCAID)
  if (source %in% c("apcd", "mcaid", "mcaid_mcare")) {
    geo_county_code_sql <- timevar_gen_sql(var = "geo_county_code", pct = F)
    
    if (!is.null(geo_county_code)) {
      geo_county_code_where_sql <- glue::glue_sql(
        " AND geo_county_final.geo_county_code IN ({geo_county_code*})",
        .con = conn)
    } else {
      geo_county_code_where_sql <- DBI::SQL('')
    }
  } else {
    geo_county_code_sql <- DBI::SQL('')
    geo_county_code_where_sql <- DBI::SQL('')
  }
  
  # ACH (APCD)
  if (source %in% c("apcd")) {
    geo_ach_code_sql <- timevar_gen_sql(var = "geo_ach_code", pct = F)
    
    if (!is.null(geo_ach_code)) {
      geo_ach_code_where_sql <- glue::glue_sql(
        " AND geo_ach_code_final.geo_ach_code IN ({geo_ach_code*})",
        .con = conn)
    } else {
      geo_ach_code_where_sql <- DBI::SQL('')
    }
  } else {
    geo_ach_code_sql <- DBI::SQL('')
    geo_ach_code_where_sql <- DBI::SQL('')
  }
  
  # King County (APCD/MCARE)
  if (source %in% c("mcaid_mcare", "mcare")) {
    geo_kc_sql <- timevar_gen_sql(var = "geo_kc", pct = T)
    
    if (!is.null(geo_kc_min)) {
      geo_kc_where_sql <- glue::glue_sql(
        " AND geo_kc_final.geo_kc_pct >= {geo_kc_min} ",
        .con = conn)
    } else {
      geo_kc_where_sql <- DBI::SQL('')
    }
  } else {
    geo_kc_sql <- DBI::SQL('')
    geo_kc_where_sql <- DBI::SQL('')
  }

  
  
  #### TIME-VARYING VARIABLES ####
  # Be sure to end these with a comma
  if (source == "apcd") {
    timevar_vars <- glue::glue_sql(
      "",
      .con = conn)
  } else if (source == "mcaid") {
    timevar_vars <- glue::glue_sql(
      " dual_final.dual, dual_final.dual_pct, 
      bsp_group_name_final.bsp_group_name, bsp_group_name_final.bsp_group_name_days, 
      full_benefit_final.full_benefit, full_benefit_final.full_benefit_pct, 
      cov_type_final.cov_type, cov_type_final.cov_type_days, 
      mco_id_final.mco_id, mco_id_final.mco_id_days, 
      geo_zip_final.geo_zip, geo_zip_final.geo_zip_days, 
      geo_hra_code_final.geo_hra_code, geo_hra_code_final.geo_hra_code_days, 
      geo_county_code_final.geo_county_code, geo_county_code_final.geo_county_code_days, "
      , .con = conn)
  } else if (source == "mcaid_mcare") {
    timevar_vars <- glue::glue_sql(
      " mcaid_final.mcaid, mcaid_final.mcaid_pct,
      mcare_final.mcare, mcare_final.mcare_pct,
      apde_dual_final.apde_dual, apde_dual_final.apde_dual_pct,
      bsp_group_name_final.bsp_group_name, bsp_group_name_final.bsp_group_name_days, 
      full_benefit_final.full_benefit, full_benefit_final.full_benefit_pct, 
      cov_type_final.cov_type, cov_type_final.cov_type_days, 
      mco_id_final.mco_id, mco_id_final.mco_id_days, 
      part_a_final.part_a, part_a_final.part_a_pct, 
      part_b_final.part_b, part_b_final.part_b_pct, 
      part_c_final.part_c, part_c_final.part_c_pct, 
      buy_in_final.buy_in, buy_in_final.buy_in_pct, 
      geo_zip_final.geo_zip, geo_zip_final.geo_zip_days, 
      geo_hra_code_final.geo_hra_code, geo_hra_code_final.geo_hra_code_days,  
      geo_county_code_final.geo_county_code, geo_county_code_final.geo_county_code_days, 
      geo_kc_final.geo_kc, geo_kc_final.geo_kc_pct, " 
      , .con = conn)
  } else if (source == "mcare") {
    timevar_vars <- glue::glue_sql(
      " dual_final.dual, dual_final.dual_pct, 
      part_a_final.part_a, part_a_final.part_a_pct, 
      part_b_final.part_b, part_b_final.part_b_pct, 
      part_c_final.part_c, part_c_final.part_c_pct, 
      buy_in_final.buy_in, buy_in_final.buy_in_pct,  
      geo_zip_final.geo_zip, geo_zip_final.geo_zip_days, 
      geo_kc_final.geo_kc, geo_kc_final.geo_kc_pct, " 
      , .con = conn)
  }
  
  #### CORE QUERY ####
  core_sql <- glue::glue_sql(
    "SELECT demo.*,
      {timevar_vars} 
      timevar.cov_days, timevar.duration, timevar.cov_pct, timevar.covgap_max 
      FROM
      (SELECT DISTINCT {demo_vars}
        from PHClaims.final.{`paste0(source, '_elig_demo')`}
        WHERE 1 = 1 {id_sql} 
        {age_min_sql} {age_max_sql} 
        {female_sql} {male_sql} {gender_me_sql} {gender_recent_sql} 
        {race_aian_sql} {race_asian_sql} {race_black_sql} {race_latino_sql} 
        {race_nhpi_sql} {race_white_sql} {race_unk_sql} {race_me_sql} 
        {race_eth_me_sql} {race_recent_sql} {race_eth_recent_sql} 
        {lang_amharic_sql} {lang_arabic_sql} {lang_chinese_sql} {lang_english_sql} 
        {lang_korean_sql} {lang_russian_sql} {lang_somali_sql} {lang_spanish_sql}
        {lang_ukrainian_sql} {lang_vietnamese_sql} {lang_me_sql} {lang_recent_sql}
        {geo_kc_ever_sql}
      ) demo
      INNER JOIN
      (SELECT {id_name}, cov_days, duration, cov_pct, covgap_max 
        FROM ##cov_time_tot) timevar
        ON demo.{id_name} = timevar.{id_name}
      {mcaid_cov_sql} {mcare_cov_sql} {dual_sql}  {bsp_group_name_sql} 
      {full_benefit_sql} {cov_type_sql} {mco_id_sql} 
      {part_a_sql} {part_b_sql} {part_c_sql} {buy_in_sql} 
      {geo_zip_sql} {geo_hra_code_sql} {geo_school_code_sql} 
      {geo_county_code_sql} {geo_ach_code_sql} {geo_kc_sql}
      WHERE 1 = 1 
      {mcaid_where_sql} {mcare_where_sql} {dual_where_sql} 
      {bsp_group_name_where_sql} {full_benefit_where_sql}
      {cov_type_where_sql} {mco_id_where_sql} {part_a_where_sql} 
      {part_b_where_sql} {part_c_where_sql} {buy_in_where_sql}
      {geo_zip_where_sql} {geo_hra_code_where_sql} {geo_school_code_where_sql}
      {geo_county_code_where_sql} {geo_ach_code_where_sql} {geo_kc_where_sql}"
    , .con = conn)
  
  if (show_query == T) {
    print(core_sql)
  }
  
  
  output <- dbGetQuery(conn, core_sql)
  return(output)
  
}
  
