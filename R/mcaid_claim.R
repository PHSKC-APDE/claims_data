#' @title Medicaid member claim summary
#' 
#' @description \code{mcaid_claim_f} queries a SQL server to return Medicaid member eligibility, demographic and claim summary information.
#' 
#' @details LARGELY FOR INTERNAL USE
#' This function builds and sends a SQL query to return a data set of Medicaid member and claim information
#' using specified parameters, including coverage time period, coverage characteristics 
#' (e.g. Medicare dual eligibility), and member demographics. \code{mcaid_Claim_f} is a wrapper for \code{mcaid_elig_f}
#' 
#' @param server SQL server connection created using \code{odbc} package
#' @param from_date Begin date for Medicaid coverage period, "YYYY-MM-DD", defaults to 12 months prior to today's date
#' @param to_date End date for Medicaid coverage period, "YYYY-MM-DD", defaults to 6 months prior to today's date
#' @param covmin Minimum coverage required during requested date range (percent scale), defaults to 0
#' @param ccov_min Minimum continuous coverage required during requested date range (days), defaults to 1
#' @param covgap_max Maximum gap in continuous coverage allowed during requested date range (days), defaults to null
#' @param dualmax Maximum Medicare-Medicaid dual eligibility coverage allowed during requested date range (percent scale), defaults to 100
#' @param agemin Minimum age for cohort (integer), age is calculated as of last day of requested date range, defaults to 0
#' @param agemax Maximum age for cohort (integer), age is calculated as of last day of requested date range, defaults to 200
#' @param female Alone or in combination female gender over entire member history, defaults to null
#' @param male Alone or in combination female gender over entire member history, defaults to null
#' @param aian Alone or in combination American Indian/Alaska Native race over entire member history, defaults to null
#' @param asian Alone or in combination Asian race over entire member history, defaults to null
#' @param black Alone or in combination Black race over entire member history, defaults to null
#' @param latino Alone or in combination Latino race over entire member history, defaults to null
#' @param nhpi Alone or in combination Native Hawaiian/Pacific Islander race over entire member history, defaults to null
#' @param white Alone or in combination white race over entire member history, defaults to null
#' @param english Alone or in combination English written or spoken language over entire member history, defaults to null
#' @param spanish Alone or in combination Spanish written or spoken language over entire member history, defaults to null
#' @param vietnamese Alone or in combination Vietnamese written or spoken language over entire member history, defaults to null
#' @param chinese Alone or in combination Chinese written or spoken language over entire member history, defaults to null
#' @param somali Alone or in combination Somali written or spoken language over entire member history, defaults to null
#' @param russian Alone or in combination Russian written or spoken language over entire member history, defaults to null
#' @param arabic Alone or in combination Arabic written or spoken language over entire member history, defaults to null
#' @param korean Alone or in combination Korean written or spoken language over entire member history, defaults to null
#' @param ukrainian Alone or in combination Ukrainian written or spoken language over entire member history, defaults to null
#' @param amharic Alone or in combination Amharic written or spoken language over entire member history, defaults to null
#' @param maxlang Most frequently reported spoken/written language, e.g. "SOMALI,ARABIC", defaults to null
#' @param zip Most frequently reported ZIP code during requested date range, eg. "98103,98105", defaults to null
#' @param region Most frequently mapped HRA based region during requested date range, e.g, "east,north,seattle,south", defaults to null
#' @param id List of requested Medicaid ProviderOne IDs, defaults to null
#'
#' @examples
#' \dontrun{
#' mcaid_claim_f(server = db.claims51, from_date = "2017-01-01", to_date = "2017-06-30")
#' mcaid_claim_f(server = db.claims51, from_date = "2017-01-01", to_date = "2017-06-30", agemin = 18, 
#' agemax = 64, maxlang = "ARABIC,SOMALI", zip = "98103,98105")  
#' }
#' 
#' @export
mcaid_claim_f <- function(server, from_date = Sys.Date() - months(12), to_date = Sys.Date() - months(6), covmin = 0, ccov_min = 1,
                           covgap_max = "null", dualmax = 100, agemin = 0, agemax = 200, female = "null", male = "null", 
                           aian = "null", asian = "null", black = "null", nhpi = "null", white = "null", latino = "null",
                           zip = "null", region = "null", english = "null", spanish = "null", vietnamese = "null",
                           chinese = "null", somali = "null", russian = "null", arabic = "null", korean = "null",
                           ukrainian = "null", amharic = "null", maxlang = "null", id = "null") {
  
  #Error checks
  if(missing(server)) {
    stop("please provide a SQL server where data resides")
  }
  
  if(from_date > to_date & !missing(from_date) & !missing(to_date)) {
    stop("from_date date must be <= to_date date")
  }
  
  if(missing(from_date) & missing(to_date)) {
    print("Default from_date and to_date dates used - 12 and 6 months prior to today's date, respectively")
  }
  
  if((missing(from_date) & !missing(to_date)) | (!missing(from_date) & missing(to_date))) {
    stop("If from_date date provided, to_date date must also be provided. And vice versa.")
  }
  
  if(!is.numeric(covmin) | covmin < 0 | covmin > 100){
    stop("Coverage requirement must be numeric between 0 and 100")
  }
  
  if(!is.numeric(dualmax) | dualmax < 0 | dualmax > 100){
    stop("Dual eligibility must be numeric between 0 and 100")
  }
  
  if(!is.numeric(ccov_min) | ccov_min < 1){
    stop("Minimum continuous coverage days must be a positive integer greater than 0")
  }
  
  if((!is.numeric(covgap_max) | covgap_max < 0) & !is.character(covgap_max)){
    stop("Maximum continuous coverage gap must be a positive integer")
  }
  
  if(!is.numeric(agemin) | !is.numeric(agemax)) {
    stop("Age min and max must be provided as numerics")
  }
  
  if(agemin > agemax & !missing(agemin) & !missing(agemax)) {
    stop("Minimum age must be <= maximum age")
  }
  
  
  if(!(aian %in% c("null",0, 1)) | !(asian %in% c("null",0, 1)) | !(black %in% c("null",0, 1)) |
     !(nhpi %in% c("null",0, 1)) | !(white %in% c("null",0, 1)) | !(latino %in% c("null",0, 1)) |
     !(female %in% c("null",0, 1)) | !(male %in% c("null",0, 1)) | !(english %in% c("null",0, 1)) |
     !(spanish %in% c("null",0, 1)) | !(vietnamese %in% c("null",0, 1)) | !(chinese %in% c("null",0, 1)) |
     !(somali %in% c("null",0, 1)) | !(russian %in% c("null",0, 1)) | !(arabic %in% c("null",0, 1)) |
     !(korean %in% c("null",0, 1)) | !(ukrainian %in% c("null",0, 1)) | !(amharic %in% c("null",0, 1))) {
    stop("Race, sex and language parameters must be left missing or set to 'null', 0 or 1")
  }
  
  if(!is.character(zip) | !is.character(region) | !is.character(maxlang) | !is.character(id)) {
    stop("Geographic, 'maxlang' and 'id' parameters must be input as comma-separated characters with no spaces between items")
  }
  
  #Run parameters message
  cat(paste(
        "SQL server: ", tail(as.character(enquo(server)),1), "\n",
        "You have selected a Medicaid member cohort with the following characteristics:\n",
        "Coverage begin date: ", from_date, "(inclusive)\n",
        "Coverage end date: ", to_date, " (inclusive)\n",
        "Coverage requirement: ", covmin, " percent or more of requested date range\n",
        "Minimum continuous coverage requirement: ", ccov_min, " days during requested date range\n",
        "Maximum continuous coverage gap: ", covgap_max, " days during requested date range\n",
        "Medicare-Medicaid dual eligibility: ", dualmax, " percent or less of requested date range\n",
        "Minimum age: ", agemin, " years and older\n",
        "Maximum age: ", agemax, " years and younger\n",    
        "Female alone or in combination, ever: ", female, "\n",
        "Male alone or in combination, ever: ", male, "\n",  
        "AI/AN alone or in combination, ever: ", aian, "\n",
        "Asian alone or in combination, ever: ", asian, "\n",   
        "Black alone or in combination, ever: ", black, "\n",
        "NH/PI alone or in combination, ever: ", nhpi, "\n",
        "White alone or in combination, ever: ", white, "\n",
        "Latino alone or in combination, ever: ", latino, "\n",
        "ZIP codes: ", zip, "\n",
        "ZIP-based regions: ", region, "\n",
        "English language alone or in combination, ever: ", english, "\n",  
        "Spanish language alone or in combination, ever: ", spanish, "\n",
        "Vietnamese language alone or in combination, ever: ", vietnamese, "\n",   
        "Chinese language alone or in combination, ever: ", chinese, "\n",
        "Somali language alone or in combination, ever: ", somali, "\n",
        "Russian language alone or in combination, ever: ", russian, "\n",
        "Arabic language alone or in combination, ever: ", arabic, "\n",
        "Korean language alone or in combination, ever: ", korean, "\n",
        "Ukrainian language alone or in combination, ever: ", ukrainian, "\n",
        "Amharic language alone or in combination, ever: ", amharic, "\n",
        "Languages: ", maxlang, "\n",
        "Requested Medicaid IDs: ", id, "\n",
        sep = ""))
  
  #Derived variables
  
  duration <- as.numeric(as.Date(to_date) - as.Date(from_date)) + 1
  
  #Build SQL query
  exec1 <- "exec PHClaims.dbo.sp_mcaidcohort_sql"
  exec2 <- "exec PHClaims.dbo.sp_mcaid_claims_r"
  
  from_date_t <- paste("@from_date = \'", from_date, "\',", sep = "")
  to_date_t1 <- paste("@to_date = \'", to_date, "\',", sep = "") #comma included for elig cohort sp
  to_date_t2 <- paste("@to_date = \'", to_date, "\'", sep = "") #no comma for claims sp
  duration_t <- paste("@duration = ", duration, ",", sep = "")
  covmin_t <- paste("@covmin = ", covmin, ",", sep = "")
  ccov_min_t <- paste("@ccov_min = ", ccov_min, ",", sep = "")
  covgap_max_t <- paste("@covgap_max = ", covgap_max, ",", sep = "")
  dualmax_t <- paste("@dualmax = ", dualmax, ",", sep = "")
  
  agemin_t <- paste("@agemin = ", agemin, ",", sep = "")
  agemax_t <- paste("@agemax = ", agemax, ",", sep = "")
  
  female_t <- paste("@female = ", female, ",", sep = "")
  male_t <- paste("@male = ", male, ",", sep = "")
  
  aian_t <- paste("@aian = ", aian, ",", sep = "")
  asian_t <- paste("@asian = ", asian, ",", sep = "")
  black_t <- paste("@black = ", black, ",", sep = "")
  nhpi_t <- paste("@nhpi = ", nhpi, ",", sep = "")
  white_t <- paste("@white = ", white, ",", sep = "")
  latino_t <- paste("@latino = ", latino, ",", sep = "")
  
  ifelse(missing(zip), 
         zip_t <- paste("@zip = ", zip, ",", sep = ""),
         zip_t <- paste("@zip = \'", zip, "\',", sep = ""))
  ifelse(missing(region), 
         region_t <- paste("@region = ", region, ",", sep = ""),
         region_t <- paste("@region = \'", region, "\',", sep = ""))
  
  
  english_t <- paste("@english = ", english, ",", sep = "")
  spanish_t <- paste("@spanish = ", spanish, ",", sep = "")
  vietnamese_t <- paste("@vietnamese = ", vietnamese, ",", sep = "")
  chinese_t <- paste("@chinese = ", chinese, ",", sep = "")
  somali_t <- paste("@somali = ", somali, ",", sep = "")
  russian_t <- paste("@russian = ", russian, ",", sep = "")
  arabic_t <- paste("@arabic = ", arabic, ",", sep = "")
  korean_t <- paste("@korean = ", korean, ",", sep = "")
  ukrainian_t <- paste("@ukrainian = ", ukrainian, ",", sep = "")
  amharic_t <- paste("@amharic = ", amharic, ",", sep = "")
  
  ifelse(missing(maxlang), 
         maxlang_t <- paste("@maxlang = ", maxlang, ",", sep = ""),
         maxlang_t <- paste("@maxlang = \'", maxlang, "\',", sep = ""))
  
  ifelse(missing(id), 
         id_t <- paste("@id = ", id, sep = ""),
         id_t <- paste("@id = \'", id, "\'", sep = ""))
  
  sql1 <- paste(exec1, from_date_t, to_date_t1, duration_t, covmin_t, ccov_min_t, covgap_max_t, dualmax_t, agemin_t, agemax_t, female_t, male_t, 
        aian_t, asian_t, black_t, nhpi_t, white_t, latino_t, zip_t, region_t, english_t, spanish_t,
        vietnamese_t, chinese_t, somali_t, russian_t, arabic_t, korean_t, ukrainian_t, amharic_t,
        maxlang_t, id_t, sep = " ")
  
  sql2 <- paste(exec2, from_date_t, to_date_t2, sep = " ")

  #Execute batched SQL statements
  sqlbatch_f(server, list(sql1, sql2))
}