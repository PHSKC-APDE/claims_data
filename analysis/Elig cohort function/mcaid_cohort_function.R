###############################################################################
# Eli Kern
# 2018-1-31
# APDE
# Function to receive parameters for Medicaid eligbility cohort and return SQL command to
# be used in mcaid_cohort_process R script

###############################################################################

##### Call in libraries #####
library(lubridate) # Used to manipulate dates

#### Define function #####
mcaid_cohort_f <- function(begin = Sys.Date() - months(12), end = Sys.Date() - months(6), covmin = 0,
                           agemin = 0, agemax = 200, female = "null", male = "null", aian = "null", 
                           asian = "null", black = "null", nhpi = "null", white = "null", latino = "null",
                           zip = "null", region = "null") {
  
  #Error checks
  if(begin > end & !missing(begin) & !missing(end)) {
    stop("Begin date must be <= end date")
  }
  
  if(missing(begin) & missing(end)) {
    print("Default begin and end dates used - 12 and 6 months prior to today's date, respectively")
  }
  
  if((missing(begin) & !missing(end)) | (!missing(begin) & missing(end))) {
    stop("If begin date provided, end date must also be provided. And vice versa.")
  }
  
  if(!is.numeric(covmin) | covmin < 0 | covmin > 100){
    stop("Coverage requirement must be numeric between 0 and 100")
  }
  
  if(!is.numeric(agemin) | !is.numeric(agemax)) {
    stop("Age min and max must be provided as numerics")
  }
  
  if(agemin > agemax & !missing(agemin) & !missing(agemax)) {
    stop("Minimum age must be <= maximum age")
  }
  
  
  if(!(aian %in% c("null",0, 1)) | !(asian %in% c("null",0, 1)) | !(black %in% c("null",0, 1)) |
     !(nhpi %in% c("null",0, 1)) | !(white %in% c("null",0, 1)) | !(latino %in% c("null",0, 1)) |
     !(female %in% c("null",0, 1)) | !(male %in% c("null",0, 1))) {
    stop("Race and sex parameters must be left missing or set to 'null', 0 or 1")
  }
  
  if(!is.character(zip) | !is.character(region)) {
    stop("Geographic parameters must be input as comma-separated characters with no spaces between items")
  }
  
  #Run parameters message
  cat(paste(
        "You have selected a Medicaid member cohort with the following characteristics:\n",
        "Coverage start date: ", begin, "(inclusive)\n",
        "Coverage end date: ", end, " (inclusive)\n",
        "Coverage requirement: ", covmin, " percent or more of requested date range\n",
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
        sep = ""))
  
  #Derived variables
  
  duration <- as.numeric(as.Date(end) - as.Date(begin)) + 1
  
  #Build SQL query
  exec <- "exec PH_APDEStore.[PH\\KERNELI].sp_mcaidcohort"
  
  begin_t <- paste("@begin = \'", begin, "\',", sep = "")
  end_t <- paste("@end = \'", end, "\',", sep = "")
  duration_t <- paste("@duration = ", duration, ",", sep = "")
  covmin_t <- paste("@covmin = ", covmin, ",", sep = "")
  
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
  region_t <- paste("@region = ", region, sep = "")
  
  paste(exec, begin_t, end_t, duration_t, covmin_t, agemin_t, agemax_t, female_t, male_t, 
        aian_t, asian_t, black_t, nhpi_t, white_t, latino_t, zip_t, region_t, sep = " ")
}





##Testing function

#mcaid_cohort_f()
#mcaid_cohort_f(begin = "2017-01-01", end = "2017-06-30", agemin = 18, agemax = 64, male = 1, black = 1, covmin = 50)
# 
#mcaid_cohort_f(begin = "2017-01-01", end = "2017-06-30", male = 1)
# mcaid_cohort_f(begin = "2017-01-01", end = "2017-06-30", agemin = 18, agemax = 64)
# 
# mcaid_cohort_f(begin = "2017-01-01", end = "2017-06-30")
# mcaid_cohort_f()
# mcaid_cohort_f(begin = "2017-01-02", end = "2017-01-01")
# mcaid_cohort_f(begin = "2017-01-01")
# mcaid_cohort_f(end = "2017-01-01")
