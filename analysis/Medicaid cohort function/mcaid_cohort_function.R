###############################################################################
# Eli Kern
# 2018-1-31
# APDE
# Function to generate SQL query to select Medicaid eligibility cohort with specified parameters
###############################################################################

#### Define function #####
mcaid_cohort_f <- function(from_date = Sys.Date() - months(12), to_date = Sys.Date() - months(6), covmin = 0,
                           dualmax = 100, agemin = 0, agemax = 200, female = "null", male = "null", 
                           aian = "null", asian = "null", black = "null", nhpi = "null", white = "null", latino = "null",
                           zip = "null", zregion = "null", english = "null", spanish = "null", vietnamese = "null",
                           chinese = "null", somali = "null", russian = "null", arabic = "null", korean = "null",
                           ukrainian = "null", amharic = "null", maxlang = "null", id = "null") {
  
  #Error checks
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
  
  if(!is.character(zip) | !is.character(zregion) | !is.character(maxlang) | !is.character(id)) {
    stop("Geographic, 'maxlang' and 'id' parameters must be input as comma-separated characters with no spaces between items")
  }
  
  #Run parameters message
  cat(paste(
        "You have selected a Medicaid member cohort with the following characteristics:\n",
        "Coverage begin date: ", from_date, "(inclusive)\n",
        "Coverage end date: ", to_date, " (inclusive)\n",
        "Coverage requirement: ", covmin, " percent or more of requested date range\n",
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
        "ZIP-based regions: ", zregion, "\n",
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
  exec <- "exec PH_APDEStore.dbo.sp_mcaidcohort"
  
  from_date_t <- paste("@from_date = \'", from_date, "\',", sep = "")
  to_date_t <- paste("@to_date = \'", to_date, "\',", sep = "")
  duration_t <- paste("@duration = ", duration, ",", sep = "")
  covmin_t <- paste("@covmin = ", covmin, ",", sep = "")
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
  ifelse(missing(zregion), 
         zregion_t <- paste("@region = ", zregion, ",", sep = ""),
         zregion_t <- paste("@region = \'", zregion, "\',", sep = ""))
  
  
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
  
  paste(exec, from_date_t, to_date_t, duration_t, covmin_t, dualmax_t, agemin_t, agemax_t, female_t, male_t, 
        aian_t, asian_t, black_t, nhpi_t, white_t, latino_t, zip_t, zregion_t, english_t, spanish_t,
        vietnamese_t, chinese_t, somali_t, russian_t, arabic_t, korean_t, ukrainian_t, amharic_t,
        maxlang_t, id_t, sep = " ")
}