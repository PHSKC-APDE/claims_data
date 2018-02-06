###############################################################################
# Eli Kern
# 2018-2-5

# Code to create a SQL table dbo.mcaid_elig_demoever which holds SSN, DOB, gender, race, and language
# One row per ID, one SSN and one DOB per ID (frequency-based selection)
# Gender, race, and language are alone or in combination EVER variables
# Data elements: ID, BLANK

###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(car) # used to recode variables
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(RecordLinkage) # used to clean up duplicates in the data
library(phonics) # used to extract phonetic version of names

##### Set date origin #####
origin <- "1970-01-01"

##### Define global useful functions #####

#Recode variables using CAR package
recode2 <- function ( data, fields, recodes, as.factor.result = FALSE ) {
  for ( i in which(names(data) %in% fields) ) { # iterate over column indexes that are present in the passed dataframe that are also included in the fields list
    data[,i] <- car::recode( data[,i], recodes, as.factor.result = as.factor.result )
  }
  data
}

##### Connect to the SQL server #####
db.claims51 <- odbcConnect("PHClaims51")

#################################################################
##### Bring in Medicaid eligibility data for DOB processing #####
#Note to bring in test subset of Medicaid data, insert "top 100000" between SELECT and z.MEDICAID_RECIPIENT_ID
#################################################################

ptm01 <- proc.time() # Times how long this query takes
elig_dob <- sqlQuery(
  db.claims51,
  " select distinct y.MEDICAID_RECIPIENT_ID as id, y.SOCIAL_SECURITY_NMBR as ssn, y.BIRTH_DATE as dob, count(*) as row_cnt
	FROM (
  SELECT z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.BIRTH_DATE
  FROM [PHClaims].[dbo].[NewEligibility] as z
) as y
  group by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.BIRTH_DATE
  order by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, row_cnt desc, y.BIRTH_DATE",
  stringsAsFactors = FALSE
  )
proc.time() - ptm01

#Code to find duplicated Medicaid IDs
elig_dob <- elig_dob %>%
  group_by(id) %>%
  mutate(
    id_cnt = n()
  ) %>%
  ungroup()

#Code to find different DOBs by ID-SSN sets
elig_dob <- elig_dob %>%
  group_by(id, ssn) %>%
  mutate(
    dob_cnt = n()
  ) %>%
  ungroup()

#### SSN and DOB cleanup ####
# Dealing with multiple SSNs
ssn.tmp <- elig_dob %>%
  filter(!is.na(ssn)) %>%
  select(id, ssn, row_cnt) %>%
  arrange(id, row_cnt) %>%
  distinct(id, ssn, .keep_all = TRUE) %>%
  group_by(id) %>%
  # where there is a tie, the first SSN is selected, which is an issue if the data are sorted differently
  # currently takes the most frequently used SSN
  slice(which.max(row_cnt)) %>%
  ungroup() %>%
  select(id, ssn)

# Merge back with the primary data and update SSN
elig_dob <- left_join(elig_dob, ssn.tmp, by = c("id"))
rm(ssn.tmp) # remove temp data frames to save memory

# Make new variable with cleaned up SSN
elig_dob <- mutate(elig_dob, ssnnew = ifelse(!is.na(ssn.y), ssn.y, ssn.x))
                   
#Filter to distinct
elig_dob <- distinct(elig_dob, id, ssnnew, dob, row_cnt)

# Dealing with multiple DOBs
dob.tmp <- elig_dob %>%
  filter(!is.na(dob)) %>%
  select(id, dob, row_cnt) %>%
  arrange(id, row_cnt) %>%
  distinct(id, dob, .keep_all = TRUE) %>%
  group_by(id) %>%
  # where there is a tie, the first DOB is selected, which is an issue if the data are sorted differently
  # currently takes the most frequently used DOB
  slice(which.max(row_cnt)) %>%
  ungroup() %>%
  select(id, dob)

# Merge back with the primary data and update SSN
elig_dob <- left_join(elig_dob, dob.tmp, by = c("id"))
rm(dob.tmp) # remove temp data frames to save memory

# Make new variable with cleaned up DOB
elig_dob <- mutate(elig_dob, dobnew = ymd(as.Date(ifelse(!is.na(dob.y), dob.y, dob.x))))

#Filter to distinct
elig_dob <- distinct(elig_dob, id, ssnnew, dobnew)

#################################################################
##### Bring in Medicaid eligibility data for gender, race and language processing #####
#Note to bring in test subset of Medicaid data, insert "top 100000" between SELECT and z.MEDICAID_RECIPIENT_ID
#################################################################

##### Bring in Medicaid eligibility data #####
ptm01 <- proc.time() # Times how long this query takes - 172 sec
elig_demoever <- sqlQuery(
  db.claims51,
  " select distinct y.CLNDR_YEAR_MNTH as calmo, y.MEDICAID_RECIPIENT_ID as id, y.GENDER as gender, y.RACE1 as race1, y.RACE2 as race2, 
      y.RACE3 as race3, y.RACE4 as race4, y.HISPANIC_ORIGIN_NAME as hispanic, y.SPOKEN_LNG_NAME as 'slang', y.WRTN_LNG_NAME as 'wlang'
    from (
    select z.CLNDR_YEAR_MNTH, z.MEDICAID_RECIPIENT_ID, z.GENDER, z.RACE1, z.RACE2, z.RACE3, z.RACE4, z.HISPANIC_ORIGIN_NAME,
      z.SPOKEN_LNG_NAME, z.WRTN_LNG_NAME
    from [PHClaims].[dbo].[NewEligibility] as z
    ) as y",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

##### Convert calendar month to calendar start and end dates for interval overlap comparison #####
elig_demoever <- elig_demoever %>%
  mutate(
    calstart = ymd(paste(as.character(calmo), "01", sep = "")),
    calend = ymd(paste(as.character(calmo), days_in_month(ymd(paste(as.character(calmo), "01", sep = ""))), sep = ""))
  )

##### Set strings to UPPERCASE #####
elig_demoever <- elig_demoever %>%
  mutate_at(
    vars(gender:wlang),
    toupper
  )


#### Set NOT PROVIDED and OTHER race to null ####
#### Set Other Language, Undetermined, to null ####
nullrace_txt <- c("NOT PROVIDED", "OTHER")
nulllang_txt <- c("UNDETERMINED", "OTHER LANGUAGE")

elig_demoever <- elig_demoever %>%
  mutate_at(
    vars(race1:hispanic),
    str_replace, pattern = paste(nullrace_txt, collapse = '|'), replacement = NA_character_
  ) %>%
  mutate_at(
    vars(slang:wlang),
    str_replace, pattern = paste(nulllang_txt, collapse = '|'), replacement = NA_character_
  )

#############################
#### Process gender data ####
#############################

elig_gender <- select(elig_demoever, id, gender, calstart, calend)

#### Create alone or in combination gender variables ####
elig_gender <- elig_gender %>%
  mutate(
    female = ifelse(str_detect(gender, "FEMALE"), 1, 0),
    male = ifelse(str_detect(gender, "^MALE$"), 1, 0)
  )


##### For each gender variable, count number of rows where variable = 1. ##### 
##### Divide this number by total number of rows (eg months) where gender is non-missing. ##### 
##### Create _t variables for each gender variable to hold this percentage. ##### 


#Create a variable to flag if all race vars are NA where Not Hispanic is considered NA as well
elig_gender <- elig_gender %>%
  mutate(
    genderna = is.na(gender)
  )

#Create gender person time vars
elig_gender <- elig_gender %>%
  group_by(id) %>%
  mutate(
    female_t = round((length(female[female == 1 & !is.na(female)]) / length(genderna[genderna == FALSE]) * 100), 1),
    male_t = round((length(male[male == 1 & !is.na(male)]) / length(genderna[genderna == FALSE]) * 100), 1)
  ) %>%
  ungroup()

#Replace NA person time variables with 0
elig_gender <- elig_gender %>%
  mutate_at(
    vars(female_t, male_t),
    recode, .missing = 0
  )

#### Copy all non-missing gender variable values to all rows within each ID. ####
elig_gender <- elig_gender %>%
  group_by(id) %>%
  mutate_at(
    vars(female, male),
    funs(max(., na.rm = TRUE))
  ) %>%
  ungroup()

#Replace infinity values with NA (these were generated by max function applied to NA rows)
elig_gender <- elig_gender %>%
  mutate_at(
    vars(female, male),
    function(x) replace(x, is.infinite(x),NA)
  )

##### Collapse to one row per ID given we have alone or in combo EVER gender variables #####
elig_gender_final <- distinct(elig_gender, id, female, male, female_t, male_t)
rm(elig_gender)


#############################
#### Process race data ####
#############################

elig_race <- select(elig_demoever, id, race1:hispanic, calend, calstart)

#### Create alone or in combination race variables ####

aian_txt <- c("ALASKAN NATIVE", "AMERICAN INDIAN")
black_txt <- c("BLACK")
asian_txt <- c("ASIAN")
nhpi_txt <- c("HAWAIIAN", "PACIFIC ISLANDER")
white_txt <- c("WHITE")
latino_txt <- c("^HISPANIC$")

elig_race$aian <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, paste(aian_txt, collapse = '|'))), na.rm = TRUE)
elig_race$asian <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, asian_txt)), na.rm = TRUE)
elig_race$black <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, black_txt)), na.rm = TRUE)
elig_race$nhpi <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, paste(nhpi_txt, collapse = '|'))), na.rm = TRUE)
elig_race$white <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, white_txt)), na.rm = TRUE)
elig_race$latino <- rowSums(sapply(elig_race[c("hispanic")], function(x) str_detect(x, latino_txt)), na.rm = TRUE)

#As the same race can sometimes be listed more than once across the race variables, replace all sums > 1 with 1
elig_race <- elig_race %>%
  mutate_at(
    vars(aian:latino),
    funs(ifelse(.>1, 1, .))
  )

##Replace race vars with NA if race1 is NA, latino with NA if hispanic is NA

#Function to replace 1 variable with NA if a 2nd variable is NA
na_check <- function(x, y) {
  ifelse(is.na(x), NA, y)
}
#sapply(elig_race[c("aian")], function(x) na_check(elig_race$race1, x))


elig_race <- elig_race %>%
  mutate_at(
    vars(aian, asian, black, nhpi, white),
    funs(na_check(elig_race$race1, .))
  ) %>%
  mutate_at(
    vars(latino),
    funs(na_check(elig_race$hispanic, .))
  )

##### For each race variable, count number of rows where variable = 1. ##### 
##### Divide this number by total number of rows (eg months) where at least one race variable is non-missing. ##### 
##### Create _t variables for each race variable to hold this percentage. ##### 

#Create a variable to flag if all race vars are NA where Not Hispanic is considered NA as well
elig_race <- elig_race %>%
  mutate(
    racena = is.na(race1) & (is.na(hispanic) | hispanic == "NOT HISPANIC")
  )

#Create race person time vars
elig_race <- elig_race %>%
  group_by(id) %>%
  mutate(
    #total_n = length(racena[racena == FALSE]),
    #aian_n = length(aian[aian == 1 & !is.na(aian)]),
    aian_t = round((length(aian[aian == 1 & !is.na(aian)]) / length(racena[racena == FALSE]) * 100), 1),
    asian_t = round((length(asian[asian == 1 & !is.na(asian)]) / length(racena[racena == FALSE]) * 100), 1),
    black_t = round((length(black[black == 1 & !is.na(black)]) / length(racena[racena == FALSE]) * 100), 1),
    nhpi_t = round((length(nhpi[nhpi == 1 & !is.na(nhpi)]) / length(racena[racena == FALSE]) * 100), 1),
    white_t = round((length(white[white == 1 & !is.na(white)]) / length(racena[racena == FALSE]) * 100), 1),
    latino_t = round((length(latino[latino == 1 & !is.na(latino)]) / length(racena[racena == FALSE]) * 100), 1)
  ) %>%
  ungroup()

#Replace NA person time variables with 0
elig_race <- elig_race %>%
  mutate_at(
    vars(aian_t, asian_t, black_t, nhpi_t, white_t, latino_t),
    recode, .missing = 0
  )

#### Copy all non-missing race variable values to all rows within each ID. ####
elig_race <- elig_race %>%
  group_by(id) %>%
  mutate_at(
    vars(aian, asian, black, nhpi, white),
    funs(max(., na.rm = TRUE))
  ) %>%
  mutate_at(
    vars(latino),
    funs(max(., na.rm = TRUE))    
  ) %>%
  ungroup()

#Replace infinity values with NA (these were generated by max function applied to NA rows)
elig_race <- elig_race %>%
  mutate_at(
    vars(aian, asian, black, nhpi, white, latino),
    function(x) replace(x, is.infinite(x),NA)
  )

##### Collapse to one row per ID given we have alone or in combo EVER race variables #####
elig_race_final <- distinct(elig_race, id, aian, asian, black, nhpi, white, latino, aian_t, asian_t, black_t, nhpi_t, white_t, latino_t)
rm(elig_race)

#############################
#### Process language data ####
#############################

elig_lang <- select(elig_demoever, id, slang, wlang, calend, calstart)
#rm(elig_demoever)

#### Create alone or in combination lang variables for King County tier 1 and 2 translation languages with Arabic in place of Punjabi ####

english_txt <- c("^ENGLISH$")
spanish_txt <- c("^SPANISH; CASTILIAN$", "^SPANISH$", "^CASTILIAN$")
vietnamese_txt <- c("VIETNAMESE")
chinese_txt <- c("^CHINESE$", "^HAKKA CHINESE$", "^MANDARIN$", "^CANTONESE$")
somali_txt <- c("^SOMALI$")
russian_txt <- c("^RUSSIAN$")
arabic_txt <- c("^ARABIC$")
korean_txt <- c("^KOREAN$")
ukrainian_txt <- c("^UKRAINIAN$")
amharic_txt <- c("^AMHARIC$")

elig_lang$english <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(english_txt, collapse = '|'))), na.rm = TRUE)
elig_lang$spanish <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(spanish_txt, collapse = '|'))), na.rm = TRUE)
elig_lang$vietnamese <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(vietnamese_txt, collapse = '|'))), na.rm = TRUE)
elig_lang$chinese <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(chinese_txt, collapse = '|'))), na.rm = TRUE)
elig_lang$somali <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(somali_txt, collapse = '|'))), na.rm = TRUE)
elig_lang$russian <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(russian_txt, collapse = '|'))), na.rm = TRUE)
elig_lang$arabic <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(arabic_txt, collapse = '|'))), na.rm = TRUE)
elig_lang$korean <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(korean_txt, collapse = '|'))), na.rm = TRUE)
elig_lang$ukrainian <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(ukrainian_txt, collapse = '|'))), na.rm = TRUE)
elig_lang$amharic <- rowSums(sapply(elig_lang[c("slang", "wlang")], function(x) str_detect(x, paste(amharic_txt, collapse = '|'))), na.rm = TRUE)


#As the same language can sometimes be listed for both spoken and written language, replace all sums > 1 with 1
elig_lang <- elig_lang %>%
  mutate_at(
    vars(english:amharic),
    funs(ifelse(.>1, 1, .))
  )

##Replace lang vars with NA if slang and wlang are both NA

#Function to replace 1 variable with NA if a 2nd variable is NA
na_check_2 <- function(x, y, z) {
  ifelse(is.na(x) & is.na(y), NA, z)
}
#sapply(elig_lang[c("english")], function(x) na_check_2(elig_lang$slang, elig_lang$wlang,  x))

elig_lang <- elig_lang %>%
  mutate_at(
    vars(english:amharic),
    funs(na_check_2(elig_lang$slang, elig_lang$wlang, .))
  )

##### For each language variable, count number of rows where variable = 1. ##### 
##### Divide this number by total number of rows (eg months) where at least one language variable is non-missing. ##### 
##### Create _t variables for each lang variable to hold this percentage. ##### 

#Create a variable to flag if all lang vars are NA
elig_lang <- elig_lang %>%
  mutate(
    langna = is.na(slang) & is.na(wlang)
  )

#Create lang person time vars
elig_lang <- elig_lang %>%
  group_by(id) %>%
  mutate(
    english_t = round((length(english[english == 1 & !is.na(english)]) / length(langna[langna == FALSE]) * 100), 1),
    spanish_t = round((length(spanish[spanish == 1 & !is.na(spanish)]) / length(langna[langna == FALSE]) * 100), 1),
    vietnamese_t = round((length(vietnamese[vietnamese == 1 & !is.na(vietnamese)]) / length(langna[langna == FALSE]) * 100), 1),
    chinese_t = round((length(chinese[chinese == 1 & !is.na(chinese)]) / length(langna[langna == FALSE]) * 100), 1),
    somali_t = round((length(somali[somali == 1 & !is.na(somali)]) / length(langna[langna == FALSE]) * 100), 1),
    russian_t = round((length(russian[russian == 1 & !is.na(russian)]) / length(langna[langna == FALSE]) * 100), 1),
    arabic_t = round((length(arabic[arabic == 1 & !is.na(arabic)]) / length(langna[langna == FALSE]) * 100), 1),
    korean_t = round((length(korean[korean == 1 & !is.na(korean)]) / length(langna[langna == FALSE]) * 100), 1),
    ukrainian_t = round((length(ukrainian[ukrainian == 1 & !is.na(ukrainian)]) / length(langna[langna == FALSE]) * 100), 1),
    amharic_t = round((length(amharic[amharic == 1 & !is.na(amharic)]) / length(langna[langna == FALSE]) * 100), 1)
  ) %>%
  ungroup()

#Replace NA person time variables with 0
elig_lang <- elig_lang %>%
  mutate_at(
    vars(english_t:amharic_t),
    recode, .missing = 0
  )

#### Copy all non-missing language variable values to all rows within each ID. ####
elig_lang <- elig_lang %>%
  group_by(id) %>%
  mutate_at(
    vars(english:amharic),
    funs(max(., na.rm = TRUE))
  ) %>%
  ungroup()

#Replace infinity values with NA (these were generated by max function applied to NA rows)
elig_lang <- elig_lang %>%
  mutate_at(
    vars(english:amharic),
    function(x) replace(x, is.infinite(x),NA)
  )

#### Select most frequently reported language per ID ####
#Count rows associated with each spoken and written language by ID
lang.tmp <- select(elig_lang, id, slang, wlang) %>%
  group_by(id, slang) %>%
  mutate(slang_row_cnt = n()) %>%
  ungroup()

lang.tmp <- lang.tmp %>%
  group_by(id, wlang) %>%
  mutate(wlang_row_cnt = n()) %>%
  ungroup()

#Create two new datasets that include the most frequently reported spoken and written language by ID
slang.tmp <- lang.tmp %>%
  filter(!is.na(slang)) %>%
  select(id, slang, slang_row_cnt) %>%
  arrange(id, slang_row_cnt) %>%
  distinct(id, slang, .keep_all = TRUE) %>%
  group_by(id) %>%
  # where there is a tie, the first SSN is selected, which is an issue if the data are sorted differently
  # currently takes the most frequently used SSN
  slice(which.max(slang_row_cnt)) %>%
  ungroup() %>%
  select(id, slang, slang_row_cnt)

wlang.tmp <- lang.tmp %>%
  filter(!is.na(wlang)) %>%
  select(id, wlang, wlang_row_cnt) %>%
  arrange(id, wlang_row_cnt) %>%
  distinct(id, wlang, .keep_all = TRUE) %>%
  group_by(id) %>%
  # where there is a tie, the first SSN is selected, which is an issue if the data are sorted differently
  # currently takes the most frequently used SSN
  slice(which.max(wlang_row_cnt)) %>%
  ungroup() %>%
  select(id, wlang, wlang_row_cnt)

#Merge spoken and written language taken the most frequent as a single language variable (max language)
swlang.tmp <- full_join(slang.tmp, wlang.tmp, by = c("id")) %>%
  mutate(
    maxlang = ifelse(is.na(slang), wlang,
                     ifelse(is.na(wlang), slang,
                            ifelse(wlang_row_cnt >= slang_row_cnt, wlang, slang)))
  ) %>%
  select(id, maxlang)

# Merge back with the primary data
elig_lang <- left_join(elig_lang, swlang.tmp, by = c("id"))
rm(lang.tmp, slang.tmp, wlang.tmp, swlang.tmp) # remove temp data frames to save memory


##### Collapse to one row per ID given we have alone or in combo EVER race variables #####
elig_lang_final <- distinct(elig_lang, id, maxlang, english, spanish, vietnamese, chinese, somali, russian, arabic, korean, ukrainian, amharic,
                            english_t, spanish_t, vietnamese_t, chinese_t, somali_t, russian_t, arabic_t, korean_t, ukrainian_t, amharic_t)
remove(elig_lang)

#############################
#### Join all tables ####
#############################

elig_demoever_final <- inner_join(inner_join(inner_join(elig_dob, elig_gender_final, by = c("id")), elig_race_final, by = c("id")),
                                  elig_lang_final, by = c("id"))

#Test to make sure no IDs are duplicated
test <- elig_demoever_final %>%
  group_by(id) %>%
  count(id)
max(test$n)
rm(test)

#Drop individual tables
rm(elig_dob, elig_gender_final, elig_race_final, elig_lang_final)

#Test to make sure all IDs in original data table are included in final
#count(distinct(elig_demoever_final, id))
#count(distinct(elig_demoever, id))

##### Save dob.mcaid_elig_demoever to SQL server 51 #####
#This took 41 min to upload, 851,573 rows x 40 variables
#sqlDrop(db.claims51, "dbo.mcaid_elig_ever") # Commented out because not always necessary
ptm02 <- proc.time() # Times how long this query takes
sqlSave(
  db.claims51,
  elig_demoever_final,
  tablename = "dbo.mcaid_elig_demoever",
  rownames = FALSE,
  fast = TRUE,
  varTypes = c(
    id = "Varchar(255)",  
    ssnnew = "Varchar(255)",  
    dobnew = "Date"
  )
)
proc.time() - ptm02















