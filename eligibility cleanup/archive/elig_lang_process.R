###############################################################################
# Eli Kern
# 2018-2-2

# Code to create a SQL table dbo.mcaid_elig_lang which holds Medicaid member language characteristics
# Use to select a language-based cohort for a given date range
# Data elements: ID, alone or in combination top tier language flags, max language field

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

##### Connect to SQL servers #####
db.claims51 <- odbcConnect("PHClaims51")

##### Bring in Medicaid eligibility data  #####
#Note to bring in test subset of Medicaid data, insert "top 100000" between SELECT and z.FIRST COLUMN
ptm01 <- proc.time() # Times how long this query takes - 172 sec
elig_lang <- sqlQuery(
  db.claims51,
  " select distinct y.CLNDR_YEAR_MNTH as 'calmo', y.MEDICAID_RECIPIENT_ID as 'id', y.SPOKEN_LNG_NAME as 'slang', 
      y.WRTN_LNG_NAME as 'wlang'
    FROM (
    select z.CLNDR_YEAR_MNTH, z.MEDICAID_RECIPIENT_ID, z.SPOKEN_LNG_NAME, z.WRTN_LNG_NAME
      FROM [PHClaims].[dbo].[NewEligibility] as z
    ) as y",
  stringsAsFactors = FALSE
  )
proc.time() - ptm01

##### Convert calendar month to calendar start and end dates for interval overlap comparison #####
elig_lang <- elig_lang %>%
  mutate(
    calstart = ymd(paste(as.character(calmo), "01", sep = "")),
    calend = ymd(paste(as.character(calmo), days_in_month(ymd(paste(as.character(calmo), "01", sep = ""))), sep = ""))
  )

##### Set lang to UPPERCASE #####
elig_lang <- elig_lang %>%
  mutate_at(
    vars(slang:wlang),
    toupper
  )

#### Set Other Language, Undetermined, to null ####
nullrace_txt <- c("UNDETERMINED", "OTHER LANGUAGE")

elig_lang <- elig_lang %>%
  mutate_at(
    vars(slang:wlang),
    str_replace, pattern = paste(nullrace_txt, collapse = '|'), replacement = NA_character_
  )

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

#Test to make sure no IDs are duplicated
test <- elig_lang_final %>%
  group_by(id) %>%
  count(id)

#Test to make sure all IDs in original data table are included in final
count(distinct(elig_lang_final, id))
count(distinct(elig_lang, id))

##### Save dob.mcaid_elig_lang to SQL server 51 #####
#This took 38 mins for 851,573 obs with 22 variables
#sqlDrop(db.claims51, "dbo.mcaid_elig_lang") # Commented out because not always necessary
ptm03 <- proc.time() # Times how long this query take
sqlSave(
  db.claims51,
  elig_lang_final,
  tablename = "dbo.mcaid_elig_lang",
  rownames = FALSE,
  fast = TRUE,
  varTypes = c(
    id = "Varchar(255)"
  )
)
proc.time() - ptm03










