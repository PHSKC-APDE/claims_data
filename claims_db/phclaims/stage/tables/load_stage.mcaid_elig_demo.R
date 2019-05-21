###############################################################################
# Eli Kern and Alastair Matheson
# 2018-2-5

# Code to create a SQL table dbo.mcaid_elig_demoever which holds SSN, DOB, gender, race, and language
# One row per ID, one SSN and one DOB per ID (frequency-based selection)
# Gender, race, and language are alone or in combination EVER variables
# Data elements: ID, BLANK

## 2018-05-22 updates:
# Add in multiple gender and multiple race variables
# Add in unknown gender, race, and language variables

## 2018-07-17 updates:
# Converted most code to use data.table package due to large size of data
# Removed vestigal code and other tidying

## 2019-05-10 updates:
# Using new standarized varnames
# No longer capturing SSN (will be in alias table instead)


###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, warning.length = 8170)
origin <- "1970-01-01"

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(tidyverse) # Used to manipulate data
library(lubridate) # Used to manipulate dates
library(data.table) # Useful for large data sets

db_claims <- dbConnect(odbc(), "PHClaims51")


#################################################################
##### Bring in Medicaid eligibility data for DOB processing #####
#Note to bring in test subset of Medicaid data, insert "top 100000" between SELECT and z.MEDICAID_RECIPIENT_ID
#################################################################

elig_dob <- dbGetQuery(
  db_claims,
  # select most frequently reported SSN and DOB per Medicaid ID
  "SELECT id.id_mcaid, dob.dob
  
  FROM (
    SELECT DISTINCT MEDICAID_RECIPIENT_ID as 'id_mcaid'
    FROM stage.mcaid_elig
  ) id
  
  LEFT JOIN (
    SELECT b.id_mcaid, cast(b.dob as date) as 'dob'
    FROM (
      SELECT a.id_mcaid, a.dob, row_number() OVER 
        (PARTITION BY a.id_mcaid order by a.id_mcaid, a.dob_cnt desc, a.dob) AS 'dob_rank'
      FROM (
        SELECT MEDICAID_RECIPIENT_ID as 'id_mcaid', BIRTH_DATE as 'dob', count(BIRTH_DATE) as 'dob_cnt'
        FROM stage.mcaid_elig
        WHERE BIRTH_DATE is not null
        GROUP BY MEDICAID_RECIPIENT_ID, BIRTH_DATE
      ) a
    ) b
    WHERE b.dob_rank = 1
  ) dob
  
  ON id.id_mcaid = dob.id_mcaid"
)



#################################################################
##### Bring in Medicaid eligibility data for gender, race and language processing #####
#Note to bring in test subset of Medicaid data, insert "top 100000" between SELECT and z.MEDICAID_RECIPIENT_ID
#################################################################

### Bring in Medicaid eligibility data
system.time( # Times how long this query takes (~320s)
  elig_demoever <- dbGetQuery(
  db_claims,
  "SELECT DISTINCT CLNDR_YEAR_MNTH as calmo, MEDICAID_RECIPIENT_ID as id_mcaid, 
      GENDER as gender, RACE1_NAME as race1, RACE2_NAME as race2, 
      RACE3_NAME as race3, RACE4_NAME as race4, HISPANIC_ORIGIN_NAME as hispanic, 
      SPOKEN_LNG_NAME as 'slang', WRTN_LNG_NAME as 'wlang'
    FROM [PHClaims].[stage].[mcaid_elig]")
)

# Convert to data table
elig_demoever <- setDT(elig_demoever)


### Set strings to UPPERCASE
cols <- c("gender", "race1", "race2", "race3", "race4", "hispanic", "slang", "wlang")
elig_demoever[, (cols) := lapply(.SD, toupper), .SDcols = cols]


### Set NOT PROVIDED and OTHER race to null
### Set Other Language, Undetermined, to null
nullrace_txt <- c("NOT PROVIDED", "OTHER")
nulllang_txt <- c("UNDETERMINED", "OTHER LANGUAGE")

cols <- c("race1", "race2", "race3", "race4", "hispanic")
elig_demoever[, (cols) := 
                   lapply(.SD, function(x)
                          str_replace(x, 
                                      pattern = paste(nullrace_txt, collapse = '|'), 
                                      replacement = NA_character_)), 
                 .SDcols = cols]

cols <- c("slang", "wlang")
elig_demoever[, (cols) := 
                   lapply(.SD, function(x)
                     str_replace(x, 
                                 pattern = paste(nulllang_txt, collapse = '|'), 
                                 replacement = NA_character_)), 
                 .SDcols = cols]



#############################
#### Process gender data ####
#############################

elig_gender <- elig_demoever[, c("id_mcaid", "calmo", "gender")]

### Create alone or in combination gender variables
elig_gender[, ':=' (female = ifelse(str_detect(gender, "FEMALE"), 1, 0),
                    male = ifelse(str_detect(gender, "^MALE$"), 1, 0))]


### For each gender variable, count number of rows where variable = 1.
### Divide this number by total number of rows (months) where gender is non-missing.
### Create _t variables for each gender variable to hold this percentage.

# Create a variable to flag if gender var is missing
elig_gender[, genderna := is.na(gender), ]

# Create gender person time vars
elig_gender[, ':=' (female_t = round(length(female[female == 1 & !is.na(female)]) / 
                                 length(genderna[genderna == FALSE]) * 100, 1),
                    male_t = round(length(male[male == 1 & !is.na(male)]) / 
                                 length(genderna[genderna == FALSE]) * 100, 1))
                 , by = "id_mcaid"]


# Replace NA person time variables with 0
elig_gender[, c("female_t", "male_t") := 
              list(recode(female_t, .missing = 0),
                   recode(male_t, .missing = 0))
            , ]


### Copy all non-missing gender variable values to all rows within each ID
# First make collapsed max of genders for each ID
elig_gender_sum <- elig_gender[, .(female = max(female, na.rm = T), 
                                          male = max(male, na.rm = T)),
                                      by = "id_mcaid"]
#Replace infinity values with NA (generated by max function applied to NA rows)
cols <- c("female", "male")
elig_gender_sum[, (cols) := 
                   lapply(.SD, function(x)
                     replace(x, is.infinite(x), NA)), 
                 .SDcols = cols]


# Now join back to main data and overwrite existing female/male vars
elig_gender[elig_gender_sum, c("female", "male") := list(i.female, i.male), 
               on = "id_mcaid"]
rm(elig_gender_sum)


### Find the most recent gender variable
elig_gender_recent <- elig_gender[elig_gender[order(id_mcaid, -calmo), .I[1], by = "id_mcaid"]$V1]
elig_gender_recent[, gender_recent := case_when(female == 1 & male == 1 ~ "Multiple",
                                                female == 1 ~ "Female",
                                                male == 1 ~ "Male",
                                                TRUE ~ "Unknown")]
elig_gender_recent[, c("calmo", "gender", "female", "male",
                       "genderna", "female_t", "male_t") := NULL]

# Join gender_recent back to the main data
elig_gender[elig_gender_recent, gender_recent := i.gender_recent, on = "id_mcaid"]

rm(elig_gender_recent)

### Collapse to one row per ID given we have alone or in combo EVER gender variables
# First remove unwanted variables
elig_gender[, c("calmo", "gender", "genderna") := NULL]
elig_gender_final <- unique(elig_gender)

#Add in variables for multiple gender (mutually exclusive categories) and missing gender
elig_gender_final[, gender_me := case_when(female_t > 0 & male_t > 0 ~ "Multiple",
                                           female == 1 ~ "Female",
                                           male == 1 ~ "Male",
                                           TRUE ~ "Unknown")]
setcolorder(elig_gender_final, c("id_mcaid", "gender_me", "gender_recent", 
                              "female", "male", "female_t", "male_t"))


#Drop temp table
rm(elig_gender)
gc()


#############################
#### Process race data ####
#############################

elig_race <- elig_demoever[, c("calmo", "id_mcaid", "race1", "race2", "race3", 
                               "race4", "hispanic")]


### Create alone or in combination race variables
aian_txt <- c("ALASKAN NATIVE", "AMERICAN INDIAN")
black_txt <- c("BLACK")
asian_txt <- c("ASIAN")
nhpi_txt <- c("HAWAIIAN", "PACIFIC ISLANDER")
white_txt <- c("WHITE")
latino_txt <- c("^HISPANIC$")

cols <- c("race1", "race2", "race3", "race4")
elig_race[, race_aian := rowSums(sapply(.SD, function(x)
  str_detect(x, paste(aian_txt, collapse = '|'))), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, race_asian := rowSums(sapply(.SD, function(x) str_detect(x, asian_txt)), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, race_black := rowSums(sapply(.SD, function(x) str_detect(x, black_txt)), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, race_nhpi := rowSums(sapply(.SD, function(x)
  str_detect(x, paste(nhpi_txt, collapse = '|'))), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, race_white := rowSums(sapply(.SD, function(x) str_detect(x, white_txt)), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, race_latino := str_detect(hispanic, latino_txt) * 1]


# Same race can be listed more than once across race variables, replace sums > 1 with 1
cols <- c("race_aian", "race_asian", "race_black", 
          "race_nhpi", "race_white", "race_latino")
elig_race[, (cols) := 
            lapply(.SD, function(x) if_else(x > 1, 1, x)), 
          .SDcols = cols]

# Replace race vars with NA if all race vars are NA, (latino already NA if hispanic is NA)
cols <- c("race_aian", "race_asian", "race_black", "race_nhpi", "race_white")
elig_race[, (cols) := 
            lapply(.SD, function(x) 
              if_else(is.na(race1) & is.na(race2) & is.na(race3) &
                        is.na(race4), NA_real_, x)), 
          .SDcols = cols]


### For each race variable, count number of rows where variable = 1.
# Divide this number by total number of rows (months) where at least one race variable is non-missing.
# Create _t variables for each race variable to hold this percentage.

# Create a variable to flag if all race vars are NA and Latino also 0 or NA
# Can just check aian since this is only NA if all race fields are NA
elig_race[, race_na := is.na(race_aian) & (is.na(race_latino) | race_latino == 0), ]

# Create another var to count number of NA rows per ID
# (saves having to calculate it each time below)
elig_race[, race_na_len := length(race_na[race_na == FALSE]),
          by = "id_mcaid"]

# Create race person time vars
elig_race[, ':=' (
                race_aian_t = round(length(race_aian[race_aian == 1 & !is.na(race_aian)]) / 
                         race_na_len * 100, 1),
                race_asian_t = round(length(race_asian[race_asian == 1 & !is.na(race_asian)]) / 
                         race_na_len * 100, 1),
                race_black_t = round(length(race_black[race_black == 1 & !is.na(race_black)]) / 
                         race_na_len * 100, 1),
                race_nhpi_t = round(length(race_nhpi[race_nhpi == 1 & !is.na(race_nhpi)]) / 
                         race_na_len * 100, 1),
                race_white_t = round(length(race_white[race_white == 1 & !is.na(race_white)]) / 
                         race_na_len * 100, 1),
                race_latino_t = round(length(race_latino[race_latino == 1 & !is.na(race_latino)]) / 
                         race_na_len * 100, 1)
              )
            , by = "id_mcaid"]


# Replace NA person time variables with 0
cols <- c("race_aian_t", "race_asian_t", "race_black_t", 
          "race_nhpi_t", "race_white_t", "race_latino_t")
elig_race[, (cols) := 
            lapply(.SD, function(x) recode(x, .missing = 0)), 
          .SDcols = cols]


### Copy all non-missing race variable values to all rows within each ID.
# First make collapsed max of race for each ID
elig_race_sum <- elig_race[, .(race_aian = max(race_aian, na.rm = T),
                               race_asian = max(race_asian, na.rm = T),
                               race_black = max(race_black, na.rm = T),
                               race_nhpi = max(race_nhpi, na.rm = T),
                               race_white = max(race_white, na.rm = T),
                               race_latino = max(race_latino, na.rm = T)),
                               by = "id_mcaid"]


#Replace infinity values with NA (generated by max function applied to NA rows)
cols <- c("race_aian", "race_asian", "race_black", 
          "race_nhpi", "race_white", "race_latino")
elig_race_sum[, (cols) := 
                  lapply(.SD, function(x)
                    replace(x, is.infinite(x), NA)), 
                .SDcols = cols]
# Now join back to main data
elig_race[elig_race_sum, c("race_aian", "race_asian", "race_black", 
                           "race_nhpi", "race_white", "race_latino") := 
            list(i.race_aian, i.race_asian, i.race_black, 
                 i.race_nhpi, i.race_white, i.race_latino), 
            on = "id_mcaid"]
rm(elig_race_sum)
gc()


### Find most recent race
elig_race_recent <- elig_race[elig_race[order(id_mcaid, -calmo), .I[1], by = "id_mcaid"]$V1]

elig_race_recent[, ':=' 
                 # Multiple race, Latino excluded
                 (race_recent = case_when(race_aian + race_asian + race_black + 
                                           race_nhpi + race_white > 1  ~ "Multiple",
                                         race_aian == 1 ~ "AI/AN",
                                         race_asian == 1 ~ "Asian",
                                         race_black == 1 ~ "Black",
                                         race_nhpi == 1 ~ "NH/PI",
                                         race_white == 1 ~ "White",
                                         TRUE ~ "Unknown"),
                 # Multiple race, Latino included as race
                 # Note OR condition to account for NA values in latino that may make race + latino sum to NA
                 race_eth_recent = case_when((race_aian + race_asian + race_black + 
                                                 race_nhpi + race_white + race_latino > 1) | 
                                            ((race_aian + race_asian + race_black + 
                                                race_nhpi + race_white) > 1)  ~ "Multiple",
                                          race_aian == 1 ~ "AI/AN",
                                          race_asian == 1 ~ "Asian",
                                          race_black == 1 ~ "Black",
                                          race_nhpi == 1 ~ "NH/PI",
                                          race_white == 1 ~ "White",
                                          race_latino == 1 ~ "Latino",
                                          TRUE ~ "Unknown"))]
elig_race_recent <- elig_race_recent[, c("id_mcaid", "race_recent", "race_eth_recent")]

# Join race_recent and race_eth_recent back to the main data
elig_race[elig_race_recent, ':=' (race_recent = i.race_recent,
                                  race_eth_recent = i.race_eth_recent), 
          on = "id_mcaid"]

rm(elig_race_recent)


### Collapse to one row per ID given we have alone or in combo EVER race variables
# First remove unwanted variables
elig_race[, c("calmo", "race1", "race2", "race3", "race4", "hispanic", 
              "race_na", "race_na_len") := NULL]
elig_race_final <- unique(elig_race)

# Add in variables for multiple race (mutually exclusive categories) and missing race
elig_race_final[, ':=' (
  # Multiple race, Latino excluded
  race_me = case_when(race_aian + race_asian + race_black + 
                             race_nhpi + race_white > 1  ~ "Multiple",
                           race_aian == 1 ~ "AI/AN",
                           race_asian == 1 ~ "Asian",
                           race_black == 1 ~ "Black",
                           race_nhpi == 1 ~ "NH/PI",
                           race_white == 1 ~ "White",
                           TRUE ~ "Unknown"),
   # Multiple race, Latino included as race
   # Note OR condition to account for NA values in latino that may make race + latino sum to NA
   race_eth_me = case_when((race_aian + race_asian + race_black + 
                                  race_nhpi + race_white + race_latino > 1) | 
                                 ((race_aian + race_asian + race_black + 
                                     race_nhpi + race_white) > 1)  ~ "Multiple",
                               race_aian == 1 ~ "AI/AN",
                               race_asian == 1 ~ "Asian",
                               race_black == 1 ~ "Black",
                               race_nhpi == 1 ~ "NH/PI",
                               race_white == 1 ~ "White",
                               race_latino == 1 ~ "Latino",
                               TRUE ~ "Unknown"))]

setcolorder(elig_race_final, c("id_mcaid", "race_me", "race_eth_me",
                               "race_recent", "race_eth_recent",
                               "race_aian", "race_asian", "race_black",
                               "race_nhpi", "race_white", "race_latino",
                               "race_aian_t", "race_asian_t", "race_black_t",
                               "race_nhpi_t", "race_white_t", "race_latino_t"))

#Drop temp table
rm(elig_race)
gc()


###############################
#### Process language data ####
###############################

elig_lang <- elig_demoever[, c("calmo", "id_mcaid", "slang", "wlang")]


### Create alone or in combination lang variables for King County tier 1 and 2 
# translation languages with Arabic in place of Punjabi
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


cols <- c("slang", "wlang")

elig_lang[, lang_english := rowSums(sapply(.SD, function(x) str_detect(x, english_txt)),
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, lang_spanish := rowSums(sapply(.SD, function(x) str_detect(x, paste(spanish_txt, collapse = '|'))), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, lang_vietnamese := rowSums(sapply(.SD, function(x) str_detect(x, vietnamese_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, lang_chinese := rowSums(sapply(.SD, function(x) str_detect(x, paste(chinese_txt, collapse = '|'))),
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, lang_somali := rowSums(sapply(.SD, function(x) str_detect(x, somali_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, lang_russian := rowSums(sapply(.SD, function(x) str_detect(x, russian_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, lang_arabic := rowSums(sapply(.SD, function(x) str_detect(x, arabic_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, lang_korean := rowSums(sapply(.SD, function(x) str_detect(x, korean_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, lang_ukrainian := rowSums(sapply(.SD, function(x) str_detect(x, ukrainian_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, lang_amharic := rowSums(sapply(.SD, function(x) str_detect(x, amharic_txt)), 
                               na.rm = TRUE), .SDcols = cols]


# Same langs can be listed more than once across written/spoken, replace sums > 1 with 1
cols <- c("lang_english", "lang_spanish", "lang_vietnamese", "lang_chinese", 
          "lang_somali", "lang_russian", "lang_arabic", "lang_korean", 
          "lang_ukrainian", "lang_amharic")
elig_lang[, (cols) := 
            lapply(.SD, function(x) if_else(x > 1, 1, x)), 
          .SDcols = cols]


##Replace lang vars with NA if slang and wlang are both NA
elig_lang[, (cols) := 
            lapply(.SD, function(x) 
              if_else(is.na(slang) & is.na(wlang), NA_real_, x)), 
          .SDcols = cols]


### For each language variable, count number of rows where variable = 1.
# Divide this number by total number of rows (months) where at least one language variable is non-missing.
# Create _t variables for each lang variable to hold this percentage.

#Create a variable to flag if all lang vars are NA
elig_lang[, lang_na := is.na(slang) & is.na(wlang), ]

# Create another var to count number of NA rows per ID
# (saves having to calculate it each time below)
elig_lang[, lang_na_len := length(lang_na[lang_na == FALSE]),
          by = "id_mcaid"]

#Create lang person time vars
elig_lang[, ':=' (
  lang_english_t = round((length(lang_english[lang_english == 1 & !is.na(lang_english)]) / 
                            lang_na_len * 100), 1),
  lang_spanish_t = round((length(lang_spanish[lang_spanish == 1 & !is.na(lang_spanish)]) / 
                            lang_na_len * 100), 1),
  lang_vietnamese_t = round((length(lang_vietnamese[lang_vietnamese == 1 & !is.na(lang_vietnamese)]) / 
                               lang_na_len * 100), 1),
  lang_chinese_t = round((length(lang_chinese[lang_chinese == 1 & !is.na(lang_chinese)]) / 
                            lang_na_len * 100), 1),
  lang_somali_t = round((length(lang_somali[lang_somali == 1 & !is.na(lang_somali)]) / 
                           lang_na_len * 100), 1),
  lang_russian_t = round((length(lang_russian[lang_russian == 1 & !is.na(lang_russian)]) / 
                            lang_na_len * 100), 1),
  lang_arabic_t = round((length(lang_arabic[lang_arabic == 1 & !is.na(lang_arabic)]) / 
                           lang_na_len * 100), 1),
  lang_korean_t = round((length(lang_korean[lang_korean == 1 & !is.na(lang_korean)]) / 
                           lang_na_len * 100), 1),
  lang_ukrainian_t = round((length(lang_ukrainian[lang_ukrainian == 1 & !is.na(lang_ukrainian)]) / 
                              lang_na_len * 100), 1),
  lang_amharic_t = round((length(lang_amharic[lang_amharic == 1 & !is.na(lang_amharic)]) / 
                            lang_na_len * 100), 1)
  ), by = "id_mcaid"]


# Replace NA person time variables with 0
cols <- c("lang_english_t", "lang_spanish_t", "lang_vietnamese_t", "lang_chinese_t", "lang_somali_t", 
          "lang_russian_t", "lang_arabic_t", "lang_korean_t", "lang_ukrainian_t", "lang_amharic_t")
elig_lang[, (cols) := 
            lapply(.SD, function(x) recode(x, .missing = 0)), 
          .SDcols = cols]


### Copy all non-missing language variable values to all rows within each ID
# First make collapsed max of lang for each ID
elig_lang_sum <- elig_lang[, .(lang_english = max(lang_english, na.rm = T),
                               lang_spanish = max(lang_spanish, na.rm = T),
                               lang_vietnamese = max(lang_vietnamese, na.rm = T),
                               lang_chinese = max(lang_chinese, na.rm = T),
                               lang_somali = max(lang_somali, na.rm = T),
                               lang_russian = max(lang_russian, na.rm = T),
                               lang_arabic = max(lang_arabic, na.rm = T),
                               lang_korean = max(lang_korean, na.rm = T),
                               lang_ukrainian = max(lang_ukrainian, na.rm = T),
                               lang_amharic = max(lang_amharic, na.rm = T)),
                           by = "id_mcaid"]
#Replace infinity values with NA (generated by max function applied to NA rows)
cols <- c("lang_english", "lang_spanish", "lang_vietnamese", "lang_chinese", 
          "lang_somali", "lang_russian", "lang_arabic", "lang_korean", 
          "lang_ukrainian", "lang_amharic")
elig_lang_sum[, (cols) := 
                lapply(.SD, function(x)
                  replace(x, is.infinite(x), NA)), 
              .SDcols = cols]
# Now join back to main data
elig_lang[elig_lang_sum, c("lang_english", "lang_spanish", "lang_vietnamese", "lang_chinese", 
                           "lang_somali", "lang_russian", "lang_arabic", "lang_korean", 
                           "lang_ukrainian", "lang_amharic") := 
            list(i.lang_english, i.lang_spanish, i.lang_vietnamese, i.lang_chinese, i.lang_somali, 
                 i.lang_russian, i.lang_arabic, i.lang_korean, i.lang_ukrainian, i.lang_amharic), 
          on = "id_mcaid"]
rm(elig_lang_sum)
gc()


### Select most frequently reported language per ID
# Count spoken language rows by ID and language
slang_tmp <- elig_lang[!is.na(slang), .(row_cnt_s = .N),
                       by = c("id_mcaid", "slang")]
slang_tmp[, lang_max := slang]
slang_tmp <- slang_tmp[, c("id_mcaid", "lang_max", "row_cnt_s")]
slang_tmp <- unique(slang_tmp)


#Count written language rows by ID and language
wlang_tmp <- elig_lang[!is.na(wlang), .(row_cnt_w = .N),
                       by = c("id_mcaid", "wlang")]
wlang_tmp[, lang_max := wlang]
wlang_tmp <- wlang_tmp[, c("id_mcaid", "lang_max", "row_cnt_w")]
wlang_tmp <- unique(wlang_tmp)


#Join written and spoken language counts and sum by ID and language
#Assign random number to each ID and language, and sort by ID and random number (this helps with selecting lang_max when tied)
swlang_tmp <- merge(slang_tmp, wlang_tmp, by = c("id_mcaid", "lang_max"), all = T)
set.seed(98104)
swlang_tmp[, c("lang_cnt", "rand") :=
             list(sum(row_cnt_s, row_cnt_w, na.rm = TRUE),
                  runif(1, 0, 1)),
           by = c("id_mcaid", "lang_max")]

setorder(swlang_tmp, id_mcaid, -lang_cnt, rand)

# Slice data to one language per ID (most frequently reported)
swlang_tmp <- swlang_tmp[, head(.SD, 1), by = "id_mcaid"]
swlang_tmp[, c("row_cnt_s", "row_cnt_w", "rand", "lang_cnt") := NULL]


# Merge back with the primary data and make unknown if NA
elig_lang[swlang_tmp, lang_max := i.lang_max, on = "id_mcaid"]
elig_lang[, lang_max := ifelse(is.na(lang_max), "Unknown", lang_max)]

rm(slang_tmp, wlang_tmp, swlang_tmp)
gc()

### Collapse to one row per ID given we have alone or in combo EVER language variables
elig_lang[, c("calmo", "slang", "wlang", "lang_na", "lang_na_len") := NULL]
setcolorder(elig_lang, c("id_mcaid", "lang_max", "lang_english", "lang_spanish", 
                         "lang_vietnamese", "lang_chinese", "lang_somali", 
                         "lang_russian", "lang_arabic", "lang_korean", 
                         "lang_ukrainian", "lang_amharic", 
                         "lang_english_t", "lang_spanish_t", "lang_vietnamese_t", 
                         "lang_chinese_t", "lang_somali_t", "lang_russian_t", 
                         "lang_arabic_t", "lang_korean_t", "lang_ukrainian_t", 
                         "lang_amharic_t"))
elig_lang_final <- unique(elig_lang)


# Drop temp table
remove(elig_lang)
gc()


#############################
#### Join all tables ####
#############################

elig_demoever_final <- list(elig_dob, elig_gender_final, elig_race_final, elig_lang_final) %>%
  Reduce(function(df1, df2) left_join(df1, df2, by = "id_mcaid"), .)


### Add in date for last run
elig_demoever_final <- elig_demoever_final %>%
  mutate(last_run = Sys.time())


#### Load to SQL server ####
# Set up table name
tbl_id_meta <- DBI::Id(schema = "stage", table = "mcaid_elig_demo")

# Write data
dbWriteTable(db_claims, tbl_id_meta, 
             value = as.data.frame(elig_demoever_final), overwrite = T)

#Drop individual tables
rm(elig_dob, elig_gender_final, elig_race_final, elig_lang_final, elig_demoever)
rm(tbl_id_meta)
rm(elig_demoever_final)
rm(list = ls(pattern = "_txt"))
rm(cols, origin)
rm(create_table_f)
gc()

