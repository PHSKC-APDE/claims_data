###############################################################################
# Eli Kern and Alastair Matheson
# 2018-2-5

# Code to create a SQL table dbo.mcaid_elig_demoever which holds SSN, DOB, gender, race, and language
# One row per ID, one SSN and one DOB per ID (frequency-based selection)
# Gender, race, and language are alone or in combination EVER variables
# Data elements: ID, BLANK

## 5/22/2018 updates:
# Add in multiple gender and multiple race variables
# Add in unknown gender, race, and language variables

## 2018-07-17 updates:
# Converted most code to use data.table package due to large size of data
# Removed vestigal code and other tidying

###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)
origin <- "1970-01-01"

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(tidyverse) # Used to manipulate data
library(lubridate) # Used to manipulate dates
library(data.table) # Useful for large data sets

#### Connect to the SQL server ####
db.claims51 <- dbConnect(odbc(), "PHClaims51")


#################################################################
##### Bring in Medicaid eligibility data for DOB processing #####
#Note to bring in test subset of Medicaid data, insert "top 100000" between SELECT and z.MEDICAID_RECIPIENT_ID
#################################################################

elig_dob <- dbGetQuery(
  db.claims51,
  # select most frequently reported SSN and DOB per Medicaid ID
  "select id.id, ssn.ssnnew, dob.dobnew
  
  from (
    select distinct MEDICAID_RECIPIENT_ID as 'id'
    from PHClaims.dbo.mcaid_elig_raw
  ) as id
  
  left join (
    select b.id, b.ssn as 'ssnnew'
    from (
      select a.id, a.ssn, row_number() over (partition by a.id order by a.id, a.ssn_cnt desc, a.ssn) as 'ssn_rank'
      from (
        select distinct MEDICAID_RECIPIENT_ID as 'id', SOCIAL_SECURITY_NMBR as 'ssn', count(SOCIAL_SECURITY_NMBR) as 'ssn_cnt'
        from PHClaims.dbo.mcaid_elig_raw
        where SOCIAL_SECURITY_NMBR is not null
        group by MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR
      ) as a
    ) as b
    where b.ssn_rank = 1
  ) as ssn
  
  on id.id = ssn.id
  
  left join(
    select b.id, cast(b.dob as date) as 'dobnew'
    from (
      select a.id, a.dob, row_number() over (partition by a.id order by a.id, a.dob_cnt desc, a.dob) as 'dob_rank'
      from (
        select MEDICAID_RECIPIENT_ID as 'id', BIRTH_DATE as 'dob', count(BIRTH_DATE) as 'dob_cnt'
        from PHClaims.dbo.mcaid_elig_raw
        where BIRTH_DATE is not null
        group by MEDICAID_RECIPIENT_ID, BIRTH_DATE
      ) as a
    ) as b
    where b.dob_rank = 1
  ) as dob
  
  on id.id = dob.id"
)



#################################################################
##### Bring in Medicaid eligibility data for gender, race and language processing #####
#Note to bring in test subset of Medicaid data, insert "top 100000" between SELECT and z.MEDICAID_RECIPIENT_ID
#################################################################

### Bring in Medicaid eligibility data
system.time( # Times how long this query takes (~520s)
  elig_demoever <- dbGetQuery(
  db.claims51,
  "SELECT DISTINCT y.CLNDR_YEAR_MNTH as calmo, y.MEDICAID_RECIPIENT_ID as id, 
      y.GENDER as gender, y.RACE1 as race1, y.RACE2 as race2, 
      y.RACE3 as race3, y.RACE4 as race4, y.HISPANIC_ORIGIN_NAME as hispanic, 
      y.SPOKEN_LNG_NAME as 'slang', y.WRTN_LNG_NAME as 'wlang'
    FROM (
      SELECT z.CLNDR_YEAR_MNTH, z.MEDICAID_RECIPIENT_ID, z.GENDER, z.RACE1, 
            z.RACE2, z.RACE3, z.RACE4, z.HISPANIC_ORIGIN_NAME,
            z.SPOKEN_LNG_NAME, z.WRTN_LNG_NAME
      FROM [PHClaims].[dbo].[mcaid_elig_raw] as z
    ) as y"
)
)

# Convert to data table and remove calmonth (no longer needed)
elig_demoever <- setDT(elig_demoever)
elig_demoever <- elig_demoever[, c("id", "gender", "race1", "race2", "race3", 
                                   "race4", "hispanic", "slang", "wlang")]


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

elig_gender <- elig_demoever[, c("id", "gender")]

### Create alone or in combination gender variables
elig_gender[, c("female", "male") := 
                     list(ifelse(str_detect(gender, "FEMALE"), 1, 0),
                          ifelse(str_detect(gender, "^MALE$"), 1, 0))]


### For each gender variable, count number of rows where variable = 1.
### Divide this number by total number of rows (i.e., months) where gender is non-missing.
### Create _t variables for each gender variable to hold this percentage.

# Create a variable to flag if gender var is missing
elig_gender[, genderna := is.na(gender), ]

# Create gender person time vars
elig_gender[, c("female_t", "male_t") := 
                   list(round((length(female[female == 1 & !is.na(female)]) / 
                                 length(genderna[genderna == FALSE]) * 100), 1),
                        round((length(male[male == 1 & !is.na(male)]) / 
                                 length(genderna[genderna == FALSE]) * 100), 1))
                 , by = "id"]


# Replace NA person time variables with 0
  elig_gender[, c("female_t", "male_t") := 
                   list(recode(female_t, .missing = 0),
                        recode(male_t, .missing = 0))
                 , ]


### Copy all non-missing gender variable values to all rows within each ID
# First make collapsed max of genders for each ID
elig_gender_sum <- elig_gender[, .(female = max(female, na.rm = T), 
                                          male = max(male, na.rm = T)),
                                      by = "id"]
#Replace infinity values with NA (generated by max function applied to NA rows)
cols <- c("female", "male")
elig_gender_sum[, (cols) := 
                   lapply(.SD, function(x)
                     replace(x, is.infinite(x), NA)), 
                 .SDcols = cols]


# Now join back to main data
elig_gender[elig_gender_sum, c("female", "male") := list(i.female, i.male), 
               on = "id"]
rm(elig_gender_sum)


### Collapse to one row per ID given we have alone or in combo EVER gender variables
elig_gender <- elig_gender[, c("id", "female", "male", "female_t", "male_t")]
elig_gender_final <- unique(elig_gender)

#Add in variables for multiple gender (mutually exclusive categories) and missing gender
elig_gender_final <- elig_gender_final %>%
  mutate(
    gender_mx = case_when(
      female_t > 0 & male_t >0 ~ "Multiple",
      female == 1 ~ "Female",
      male == 1 ~ "Male",
      TRUE ~ NA_character_
    ),
    gender_unk = case_when(
      is.na(gender_mx) ~ 1,
      !is.na(gender_mx) ~ 0,
      TRUE ~ NA_real_
    )
  ) %>%
select(., id, gender_mx, female, male, female_t, male_t, gender_unk)

#Drop temp table
rm(elig_gender)
gc()


#############################
#### Process race data ####
#############################

elig_race <- elig_demoever[, c("id", "race1", "race2", "race3", "race4", "hispanic")]


### Create alone or in combination race variables
aian_txt <- c("ALASKAN NATIVE", "AMERICAN INDIAN")
black_txt <- c("BLACK")
asian_txt <- c("ASIAN")
nhpi_txt <- c("HAWAIIAN", "PACIFIC ISLANDER")
white_txt <- c("WHITE")
latino_txt <- c("^HISPANIC$")

cols <- c("race1", "race2", "race3", "race4")
elig_race[, aian := rowSums(sapply(.SD, function(x)
  str_detect(x, paste(aian_txt, collapse = '|'))), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, asian := rowSums(sapply(.SD, function(x) str_detect(x, asian_txt)), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, black := rowSums(sapply(.SD, function(x) str_detect(x, black_txt)), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, nhpi := rowSums(sapply(.SD, function(x)
  str_detect(x, paste(nhpi_txt, collapse = '|'))), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, white := rowSums(sapply(.SD, function(x) str_detect(x, white_txt)), 
  na.rm = TRUE), .SDcols = cols]
elig_race[, latino := str_detect(hispanic, latino_txt) * 1]


# Same race can be listed more than once across race variables, replace sums > 1 with 1
cols <- c("aian", "asian", "black", "nhpi", "white", "latino")
elig_race[, (cols) := 
            lapply(.SD, function(x) if_else(x > 1, 1, x)), 
          .SDcols = cols]

# Replace race vars with NA if all race vars are NA, (latino already NA if hispanic is NA)
cols <- c("aian", "asian", "black", "nhpi", "white")
elig_race[, (cols) := 
            lapply(.SD, function(x) 
              if_else(is.na(race1) & is.na(race2) & is.na(race3) &
                        is.na(race4), NA_real_, x)), 
          .SDcols = cols]


### For each race variable, count number of rows where variable = 1.
# Divide this number by total number of rows (eg months) where at least one race variable is non-missing.
# Create _t variables for each race variable to hold this percentage.

# Create a variable to flag if all race vars are NA and Latino also 0 or NA
# Can just check aian since this is only NA if all race fields are NA
elig_race[, racena := is.na(aian) & (is.na(latino) | latino == 0), ]

# Create race person time vars
elig_race[, c("aian_t", "asian_t", "black_t", "nhpi_t",
              "white_t", "latino_t") := 
              list(
                round((length(aian[aian == 1 & !is.na(aian)]) / 
                         length(racena[racena == FALSE]) * 100), 1),
                round((length(asian[asian == 1 & !is.na(asian)]) / 
                         length(racena[racena == FALSE]) * 100), 1),
                round((length(black[black == 1 & !is.na(black)]) / 
                         length(racena[racena == FALSE]) * 100), 1),
                round((length(nhpi[nhpi == 1 & !is.na(nhpi)]) / 
                         length(racena[racena == FALSE]) * 100), 1),
                round((length(white[white == 1 & !is.na(white)]) / 
                         length(racena[racena == FALSE]) * 100), 1),
                round((length(latino[latino == 1 & !is.na(latino)]) / 
                         length(racena[racena == FALSE]) * 100), 1)
              )
            , by = "id"]


# Replace NA person time variables with 0
cols <- c("aian_t", "asian_t", "black_t", "nhpi_t", "white_t", "latino_t")
elig_race[, (cols) := 
            lapply(.SD, function(x) recode(x, .missing = 0)), 
          .SDcols = cols]


### Copy all non-missing race variable values to all rows within each ID.
# First make collapsed max of race for each ID
elig_race_sum <- elig_race[, .(aian = max(aian, na.rm = T),
                                   asian = max(asian, na.rm = T),
                                   black = max(black, na.rm = T),
                                   nhpi = max(nhpi, na.rm = T),
                                   white = max(white, na.rm = T),
                                   latino = max(latino, na.rm = T)),
                               by = "id"]


#Replace infinity values with NA (generated by max function applied to NA rows)
cols <- c("aian", "asian", "black", "nhpi", "white", "latino")
elig_race_sum[, (cols) := 
                  lapply(.SD, function(x)
                    replace(x, is.infinite(x), NA)), 
                .SDcols = cols]
# Now join back to main data
elig_race[elig_race_sum, c("aian", "asian", "black", "nhpi", "white", "latino") := 
            list(i.aian, i.asian, i.black, i.nhpi, i.white, i.latino), 
            on = "id"]
rm(elig_race_sum)
gc()



### Collapse to one row per ID given we have alone or in combo EVER race variables
elig_race <- elig_race[, c("id", "aian", "asian", "black", "nhpi", "white", 
                           "latino", "aian_t", "asian_t", "black_t", 
                           "nhpi_t", "white_t", "latino_t")]
elig_race_final <- unique(elig_race)

# Add in variables for multiple race (mutually exclusive categories) and missing race
elig_race_final <- elig_race_final %>%
  mutate(
    # Multiple race, Latino included as race
    # Note OR condition to account for NA values in latino that may make race + latino sum to NA
    race_eth_mx = case_when(
      (aian + asian + black + nhpi + white + latino > 1) | 
        ((aian + asian + black + nhpi + white) > 1)  ~ "Multiple",
      aian == 1 ~ "AI/AN",
      asian == 1 ~ "Asian",
      black == 1 ~ "Black",
      nhpi == 1 ~ "NH/PI",
      white == 1 ~ "White",
      latino == 1 ~ "Latino",
      TRUE ~ NA_character_
    ),
    # Multiple race, Latino excluded
    race_mx = case_when(
      aian + asian + black + nhpi + white > 1  ~ "Multiple",
      aian == 1 ~ "AI/AN",
      asian == 1 ~ "Asian",
      black == 1 ~ "Black",
      nhpi == 1 ~ "NH/PI",
      white == 1 ~ "White",
      TRUE ~ NA_character_
    ),
    # Race missing if multiple race/ethnicity variable is NA
    race_unk = case_when(
      is.na(race_eth_mx) ~ 1,
      !is.na(race_eth_mx) ~ 0,
      TRUE ~ NA_real_
    )
  ) %>%
  select(., id, race_eth_mx, race_mx, aian, asian, black, nhpi, white, latino, 
         aian_t, asian_t, black_t, nhpi_t, white_t, latino_t, race_unk)

#Drop temp table
rm(elig_race)
gc()


###############################
#### Process language data ####
###############################

elig_lang <- select(elig_demoever, id, slang, wlang)
gc()

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

elig_lang[, english := rowSums(sapply(.SD, function(x) str_detect(x, english_txt)),
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, spanish := rowSums(sapply(.SD, function(x) str_detect(x, paste(spanish_txt, collapse = '|'))), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, vietnamese := rowSums(sapply(.SD, function(x) str_detect(x, vietnamese_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, chinese := rowSums(sapply(.SD, function(x) str_detect(x, paste(chinese_txt, collapse = '|'))),
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, somali := rowSums(sapply(.SD, function(x) str_detect(x, somali_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, russian := rowSums(sapply(.SD, function(x) str_detect(x, russian_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, arabic := rowSums(sapply(.SD, function(x) str_detect(x, arabic_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, korean := rowSums(sapply(.SD, function(x) str_detect(x, korean_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, ukrainian := rowSums(sapply(.SD, function(x) str_detect(x, ukrainian_txt)), 
                               na.rm = TRUE), .SDcols = cols]
elig_lang[, amharic := rowSums(sapply(.SD, function(x) str_detect(x, amharic_txt)), 
                               na.rm = TRUE), .SDcols = cols]

# Helps to clean out memory after this step
gc()

# Same langs can be listed more than once across written/spoken, replace sums > 1 with 1
cols <- c("english", "spanish", "vietnamese", "chinese", "somali", "russian",
          "arabic", "korean", "ukrainian", "amharic")
elig_lang[, (cols) := 
            lapply(.SD, function(x) if_else(x > 1, 1, x)), 
          .SDcols = cols]


##Replace lang vars with NA if slang and wlang are both NA
cols <- c("english", "spanish", "vietnamese", "chinese", "somali", "russian",
          "arabic", "korean", "ukrainian", "amharic")
elig_lang[, (cols) := 
            lapply(.SD, function(x) 
              if_else(is.na(slang) & is.na(wlang), NA_real_, x)), 
          .SDcols = cols]


### For each language variable, count number of rows where variable = 1.
# Divide this number by total number of rows (eg months) where at least one language variable is non-missing.
# Create _t variables for each lang variable to hold this percentage.

#Create a variable to flag if all lang vars are NA
elig_lang[, langna := is.na(slang) & is.na(wlang), ]

#Create lang person time vars
elig_lang[, c("english_t", "spanish_t", "vietnamese_t", "chinese_t", "somali_t", 
              "russian_t", "arabic_t", "korean_t", "ukrainian_t", "amharic_t") := 
            list(
              round((length(english[english == 1 & !is.na(english)]) / 
                       length(langna[langna == FALSE]) * 100), 1),
              round((length(spanish[spanish == 1 & !is.na(spanish)]) / 
                       length(langna[langna == FALSE]) * 100), 1),
              round((length(vietnamese[vietnamese == 1 & !is.na(vietnamese)]) / 
                       length(langna[langna == FALSE]) * 100), 1),
              round((length(chinese[chinese == 1 & !is.na(chinese)]) / 
                       length(langna[langna == FALSE]) * 100), 1),
              round((length(somali[somali == 1 & !is.na(somali)]) / 
                       length(langna[langna == FALSE]) * 100), 1),
              round((length(russian[russian == 1 & !is.na(russian)]) / 
                       length(langna[langna == FALSE]) * 100), 1),
              round((length(arabic[arabic == 1 & !is.na(arabic)]) / 
                       length(langna[langna == FALSE]) * 100), 1),
              round((length(korean[korean == 1 & !is.na(korean)]) / 
                       length(langna[langna == FALSE]) * 100), 1),
              round((length(ukrainian[ukrainian == 1 & !is.na(ukrainian)]) / 
                       length(langna[langna == FALSE]) * 100), 1),
              round((length(amharic[amharic == 1 & !is.na(amharic)]) / 
                       length(langna[langna == FALSE]) * 100), 1)
            )
          , by = "id"]


# Replace NA person time variables with 0
cols <- c("english_t", "spanish_t", "vietnamese_t", "chinese_t", "somali_t", 
          "russian_t", "arabic_t", "korean_t", "ukrainian_t", "amharic_t")
elig_lang[, (cols) := 
            lapply(.SD, function(x) recode(x, .missing = 0)), 
          .SDcols = cols]


### Copy all non-missing language variable values to all rows within each ID
# First make collapsed max of lang for each ID
elig_lang_sum <- elig_lang[, .(english = max(english, na.rm = T),
                               spanish = max(spanish, na.rm = T),
                               vietnamese = max(vietnamese, na.rm = T),
                               chinese = max(chinese, na.rm = T),
                               somali = max(somali, na.rm = T),
                               russian = max(russian, na.rm = T),
                               arabic = max(arabic, na.rm = T),
                               korean = max(korean, na.rm = T),
                               ukrainian = max(ukrainian, na.rm = T),
                               amharic = max(amharic, na.rm = T)),
                           by = "id"]
#Replace infinity values with NA (generated by max function applied to NA rows)
cols <- c("english", "spanish", "vietnamese", "chinese", "somali", "russian",
          "arabic", "korean", "ukrainian", "amharic")
elig_lang_sum[, (cols) := 
                lapply(.SD, function(x)
                  replace(x, is.infinite(x), NA)), 
              .SDcols = cols]
# Now join back to main data
elig_lang[elig_lang_sum, c("english", "spanish", "vietnamese", "chinese", 
                           "somali", "russian", "arabic", "korean", 
                           "ukrainian", "amharic") := 
            list(i.english, i.spanish, i.vietnamese, i.chinese, i.somali, 
                 i.russian, i.arabic, i.korean, i.ukrainian, i.amharic), 
          on = "id"]
rm(elig_lang_sum)
gc()


### Select most frequently reported language per ID
# Count spoken language rows by ID and language
slang_tmp <- elig_lang[!is.na(slang), row_cnt_s := .N,
                       by = c("id", "slang")]
slang_tmp[, maxlang := slang]
slang_tmp <- slang_tmp[, c("id", "maxlang", "row_cnt_s")]
slang_tmp <- unique(slang_tmp)

#Count written language rows by ID and language
wlang_tmp <- elig_lang[!is.na(wlang), row_cnt_w := .N,
                       by = c("id", "wlang")]
wlang_tmp[, maxlang := wlang]
wlang_tmp <- wlang_tmp[, c("id", "maxlang", "row_cnt_w")]
wlang_tmp <- unique(wlang_tmp)


#Join written and spoken language counts and sum by ID and language
#Assign random number to each ID and language, and sort by ID and random number (this helps with selecting maxlang when tied)
set.seed(580493617)

swlang_tmp <- merge(slang_tmp, wlang_tmp, by = c("id", "maxlang"), all = T)
swlang_tmp[, c("lang_cnt", "rand") :=
             list(sum(row_cnt_s, row_cnt_w, na.rm = TRUE),
                  runif(1, 0, 1)),
           by = c("id", "maxlang")]
swlang_tmp <- swlang_tmp[, c("id", "maxlang", "lang_cnt", "rand")][order(id, -lang_cnt, rand)]

# Slice data to one language per ID (most frequently reported)
swlang_tmp <- swlang_tmp[, head(.SD, 1), by = "id"]
swlang_tmp <- swlang_tmp[, c("id", "maxlang")]

rm(slang.tmp, wlang.tmp)

# Merge back with the primary data
elig_lang[swlang_tmp, maxlang := list(i.maxlang), on = "id"]
rm(slang_tmp, wlang_tmp, swlang_tmp)
gc()

### Collapse to one row per ID given we have alone or in combo EVER language variables
elig_lang <- elig_lang[, c("id", "maxlang", "english", "spanish", "vietnamese", 
                           "chinese", "somali", "russian", "arabic", "korean", 
                           "ukrainian", "amharic", "english_t", "spanish_t", 
                           "vietnamese_t", "chinese_t", "somali_t", "russian_t", 
                           "arabic_t", "korean_t", "ukrainian_t", "amharic_t")]
elig_lang_final <- unique(elig_lang)

# Add in variable for missing language
elig_lang_final <- elig_lang_final %>%
  mutate(
    lang_unk = case_when(
      is.na(maxlang) ~ 1,
      !is.na(maxlang) ~ 0,
      TRUE ~ NA_real_
    )
  ) %>%
  select(., id, maxlang, english, spanish, vietnamese, chinese, somali, russian, 
         arabic, korean, ukrainian, amharic, english_t, spanish_t, vietnamese_t, 
         chinese_t, somali_t, russian_t, arabic_t, korean_t, ukrainian_t, 
         amharic_t, lang_unk)

# Drop temp table
remove(elig_lang)
gc()


#############################
#### Join all tables ####
#############################

elig_demoever_final <- list(elig_dob, elig_gender_final, elig_race_final, elig_lang_final) %>%
  Reduce(function(df1, df2) left_join(df1, df2, by = "id"), .)




#### Save dob.mcaid_elig_demoever to SQL server 51 ####
# Remove/delete table if it already exists AND you have changed the data structure (not usually needed)
dbRemoveTable(db.claims51, name = "mcaid_elig_demoever_load")

# Write your data frame. Note that the package adds a dbo schema so don’t include that in the name.
# Also, you can append = T rather than overwrite = T if desired. 
# Overwrite does what you would expect without needing to delete the whole table
dbWriteTable(db.claims51, name = "mcaid_elig_demoever_load", 
             value = as.data.frame(elig_demoever_final), overwrite = T,
             field.types = c(
               spanish_t = "decimal(4,1)"
             ))

#Drop individual tables
rm(elig_dob, elig_gender_final, elig_race_final, elig_lang_final, elig_demoever)
rm(elig_demoever_final)
rm(list = ls(pattern = "_txt"))
rm(cols)
gc()

