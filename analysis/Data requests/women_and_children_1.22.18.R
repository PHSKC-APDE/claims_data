###############################################################################
# Eli Kern
# 2018-1-22

# Proportion of Medicaid membership that are children (18 and under), and that are women of reproductive age (15-44)
# June 30, 2017 chosen as reference date

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

##### Connect to the servers #####
#db.claims50 <- odbcConnect("PHClaims50")
db.claims51 <- odbcConnect("PHClaims51")
#db.apde <- odbcConnect("PH_APDEStore50")
#db.apde51 <- odbcConnect("PH_APDEStore51")

##### Bring in 44-year old and under individuals from SQL server dbo.mcaid_elig_dob #####
elig_44 <- sqlQuery(
  db.claims51,
  " SELECT id, dobnew 
    FROM PHClaims.dbo.mcaid_elig_dob
    WHERE dobnew >= '1972-07-01'",
  stringsAsFactors = FALSE
)

#summarise(elig_44, maxdob = max(dobnew), mindob = min(dobnew))

#### Bring gender from dbo.mcaid_elig_gender for folks with coverage on 6/30/2017 ####
elig_gender_63017 <- sqlQuery(
  db.claims51,
  " SELECT *
  FROM PHClaims.dbo.mcaid_elig_gender
  WHERE '2017-06-30' BETWEEN calstart AND calend",
  stringsAsFactors = FALSE
)

#### Join to subset to those with coverage on 6/30/2017 and 44 or younger
elig_join <- inner_join(elig_gender_63017, elig_44, c("id"))
elig_join <- elig_join %>%
  mutate(age = as.integer((ymd("2017-06-30") - ymd(dobnew)) / 365.25))

# #Count # of rows per ID
# temp <- elig_gender_63017 %>%
#   group_by(id) %>%
#   mutate(
#     id_cnt = n()
#   ) %>%
#   ungroup()
           
#count kids (<19)
elig_join %>%
  filter(age < 19) %>%
  tally()

#count women of reproductive age (15-44)
elig_join %>%
  filter(age >= 15 & age <= 44 & gender == "FEMALE") %>%
  tally()

#combined count
elig_join %>%
  filter(age < 19 & gender == "MALE") %>%
  tally()
  
elig_join %>%
  filter(age < 15 & gender == "FEMALE") %>%
  tally()

elig_join %>%
  filter(age >=15 & age <=44 & gender == "FEMALE") %>%
  tally()

