###############################################################################
# Eli Kern
# 2018-1-10

# Data request from Alex O'Reilly from City of Bellevue
# How many Medicaid members age 21+ in East Region of King County?
# I've chosen date of June 30, 2017 based on our current data timeline

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

##### Bring in address data for folks who have coverage on June 30, 2017 #####
#Eventually I will pull from dbo.elig_address on SQL server, but for now I have generated this table using another script - elig_address_final

elig_address_063017 <- elig_address_final

##### Bring in 21-year olds from SQL server dbo.elig_dob #####
elig_21 <- sqlQuery(
  db.claims51,
  " SELECT id, dobnew 
    FROM PHClaims.dbo.elig_dob
    WHERE dobnew <= '1997-06-30'",
  stringsAsFactors = FALSE
)

#summarise(elig_21, maxdob = max(dobnew), mindob = min(dobnew))

##### Merge age to address file and subset to 21+ #####
dr1 <- inner_join(elig_address_063017, elig_21, c("id"))

##### De-duplicate IDs with multiple rows (usuaslly ITA holds) #####
#Flag rows that have multiple ZIPs recorded in this time period, remove these ZIP and region-level counts

#Code to find duplicated Medicaid IDs
dr2 <- dr1 %>%
  group_by(id) %>%
  mutate(
    #id_cnt = n(),
    #flag rows belong to IDs with more than 1 ZIP code in this time window
    zip_mult = max(min_rank(zip_new)) > 1,
    #rank rows to deduplicate
    rank_row = min_rank(row_number())
  ) %>%
  ungroup() %>%
  #Filter to 1 obs per ID
  filter(rank_row == 1)

##### Tabulate persons by ZIP code and region #####
zip_tab <- dr2 %>%
  filter(zip_mult == FALSE) %>%
  group_by(zip_new) %>%
  summarise(id_cnt = n()) %>%
  ungroup()

reg_tab <- dr2 %>%
  filter(zip_mult == FALSE) %>%
  group_by(kcreg_zip) %>%
  summarise(id_cnt = n()) %>%
  ungroup()

#Make sure NA region rows are non-King ZIPs
#temp <- filter(dr2, is.na(kcreg_zip))
#count(temp,cntyname_new)

