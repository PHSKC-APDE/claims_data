###############################################################################
# Code to create a cleaned address table from the Medicaid eligibility data
# 
# Goal: tidy up and standardize addresses so that they can be geocoded
#
# Alastair Matheson (PHSKC-APDE)
# 2017-05-19
# 
###############################################################################


##### Notes #####
# Any manipulation in R will not carry over to the SQL tables unless uploaded to the SQL server


##### Set up global parameter and call in libraries #####
options(max.print = 250, tibble.print_max = 30, scipen = 999)

library(RODBC) # used to connect to SQL server
library(openxlsx) # used to read in Excel files
library(dplyr) # used to manipulate data
library(stringr) # used to manipulate string variables
library(RCurl) # Used to read in data via URL


##### Connect to the servers #####
db.claims51 <- odbcConnect("PHClaims51")


#### Bring in all eligibility data ####
ptm01 <- proc.time() # Times how long this query takes (~100 secs)
elig_add <-
  sqlQuery(
    db.claims51,
    "SELECT DISTINCT RSDNTL_ADRS_LINE_1 AS 'add1', RSDNTL_ADRS_LINE_2 AS 'add2', RSDNTL_CITY_NAME AS 'city', 
    RSDNTL_STATE_CODE AS 'state', RSDNTL_POSTAL_CODE AS 'zip', RSDNTL_COUNTY_CODE AS 'cntyfips', RSDNTL_COUNTY_NAME AS 'cntyname'
    FROM dbo.NewEligibility
    ORDER BY add1, add2, city, zip",
    stringsAsFactors = FALSE
  )
proc.time() - ptm01

# Make a copy of the dataset to avoid having to reread it
elig_add_bk <- elig_add



##### Items to resolve for address cleanup #####
# 1) City name spelling errors
# 2) Specfic addresses
# 3) Special addresses: homeless, PO box, confidential, care of

# 3) Address cleaning and resolve formatting issues
# 4) Decide what to do with addresses that indicate the person is homeless
# 5) Others?


#### INITIAL CAPITALIZATION ####
elig_add <- elig_add %>%
  mutate_at(vars(add1, add2, city, state, cntyname),
            funs(toupper(.)))


#### CITY SPELLING CORRECTIONS ####
# Goal: Tidy up city name spellings

# Bring in city lookup table from Github
city.lookup <- read.csv(text = getURL("https://raw.githubusercontent.com/PHSKC-APDE/Medicaid/master/eligibility%20cleanup/City%20lookup.csv"), 
                         header = TRUE, stringsAsFactors = FALSE)

# Set up variables for writing
elig_add <- mutate(elig_add, city_new = NA)
city.lookup$numfound <- 0

# Use lookup table
for(i in 1:nrow(city.lookup)) {
  found.bools = str_detect(elig_add$city, city.lookup$ICU_regex[i]) #create bool array from regex
  elig_add$city_new[found.bools] = city.lookup$city[i]  # reassign from bool array
  city.lookup$numfound[i] = sum(found.bools, na.rm = T) # save how many we found
}
# Report out progress
cat("Finished: Updated",
    sum(!is.na(elig_add$city_new)),
    "of",
    nrow(elig_add),
    "\n")
# Pass the unchanged cities through
elig_add$city_new[is.na(elig_add$city_new)] = elig_add$city[is.na(elig_add$city_new)]


#### SPECIFIC ADDRESSES ####
# Some addresses have specific issues than cannot be addressed via rules
# However, these specific addresses should not be shared publically
adds_specific <- read.xlsx("//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Data processing protocols/Geocoding/Medicaid_eligibility_specific_addresses_fix - DO NOT SHARE FILE.xlsx")
elig_add <- left_join(elig_add, adds_specific, by = c("add1", "add2", "city", "state", "zip", "cntyfips", "cntyname")) %>%
  select(-(notes))

# Bring over addresses not matched (could also collapse to a mutate_at statement)
elig_add <- elig_add %>%
  mutate(
    add1_new = ifelse(is.na(overridden), add1, add1_new),
    add2_new = ifelse(is.na(overridden), add2, add2_new),
    # Override earlier corrected city names with specific match 
    # (use specific match because sometimes the city was wrong, not just misspelt)
    city_new = ifelse(is.na(overridden), city_new.x, city_new.y),
    state_new = ifelse(is.na(overridden), state, state_new),
    zip_new = ifelse(is.na(overridden), zip, zip_new),
    cntyfips_new = ifelse(is.na(overridden), cntyfips, cntyfips_new),
    cntyname_new = ifelse(is.na(overridden), cntyname, cntyname_new)
  ) %>%
  select(add1:add2_new, city_new, state_new:overridden, -(city_new.x), -(city_new.y))


#### SPECIAL ADDRESS TYPES ####
### Confidential addresses
# Make flag to show that this address was confidential but treat as normal address for geocoding purposes
elig_add <- elig_add %>%
  mutate(confidential = ifelse(is.na(overridden) &
                                 (str_detect(add1_new, "CONF") | str_detect(add2_new, "CONF")), 1, confidential),
         add1_new = ifelse(is.na(overridden) & str_detect(add1_new, "CONF"), NA, add1_new),
         add2_new = ifelse(is.na(overridden) & str_detect(add2_new, "CONF"), NA, add2_new),
         # Move addresses into add1_new if applicable
         add1_new = ifelse(is.na(overridden) & is.na(add1_new) & confidential == 1 & !is.na(confidential), add2_new, add1_new),
         add2_new = ifelse(is.na(overridden) & add1_new == add2_new & confidential == 1 & !is.na(confidential), NA, add2_new)
  )


### Homeless individuals
# Lots of addresses have some reference to being homeless
# Plan to rewrite all of these to the same standard:
# add1 = HOMELESS; add2 = blank; keep original city, zip, state, and county unless manually corrected
# Exception: transitional housing addresses receive the standard clean up
# Also write to a flag
elig_add <- elig_add %>%
  mutate(
    homeless = ifelse(is.na(overridden) & str_detect(paste0(add1_new, add2_new, city_new), "HOMELESS|COUCH SURF|COUNCH") == T,
                      1, homeless),
    add1_new = if_else(is.na(overridden) & homeless == 1 & !is.na(homeless), "HOMELESS", add1_new),
    add2_new = if_else(is.na(overridden) & homeless == 1 & !is.na(homeless), "", add2_new)
  )


### Mail boxes (PO box, PMB, etc.)
# Ideally would obtain a list of all mail stop providers and match on address
# For now look for non-residential ZIPs and key words
# Write to a similar standard as homeless (blank address, retain city and zip)
elig_add <- elig_add %>%
  mutate(mailbox = ifelse(is.na(overridden) & 
                            (str_detect(add1_new, "[:space:]BOX[:space:]|PMB") |
                               str_detect(add2_new, "[:space:]BOX[:space:]|PMB") |
                               zip_new %in% c(98009, 98013, 98015, 98025, 98035, 98041, 98050, 98054, 98062, 98063, 98064, 
                                              98068, 98071, 98073, 98082, 98083, 98089, 98093, 98111, 98113, 98114, 98124, 
                                              98127, 98129, 98131, 98132, 98138, 98139, 98141, 98145, 98154, 98158, 98160, 
                                              98161, 98164, 98165, 98170, 98171, 98174, 98175, 98181, 98185, 98190, 98191, 
                                              98194)
                            ),
                   1, mailbox),
         add1_new = ifelse(is.na(overridden) & mailbox == 1 & !is.na(mailbox), NA, add1_new),
         add2_new = ifelse(is.na(overridden) & mailbox == 1 & !is.na(mailbox), NA, add2_new)
         )


### Care of addresses
# Fix address stucture so that add1 = street and add2 = apartment/unit but make flag to indicate the care/of part
# First flag such addresses
elig_add <- elig_add %>%
  mutate(care_of = ifelse(is.na(overridden) & 
                            (str_detect(add1_new, "C/O|^CARE OF|[:space:]CARE OF") |
                            str_detect(add2_new, "C/O|^CARE OF|[:space:]CARE OF")),
                          1, care_of),
         # Get rid of who/what the address is care of (address cleaning below will fix remaining issues)
         add1_new = ifelse(!is.na(care_of) & care_of == 1 & is.na(overridden) & str_detect(add1_new, "C/O|^CARE OF|[:space:]CARE OF"),
                           str_sub(add1_new, 1, str_locate(add1_new, "C/O|^CARE OF|[:space:]CARE OF") - 1),
                           add1_new),
         add2_new = ifelse(!is.na(care_of) & care_of == 1 & is.na(overridden) & str_detect(add2_new, "C/O|^CARE OF|[:space:]CARE OF"),
                           str_sub(add2_new, 1, str_locate(add2_new, "C/O|^CARE OF|[:space:]CARE OF") - 1),
                           add2_new)
  )



#### GENERAL ADDRESSES ####
# (NB. a separate geocoding process may fill in some gaps later)

### Standardize road names
elig_add <- elig_add %>%
  mutate(
    # standardize street names
    add1_new = str_replace_all(add1_new, "([:space:]|^)AVENUE|[:space:]AV([:space:]|$)", " AVE "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)BOULEVARD([:space:]|$)", " BLVD "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)CIRCLE([:space:]|$)", " CIR "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)COURT([:space:]|$)", " CT "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)DRIVE([:space:]|$)", " DR "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)HIGHWAY([:space:]|$)", " HWY "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)LANE([:space:]|$)", " LN "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)NORTH([:space:]|$)", " N "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)NORTH EAST([:space:]|$)", " NE "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)NORTH WEST([:space:]|$)", " NW "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)PARKWAY([:space:]|$)", " PKWY "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)PLACE([:space:]|$)", " PL "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)ROAD([:space:]|$)", " RD "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)SO([:space:]|$)|[:space:]SO([:space:]|$)", " S "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)SOUTH EAST([:space:]|$)", " SE "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)SOUTH WEST([:space:]|$)", " SW "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)STREET([:space:]|$)", " ST "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)STST([:space:]|$)", " ST "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)WY([:space:]|$)", " WAY "),
    add1_new = str_replace_all(add1_new, "([:space:]|^)WEST([:space:]|$)", " W "),
    # Need to cover both address fields
    add2_new = str_replace_all(add2_new, "([:space:]|^)AVENUE|[:space:]AV([:space:]|$)", " AVE "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)BOULEVARD([:space:]|$)", " BLVD "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)CIRCLE([:space:]|$)", " CIR "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)COURT([:space:]|$)", " CT "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)DRIVE([:space:]|$)", " DR "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)HIGHWAY([:space:]|$)", " HWY "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)LANE([:space:]|$)", " LN "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)NORTH([:space:]|$)", " N "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)NORTH EAST([:space:]|$)", " NE "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)NORTH WEST([:space:]|$)", " NW "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)PARKWAY([:space:]|$)", " PKWY "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)PLACE([:space:]|$)", " PL "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)ROAD([:space:]|$)", " RD "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)SO([:space:]|$)|[:space:]SO([:space:]|$)", " S "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)SOUTH EAST([:space:]|$)", " SE "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)SOUTH WEST([:space:]|$)", " SW "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)STREET([:space:]|$)", " ST "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)STST([:space:]|$)", " ST "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)WY([:space:]|$)", " WAY "),
    add2_new = str_replace_all(add2_new, "([:space:]|^)WEST([:space:]|$)", " W ")
  )

### Clean up road name in wrong field
cardinals <- "N|E|S|W|NE|SE|SW|NW"
roadtypes <- "AVE|BLVD|CIR|CT|DR|HWY|LN|PKWY|PL|RD|ST|WAY"
# Check that no Saint (ST) names are caught up below

road_pattern1 <- paste0("^(", roadtypes, ")[:space:][:space:]*(", cardinals, ")*")
road_pattern2 <- paste0("^(", cardinals, ")$")

elig_add <- elig_add %>%
  mutate(
    add1_new = ifelse(str_detect(add2_new, road_pattern1) & is.na(overridden) & !is.na(add2_new), 
                       paste(add1_new, str_sub(add2_new, 1, str_locate(add2_new, road_pattern1)[, 2]), sep = " "), add1_new),
    add2_new = ifelse(str_detect(add2_new, road_pattern1) & is.na(overridden) & !is.na(add2_new),
                       str_sub(add2_new, str_locate(add2_new, road_pattern1)[, 2] + 1, str_length(add2_new)), add2_new),
    add1_new = ifelse(str_detect(add2_new, road_pattern2) & is.na(overridden) & !is.na(add2_new),
                       paste(add1_new, add2_new, sep = " "), add1_new),
    add2_new = ifelse(str_detect(add2_new, paste0("^(", cardinals, ")$")) & is.na(overridden) & !is.na(add2_new), NA, add2_new)
  )


### Initial cleaning
elig_add <- elig_add %>%
  mutate(
    # Remove straight duplicates of addresses
    add2_new = ifelse(is.na(overridden) & add1_new == add2_new & !is.na(add1_new), NA, add2_new),
    # Replace 'SP' with 'SPC' (check that there are no other "SPC" strings first)
    add1_new = ifelse(str_detect(add1_new, "(^SP|[:space:]+SP)[:space:]*[:digit:]+$") & is.na(overridden), 
                      str_replace(add1_new, "SP", "SPC"), add1_new),
    add2_new = ifelse(str_detect(add2_new, "(^SP|[:space:]+SP)[:space:]*[:digit:]+$") & !is.na(overridden),
                      str_replace(add2_new, "SP", "SPC"), add2_new)
  )


### Figure out when apartments are in wrong field
# Set up list of secondary designators
secondary <- c("#", "\\$", "APT", "APPT", "APARTMENT", "APRT", "ATPT","BOX", "BLDG", "BLD", "BLG", "BUILDING", "DUPLEX", "FL ", 
               "FLOOR", "HOUSE", "LOT", "LOWER", "LOWR", "LWR", "REAR", "RM", "ROOM", "SLIP", "STE", "SUITE", "SPACE", "SPC", "STUDIO",
               "TRAILER", "TRAILOR", "TLR", "TRL", "TRLR", "UNIT", "UPPER", "UPPR", "UPSTAIRS")
secondary_init <- c("^#", "^\\$", "^APT", "^APPT","^APARTMENT", "^APRT", "^ATPT", "^BOX", "^BLDG", "^BLD", "^BLG", "^BUILDING", "^DUPLEX", "^FL ", 
               "^FLOOR", "^HOUSE", "^LOT", "^LOWER", "^LOWR", "^LWR", "^REAR", "^RM", "^ROOM", "^SLIP", "^STE", "^SUITE", "^SPACE", "^SPC", 
               "^STUDIO", "^TRAILER", "^TRAILOR", "^TLR", "^TRL", "^TRLR", "^UNIT", "^UPPER", "^UPPR", "^UPSTAIRS")


elig_add <- elig_add %>%
  mutate(
    # Set up field to track different issues
    movetype = 0,
    # Move street addresses into add1_new if there is nothing already there (movetype = 1)
    movetype = ifelse(is.na(overridden) & is.na(homeless) & is.na(mailbox) &
                        (is.na(add1_new) | add1_new == "") & !is.na(add2_new), 
                      1, movetype),
    # Remove name of apartment building or care facility if that is all that is in add1_new (movetype = 2)
    # Need to exempt # otherwise these unit numbers are caught up in this logic
    movetype = ifelse(is.na(overridden) & is.na(homeless) & is.na(mailbox) & movetype == 0 &
                        str_detect(add1_new, paste0("(^|[:space:])(", paste(secondary, collapse = "|"), ")([:space:]|$)")) == F & 
                                     str_detect(add1_new, "#") == F & str_detect(add1_new, "[:digit:]") == F, 
                      2, movetype),
    # ID when building number is in add1_new and address and apt number are in add2_new (movetype = 3)
    # sometimes there is a XX-XX building/apt number in add1 and also an apt number in add2
    movetype = ifelse(is.na(overridden) & is.na(homeless) & is.na(mailbox) & !is.na(add2_new) & movetype == 0 &
                         (str_detect(add1_new, paste(secondary_init, collapse = "|")) |
                            str_detect(add1_new, "^[:digit:]+-[:digit:]+$")) &
                         str_detect(add2_new, paste0("[:space:]*", paste(secondary, collapse = "|"))),
                      3, movetype),
    # ID other situations when apts are in the add1_new field and addresses are in the add2_new field (movetype = 4)
    movetype = ifelse(is.na(overridden) & is.na(homeless) & is.na(mailbox) & !is.na(add2_new) & movetype == 0 &
                         (str_detect(add1_new, paste0("[:space:]*", paste(secondary_init, collapse = "|"))) |
                            str_detect(add1_new, "^[:digit:]+-[:digit:]+$")) & str_detect(add2_new, "[:digit:]"),
                      4, movetype),
    # ID when secondary designator prefix is at the end of add1_new and add2_new is only a number (movetype = 5)
    movetype = ifelse(is.na(overridden) & is.na(homeless) & is.na(mailbox) & !is.na(add2_new) & movetype == 0 &
                        str_detect(add1_new, paste0("(", paste(secondary, collapse = "|"), ")$")) & str_detect(add2_new, "^[:digit:]+$"),
                      5, movetype),
    # ID apartment numbers that need to move to add2_new if that field currently blank (movetype = 6)
    movetype = ifelse(is.na(overridden) & is.na(homeless) & is.na(mailbox) & (is.na(add2_new) | add2_new == "") & movetype == 0 &
                        str_detect(add1_new, paste0("(", paste(secondary, collapse = "|"), ")[:space:]*[:punct:]*[:alnum:]+[:space:]*[:punct:]*[:alnum:]*$")),
                      6, movetype)
    #,
    # ID any straggler apartments ending in a single letter or letter prefix (movetype = 7)
    # Note: currently has too many false positives to be useful, requires more manual correction of odd addresses
    # movetype = ifelse(is.na(overridden) & is.na(homeless) & !is.na(add1) & movetype == 0 &
    #                     str_detect(add1, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$"),
    #                   7, movetype)
  )


### Move apartments to the correct spot
# Need to set up new add1 and add2 fields because the data are not moved simultaneously
elig_add <- elig_add %>%
  mutate(
    # Move street addresses into add1 if there is nothing already there (movetype = 1)
    add1_new2 = ifelse(movetype == 1, add2_new, add1_new),
    add2_new2 = ifelse(movetype == 1, "", add2_new),
    # Remove name of apartment building if that is all that is in add1 (movetype = 2)
    add1_new2 = ifelse(movetype == 2, add2_new, add1_new2),
    add2_new2 = ifelse(movetype == 2, NA, add2_new2),
    # Merge together all secondary designators and numbers (movetype = 3)
    # Note, we don't really need to track apartment numbers closely as they will not be geocoded
    # We just need them all in one place out of the way
    add1_new2 = ifelse(movetype == 3,
                      # Keep initial street address from add2 without the secondary designator
                      str_sub(add2_new, 1, str_locate(add2_new,  paste0("[:space:]*", paste(secondary, collapse = "|")))[,1] - 1),
                      add1_new2),
    add2_new2 = ifelse(movetype == 3,
                      paste0(
                        # Keep secondary designator from add2, plus a space
                        str_sub(add2_new, str_locate(add2_new, paste(secondary, collapse = "|"))[,1], str_length(add2_new)), " ",
                        # Add in the secondary designator from add1
                        add1_new),
                      add2_new2),
    # Switch around street addresses and apartment numbers (movetype = 4)
    add1_new2 = ifelse(movetype == 4, add2_new, add1_new2),
    add2_new2 = ifelse(movetype == 4, add1_new, add2_new2),
    # Move secondary designator to add2 and join with number there (movetype = 5),
    add1_new2 = ifelse(movetype == 5, str_sub(add1_new, 1, str_locate(add1_new, paste0("(", paste(secondary, collapse = "|"), ")$"))[,1] - 1),
                      add1_new2),
    add2_new2 = ifelse(movetype == 5, paste(str_sub(add1_new, str_locate(add1_new, paste0("[:space:]*(", paste(secondary, collapse = "|"), ")$"))[,1], 
                                                    str_length(add1_new)),
                                           add2_new, sep = " "), add2_new2),
    # Move secondary address line to add2 (movetype = 6)
    add1_new2 = ifelse(movetype == 6, str_sub(add1_new, 1, str_locate(add1_new, paste(secondary, collapse = "|"))[,1] - 1), add1_new2),
    add2_new2 = ifelse(movetype == 6, str_sub(add1_new, str_locate(add1_new, paste(secondary, collapse = "|"))[,1], str_length(add1_new)), add2_new2),
    # Bring in remaining addresses
    add1_new2 = ifelse(movetype == 0 | is.na(movetype), add1_new, add1_new2),
    add2_new2 = ifelse(movetype == 0 | is.na(movetype), add2_new, add2_new2),
    
    # Strip remaining punctutation and spaces
    add1_new2 = str_replace_all(add1_new2, "[-]+", " "),
    add1_new2 = str_replace_all(add1_new2, '\\"|\\.', ""),
    
    add2_new2 = str_replace(add2_new2, "- ", "-"),
    add2_new2 = str_replace(add2_new2, "# -", "#"),
    add2_new2 = str_replace(add2_new2, "APT -", "APT "),
    add2_new2 = str_replace_all(add2_new2, '\\"|\\.', ""),
    add2_new2 = str_replace_all(add2_new2, "# ", "#"),
    add2_new2 = str_replace(add2_new2, "[:space:]*-[:space:]*", "-")
    
  ) %>%
  # Remove any whitespace generated in the process
  mutate_at(vars(add1_new2, add2_new2), funs(str_trim(.))) %>%
  mutate_at(vars(add1_new2, add2_new2), funs(str_replace_all(., "[:space:]+", " ")))


# Overwrite intermediate address fields
elig_add <- elig_add %>%
  mutate(add1_new = add1_new2, add2_new = add2_new2) %>%
  select(-add1_new2, -add2_new2)


#### ADDRESSES FOR GEOCODING ####
# Pull out distinct addresses
today <- Sys.Date()
address <- select(elig_add, add1_new, city_new:zip_new) %>%
  distinct() %>%
  arrange(add1_new, city_new, zip_new)
write.xlsx(address, file = 
             paste0("//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Data processing protocols/Geocoding/Distinct addresses_", today, ".xlsx"),
           col.names = TRUE)
# NB. If R throws an error about not having Rtools installed, run this command:
# Sys.setenv(R_ZIPCMD= "C:/Rtools/bin/zip")



#### Save cleaned and consolidated person table ####
elig_clean <- elig_add %>% select(-movetype) %>% distinct()

# Quicker to export xlsx and import via SQL Mgmt Server Studio?
write.xlsx(elig_clean, file = 
             paste0("//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Data processing protocols/Geocoding/elig_clean_", today, ".xlsx"),
           col.names = TRUE)

ptm02 <- proc.time() # Times how long this query takes
#sqlDrop(db.apde, "dbo.medicaid_elig_consolidated_noRAC")
sqlSave(db.apde51, elig.clean, tablename = "dbo.mcaid_elig_address_clean")
proc.time() - ptm02




