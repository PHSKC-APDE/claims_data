## Header ----
  # Author: Danny Colombara
  # Date: September 4, 2019
  # R version: 3.5.3
  # Puprpose: Establish Medicare - PHA links using deterministic and probabilistic methods
  #

## Set up R environment #####
  options(max.print = 350, tibble.print_max = 30, scipen = 999)
  
  library(odbc) # Used to connect to SQL server
  library(openxlsx) # Used to import/export Excel files
  library(lubridate) # Used to manipulate dates
  library(tidyverse) # Used to manipulate data
  library(data.table) # used to manipulate data
  library(RecordLinkage) # used to make the linkage
  library(phonics) # used to extract phonetic version of names
  
  start.time <- Sys.time()

## (1) Connect to the servers #####
db.apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db_claims <- dbConnect(odbc(), "PHClaims51")


## (2) Load data #####
  ### Housing
        pha_longitudinal <- setDT(odbc::dbGetQuery(db.apde51, "SELECT pid, ssn_id_m6, lname_new_m6, fname_new_m6, mname_new_m6, 
             dob_m6, gender_new_m6, enddate FROM stage.pha"))
      
      # Limit to one row per person and only variables used for merging (use most recent row of data)
      # Filter if person's most recent enddate is <2011 since they can't match to Medicare
        pha <- pha_longitudinal %>%
          filter(year(enddate) >= 2012) %>%
          distinct(pid, ssn_id_m6, lname_new_m6, fname_new_m6, mname_new_m6, 
                   dob_m6, gender_new_m6, enddate) %>%
          arrange(pid, ssn_id_m6, lname_new_m6, fname_new_m6, mname_new_m6, 
                  dob_m6, gender_new_m6, enddate) %>%
          group_by(pid, ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6) %>%
          slice(n()) %>%
          ungroup() %>%
          rename(ssn = ssn_id_m6, name_srnm = lname_new_m6, 
                 name_gvn = fname_new_m6, name_mdl = mname_new_m6, 
                 dob = dob_m6, gender = gender_new_m6) %>%
          select(-(enddate))
  
      # clean up and standardize for comparison with Medicare
        setDT(pha)
        pha[ssn == "111111111", ssn := NA_character_] # SSN 111-11-1111 appears to be a filler SSN (i.e., it is not real and applies to many people)
        pha[, pid := as.character(pid)] # convert to character so same type as id_mcare
        
  ### Medicare
        mcare <- setDT(odbc::dbGetQuery(db_claims, "SELECT * FROM stage.xwalk_02_linkage_prep_mcare"))
        mcare[!(gender_me %in% c("Female", "Male")), gender := NA_integer_]
        mcare[gender_me == "Female", gender := 1] # match coding in PHA
        mcare[gender_me == "Male", gender := 2] # match coding in PHA
        mcare[, c("last_run", "gender_me", "gender_female", "gender_male") := NULL]
        mcare <- mcare[id_mcare != "GGGGGGohoFoQoPQ"] # duplicate with GGGGGGohoFhGPGP, which is the one that linked with Mcaid

## (3) Clean up & standardize data ----
      cleaning.function <- function(dt){
        # Extract date components
        dt[, dob_y := as.character(year(dob))] # extract year
        dt[, dob_m := as.character(month(dob))] # extract month
        dt[, dob_d := as.character(day(dob))] # extract day
        dt[, c("dob") := NULL] # drop vars that are not needed
        
        # Last names
        dt[, name_srnm := gsub("[0-9]", "", name_srnm)]  # remove any numbers that may be present in last name
        dt[, name_srnm := gsub("'", "", name_srnm)]  # remove apostrophes from last name, e.g., O'BRIEN >> OBRIEN
        dt[, name_srnm := gsub("\\.", "", name_srnm)]  # remove periods from last name, e.g., JONES JR. >> JONES JR
        dt[, name_srnm := gsub(" SR$", "", name_srnm)]  # remove all " SR" suffixes from last name
        dt[, name_srnm := gsub(" JR$", "", name_srnm)]  # remove all " JR" suffixes from last name
        dt[, name_srnm := gsub("-JR$", "", name_srnm)]  # remove all "-JR" suffixes from last name
        dt[, name_srnm := gsub("JR$", "", name_srnm)]  # remove all "JR" suffixes from last name
        dt[, name_srnm := gsub("JR I$", "", name_srnm)]  # remove all "JR I" suffixes from last name
        dt[, name_srnm := gsub("JR II$", "", name_srnm)]  # remove all "JR II" suffixes from last name
        dt[, name_srnm := gsub("JR III$", "", name_srnm)]  # remove all "JR III" suffixes from last name
        dt[, name_srnm := gsub(" II$", "", name_srnm)]  # remove all " II" suffixes from last name
        dt[, name_srnm := gsub(" III$", "", name_srnm)]  # remove all " III" suffixes from last name
        dt[, name_srnm := gsub(" IV$", "", name_srnm)]  # remove all " IV" suffixes from last name
        #dt[, name_gvn := gsub("-", " ", name_gvn)]  # commented out but left here to document that this made the matching worse
        
        # First names
        dt[, name_gvn := gsub("[0-9]", "", name_gvn)]  # remove any numbers that may be present in first names
        dt[, name_gvn := gsub("\\.", "", name_gvn)]  # remove periods from first name, e.g., JONES JR. >> JONES JR
        dt[name_gvn!="JR", name_gvn := gsub("JR$", "", name_gvn)]  # remove all "JR" suffixes from first name, but keep if it is the full first name
        dt[, name_gvn := gsub(" JR$", "", name_gvn)]  # remove all " JR" suffixes from first name
        dt[, name_gvn := gsub("-JR$", "", name_gvn)]  # remove all "-JR" suffixes from first name
        dt[, name_gvn := gsub("JR I$", "", name_gvn)]  # remove all "JR I" suffixes from first name
        dt[, name_gvn := gsub("JR II$", "", name_gvn)]  # remove all "JR II" suffixes from first name
        dt[, name_gvn := gsub("JR III$", "", name_gvn)]  # remove all "JR III" suffixes from first name
        dt[, name_gvn := gsub("-", "", name_gvn)]  # remove all hyphens from first names because use is inconsistent. Found that replacing with a space was worse
        
        # Middle initials
        dt[, name_mdl := gsub("[0-9]", "", name_mdl)]  # remove any numbers that may be present in middle initial
        dt[!(grep("[A-Z]", name_mdl)), name_mdl := NA] # when middle initial is not a letter, replace it with NA
        
        # Ensure SSN are nine digits by adding preceding zeros
        dt[nchar(ssn) < 9, ssn := str_pad(ssn, 9, pad = "0")]
        
        # Set column order
        setcolorder(dt, c(grep("id", names(dt), value = TRUE), "ssn", "name_srnm", "name_gvn", "name_mdl", "gender", "dob_y", "dob_m", "dob_d"))
        
        # Not useable if missing SSN & DOB
        dt <- dt[!(is.na(ssn) & is.na(dob_y) & is.na(dob_m) & is.na(dob_d))]
        
      }
      
      cleaning.function(mcare)
      cleaning.function(pha)
      
## (4) Create function to subset out linked pairs using a defined cutpoint ####
      get.linked.pairs <- function(mod.pairs, cutpoint){          
        # convert weight to numeric
        mod.pairs[, Weight := as.numeric(as.character(Weight))]
        
        # drop empty rows
        mod.pairs <- mod.pairs[id !=""]
        
        # ascribe same weight to both rows of each pair
        mod.pairs[, pair.id := .I] # unique identifier for each row
        mod.pairs[pair.id %% 2 == 0, pair.id := as.integer(pair.id - 1)] # since they are pairs, change all pair.id for even rows to pair.id for previous odd row
        mod.pairs[, Weight := sum(Weight, na.rm = T), by = c("pair.id")] 
        
        # save the pairs above the threshhold
        mod.pairs <- mod.pairs[Weight >= cutpoint, ]
        mod.pairs[, id_mcare := as.character(id_mcare)]
        mod.mcare <- mod.pairs[id_mcare %like% "^G" & nchar(id_mcare) == 15] # Mcare IDs start with G and are 15 digits long
        mod.pha <- mod.pairs[!(id_mcare %in% mod.mcare$id_mcare)] # if not MCARE ID, must be PHA
        
        # create linkage file when ssn is blocking field
        mod.match <- merge(mod.pha, mod.mcare, by = c("pair.id"), all = TRUE)
        mod.match <- mod.match[, .(id_mcare.x, id_mcare.y)] # keep only the ids
        setnames(mod.match, c("id_mcare.x", "id_mcare.y"), c("pid", "id_mcare"))
        
        # save mod.match
        return(mod.match)
      }   

## (5) Create function to check for nested names ####
NameContains <- function(str1, str2){ # From Eric Ossiander, WA DOH
  # Function to compare two strings.
  # Returns:
  #   1 if the shorter string is contained in the other
  #   0 otherwise
  # if the shorter string is longer than 6 characters, then only
  # the first 6 characters are used.
  score     <- rep(NA,length(str1))
  longname  <- rep(NA,length(str1))
  shortname <- rep(NA,length(str1))
  for(i in 1:length(str1)){
    if(str1[i]=='' | str2[i]=='' | is.na(str1[i]) | is.na(str2[i])) score[i] <- NA else{
      if(str1[i] == str2[i]) score[i] <- 1 else {
        if(nchar(str1[i]) >= nchar(str2[i])){longname[i] <- str1[i];
        shortname[i] <- str2[i]} else {longname[i] <- str2[i]; shortname[i] <- str1[i]}
        if(nchar(shortname[i]) < 3) score[i] <- 0 else {
          if(nchar(shortname[i]) > 6)shortname[i] <- substr(shortname[i],1,6)
          score[i] <- if(grepl(shortname[i],longname[i]))1 else 0
        }}}}
  return(score)
}

## (6) ---------- MATCH DATA ---------- ####
    # Match  1 - Perfect Deterministic ####
          perfect.match <- merge(mcare, pha, by = c("ssn", "dob_y", "dob_m", "dob_d", "gender", "name_srnm", "name_gvn", "name_mdl"), all=FALSE) 
          perfect.match <- perfect.match[, .(pid, id_mcare)] # keep the paired ids only
          
          #removing the perfectly linked data from the Mcare & PHA datasets because there is no need to perform additional record linkage functions on them
          mcare <- mcare[!(id_mcare %in% perfect.match$id_mcare)]
          pha <- pha[!(pid %in% perfect.match$pid)]
    
    # Match  2 - Perfect Determinisitic, allowing for last name change for females ----
          near.perfect.match <- merge(mcare, pha, by = c("ssn", "dob_y", "dob_m", "dob_d", "gender", "name_gvn", "name_mdl"), all=FALSE) 
          near.perfect.match <- near.perfect.match[, .(pid, id_mcare)] # keep the paired ids only
          
          # remove the linked data from the two parent datasets so we don't try to link them again
          mcare <- mcare[!(id_mcare %in% near.perfect.match$id_mcare)]
          pha <- pha[!(pid %in% near.perfect.match$pid)]
    
    # Match  3 - Probabilistic: Block on SSN, DOB, gender & middle initial, soundex for first & last name ----
            mod3 <- compare.linkage(mcare, pha, 
                                    blockfld = c("ssn", "dob_y", "dob_m", "dob_d", "gender", "name_mdl"), # blocking
                                    phonetic = c("name_srnm", "name_gvn"), phonfun = soundex)  # use phonetics for names, rather than spelling
    
          # get summary of potential pairs
            summary(mod3) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
            mod3.weights <- epiWeights(mod3) 
            summary(mod3.weights)
            
          # get paired data, with weights, as a dataset
            mod3.pairs <- setDT(getPairs(mod3.weights, single.rows = TRUE))
            mod3.pairs.long <- setDT(getPairs(mod3.weights, single.rows = FALSE)) # easier to compare when long, but need wide to extract the id pairs
          
          # classify pairs using a threshhold
            summary(epiClassify(mod3.weights, threshold.upper = 0.46)) # Visually confirmed that matches above threshhold are strongly plausible
          
          # get linked pairs
            mod3.match <- get.linked.pairs(mod3.pairs.long, 0.46)       
          
          # remove the linked data from the two parent datasets so we don't try to link them again
            mcare <- mcare[!(id_mcare %in% mod3.match$id_mcare)]
            pha <- pha[!(pid %in% mod3.match$pid)]
    
          # clean objects in memory
          rm(mod3, mod3.weights, mod3.pairs, mod3.pairs.long) # drop tables only needed to form the linkage      
          
    # Match  4 - Probabilistic: Block on SSN, string compare middle initial, gender, dob, soundex on first & last name ----
            mod4 <- compare.linkage(mcare, pha, blockfld = c("ssn"),
                                    strcmp = c("name_mdl", "gender", "dob_y", "dob_m", "dob_d"),
                                    phonetic = c("name_srnm", "name_gvn"), phonfun = soundex)
          
          # get summary of potential pairs
            summary(mod4) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
            mod4.weights <- epiWeights(mod4) 
            summary(mod4.weights)
            
          # get paired data, with weights, as a dataset
            mod4.pairs <- setDT(getPairs(mod4.weights, single.rows = TRUE))
            mod4.pairs.long <- setDT(getPairs(mod4.weights, single.rows = FALSE)) # easier to compare when long, but need wide to extract the id pairs
          
          # classify pairs using a threshhold
            summary(epiClassify(mod4.weights, threshold.upper = 0.39)) # Visually confirmed that matches above threshhold are strongly plausible
          
          # get linked pairs
            mod4.match <- get.linked.pairs(mod4.pairs.long, 0.39)       
          
          # remove the linked data from the two parent datasets so we don't try to link them again
            pha <- pha[!(pid %in% mod4.match$pid)]
            mcare <- mcare[!(id_mcare %in% mod4.match$id_mcare)]
            
          # clean objects in memory
            rm(mod4, mod4.weights, mod4.pairs, mod4.pairs.long) # drop tables only needed to form the linkage   
          
    # Match  5 - Probabilistic: Block on DOB, string compare for SSN, gender, & middle initial, soundex for first and last name ####
          mod5 <- compare.linkage(mcare, pha, 
                                  blockfld = c("dob_y", "dob_m", "dob_d"), # blocking
                                  strcmp = c("ssn", "gender","name_mdl"), # computer similarity between two
                                  phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
          
          # get summary of potential pairs
          summary(mod5) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod5.weights <- epiWeights(mod5) 
          summary(mod5.weights)
          
          # get paired data, with weights, as a dataset
          mod5.pairs <- setDT(getPairs(mod5.weights, single.rows = TRUE))
          mod5.pairs.long <- setDT(getPairs(mod5.weights, single.rows = FALSE)) # easier to compare when long, but need wide to extract the id pairs
          
          # classify pairs using a threshhold
          summary(epiClassify(mod5.weights, threshold.upper = 0.62)) # Visually confirmed that matches above threshhold are strongly plausible
          
          # get linked pairs
          mod5.match <- get.linked.pairs(mod5.pairs.long, 0.62)       
          
          # remove the linked data from the two parent datasets so we don't try to link them again
          pha <- pha[!(pid %in% mod5.match$pid)]
          mcare <- mcare[!(id_mcare %in% mod5.match$id_mcare)]
          
          # clean objects in memory
          rm(mod5, mod5.weights, mod5.pairs, mod5.pairs.long) # drop tables only needed to form the linkage   
          
    # Match  6 - Probabilistic: Block on DOB + last name + gender, string compare for first name, exclude SSN ... when PHA missing SSN ----
          pha.mi.ssn <- pha[is.na(ssn)] # try linkage with pha data missing SSN
          
          mod6 <- compare.linkage(mcare, pha.mi.ssn, 
                                  blockfld = c("dob_y", "dob_m", "dob_d", "name_srnm", "gender"), # blocking
                                  strcmp = c("name_mdl", "name_gvn"), # compare similarity between two
                                  exclude = c("ssn") )
          
          # get summary of potential pairs
          summary(mod6) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod6.weights <- epiWeights(mod6) 
          summary(mod6.weights)
          
          # get paired data, with weights, as a dataset
          mod6.pairs <- setDT(getPairs(mod6.weights, single.rows = TRUE))
          mod6.pairs.long <- setDT(getPairs(mod6.weights, single.rows = FALSE))
          
          # visualize the weight distribution
          hist(as.numeric(as.character(mod6.pairs$Weight)), breaks = 100) 
          
          # classify pairs using a threshhold
          summary(epiClassify(mod6.weights, threshold.upper = 0.62)) # based on visual inspection of curve and dataset with weights     
          
          # get linked pairs
          mod6.match <- get.linked.pairs(mod6.pairs.long, 0.62)            
          
          # drop the linked data from the two parent datasets so we don't try to link them again
          pha <- pha[!(pid %in% mod6.match$pid)]
          mcare <- mcare[!(id_mcare %in% mod6.match$id_mcare)]            
          
          # clean objects in memory
          rm(mod6, mod6.weights, mod6.pairs, mod6.pairs.long, pha.mi.ssn) # drop tables only needed to form the linkage        
          
    # Match  7 - Probabilistic: Block on DOB + last name + gender, string compare for first name, exclude SSN ... when Mcare missing SSN ----
          mcare.mi.ssn <- mcare[is.na(ssn)] # try linkage with pha data missing SSN
          
          mod7 <- compare.linkage(mcare.mi.ssn, pha, 
                                  blockfld = c("dob_y", "dob_m", "dob_d", "name_srnm", "gender"), # blocking
                                  strcmp = c("name_mdl", "name_gvn"), # compare similarity between two
                                  exclude = c("ssn") )
          
          # get summary of potential pairs
          summary(mod7) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod7.weights <- epiWeights(mod7) 
          summary(mod7.weights)
          
          # get paired data, with weights, as a dataset
          mod7.pairs <- setDT(getPairs(mod7.weights, single.rows = TRUE))
          mod7.pairs.long <- setDT(getPairs(mod7.weights, single.rows = FALSE))
          
          # visualize the weight distribution
          hist(as.numeric(as.character(mod7.pairs$Weight)), breaks = 100) 
          
          # classify pairs using a threshhold
          summary(epiClassify(mod7.weights, threshold.upper = 0.62)) # based on visual inspection of curve and dataset with weights     
          
          # get linked pairs
          mod7.match <- get.linked.pairs(mod7.pairs.long, 0.62)            
          
          # drop the linked data from the two parent datasets so we don't try to link them again
          pha <- pha[!(pid %in% mod7.match$pid)]
          mcare <- mcare[!(id_mcare %in% mod7.match$id_mcare)]            
          
          # clean objects in memory
          rm(mod7, mod7.weights, mod7.pairs, mod7.pairs.long, mcare.mi.ssn) # drop tables only needed to form the linkage       
          
    # Match  8 - Probabilistic: Block year + mo + all names + gender, string compare SSN + day ----      
          mod8 <- compare.linkage(mcare, pha, 
                                  blockfld = c("dob_y", "dob_m", "name_mdl", "name_gvn", "name_srnm", "gender"), # blocking
                                  strcmp = c("ssn", "dob_d")) # computer similarity between two
          
          # get summary of potential pairs
          summary(mod8) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod8.weights <- epiWeights(mod8) 
          summary(mod8.weights)
          
          # get paired data, with weights, as a dataset
          mod8.pairs <- setDT(getPairs(mod8.weights, single.rows = TRUE))
          mod8.pairs.long <- setDT(getPairs(mod8.weights, single.rows = FALSE))
          
          # classify pairs using a threshhold
          summary(epiClassify(mod8.weights, threshold.upper = 0.72)) # based on visual inspection of curve and dataset with weights     
          
          # get linked pairs
          mod8.match <- get.linked.pairs(mod8.pairs.long, 0.72)            
          
          # drop the linked data from the two parent datasets so we don't try to link them again
          pha <- pha[!(pid %in% mod8.match$pid)]
          mcare <- mcare[!(id_mcare %in% mod8.match$id_mcare)]            
          
          # clean objects in memory
          rm(mod8, mod8.pairs, mod8.pairs.long, mod8.weights)      
          
    # Match  9 - Probabilistic: Block year + mo + all names + gender, string compare SSN + day, names use exact spelling ... NOT USEFUL----    
          mod9 <- compare.linkage(mcare, pha, 
                                  blockfld = c("dob_y", "dob_m", "name_mdl", "name_gvn", "name_srnm", "gender"), # blocking
                                  strcmp = c("ssn", "dob_d"), strcmpfun = levenshteinSim)  # computer similarity between two
          # used levenshtein distance because it is more conserviative and causes greater separation with more than minor SSN deviations
          
          # get summary of potential pairs
          summary(mod9) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod9.weights <- epiWeights(mod9) 
          summary(mod9.weights)
          
          # get paired data, with weights, as a dataset
          mod9.pairs <- setDT(getPairs(mod9.weights, single.rows = TRUE))
          mod9.pairs.long <- setDT(getPairs(mod9.weights, single.rows = FALSE))
          
          # visualize the weight distribution
          hist(as.numeric(as.character(mod9.pairs$Weight)), breaks = 100) 
          
          # classify pairs using a threshhold
          # summary(epiClassify(mod9.weights, threshold.upper = 0.99)) # based on visual inspection of curve and dataset with weights
          
          # get linked pairs
          # mod9.match <- get.linked.pairs(mod9.pairs.long, 0.99)     
          
          # remove the linked data from the two parent datasets so we don't try to link them again
          # pha <- pha[!(pid %in% mod9.match$pid)]
          # mcare <- mcare[!(id_mcare %in% mod9.match$id_mcare)]
          
          # clean objects in memory
          rm(mod9, mod9.weights, mod9.pairs, mod9.pairs.long) # drop tables only needed to form the linkage        
          
          
    # Match  10 - Probabilistic: Block DOB + gender, string compare SSN + names ----    
          mod10 <- compare.linkage(mcare, pha, 
                                   blockfld = c("dob_y", "dob_m", "dob_d", "gender"), # blocking
                                   strcmp = c("ssn", "name_mdl", "name_gvn", "name_srnm"), strcmpfun = levenshteinSim) # computer similarity between strings
          #phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
          
          # get summary of potential pairs
          summary(mod10) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod10.weights <- epiWeights(mod10) 
          summary(mod10.weights)
          
          # get paired data, with weights, as a dataset
          #mod10.pairs <- setDT(getPairs(mod10.weights, single.rows = TRUE))
          mod10.pairs.long <- setDT(getPairs(mod10.weights, single.rows = FALSE))
          
          # visualize the weight distribution
          hist(as.numeric(as.character(mod10.pairs.long$Weight)), breaks = 100) 
          
          # classify pairs using a threshhold
          summary(epiClassify(mod10.weights, threshold.upper = 0.625)) # based on visual inspection of curve and dataset with weights
          
          # get linked pairs
          mod10.match <- get.linked.pairs(mod10.pairs.long, 0.625)     
          
          # remove the linked data from the two parent datasets so we don't try to link them again
          pha <- pha[!(pid %in% mod10.match$pid)]
          mcare <- mcare[!(id_mcare %in% mod10.match$id_mcare)]
          
          # clean objects in memory
          rm(mod10, mod10.weights, mod10.pairs.long) # drop tables only needed to form the linkage                   
          
          # there were other fairly certain matches that remained using this method, but a probability cut-off woudln't work. Woudl need some kind of machine learning.
          
          
    # Match  11 - Probabilistic: MEMORY OVERLOADED ... Block DOB year + DOB mon + gender, string compare DOB day + SSN + middle initial, soundex first & last name ----
          # mod11 <- compare.linkage(mcare, pha, 
          #                         blockfld = c("dob_y", "dob_m", "gender"), # blocking
          #                         strcmp = c("ssn", "dob_d", "name_mdl"),  # computer similarity between two
          #                         phonetic = c("name_srnm", "name_gvn"), phonfun = soundex)
          # 
          # # get summary of potential pairs
          # summary(mod11) 
          # 
          # # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          # mod11.weights <- epiWeights(mod11) 
          # summary(mod11.weights)
          # gc()
          # 
          # # get paired data, with weights, as a dataset
          # # mod11.pairs <- setDT(getPairs(mod11.weights, single.rows = TRUE))
          # mod11.pairs.long <- setDT(getPairs(mod11.weights, single.rows = FALSE))
          # 
          # # classify pairs using a threshhold
          # summary(epiClassify(mod11.weights, threshold.upper = 0.72)) # based on visual inspection of curve and dataset with weights     
          # 
          # # get linked pairs
          # mod11.match <- get.linked.pairs(mod11.pairs.long, 0.72)            
          # 
          # # drop the linked data from the two parent datasets so we don't try to link them again
          # pha <- pha[!(pid %in% mod11.match$pid)]
          # mcare <- mcare[!(id_mcare %in% mod11.match$id_mcare)]            
          # 
          # # clean objects in memory
          # rm(mod11, mod11.pairs.long, mod11.weights)      
          
    # Match  12 - Determinisitc: match everything except for SSN -----
          d1.match <- merge(pha, mcare, by = c("dob_y", "dob_m", "dob_d", "gender","name_mdl", "name_srnm", "name_gvn"), all = FALSE)
          d1.match <- d1.match[, . (pid, id_mcare)] # keep identifiers only
          pha <- pha[!(pid %in% d1.match$pid)] # remove the matched rows from the parent pha dataset
          mcare <- mcare[!(id_mcare %in% d1.match$id_mcare)] # remove the matched rows from the parent mcare dataset
    
    # Match  13 - Deterministic: match everything ecept for SSN and middle initial ----
          d2.match <- merge(pha, mcare, by = c("dob_y", "dob_m", "dob_d", "gender", "name_gvn", "name_srnm"))
          d2.match <- d2.match[(is.na(ssn.x) | is.na(ssn.y)) | ssn.x == ssn.y, ] # keep when SSN matches or >= 1 SSN is missing
          d2.match <- d2.match[, . (pid, id_mcare)] # keep identifiers only
          pha <- pha[!(pid %in% d2.match$pid)] # remove the matched rows from the parent pha dataset
          mcare <- mcare[!(id_mcare %in% d2.match$id_mcare)] # remove the matched rows from the parent mcare dataset
          
    # Match  14 - Deterministic: Complete match when month and day are switched ----
          mcare.alt <- copy(mcare)
          setnames(mcare.alt, c("dob_m", "dob_d"), c("dob_m.orig", "dob_d.orig"))
          mcare.alt[, dob_m := dob_d.orig][, dob_d := dob_m.orig]
          
          d3.match <- merge(pha, mcare.alt, by = c("dob_y", "dob_m", "dob_d", "gender","name_mdl", "name_srnm", "name_gvn", "ssn"), all = FALSE)
          d3.match <- d3.match[, .(pid, id_mcare)]
          
          pha <- pha[!(pid %in% d3.match$pid)] # remove the matched rows from the parent pha dataset
          mcare <- mcare[!(id_mcare %in% d3.match$id_mcare)] # remove the matched rows from the parent mcare dataset
          rm(mcare.alt)
          
    # Match  15 - Deterministic: DOB & name when day has an additional 10 ----
          # tweak mcare data
          mcare.alt <- copy(mcare)
          setnames(mcare.alt, c("dob_d"), c("dob_d.orig"))
          mcare.alt[, dob_d := as.character(as.numeric(dob_d.orig) + 10)][, dob_d.orig := NULL]
          
          # tweak pha data  
          pha.alt <- copy(pha)
          setnames(pha.alt, c("dob_d"), c("dob_d.orig"))
          pha.alt[, dob_d := as.character(as.numeric(dob_d.orig) + 10)][, dob_d.orig := NULL]
          
          d4.match <- rbind(
            merge(pha, mcare.alt, by = c("dob_y", "dob_m", "dob_d", "gender","name_mdl", "name_srnm", "name_gvn", "ssn"), all = FALSE), 
            merge(pha.alt, mcare, by = c("dob_y", "dob_m", "dob_d", "gender","name_mdl", "name_srnm", "name_gvn", "ssn"), all = FALSE)
          )
          
          d4.match <- d4.match[, .(pid, id_mcare)]
          
          pha <- pha[!(pid %in% d4.match$pid)] # remove the matched rows from the parent pha dataset
          mcare <- mcare[!(id_mcare %in% d4.match$id_mcare)] # remove the matched rows from the parent mcare dataset
          rm(mcare.alt, pha.alt)
          
    # Match  16 - Deterministic: Switch first and last names (happens sometimes, especiall with East Asian patients) ----
          mcare.alt <- copy(mcare)
          setnames(mcare.alt, c("name_gvn", "name_srnm"), c("name_gvn.orig", "name_srnm.orig"))
          mcare.alt[, name_gvn := name_srnm.orig][, name_srnm := name_gvn.orig]
          
          d5.match <- merge(pha, mcare.alt, by = c("dob_y", "dob_m", "dob_d", "gender","name_srnm", "name_gvn"), all = FALSE)
          d5.match <- d5.match[(is.na(ssn.x) | is.na(ssn.y)) | ssn.x == ssn.y, ] # keep when SSN matches or >= 1 SSN is missing
          
          d5.match <- d5.match[, .(pid, id_mcare)]
          
          pha <- pha[!(pid %in% d5.match$pid)] # remove the matched rows from the parent pha dataset
          mcare <- mcare[!(id_mcare %in% d5.match$id_mcare)] # remove the matched rows from the parent mcare dataset
          rm(mcare.alt)
          
    
# ---------- END OF MATCHING ---------- ####
## (7) Combine matched IDs ####
      # identify tables of matched pairs
          xwalk.list <- as.list(mget(grep(".match", ls(), value = TRUE)))
      
      # combine tables of matched pairs
          xwalk <- rbindlist(xwalk.list, use.names = TRUE)       
      
      # deduplicate entire rows (just in case, should not be needed)
          xwalk <- unique(xwalk)     
      
      # check for duplicate ids
          xwalk[, dup.pid := .N, by = pid]
          # View(mcare[id_mcare %in% xwalk[dup.pid!=1]$id_mcare]) # need to reload mcare data to see these duplicates
          
          xwalk[, dup.mcare := .N, by = id_mcare]
          # View(pha[pid %in% xwalk[dup.mcare!=1]$pid]) # need to reload pid data to see these duplicates
      
      # If a PHA member has two different IDs, just keep the larger
          xwalk[, pid := as.numeric(pid)]
          setorder(xwalk, -pid)
          xwalk[, dup.mcare := 1:.N, by = id_mcare]
          xwalk <- xwalk[dup.mcare != 2] # when mcare id duplicated, it is because it matched with two PHA with same data. Keep one with larger PID
          xwalk <- xwalk[, .(pid, id_mcare)]

## (8) Load xwalk table to SQL ----
      # create last_run timestamp
          xwalk[, last_run := Sys.time()]
          
      # create table ID for SQL
          tbl_id <- DBI::Id(schema = "stage", 
                            table = "xwalk_04_linkage_mcare_pha")  
          
      # Identify the column types to be created in SQL
          sql.columns <- c("pid" = "integer", "id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "last_run" = "datetime")  
          
      # ensure column order in R is the same as that in SQL
          setcolorder(xwalk, names(sql.columns))
          
      # Write table to SQL
          dbWriteTable(db_claims, 
                       tbl_id, 
                       value = as.data.frame(xwalk),
                       overwrite = T, append = F, 
                       field.types = sql.columns)
          
      # Confirm that all rows were loaded to sql
          stage.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                                     "SELECT COUNT (*) FROM stage.xwalk_04_linkage_mcare_pha"))
          if(stage.count != nrow(xwalk))
            stop("Mismatching row count, error writing or reading data")      
          
          dbDisconnect(db_claims)   
          
## (9) Print summary of matching contributions ----
          # print out how many observations were produced by each matching method/model
          for(i in 1:length(xwalk.list)){
            print(paste0("model: ", names(xwalk.list[i]), ", obs:", nrow(xwalk.list[[i]])))
          }
          
## The end! ----
          run.time <- Sys.time() - start.time  
          print(run.time)