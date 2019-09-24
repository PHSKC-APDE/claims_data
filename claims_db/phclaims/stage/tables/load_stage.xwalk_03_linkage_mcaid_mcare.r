## Header ----
  # Author: Danny Colombara
  # Date: April 30, 2019
  # R version: 3.5.3
  # Puprpose: Establish Medicare - Medicaid links using deterministic and probabilistic methods
  #

## Set up R Environment ----
  rm(list=ls())  # clear memory
  # .libPaths("C:/Users/dcolombara/R.packages") # needed for 32 GB SAS computer.
  pacman::p_load(data.table, odbc, DBI, tidyr, lubridate, RecordLinkage) # load packages
  options(scipen = 999) # set high threshhold for R to use scientific notation
  options(warning.length = 8170) # get lengthy warnings, needed for SQL
  
  start.time <- Sys.time()
  
## (1) Create function to subset out linked pairs using a defined cutpoint ----
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
            mod.mcaid <- mod.pairs[grep("WA", id_mcare) , ] # all ids that start with a "WA" are Mcaid
            mod.mcare <- mod.pairs[!(id_mcare %in% mod.mcaid$id_mcare)] # data that is not mcaid must be mcare
            
        # create linkage file when ssn is blocking field
            mod.match <- merge(mod.mcaid, mod.mcare, by = c("pair.id"), all = TRUE)
            mod.match <- mod.match[, .(id_mcare.x, id_mcare.y)] # keep only the ids
            setnames(mod.match, c("id_mcare.x", "id_mcare.y"), c("id_mcaid", "id_mcare"))
        
        # save mod.match
           return(mod.match)
}   
  
## (2) Create function to check for nested names ####
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
  
## (3) Connect to SQL Server ----    
    db_claims <- dbConnect(odbc(), "PHClaims51")   

## (4) Load data from SQL ----
    mcaid <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcaid, ssn, dob, name_srnm, name_gvn, name_mdl, gender_me FROM stage.xwalk_01_linkage_prep_mcaid"))
    mcare <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcare, ssn, dob, name_srnm, name_gvn, name_mdl, gender_me FROM stage.xwalk_02_linkage_prep_mcare"))

## (5) Clean up & standardize datasets ----
    cleaning.function <- function(dt){
      # Change sex to numeric for improved strcmp function
      dt[gender_me == "Multiple", gender_me := "0"]
      dt[gender_me == "Male", gender_me := "1"]
      dt[gender_me == "Female", gender_me := "2"]
      dt[gender_me == "Unknown", gender_me := NA_character_]
      
      # Extract date components
      dt[, dob.year := as.character(year(dob))] # extract year
      dt[, dob.month := as.character(month(dob))] # extract month
      dt[, dob.day := as.character(day(dob))] # extract day
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

    }
    
    cleaning.function(mcare)
    cleaning.function(mcaid)

## (6) Perfect deterministic matches: identification and extraction ----
      perfect.match <- merge(mcare, mcaid, by = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me", "name_srnm", "name_gvn", "name_mdl"), all=FALSE) 
      perfect.match <- perfect.match[, .(id_mcaid, id_mcare)] # keep the paired ids only
      
      #removing the perfectly linked data from the Mcaid and Mcare datasets because there is no need to perform additional record linkage functions on them
      mcare <- mcare[!(id_mcare %in% perfect.match$id_mcare)]
      mcaid <- mcaid[!(id_mcaid %in% perfect.match$id_mcaid)]

## (7) Probabilistic linkage - performed sequentially ----
    # Model 1: No blocking  ..... Not enough memory (Error: cannot allocate vector of size 1195.6 Gb) ----
        # mod1 <- compare.linkage(mcare, mcaid, 
        #                         strcmp = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me","name_mdl"), # computer similarity between two
        #                         phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
    
    # Model 2: Block on SSN, string compare dob and gender, soundex for first & last name ----
        mod2 <- compare.linkage(mcare, mcaid, 
                                  blockfld = c("ssn"), # blocking
                                  strcmp = c("dob.year", "dob.month", "dob.day", "gender_me","name_mdl"), # computer similarity between two
                                  phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
        
        # get summary of potential pairs
            summary(mod2) 
            
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
            mod2.weights <- epiWeights(mod2) 
            summary(mod2.weights)
            
        # get paired data, with weights, as a dataset
            mod2.pairs <- setDT(getPairs(mod2.weights, single.rows = TRUE))
            mod2.pairs.long <- setDT(getPairs(mod2.weights, single.rows = FALSE)) # easier to compare when long, but need wide to extract the id pairs
            
        # visualize the weight distribution
            hist(as.numeric(as.character(mod2.pairs$Weight)), breaks = 100) 
            
        # classify pairs using a threshhold
            summary(epiClassify(mod2.weights, threshold.upper = 0.43)) # SSN is a superb identifier. Visually confirmed that matches above threshhold are strongly plausible

        # get linked pairs
            mod2.match <- get.linked.pairs(mod2.pairs.long, 0.43)       
            
        # remove the linked data from the two parent datasets so we don't try to link them again
            mcaid <- mcaid[!(id_mcaid %in% mod2.match$id_mcaid)]
            mcare <- mcare[!(id_mcare %in% mod2.match$id_mcare)]
              
        # clean objects in memory
            rm(mod2, mod2.weights, mod2.pairs, mod2.pairs.long) # drop tables only needed to form the linkage

    # Model 3: Block on DOB, string compare for SSN and gender, soundex for first and last name ----
          mod3 <- compare.linkage(mcare, mcaid, 
                                  blockfld = c("dob.year", "dob.month", "dob.day"), # blocking
                                  strcmp = c("ssn", "gender_me","name_mdl"), # computer similarity between two
                                  phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
              
        # get summary of potential pairs
            summary(mod3) 
            
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
            mod3.weights <- epiWeights(mod3) 
            summary(mod3.weights)
            
         # get paired data, with weights, as a dataset
            # mod3.pairs <- setDT(getPairs(mod3.weights, single.rows = TRUE)) # want it on one row, so can identify when SSN missing in Mcaid
            mod3.pairs.long <- setDT(getPairs(mod3.weights, single.rows = FALSE)) 
            
         # visualize the weight distribution
            hist(as.numeric(as.character(mod3.pairs$Weight)), breaks = 100) 
            
          # get linked pairs
            mod3.match <- rbind(
              mod3.pairs[!is.na(ssn.2) & Weight >= 0.595, .(id_mcare.1, id_mcaid.2)], # higher threshhold when there is a comparison SSN
              mod3.pairs[(is.na(ssn.1) | is.na(ssn.2)) & Weight >= 0.4, .(id_mcare.1, id_mcaid.2)]  # lower threshhold when there is no SSN for comparision
            )
            setnames(mod3.match, names(mod3.match), c("id_mcare", "id_mcaid"))
            
         # remove the linked data from the two parent datasets so we don't try to link them again
            mcaid <- mcaid[!(id_mcaid %in% mod3.match$id_mcaid)]
            mcare <- mcare[!(id_mcare %in% mod3.match$id_mcare)]
              
         # clean objects in memory
            rm(mod3, mod3.weights, mod3.pairs.long) # drop tables only needed to form the linkage
            
    # Model 4: Block on DOB + last name + gender, string compare for first name, exclude SSN ... when Mcaid missing SSN ----
        mcaid.mi.sss <- mcaid[is.na(ssn)] # try linkage with Mcaid data missing SSN
        
        mod4 <- compare.linkage(mcare, mcaid.mi.sss, 
                                blockfld = c("dob.year", "dob.month", "dob.day", "name_srnm", "gender_me"), # blocking
                                strcmp = c("name_mdl", "name_gvn"), # compare similarity between two
                                exclude = c("ssn") )
              
        # get summary of potential pairs
            summary(mod4) 
        
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
            mod4.weights <- epiWeights(mod4) 
            summary(mod4.weights)
        
        # get paired data, with weights, as a dataset
            mod4.pairs <- setDT(getPairs(mod4.weights, single.rows = TRUE))
            mod4.pairs.long <- setDT(getPairs(mod4.weights, single.rows = FALSE))
            
        # visualize the weight distribution
           hist(as.numeric(as.character(mod4.pairs$Weight)), breaks = 100) 
        
        # classify pairs using a threshhold
            summary(epiClassify(mod4.weights, threshold.upper = 0.60)) # based on visual inspection of curve and dataset with weights     
            
        # get linked pairs
            mod4.match <- get.linked.pairs(mod4.pairs.long, 0.60)            
            
        # drop the linked data from the two parent datasets so we don't try to link them again
            mcaid <- mcaid[!(id_mcaid %in% mod4.match$id_mcaid)]
            mcare <- mcare[!(id_mcare %in% mod4.match$id_mcare)]            

        # clean objects in memory
            rm(mod4, mod4.weights, mod4.pairs, mod4.pairs.long) # drop tables only needed to form the linkage  
            
    # Model 5: Block year + mo + all names + gender, string compare SSN + day ----      
          mod5 <- compare.linkage(mcare, mcaid, 
                                blockfld = c("dob.year", "dob.month", "name_mdl", "name_gvn", "name_srnm", "gender_me"), # blocking
                                strcmp = c("ssn", "dob.day")) # computer similarity between two

        # get summary of potential pairs
            summary(mod5) 
            
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
            mod5.weights <- epiWeights(mod5) 
            summary(mod5.weights)
            
         # get paired data, with weights, as a dataset
            mod5.pairs <- setDT(getPairs(mod5.weights, single.rows = TRUE))
            mod5.pairs.long <- setDT(getPairs(mod5.weights, single.rows = FALSE))
            
        # classify pairs using a threshhold
            summary(epiClassify(mod5.weights, threshold.upper = 0.72)) # based on visual inspection of curve and dataset with weights     
            
        # get linked pairs
            mod5.match <- get.linked.pairs(mod5.pairs.long, 0.72)            
            
        # drop the linked data from the two parent datasets so we don't try to link them again
            mcaid <- mcaid[!(id_mcaid %in% mod5.match$id_mcaid)]
            mcare <- mcare[!(id_mcare %in% mod5.match$id_mcare)]            
            
        # clean objects in memory
            rm(mod5, mod5.pairs, mod5.pairs.long, mod5.weights)
            
    # Model 6: Block year + mo + middle initial + gender, string compare SSN + day, names use soundex ... NOT USED----      
        mod6 <- compare.linkage(mcare, mcaid, 
                                blockfld = c("dob.year", "dob.month", "name_mdl", "gender_me"), # blocking
                                strcmp = c("ssn", "dob.day"), # computer similarity between two
                                phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling

        # get summary of potential pairs
            summary(mod6) 
        
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
            mod6.weights <- epiWeights(mod6) 
            summary(mod6.weights)
          
        # get paired data, with weights, as a dataset
            mod6.pairs <- setDT(getPairs(mod6.weights, single.rows = TRUE))
            mod6.pairs.long <- setDT(getPairs(mod6.weights, single.rows = FALSE))
            
        # get linked pairs
            # provides a list of potential matches, but the SSN and dob-day seem too far off to seem to be the same person  
        
        # STOP with this model because can see that it is not proving useful
            rm(mod6, mod6.pairs, mod6.pairs.long, mod6.weights)            

    # Model 7: Block year + mo + all names + gender, string compare SSN + day, names use exact spelling ----    
          mod7 <- compare.linkage(mcare, mcaid, 
                                blockfld = c("dob.year", "dob.month", "name_mdl", "name_gvn", "name_srnm", "gender_me"), # blocking
                                strcmp = c("ssn", "dob.day"), strcmpfun = levenshteinSim)  # computer similarity between two
                               # used levenshtein distance because it is more conserviative and causes greater separation with more than minor SSN deviations

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
            summary(epiClassify(mod7.weights, threshold.upper = 0.69)) # based on visual inspection of curve and dataset with weights
            
         # get linked pairs
            mod7.match <- get.linked.pairs(mod7.pairs.long, 0.69)     
          
        # remove the linked data from the two parent datasets so we don't try to link them again
            mcaid <- mcaid[!(id_mcaid %in% mod7.match$id_mcaid)]
            mcare <- mcare[!(id_mcare %in% mod7.match$id_mcare)]
          
        # clean objects in memory
            rm(mod7, mod7.weights, mod7.pairs, mod7.pairs.long) # drop tables only needed to form the linkage        

    # Model 8: Block DOB + gender, string compare SSN + names ----    
          mod8 <- compare.linkage(mcare, mcaid, 
                                blockfld = c("dob.year", "dob.month", "dob.day", "gender_me"), # blocking
                                strcmp = c("ssn", "name_mdl", "name_gvn", "name_srnm"), strcmpfun = levenshteinSim) # computer similarity between strings
                                #phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
              
        # get summary of potential pairs
            summary(mod8) 
            
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
            mod8.weights <- epiWeights(mod8) 
            summary(mod8.weights)
            
         # get paired data, with weights, as a dataset
            #mod8.pairs <- setDT(getPairs(mod8.weights, single.rows = TRUE))
            mod8.pairs.long <- setDT(getPairs(mod8.weights, single.rows = FALSE))
            
        # visualize the weight distribution
            hist(as.numeric(as.character(mod8.pairs.long$Weight)), breaks = 100) 
            
        # classify pairs using a threshhold
            summary(epiClassify(mod8.weights, threshold.upper = 0.595)) # based on visual inspection of curve and dataset with weights
            
        # get linked pairs
            mod8.match <- get.linked.pairs(mod8.pairs.long, 0.595)     
            
        # remove the linked data from the two parent datasets so we don't try to link them again
            mcaid <- mcaid[!(id_mcaid %in% mod8.match$id_mcaid)]
            mcare <- mcare[!(id_mcare %in% mod8.match$id_mcare)]
            
        # clean objects in memory
            rm(mod8, mod8.weights, mod8.pairs, mod8.pairs.long) # drop tables only needed to form the linkage                   
            
        # there were other fairly certain matches that remained using this method, but a probability cut-off woudln't work. Woudl need some kind of machine learning.

     # Model 9: NO MORE IDEAS FOR PROBABLISTIC LINKAGES ... ----
            
## (8) Manual additional deterministic linkages ----
    # Based on a manual review of some potential linkages that were note made, I found the following
            # middle initials are often missing for one or the other
            # day and month or birth are sometimes criss crossed
            # Compount names (e.g., Garcia Vasquez) are often in seeminlgy random order or only one of the two is used
            # east asian last and first names are often criss-crossed 
            # year, month, and day or birth are often off by one or two digits (typos)
            # When middle initial is missing in Mcare, it is often the first letter of the second last name in a compound last name
            
    # May use RecordLinkage to create pairs, within which I will apply some logical filtering to dermine if they are matches
    # It has become obvious to me that this is a task for Machine Learning, but I ran out of time to pursue that. 
            
    # Model 1: match everything except for SSN -----
          d1.match <- merge(mcaid, mcare, by = c("dob.year", "dob.month", "dob.day", "gender_me","name_mdl", "name_srnm", "name_gvn"), all = FALSE)
          d1.match <- d1.match[, . (id_mcaid, id_mcare)] # keep identifiers only
          mcaid <- mcaid[!(id_mcaid %in% d1.match$id_mcaid)] # remove the matched rows from the parent mcaid dataset
          mcare <- mcare[!(id_mcare %in% d1.match$id_mcare)] # remove the matched rows from the parent mcare dataset
          
    # Model 2: match everything ecept for SSN and middle initial ----
          d2.match <- merge(mcaid, mcare, by = c("dob.year", "dob.month", "dob.day", "gender_me", "name_gvn", "name_srnm"))
          d2.match <- d2.match[(is.na(ssn.x) | is.na(ssn.y)) | ssn.x == ssn.y, ] # keep when SSN matches or >= 1 SSN is missing
          d2.match <- d2.match[, . (id_mcaid, id_mcare)] # keep identifiers only
          mcaid <- mcaid[!(id_mcaid %in% d2.match$id_mcaid)] # remove the matched rows from the parent mcaid dataset
          mcare <- mcare[!(id_mcare %in% d2.match$id_mcare)] # remove the matched rows from the parent mcare dataset
                            
    # Model 3: Complete match when month and day are switched ----
            mcare.alt <- copy(mcare)
            setnames(mcare.alt, c("dob.month", "dob.day"), c("dob.month.orig", "dob.day.orig"))
            mcare.alt[, dob.month := dob.day.orig][, dob.day := dob.month.orig]
                        
            d3.match <- merge(mcaid, mcare.alt, by = c("dob.year", "dob.month", "dob.day", "gender_me","name_mdl", "name_srnm", "name_gvn", "ssn"), all = FALSE)
            d3.match <- d3.match[, .(id_mcaid, id_mcare)]
            
            mcaid <- mcaid[!(id_mcaid %in% d3.match$id_mcaid)] # remove the matched rows from the parent mcaid dataset
            mcare <- mcare[!(id_mcare %in% d3.match$id_mcare)] # remove the matched rows from the parent mcare dataset
            rm(mcare.alt)
            
    # Model 4: DOB & name when day has an additional 10 ----
          # tweak mcare data
            mcare.alt <- copy(mcare)
            setnames(mcare.alt, c("dob.day"), c("dob.day.orig"))
            mcare.alt[, dob.day := as.character(as.numeric(dob.day.orig) + 10)][, dob.day.orig := NULL]
          
          # tweak mcaid data  
            mcaid.alt <- copy(mcaid)
            setnames(mcaid.alt, c("dob.day"), c("dob.day.orig"))
            mcaid.alt[, dob.day := as.character(as.numeric(dob.day.orig) + 10)][, dob.day.orig := NULL]
            
            d4.match <- rbind(
              merge(mcaid, mcare.alt, by = c("dob.year", "dob.month", "dob.day", "gender_me","name_mdl", "name_srnm", "name_gvn", "ssn"), all = FALSE), 
              merge(mcaid.alt, mcare, by = c("dob.year", "dob.month", "dob.day", "gender_me","name_mdl", "name_srnm", "name_gvn", "ssn"), all = FALSE)
            )
            
            d4.match <- d4.match[, .(id_mcaid, id_mcare)]
            
            mcaid <- mcaid[!(id_mcaid %in% d4.match$id_mcaid)] # remove the matched rows from the parent mcaid dataset
            mcare <- mcare[!(id_mcare %in% d4.match$id_mcare)] # remove the matched rows from the parent mcare dataset
            rm(mcare.alt, mcaid.alt)
            
    # Model 5: Switch first and last names (happens sometimes, especiall with East Asian patients) ----
            mcare.alt <- copy(mcare)
            setnames(mcare.alt, c("name_gvn", "name_srnm"), c("name_gvn.orig", "name_srnm.orig"))
            mcare.alt[, name_gvn := name_srnm.orig][, name_srnm := name_gvn.orig]
            
            d5.match <- merge(mcaid, mcare.alt, by = c("dob.year", "dob.month", "dob.day", "gender_me","name_srnm", "name_gvn"), all = FALSE)
            d5.match <- d5.match[(is.na(ssn.x) | is.na(ssn.y)) | ssn.x == ssn.y, ] # keep when SSN matches or >= 1 SSN is missing
            
            d5.match <- d5.match[, .(id_mcaid, id_mcare)]
            
            mcaid <- mcaid[!(id_mcaid %in% d5.match$id_mcaid)] # remove the matched rows from the parent mcaid dataset
            mcare <- mcare[!(id_mcare %in% d5.match$id_mcare)] # remove the matched rows from the parent mcare dataset
            rm(mcare.alt)
            
    # Model 6: Check if compound names are nested in one another ----
            d6.match <- compare.linkage(mcare, mcaid, 
                                        blockfld = c("dob.year", "dob.month", "dob.day", "gender_me"), # blocking
                                        phonetic = c("name_srnm", "name_gvn"), phonfun = soundex,  # use phonetics for names, rather than spelling
                                        exclude = c( "name_mdl", "ssn"))

            d6.match <- setDT(getPairs(d6.match, single.rows = TRUE))[, is_match := 0]
            
            # check for nesting within first or last names
            d6.match[, srnm_match := NameContains(name_srnm.1, name_srnm.2)]
            d6.match[, srnm_match := NameContains(name_srnm.2, name_srnm.1)]
            d6.match[, gvn_match := NameContains(name_gvn.1, name_gvn.2)]
            d6.match[, gvn_match := NameContains(name_gvn.2, name_gvn.1)]
            
            # Keep if there is nesting (or equality) within the first and last names 
            d6.match <- d6.match[gvn_match == 1 & srnm_match == 1, ]
            
            # keep only id columns
            setnames(d6.match, c("id_mcaid.2", "id_mcare.1"), c("id_mcaid", "id_mcare"))
            d6.match <- d6.match[, .(id_mcaid, id_mcare)]
            
            mcaid <- mcaid[!(id_mcaid %in% d6.match$id_mcaid)] # remove the matched rows from the parent mcaid dataset
            mcare <- mcare[!(id_mcare %in% d6.match$id_mcare)] # remove the matched rows from the parent mcare dataset
              
## (9)Combine matched pairs ----
    # drop mcaid/mcare tables to free up memory
      rm(mcare, mcaid)
      gc()
            
    # identify tables of matched pairs
      xwalk.list <- as.list(mget(grep(".match", ls(), value = TRUE)))

    # combine tables of matched pairs
      xwalk <- rbindlist(xwalk.list, use.names = TRUE)       
            
    # deduplicate entire rows (just in case, should not be needed)
      xwalk <- unique(xwalk)
    
      
    # check for duplicate ids
      xwalk[, dup.id_mcaid := .N, by = id_mcaid]
      # View(mcare[id_mcare %in% xwalk[dup.id_mcaid!=1]$id_mcare]) # need to reload mcare data to see these duplicates
      
      xwalk[, dup.id_mcare := .N, by = id_mcare]
      # View(mcaid[id_mcaid %in% xwalk[dup.mcare!=1]$id_mcaid]) # need to reload id_mcaid data to see these duplicates
      
    # There are 38 duplicate id_mcaid, meaning they matched with two different id_mcare
      # Confirmed that these are duplicates with Medicare data (i.e., 1 person has two ids)
      # no good way to deduplicate at present, so will drop one of the two randomly
      
    # If a mcaid member has two different IDs, just keep the larger
      xwalk[, dup.mcare := 1:.N, by = id_mcaid]
      xwalk <- xwalk[dup.mcare != 2] # when mcare id duplicated, it is because it matched with two mcaid with same data. Keep one with larger id_mcaid
      xwalk <- xwalk[, .(id_mcaid, id_mcare)]

## (10) Load linkage table to SQL ----      
    # create last_run timestamp
      xwalk[, last_run := Sys.time()]
      
    # create table ID for SQL
      tbl_id <- DBI::Id(schema = "stage", 
                        table = "xwalk_03_linkage_mcaid_mcare")  
      
    # Identify the column types to be created in SQL
      sql.columns <- c("id_mcaid" = "char(11)", "id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "last_run" = "datetime")  
      
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
                                                 "SELECT COUNT (*) FROM stage.xwalk_03_linkage_mcaid_mcare"))
      if(stage.count != nrow(xwalk))
        stop("Mismatching row count, error writing or reading data")      
      
    dbDisconnect(db_claims)      
      
## (11) Print summary of matching contributions ----
      # print out how many observations were produced by each matching method/model
      for(i in 1:length(xwalk.list)){
        print(paste0("model: ", names(xwalk.list[i]), ", obs:", nrow(xwalk.list[[i]])))
      }
      
## The end! ----
    run.time <- Sys.time() - start.time  
    print(run.time)