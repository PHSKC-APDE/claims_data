## HEADER ####
  # Author: Danny Colombara
  # Date: October 25, 2019
  # Purpose: Create master ID linkage file, identifying people across Mcaid, Mcare, and PHA with a NEW ID_APDE
  #
  # Notes: When there is conflicting information, will assume the most recent data is correct because it reflects a correction
  #        The exception is when the newer data is missing, in which case it will be filled with the most recent non-missing
  # 
  #        When MBSF file changes in recording of date of birth (dob), we will assume that the most recent is correct
  #        because it would normally remain static unless a person wanted to make a correction
  #
  #        When there is a chance in race, we will treat it like a change in dob, meaning we assuming the most recent is correct  
  #
  #        When there is a change in gender, we will record both (i.e., we'll have a male and a female column, both binary)
  #        this will allow us to have sex== unknown, male, female, mutliple

## OVERVIEW ####
  # 1) Prepare file with all Mcaid Identifiers
  # 2) Prepare file with all MCARE Identifiers
  # 3) Prepare file with all PHA Identifiers
  # 4) Link Mcaid-Mcare 
  # 5) Link Mcare-PHA
  # 6) Link Mcaid-PHA
  # 7) Create 4-way linkage (APDE-MCARE-MCAID-PHA)
  # 8) Drop all temporary SQL tables 

## SET UP R ENVIRONMENT ----
    rm(list=ls()) # clear memory
    pacman::p_load(data.table, tidyverse, odbc, DBI, tidyr, RecordLinkage, lubridate) # load packages
    options("scipen"=999) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    
    start.time <- Sys.time()

## FUNCTIONS ... general data cleaning / prep procedures ----
    # Data prep ... clean names ----
      prep.names <- function(dt){
        # Remove any extraneous spaces at the beginning or end of a name
        dt[, name_gvn := gsub("^ ", "", name_gvn)]
        dt[, name_mdl := gsub("^ ", "", name_mdl)]
        dt[, name_srnm := gsub("^ ", "", name_srnm)]
        dt[, name_gvn := gsub(" $", "", name_gvn)]
        dt[, name_mdl := gsub(" $", "", name_mdl)]
        dt[, name_srnm := gsub(" $", "", name_srnm)]
        dt <- unique(dt)
        
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
        
        return(dt)
      }
    
    # Data prep ... clean dob ----
      prep.dob <- function(dt){
        # Extract date components
        dt[, dob.year := as.character(year(dob))] # extract year
        dt[, dob.month := as.character(month(dob))] # extract month
        dt[, dob.day := as.character(day(dob))] # extract day
        dt[, c("dob") := NULL] # drop vars that are not needed
        
        return(dt)
      }
    
    # Data prep .... clean sex ----
      prep.sex <- function(dt){
        # Change sex to numeric for improved strcmp function
        dt[gender_me == "Multiple", gender_me := "0"]
        dt[gender_me == "Male", gender_me := "1"]
        dt[gender_me == "Female", gender_me := "2"]
        dt[gender_me == "Unknown", gender_me := NA_character_]
        
        return(dt)
      }
    
    # RecordLinkage ... subset out linked pairs using a defined cutpoint ----
      get.linked.pairs.mcaid.mcare <- function(mod.pairs, cutpoint){          
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
      get.linked.pairs.mcare.pha <- function(mod.pairs, cutpoint){          
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
    
    # RecordLinkage ... check for nested names ####
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
    
## Identify objects/function to keep throughout entire process ----    
      keep.me <- c(ls(), "keep.me") # everything created above should be permanent
      
#### ----------------- ####
#### Prep MCAID DATA   ####   
#### ----------------- ####    
  ## (0) NOTE TO PREVENT FUTURE INSANITY ----
      # There are id_mcaid in stage.mcaid_elig that never appear in our elig_demo, so it possible for people to match with Mcare or PHA and not
      # appear in the elig_demo file
      
  ## (1) Load Mcaid data from SQL ----  
      db_claims51 <- dbConnect(odbc(), "PHClaims51")   
      
      mcaid.elig <- setDT(odbc::dbGetQuery(db_claims51, "SELECT id_mcaid, dob, gender_me, gender_female, gender_male FROM final.mcaid_elig_demo"))
  
      mcaid.names <- setDT(odbc::dbGetQuery(db_claims51, "SELECT MEDICAID_RECIPIENT_ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, CLNDR_YEAR_MNTH FROM stage.mcaid_elig"))
      setnames(mcaid.names, names(mcaid.names), c("id_mcaid", "name_gvn", "name_mdl", "name_srnm", "date"))
      
      mcaid.ssn <- setDT(odbc::dbGetQuery(db_claims51, "SELECT MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, CLNDR_YEAR_MNTH FROM stage.mcaid_elig"))
      setnames(mcaid.ssn, names(mcaid.ssn), c("id_mcaid", "ssn", "date"))
      
  ## (2) Tidy individual Mcaid data files before merging ----
      # elig ----
        mcaid.elig <- unique(mcaid.elig)
          if(nrow(mcaid.elig) - length(unique(mcaid.elig$id_mcaid)) != 0){
            stop('non-unique id_mcaid in elig')
          }
      
      # ssn ----
        # sort data
          setkey(mcaid.ssn, id_mcaid, date)
          
        # identify enrollees with a SSN and keep the newest SSN
          ssn.ok <- mcaid.ssn[!is.na(ssn)]
          ssn.ok <- ssn.ok[, rank := 1:.N, by = "id_mcaid"] 
          ssn.ok <- ssn.ok[rank == 1][, c("date", "rank") := NULL] 
          
        # identify enrollees who are missing SSN
          ssn.na <- unique(mcaid.ssn[!id_mcaid %in% ssn.ok$id_mcaid][, c("date") := NULL]) 
          
        # append the those without SSN to those with SSN
          mcaid.ssn <- rbind(ssn.ok, ssn.na)
          setkey(mcaid.ssn, id_mcaid)
          rm(ssn.ok, ssn.na)
          
        # ensure there is only one row per id
          if(nrow(mcaid.ssn) - length(unique(mcaid.ssn$id_mcaid)) != 0){
            stop('non-unique id_mcaid in ssn')
          }
          
      # name ----  
        # clean names using function
          mcaid.names <- prep.names(mcaid.names)
          mcaid.names <- prep.names(mcaid.names) # run second time because some people have suffixes like "blah JR IV", so first removes IV and second removes JR
          
        # sort data
          setkey(mcaid.names, id_mcaid, date)
          
        # Split those with consistent data from the others
          mcaid.names <- mcaid.names[, rank := 1:.N, by = c("id_mcaid", "name_gvn", "name_mdl", "name_srnm")] # rank for each set of unique data 
          mcaid.names <- mcaid.names[rank == 1][, c("rank") := NULL] # keep only most recent set of unique data rows
          mcaid.names[, dup := .N, by = id_mcaid]  # identify duplicates by id
          name.ok <- mcaid.names[dup == 1][, c("dup", "date") := NULL]
          
          name.dups <- mcaid.names[dup > 1][, dup := NULL]
          
        # For duplicate IDs, fill in missing middle initial when possible (using other obs for same id)
          mi.ok <- name.dups[!is.na(name_mdl)] # among those with multiple observations, the rows with a Middle initial
          mi.ok.copy <- copy(mi.ok) # because want a copy without a date for merging below
          mi.ok.copy <- mi.ok.copy[, c("date") := NULL] 
          
          mi.na <- name.dups[is.na(name_mdl)][, c("name_mdl") := NULL] # among those with multiple obs, rowss without a middle initial
          mi.na <- merge(mi.na, mi.ok.copy, by=c("id_mcaid", "name_gvn", "name_srnm"), all.x = TRUE, all.y = FALSE)
          
          name.dups <- rbind(mi.ok, mi.na)
          setkey(name.dups, id_mcaid, date) # sort each id by date
          name.dups[, rank := 1:.N, by = c("id_mcaid")] # note the most recent with "1"
          name.dups <- name.dups[rank == 1][, c("rank", "date") := NULL] # keep most recent
          
        # Append those with unique obs and those with deduplicated obs
          mcaid.names <- rbind(name.ok, name.dups)
          setkey(mcaid.names, id_mcaid)
          rm(mi.na, mi.ok, mi.ok.copy, name.dups, name.ok)
          
        # ensure there is only one row per id
          if(nrow(mcaid.names) - length(unique(mcaid.names$id_mcaid)) != 0){
            stop('non-unique id_mcaid in mcaid.names')
          }
  
  ## (3) Merge Mcaid elig, names, and SSN ----
      mcaid.dt <- merge(mcaid.elig, mcaid.names, by = "id_mcaid", all=TRUE)
      mcaid.dt <- merge(mcaid.dt, mcaid.ssn, by = "id_mcaid", all=TRUE)
    
  ## (4) Clean merged Mcaid dataset ----
    # without ssn, dob, and last name, there is no hope of matching
      mcaid.dt <- mcaid.dt[!(is.na(ssn) & is.na(dob) & is.na(name_srnm) ), ] 
      
    # fill in missing SSN when dob, sex, and complete name are an exact match
      ssn.ok <- mcaid.dt[!is.na(ssn), ]
      ssn.na <- mcaid.dt[is.na(ssn), ]
      ssn.na[, c("ssn") := NULL]
      ssn.na <- merge(ssn.na, ssn.ok, by = c("dob", "gender_me", "gender_female", "gender_male", "name_mdl", "name_srnm", "name_gvn"), all.x = TRUE, all.y = FALSE)
      ssn.na[, id_mcaid.y := NULL]
      setnames(ssn.na, "id_mcaid.x", "id_mcaid")
      mcaid.dt <- rbind(ssn.ok, ssn.na)
      rm(ssn.ok, ssn.na)
    
    # deduplicate when all information is exactly the same except for id_mcaid
      setorder(mcaid.dt, -id_mcaid) # sort with largest ID at top
      mcaid.dt <- mcaid.dt[, dup := 1:.N, by = c("dob", "gender_me", "gender_female", "gender_male", "name_mdl", "name_srnm", "name_gvn", "ssn")]
      mcaid.dt <- mcaid.dt[dup == 1, ] # when all data duplicate, keep only the most recent, i.e., the largest id_mcaid
      mcaid.dt[, dup := NULL]
      
    # deduplicate when ssn appears more than once 
      mcaid.dt[ssn=="123456789", ssn:=NA] # this is a garbage SSN code
      setorder(mcaid.dt, ssn, -id_mcaid) # order by ssn & id so that can identify the max id. 
      mcaid.dt[!is.na(ssn), dup := 1:.N, by="ssn"] # identify when when there are duplicate ssn (and ssn is not missing)
      mcaid.dt <- mcaid.dt[is.na(dup) | dup == 1, ][, dup := NULL] # drop when N > 1, this will keep the max mcaid id only, which is what we agreed to with Eli and Alastair
      
    # deduplicate when all data are the same except for the meciaid ID & SSN (kept in case we want to do this in future, for now, think they are distinct people)
      # mcaid.dt[!(is.na(dob) & is.na(gender_me)), dup := .N, by = c("dob", "gender_me", "name_mdl", "name_srnm", "name_gvn")] # if missing dob and gender, no assurance that same person
      # dup.ok <- mcaid.dt[dup == 1 | is.na(dup)][, dup := NULL] # not duplicated
      # dup <- mcaid.dt[dup > 1][, dup := NULL] # has duplicates
      # setorder(dup, dob, name_gvn, name_mdl, name_srnm, -id_mcaid) # sort duplicated data, with largest (newest) id_mcaid first for for each person
      # dup[, rank := 1:.N, by = c("dob", "gender_me", "name_mdl", "name_srnm", "name_gvn")]
      # dup <- dup[rank==1, ][, rank := NULL] # keep only the most recent for each set of duplicates
      # mcaid.dt <- rbind(dup.ok, dup) # combine unduplicated and de-duplicated data
      # rm(dup.ok, dup)

    # Prep sex for linkage
      mcaid.dt <- prep.sex(mcaid.dt)   
      
    # Prep dob for linkage
      mcaid.dt <- prep.dob(mcaid.dt)

  ## (5) Load Medicaid id table to SQL ----
    # create last_run timestamp
      mcaid.dt[, last_run := Sys.time()]
    
    # create table ID for SQL
      tbl_id <- DBI::Id(schema = "tmp", 
                        table = "xwalk_mcaid_prepped")  
    
    # column types for SQL
      sql.columns <- c("id_mcaid" = "CHAR(11)", "ssn" = "char(9)", 
                       "dob.year" = "char(4)", "dob.month" = "char(2)", "dob.day" = "char(2)",
                       "name_srnm" = "varchar(255)", "name_gvn" = "varchar(255)", "name_mdl" = "varchar(255)", 
                       "gender_me" = "varchar(1)", "gender_female" = "integer", "gender_male" = "integer", 
                       "last_run" = "datetime")  
    
    # ensure column order in R is the same as that in SQL
      setcolorder(mcaid.dt, names(sql.columns))
    
    # Write table to SQL
      dbWriteTable(db_claims51, 
                   tbl_id, 
                   value = as.data.frame(mcaid.dt),
                   overwrite = T, append = F, 
                   field.types = sql.columns)
    
    # Confirm that all rows were loaded to sql
      stage.count <- as.numeric(odbc::dbGetQuery(db_claims51, 
                                                 "SELECT COUNT (*) FROM tmp.xwalk_mcaid_prepped"))
      if(stage.count != nrow(mcaid.dt))
        stop("Mismatching row count, error writing or reading data")
    
  ## (6) Close ODBC connection & drop temporary files ----
      dbDisconnect(db_claims51)        
      rm(list=(setdiff(ls(), keep.me)))
      gc()
      
#### ----------------- ####
#### Prep MCARE DATA   ####   
#### ----------------- ####    
  ## (0) NOTE TO PREVENT FUTURE INSANITY ----
      # There are id_mcare in names and ssn files that never appear in our MBSS, so it possible for people to match with Mcaid or PHA and not
      # appear in the elig_demo file
      
  ## (1) Load data from SQL ----  
        db_claims51 <- dbConnect(odbc(), "PHClaims51")   
        
        mcare.elig <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT id_mcare, dob, gender_me, gender_female, gender_male FROM final.mcare_elig_demo"))
        
        mcare.names <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT bene_id, bene_srnm_name, bene_gvn_name, bene_mdl_name FROM load_raw.mcare_xwalk_edb_user_view"))
        setnames(mcare.names, names(mcare.names), c("id_mcare", "name_srnm", "name_gvn", "name_mdl"))    
        
        mcare.ssn <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT * FROM load_raw.mcare_xwalk_bene_ssn"))
        setnames(mcare.ssn, names(mcare.ssn), c("id_mcare", "ssn"))
        
  ## (2) Tidy individual data files before merging ----
        # Keep only unique rows of identifiers within a file
        if(nrow(mcare.elig) - length(unique(mcare.elig$id_mcare)) != 0){
          stop('non-unique id_mcare in mcare.elig')
        } # confirm all ids are unique in elig data
        
        mcare.names <- unique(mcare.names)
        if(nrow(mcare.names) - length(unique(mcare.names$id_mcare)) != 0){
          stop('non-unique id_mcare in mcare.names')
        } # confirm all ids are unique in names data
        
        mcare.ssn <- unique(mcare.ssn)
        mcare.ssn[, dup.id := .N, by = "id_mcare"] # identify duplicate ID
        mcare.ssn <- mcare.ssn[dup.id == 1, ][, c("dup.id"):=NULL] # No way to know which duplicate id pairing is correct, so drop them
        mcare.ssn[, dup.ssn := .N, by = "ssn"] # identify duplicate SSN
        mcare.ssn <- mcare.ssn[dup.ssn == 1, ][, c("dup.ssn"):=NULL] # No way to know which duplicate is correct, so drop them
        if(nrow(mcare.ssn) - length(unique(mcare.ssn$id_mcare)) >0){
          stop('non-unique id_mcare in mcare.ssn')
        } # confirm all id and ssn are unique
        
  ## (3) Merge Mcare identifiers together ----
        # for all of WA state, want the most complete dataset possible, regardless of whether missing SSN or any other bit of information
        mcare.dt <- merge(mcare.ssn, mcare.names, by = "id_mcare", all.x=T, all.y = T)  
        if(nrow(mcare.dt) - length(unique(mcare.dt$id_mcare)) != 0){
          stop('non-unique id_mcare!')
        }
        
        mcare.dt <- merge(mcare.dt, mcare.elig, by = "id_mcare",  all.x=T, all.y = T)
        if(nrow(mcare.dt) - length(unique(mcare.dt$id_mcare)) != 0){
          stop('non-unique id_mcare!')
        }
        
  ## (4) Run cleaning functions on Medicare data ----
        mcare.dt <- prep.names(mcare.dt)
        mcare.dt <- prep.names(mcare.dt) # run second time because some people have suffixes like "blah JR IV", so first removes IV and second removes JR
        mcare.dt <- prep.dob(mcare.dt)
        mcare.dt <- prep.sex(mcare.dt)
        
        # without ssn, dob, and last name, there is no hope of matching
        mcare.dt <- mcare.dt[!(is.na(ssn) & is.na(dob) & is.na(name_srnm) ), ] 
        
  ## (5) Deduplicate when all information is the same (name, SSN, dob, & gender) except id_mcare ----
        # eventually the integrated datahub will identify when people have multiple ids. For now, just try to keep the most recent id for linkage.
        
        # identify the duplicates
        mcare.dt[, dup := .N, by = c("name_srnm", "name_gvn", "name_mdl", "ssn", "dob.year", "dob.month", "dob.day", "gender_me")]
        mcare.dups <- mcare.dt[dup != 1 & !is.na(name_srnm), ]
        
        # choose the one to keep by the most recent enrollment year for each potential duplicate (from MBSF)
        mbsf <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT [bene_id], [bene_enrollmt_ref_yr] FROM [PHClaims].[stage].[mcare_mbsf]"))
        setnames(mbsf, c("bene_id", "bene_enrollmt_ref_yr"), c("id_mcare", "year"))
        mbsf <- mbsf[id_mcare %in% mcare.dups$id_mcare]
        mbsf <- unique(mbsf[, max(year), by = "id_mcare"])
        
        # merge MBSF max date back onto potential duplicates
        mcare.dups <- merge(mcare.dups, mbsf, by = "id_mcare")
        setnames(mcare.dups, "V1", "maxyear")
        
        # sort by identifiers, with most recent year first
        setorder(mcare.dups, name_srnm, name_gvn, name_mdl, ssn, dob.year, dob.month, dob.day, gender_me, -maxyear)
        
        # identify and drop the older ids from the main data
        mcare.dups[, dup := 1:.N, by = c("name_srnm", "name_gvn", "name_mdl", "ssn", "dob.year", "dob.month", "dob.day", "gender_me")]
        mcare.dt <- mcare.dt[!id_mcare %in% mcare.dups[dup!=1]$id_mcare]
        mcare.dt[, dup := NULL]
        rm(mcare.dups, mbsf)
        
  ## (6) Load Medicare id table to SQL ----
        # create last_run timestamp
        mcare.dt[, last_run := Sys.time()]
        
        # create table ID for SQL
        tbl_id <- DBI::Id(schema = "tmp", 
                          table = "xwalk_mcare_prepped")  
        
        # column types for SQL
        sql.columns <- c("id_mcare" = "CHAR(15)", "ssn" = "char(9)", 
                         "dob.year" = "char(4)", "dob.month" = "char(2)", "dob.day" = "char(2)",
                         "name_srnm" = "varchar(255)", "name_gvn" = "varchar(255)", "name_mdl" = "varchar(255)", 
                         "gender_me" = "varchar(1)", "gender_female" = "integer", "gender_male" = "integer", 
                         "last_run" = "datetime")  
        
        # ensure column order in R is the same as that in SQL
        setcolorder(mcare.dt, names(sql.columns))
        
        # Write table to SQL
        dbWriteTable(db_claims51, 
                     tbl_id, 
                     value = as.data.frame(mcare.dt),
                     overwrite = T, append = F, 
                     field.types = sql.columns)
        
        # Confirm that all rows were loaded to sql
        stage.count <- as.numeric(odbc::dbGetQuery(db_claims51, 
                                                   "SELECT COUNT (*) FROM tmp.xwalk_mcare_prepped"))
        if(stage.count != nrow(mcare.dt))
          stop("Mismatching row count, error writing or reading data")
        
  ## (7) Close ODBC connection and drop temporary files ----
        dbDisconnect(db_claims51)
        rm(list=(setdiff(ls(), keep.me)))
        gc()
        
#### ----------------- ####
#### Prep PHA DATA     ####   
#### ----------------- ####    
  ## (1) Load data from SQL ----
        db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
        
        pha.dt <- setDT(odbc::dbGetQuery(db_apde51, "SELECT pid, ssn_id_m6, name_srnm_m6, name_gvn_m6, name_mdl_m6, 
             dob_m6, gender_new_m6, enddate FROM stage.pha"))
        
  ## (2) Tidy PHA data ----
        # Limit to one row per person and only variables used for merging (use most recent row of data)
        # Filter if person's most recent enddate is <2011 since they can't match to Medicare
        pha.dt <- setDT(pha.dt %>%
          filter(year(enddate) >= 2012) %>%
          distinct(pid, ssn_id_m6, name_srnm_m6, name_gvn_m6, name_mdl_m6, 
                   dob_m6, gender_new_m6, enddate) %>%
          arrange(pid, ssn_id_m6, name_srnm_m6, name_gvn_m6, name_mdl_m6, 
                  dob_m6, gender_new_m6, enddate) %>%
          group_by(pid, ssn_id_m6, name_srnm_m6, name_gvn_m6, dob_m6) %>%
          slice(n()) %>%
          ungroup() %>%
          rename(ssn = ssn_id_m6, name_srnm = name_srnm_m6, 
                 name_gvn = name_gvn_m6, name_mdl = name_mdl_m6, 
                 dob = dob_m6, gender = gender_new_m6) %>%
          select(-(enddate)))
        
        # harmonize gender with Mcare/Mcaid
        pha.dt[gender == 2, gender_me := 1] # Male ==1 in Mcare/Mcaid
        pha.dt[gender == 1, gender_me := 2] # Female ==2 in Mcare/Mcaid
        pha.dt[, gender:=NULL]
        
        # SSN 111-11-1111 appears to be a filler SSN (i.e., it is not real and applies to many people)
        pha.dt[ssn == "111111111", ssn := NA]
        
        # convert PID to character so same type as id_mcare/id_mcaid
        pha.dt[, pid := as.character(pid)] 
        
  ## (3) Run cleaning functions on PHA data ----
        pha.dt <- prep.names(pha.dt)
        pha.dt <- prep.names(pha.dt) # run second time because some people have suffixes like "blah JR IV", so first removes IV and second removes JR
        pha.dt <- prep.dob(pha.dt)
        # do not run prep.sex because sex was coded differently in PHA
        
        # without ssn, dob, and last name, there is no hope of matching
        pha.dt <- pha.dt[!(is.na(ssn) & is.na(dob) & is.na(name_srnm) ), ] 
        
  ## (4) Check for duplicate SSN ----
        pha.dt[!is.na(ssn), dup:=.N, by = "ssn"]
        nrow(pha.dt[dup >1]) # almost 2,000 duplicate SSN, but no obvious way to deduplicate at this point, so will have to keep all of them
        pha.dt[, dup := NULL]
        
  ## (5) Load PHA data to SQL  ----
        # create last_run timestamp
        pha.dt[, last_run := Sys.time()]
        
        # create table ID for SQL
        tbl_id <- DBI::Id(schema = "tmp", 
                          table = "xwalk_pha_prepped")  
        
        # column types for SQL
        sql.columns <- c("pid" = "CHAR(6)", "ssn" = "char(9)", 
                         "dob.year" = "char(4)", "dob.month" = "char(2)", "dob.day" = "char(2)",
                         "name_srnm" = "varchar(255)", "name_gvn" = "varchar(255)", "name_mdl" = "varchar(255)", 
                         "gender_me" = "varchar(1)",  
                         "last_run" = "datetime")  
        
        # ensure column order in R is the same as that in SQL
        setcolorder(pha.dt, names(sql.columns))
        
        # Write table to SQL
        dbWriteTable(db_apde51, 
                     tbl_id, 
                     value = as.data.frame(pha.dt),
                     overwrite = T, append = F, 
                     field.types = sql.columns)
        
        # Confirm that all rows were loaded to sql
        stage.count <- as.numeric(odbc::dbGetQuery(db_apde51, 
                                                   "SELECT COUNT (*) FROM tmp.xwalk_pha_prepped"))
        if(stage.count != nrow(pha.dt))
          stop("Mismatching row count, error writing or reading data")
        
  ## (6) Close OBDC connection and drop temporary files ----
        dbDisconnect(db_apde51)
        rm(list=(setdiff(ls(), keep.me)))
        gc()
        
#### ----------------- ####
#### LINK MCAID-MCARE  ####   
#### ----------------- ####    

  ## (1) Load data from SQL ----
      db_claims51 <- dbConnect(odbc(), "PHClaims51")   
        
      mcaid <- setDT(odbc::dbGetQuery(db_claims51, "SELECT id_mcaid, ssn, [dob.year], [dob.month], [dob.day], name_srnm, name_gvn, name_mdl, gender_me FROM tmp.xwalk_mcaid_prepped"))
      mcare <- setDT(odbc::dbGetQuery(db_claims51, "SELECT id_mcare, ssn, [dob.year], [dob.month], [dob.day], name_srnm, name_gvn, name_mdl, gender_me FROM tmp.xwalk_mcare_prepped"))
  
  ## (2) Deterministic matches: identification and extraction ----
      # 100% perfect match
          perfect.match <- merge(mcare, mcaid, by = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me", "name_srnm", "name_gvn", "name_mdl"), all=FALSE) 
          perfect.match <- perfect.match[!(is.na(ssn) & is.na(dob.year) & is.na(dob.month) & is.na(dob.day) & is.na(name_gvn) & is.na(name_srnm) ), ]  # consider perfect match only when have SSN, dob, first name and last name
          perfect.match <- perfect.match[, .(id_mcaid, id_mcare)] # keep the paired ids only
          
          #removing the perfectly linked data from the Mcaid and Mcare datasets because there is no need to perform additional record linkage functions on them
          mcare <- mcare[!(id_mcare %in% perfect.match$id_mcare)]
          mcaid <- mcaid[!(id_mcaid %in% perfect.match$id_mcaid)]
          
      # Perfect match if one person is missing SSN
          ssn.match1 <- merge(mcare[is.na(ssn)], mcaid, by = c("dob.year", "dob.month", "dob.day", "gender_me", "name_srnm", "name_gvn", "name_mdl"), all=FALSE) 
          ssn.match2 <- merge(mcare, mcaid[is.na(ssn)], by = c("dob.year", "dob.month", "dob.day", "gender_me", "name_srnm", "name_gvn", "name_mdl"), all=FALSE) 
          ssn.match <- rbind(ssn.match1, ssn.match2); rm(ssn.match1, ssn.match2)
          ssn.match <- ssn.match[!(is.na(dob.year) & is.na(dob.month) & is.na(dob.day) & is.na(name_gvn) & is.na(name_srnm) ), ]  
          ssn.match <- ssn.match[, .(id_mcaid, id_mcare)] # keep the paired ids only
          
          mcare <- mcare[!(id_mcare %in% ssn.match$id_mcare)]
          mcaid <- mcaid[!(id_mcaid %in% ssn.match$id_mcaid)]
          
      # Perfect match if one person is missing middle initial
          mi.match1 <- merge(mcare[is.na(name_mdl)], mcaid, by = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me", "name_srnm", "name_gvn"), all=FALSE) 
          mi.match2 <- merge(mcare, mcaid[is.na(name_mdl)], by = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me", "name_srnm", "name_gvn"), all=FALSE) 
          mi.match <- rbind(mi.match1, mi.match2); rm(mi.match1, mi.match2)
          mi.match <- mi.match[!(is.na(ssn) & is.na(dob.year) & is.na(dob.month) & is.na(dob.day) & is.na(name_gvn) & is.na(name_srnm) ), ] 
          mi.match <- mi.match[, .(id_mcaid, id_mcare)] # keep the paired ids only
          
          mcare <- mcare[!(id_mcare %in% mi.match$id_mcare)]
          mcaid <- mcaid[!(id_mcaid %in% mi.match$id_mcaid)]          
          
  ## (3) Probabilistic linkage - performed sequentially ----
      # Model 1: Block on SSN, string compare dob and gender, soundex for first & last name ----
          mod1 <- compare.linkage(mcare, mcaid, 
                                  blockfld = c("ssn"), # blocking
                                  strcmp = c("dob.year", "dob.month", "dob.day", "gender_me","name_mdl"), # computer similarity between two
                                  phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
          
          # get summary of potential pairs
          summary(mod1) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod1.weights <- epiWeights(mod1) 
          summary(mod1.weights)
          
          # get paired data, with weights, as a dataset
          mod1.pairs <- setDT(getPairs(mod1.weights, single.rows = TRUE))
          mod1.pairs.long <- setDT(getPairs(mod1.weights, single.rows = FALSE)) # easier to compare when long, but need wide to extract the id pairs

          # classify pairs using a threshhold
          summary(epiClassify(mod1.weights, threshold.upper = 0.39)) # SSN is a superb identifier. Visually confirmed that matches above threshhold are strongly plausible
          
          # get linked pairs
          mod1.match <- get.linked.pairs.mcaid.mcare(mod1.pairs.long, 0.39)       
          
          # remove the linked data from the two parent datasets so we don't try to link them again
          mcaid <- mcaid[!(id_mcaid %in% mod1.match$id_mcaid)]
          mcare <- mcare[!(id_mcare %in% mod1.match$id_mcare)]
          
          # clean objects in memory
          rm(mod1, mod1.weights, mod1.pairs, mod1.pairs.long) # drop tables only needed to form the linkage
          
      # Model 2: Block on DOB, string compare for SSN and gender, soundex for first and last name ----
          mod2 <- compare.linkage(mcare, mcaid, 
                                  blockfld = c("dob.year", "dob.month", "dob.day"), # blocking
                                  strcmp = c("ssn", "gender_me","name_mdl"), # computer similarity between two
                                  phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
          
          # get summary of potential pairs
          summary(mod2) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod2.weights <- epiWeights(mod2) 
          summary(mod2.weights)
          
          # get paired data, with weights, as a dataset
           mod2.pairs <- setDT(getPairs(mod2.weights, single.rows = TRUE)) # want it on one row, so can identify when SSN missing in Mcaid
          #mod2.pairs.long <- setDT(getPairs(mod2.weights, single.rows = FALSE)) 

          # get linked pairs
          mod2.match <- rbind(
            mod2.pairs[!is.na(ssn.2) & Weight >= 0.595, .(id_mcare.1, id_mcaid.2)], # higher threshhold when there is a comparison SSN
            mod2.pairs[(is.na(ssn.1) | is.na(ssn.2)) & Weight >= 0.4, .(id_mcare.1, id_mcaid.2)]  # lower threshhold when there is no SSN for comparision
          )
          setnames(mod2.match, names(mod2.match), c("id_mcare", "id_mcaid"))
          
          # remove the linked data from the two parent datasets so we don't try to link them again
          mcaid <- mcaid[!(id_mcaid %in% mod2.match$id_mcaid)]
          mcare <- mcare[!(id_mcare %in% mod2.match$id_mcare)]
          
          # clean objects in memory
          rm(mod2, mod2.weights, mod2.pairs) # drop tables only needed to form the linkage
          
      # Model 3: Block on DOB + last name + gender, string compare for first name, exclude SSN ... when Mcaid missing SSN ----
          mcaid.mi.sss <- mcaid[is.na(ssn)] # try linkage with Mcaid data missing SSN
          
          mod3 <- compare.linkage(mcare, mcaid.mi.sss, 
                                  blockfld = c("dob.year", "dob.month", "dob.day", "name_srnm", "gender_me"), # blocking
                                  strcmp = c("name_mdl", "name_gvn"), # compare similarity between two
                                  exclude = c("ssn") )
          
          # get summary of potential pairs
          summary(mod3) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod3.weights <- epiWeights(mod3) 
          summary(mod3.weights)
          
          # get paired data, with weights, as a dataset
          mod3.pairs <- setDT(getPairs(mod3.weights, single.rows = TRUE))
          mod3.pairs.long <- setDT(getPairs(mod3.weights, single.rows = FALSE))

          # classify pairs using a threshhold
          summary(epiClassify(mod3.weights, threshold.upper = 0.60)) # based on visual inspection of curve and dataset with weights     
          
          # get linked pairs
          mod3.match <- get.linked.pairs.mcaid.mcare(mod3.pairs.long, 0.60)            
          
          # drop the linked data from the two parent datasets so we don't try to link them again
          mcaid <- mcaid[!(id_mcaid %in% mod3.match$id_mcaid)]
          mcare <- mcare[!(id_mcare %in% mod3.match$id_mcare)]            
          
          # clean objects in memory
          rm(mod3, mod3.weights, mod3.pairs, mod3.pairs.long) # drop tables only needed to form the linkage  
          
      # Model 4: Block year + mo + all names + gender, string compare SSN + day ----      
          mod4 <- compare.linkage(mcare, mcaid, 
                                  blockfld = c("dob.year", "dob.month", "name_mdl", "name_gvn", "name_srnm", "gender_me"), # blocking
                                  strcmp = c("ssn", "dob.day")) # computer similarity between two
          
          # get summary of potential pairs
          summary(mod4) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod4.weights <- epiWeights(mod4) 
          summary(mod4.weights)
          
          # get paired data, with weights, as a dataset
          mod4.pairs <- setDT(getPairs(mod4.weights, single.rows = TRUE))
          mod4.pairs.long <- setDT(getPairs(mod4.weights, single.rows = FALSE))
          
          # classify pairs using a threshhold
          summary(epiClassify(mod4.weights, threshold.upper = 0.72)) # based on visual inspection of curve and dataset with weights     
          
          # get linked pairs
          mod4.match <- get.linked.pairs.mcaid.mcare(mod4.pairs.long, 0.72)            
          
          # drop the linked data from the two parent datasets so we don't try to link them again
          mcaid <- mcaid[!(id_mcaid %in% mod4.match$id_mcaid)]
          mcare <- mcare[!(id_mcare %in% mod4.match$id_mcare)]            
          
          # clean objects in memory
          rm(mod4, mod4.pairs, mod4.pairs.long, mod4.weights)
          
      # Model 5: Block year + mo + middle initial + gender, string compare SSN + day, names use soundex ... NOT USED----      
          # MODEL DID NOT PROVE USEFUL ... KEPT HERE SO THAT TIME IS NOT WASTED TRYING THIS AGAIN
          
          # mod5 <- compare.linkage(mcare, mcaid, 
          #                         blockfld = c("dob.year", "dob.month", "name_mdl", "gender_me"), # blocking
          #                         strcmp = c("ssn", "dob.day"), # computer similarity between two
          #                         phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
          
          # get summary of potential pairs
          # summary(mod5) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          # mod5.weights <- epiWeights(mod5) 
          # summary(mod5.weights)
          
          # get paired data, with weights, as a dataset
          # mod5.pairs <- setDT(getPairs(mod5.weights, single.rows = TRUE))
          # mod5.pairs.long <- setDT(getPairs(mod5.weights, single.rows = FALSE))
          
          # get linked pairs
          # provides a list of potential matches, but the SSN and dob-day seem too far off to seem to be the same person  
          
          # STOP with this model because can see that it is not proving useful
          # rm(mod5, mod5.pairs, mod5.pairs.long, mod5.weights)            
      
      # Model 6: Block year + mo + all names + gender, string compare SSN + day, names use exact spelling ----    
          mod6 <- compare.linkage(mcare, mcaid, 
                                  blockfld = c("dob.year", "dob.month", "name_mdl", "name_gvn", "name_srnm", "gender_me"), # blocking
                                  strcmp = c("ssn", "dob.day"), strcmpfun = levenshteinSim)  # computer similarity between two
          # used levenshtein distance because it is more conserviative and causes greater separation with more than minor SSN deviations
          
          # get summary of potential pairs
          summary(mod6) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod6.weights <- epiWeights(mod6) 
          summary(mod6.weights)
          
          # get paired data, with weights, as a dataset
          mod6.pairs <- setDT(getPairs(mod6.weights, single.rows = TRUE))
          mod6.pairs.long <- setDT(getPairs(mod6.weights, single.rows = FALSE))

          # classify pairs using a threshhold
          summary(epiClassify(mod6.weights, threshold.upper = 0.69)) # based on visual inspection of curve and dataset with weights
          
          # get linked pairs
          mod6.match <- get.linked.pairs.mcaid.mcare(mod6.pairs.long, 0.69)     
          
          # remove the linked data from the two parent datasets so we don't try to link them again
          mcaid <- mcaid[!(id_mcaid %in% mod6.match$id_mcaid)]
          mcare <- mcare[!(id_mcare %in% mod6.match$id_mcare)]
          
          # clean objects in memory
          rm(mod6, mod6.weights, mod6.pairs, mod6.pairs.long) # drop tables only needed to form the linkage        
          
      # Model 7: Block DOB + gender, string compare SSN + names ----    
          mod7 <- compare.linkage(mcare, mcaid, 
                                  blockfld = c("dob.year", "dob.month", "dob.day", "gender_me"), # blocking
                                  strcmp = c("ssn", "name_mdl", "name_gvn", "name_srnm"), strcmpfun = levenshteinSim) # computer similarity between strings
          #phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
          
          # get summary of potential pairs
          summary(mod7) 
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod7.weights <- epiWeights(mod7) 
          summary(mod7.weights)
          
          # get paired data, with weights, as a dataset
          #mod7.pairs <- setDT(getPairs(mod7.weights, single.rows = TRUE))
          mod7.pairs.long <- setDT(getPairs(mod7.weights, single.rows = FALSE))

          # classify pairs using a threshhold
          summary(epiClassify(mod7.weights, threshold.upper = 0.587)) # based on visual inspection of curve and dataset with weights
          
          # get linked pairs
          mod7.match <- get.linked.pairs.mcaid.mcare(mod7.pairs.long, 0.587)     
          
          # remove the linked data from the two parent datasets so we don't try to link them again
          mcaid <- mcaid[!(id_mcaid %in% mod7.match$id_mcaid)]
          mcare <- mcare[!(id_mcare %in% mod7.match$id_mcare)]
          
          # clean objects in memory
          rm(mod7, mod7.weights, mod7.pairs.long) # drop tables only needed to form the linkage                   
          
          # there were other fairly certain matches that remained using this method, but a probability cut-off woudln't work. Woudl need some kind of machine learning.
          
      # Model 8: Block DOB + gender, + first name, string compare SSN + last name + middle initial ----
          # blocking on first name will prevent identification of twins who might share all other information and have similar SSN
          mod8 <- compare.linkage(mcare, mcaid, 
                                  blockfld = c("dob.year", "dob.month", "dob.day", "gender_me", "name_gvn"), # blocking
                                  strcmp = c("ssn", "name_mdl", "name_srnm"), strcmpfun = levenshteinSim)
          
          # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
          mod8.weights <- epiWeights(mod8) 
          summary(mod8.weights)
          
          # get paired data, with weights, as a dataset
          mod8.pairs.long <- setDT(getPairs(mod8.weights, single.rows = FALSE))
          
          # classify pairs using a threshhold
          summary(epiClassify(mod8.weights, threshold.upper = 0.99)) # set to 99 because didn't see any reasonable linkages
          
          # get linked pairs
          mod8.match <- get.linked.pairs.mcaid.mcare(mod8.pairs.long, 0.99)     
          
          # remove the linked data from the two parent datasets so we don't try to link them again
          mcaid <- mcaid[!(id_mcaid %in% mod8.match$id_mcaid)]
          mcare <- mcare[!(id_mcare %in% mod8.match$id_mcare)]
          
          # clean objects in memory
          rm(mod8, mod8.weights, mod8.pairs.long) # drop tables only needed to form the linkage            
          
          
          
  ## (4) Manual additional deterministic linkages ----
  # Based on a manual review of some potential linkages that were note made, I found the following
  # middle initials are often missing for one or the other
  # day and month or birth are sometimes criss crossed
  # Compount names (e.g., Garcia Vasquez) are often in seeminlgy random order or only one of the two is used
  # east asian last and first names are often criss-crossed 
  # year, month, and day or birth are often off by one or two digits (typos)
  # When middle initial is missing in Mcare, it is often the first letter of the second last name in a compound last name
  
  # May use RecordLinkage to create pairs, within which I will apply some logical filtering to dermine if they are matches
  # It has become obvious to me that this is a task for Machine Learning, but I ran out of time to pursue that. 
  
      # Model 1: Switch first and last names (happens sometimes, especiall with East Asian patients) ----
        mcare.alt <- copy(mcare)
        setnames(mcare.alt, c("name_gvn", "name_srnm"), c("name_gvn.orig", "name_srnm.orig"))
        mcare.alt[, name_gvn := name_srnm.orig][, name_srnm := name_gvn.orig]
        
        d1.match <- merge(mcaid, mcare.alt, by = c("dob.year", "dob.month", "dob.day", "gender_me","name_srnm", "name_gvn"), all = FALSE)
        d1.match <- d1.match[(is.na(ssn.x) | is.na(ssn.y)) | ssn.x == ssn.y, ] # keep when SSN matches or >= 1 SSN is missing
        
        d1.match <- d1.match[, .(id_mcaid, id_mcare)]
        
        mcaid <- mcaid[!(id_mcaid %in% d1.match$id_mcaid)] # remove the matched rows from the parent mcaid dataset
        mcare <- mcare[!(id_mcare %in% d1.match$id_mcare)] # remove the matched rows from the parent mcare dataset
        rm(mcare.alt)
      
      # Model 2: Check if compound names are nested in one another ----
      d2.match <- compare.linkage(mcare, mcaid, 
                                  blockfld = c("dob.year", "dob.month", "dob.day", "gender_me"), # blocking
                                  phonetic = c("name_srnm", "name_gvn"), phonfun = soundex,  # use phonetics for names, rather than spelling
                                  exclude = c( "name_mdl", "ssn"))
      
      d2.match <- setDT(getPairs(d2.match, single.rows = TRUE))[, is_match := 0]
      
      # check for nesting within first or last names
      d2.match[, srnm_match := NameContains(name_srnm.1, name_srnm.2)]
      d2.match[, srnm_match := NameContains(name_srnm.2, name_srnm.1)]
      d2.match[, gvn_match := NameContains(name_gvn.1, name_gvn.2)]
      d2.match[, gvn_match := NameContains(name_gvn.2, name_gvn.1)]
      
      # Keep if there is nesting (or equality) within the first and last names 
      d2.match <- d2.match[gvn_match == 1 & srnm_match == 1, ]
      
      # keep only id columns
      setnames(d2.match, c("id_mcaid.2", "id_mcare.1"), c("id_mcaid", "id_mcare"))
      d2.match <- d2.match[, .(id_mcaid, id_mcare)]
      
      mcaid <- mcaid[!(id_mcaid %in% d2.match$id_mcaid)] # remove the matched rows from the parent mcaid dataset
      mcare <- mcare[!(id_mcare %in% d2.match$id_mcare)] # remove the matched rows from the parent mcare dataset
      
  ## (5) Combine linked ID pairs ----
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
      xwalk[, dup.id_mcaid := 1:.N, by = id_mcaid]
      # View(mcare[id_mcare %in% xwalk[dup.id_mcaid!=1]$id_mcare]) # need to reload mcare data to see these duplicates
      
      xwalk[, dup.id_mcare := 1:.N, by = id_mcare]
      # View(mcaid[id_mcaid %in% xwalk[dup.mcare!=1]$id_mcaid]) # need to reload id_mcaid data to see these duplicates
      
      # at this point there are few (only 1 as of 10/25/2019) duplicate ids, so just drop randomly
      xwalk <- xwalk[dup.id_mcaid ==1]
      xwalk <- xwalk[dup.id_mcare ==1]
      xwalk[, c("dup.id_mcaid", "dup.id_mcare") := NULL]
      
  ## (6) Load linkage table to SQL ----      
      # create last_run timestamp
      xwalk[, last_run := Sys.time()]
      
      # create table ID for SQL
      tbl_id <- DBI::Id(schema = "tmp", 
                        table = "xwalk_mcaid_mcare")  
      
      # Identify the column types to be created in SQL
      sql.columns <- c("id_mcaid" = "char(11)", "id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "last_run" = "datetime")  
      
      # ensure column order in R is the same as that in SQL
      setcolorder(xwalk, names(sql.columns))
      
      # Write table to SQL
      dbWriteTable(db_claims51, 
                   tbl_id, 
                   value = as.data.frame(xwalk),
                   overwrite = T, append = F, 
                   field.types = sql.columns)
      
      # Confirm that all rows were loaded to sql
      stage.count <- as.numeric(odbc::dbGetQuery(db_claims51, 
                                                 "SELECT COUNT (*) FROM tmp.xwalk_mcaid_mcare"))
      if(stage.count != nrow(xwalk))
        stop("Mismatching row count, error writing or reading data")      
      
  ## (7) Close ODBC connection and drop temporary files ----    
      dbDisconnect(db_claims51)      
      rm(list=(setdiff(ls(), keep.me)))
      gc()

#### ----------------- ####
#### LINK MCARE-PHA    ####   
#### ----------------- #### 
  ## (1) Load data from SQL #####
      # Medicare ----
      db_claims51 <- dbConnect(odbc(), "PHClaims51")
      mcare <- setDT(odbc::dbGetQuery(db_claims51, "SELECT 
                                      [id_mcare], [ssn], [dob.year], [dob.month], [dob.day], [name_srnm], [name_gvn], [name_mdl], [gender_me] 
                                      FROM [PHClaims].[tmp].[xwalk_mcare_prepped]"))

      # Housing ----
      db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
      pha <- setDT(odbc::dbGetQuery(db_apde51, "SELECT 
                                      [pid], [ssn], [dob.year], [dob.month], [dob.day], [name_srnm], [name_gvn], [name_mdl], [gender_me] 
                                      FROM [PH_APDEStore].[tmp].[xwalk_pha_prepped]"))
      
  ## (2) ---------- LINK DATA ---------- ####
      # Match  1 - Perfect Deterministic ####
        match1 <- merge(mcare, pha, by = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me", "name_srnm", "name_gvn", "name_mdl"), all=FALSE) 
        match1 <- match1[!(is.na(ssn) & is.na(dob.year) & is.na(dob.month) & is.na(dob.day) & is.na(name_gvn) & is.na(name_srnm) ), ]  # consider perfect match only when have SSN, dob, first name and last name
        match1 <- match1[, .(pid, id_mcare)] # keep the paired ids only
      
        # drop if there duplicate ids (keeping largest ID, presuming it is newer)
        setorder(match1, pid)
        match1[, dup := 1:.N, by = "id_mcare"] 
        match1 <- match1[dup==1]
        
        setorder(match1, id_mcare)
        match1[, dup := 1:.N, by = "pid"] 
        match1 <- match1[dup==1]
        
        match1[, dup := NULL]
        
        # removing the perfectly linked data from the Mcare & PHA datasets because there is no need to perform additional record linkage functions on them
        mcare <- mcare[!(id_mcare %in% match1$id_mcare)]
        pha <- pha[!(pid %in% match1$pid)]
      
      # Match  2 - Perfect Determinisitic, allowing for last name change for females ----
        match2 <- merge(mcare, pha, by = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me", "name_gvn", "name_mdl"), all=FALSE) 
        match2 <- match2[!(is.na(ssn) & is.na(dob.year) & is.na(dob.month) & is.na(dob.day) & is.na(name_gvn)), ]  
        match2 <- match2[gender_me == 2]
        match2 <- match2[, .(pid, id_mcare)] # keep the paired ids only
      
        # drop if there duplicate ids (keeping largest ID, presuming it is newer)
        setorder(match2, pid)
        match2[, dup := 1:.N, by = "id_mcare"] 
        match2 <- match2[dup==1]
        
        setorder(match2, id_mcare)
        match2[, dup := 1:.N, by = "pid"] 
        match2 <- match2[dup==1]
        
        match2[, dup := NULL]
        
        # remove the linked data from the two parent datasets so we don't try to link them again
        mcare <- mcare[!(id_mcare %in% match2$id_mcare)]
        pha <- pha[!(pid %in% match2$pid)]
      
      # Match  3 - Deterministic: Switch first and last names (happens sometimes, especiall with East Asian patients) ----
        mcare.alt <- copy(mcare)
        setnames(mcare.alt, c("name_gvn", "name_srnm"), c("name_gvn.orig", "name_srnm.orig"))
        mcare.alt[, name_gvn := name_srnm.orig][, name_srnm := name_gvn.orig]
        
        match3 <- merge(pha, mcare.alt, by = c("dob.year", "dob.month", "dob.day", "gender_me","name_srnm", "name_gvn"), all = FALSE)
        match3 <- match3[(is.na(ssn.x) | is.na(ssn.y)) | ssn.x == ssn.y, ] # keep when SSN matches or >= 1 SSN is missing
        
        match3 <- match3[, .(pid, id_mcare)]
        
        pha <- pha[!(pid %in% match3$pid)] # remove the matched rows from the parent pha dataset
        mcare <- mcare[!(id_mcare %in% match3$id_mcare)] # remove the matched rows from the parent mcare dataset
        rm(mcare.alt)
      
      # Match  4 - Probabilistic: Block on SSN, DOB, gender_me & middle initial, soundex for first & last name ----
        match4 <- compare.linkage(mcare, pha, 
                                blockfld = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me", "name_mdl"), # blocking
                                phonetic = c("name_srnm", "name_gvn"), phonfun = soundex)  # use phonetics for names, rather than spelling
        
        # get summary of potential pairs
        summary(match4) 
        
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
        match4.weights <- epiWeights(match4) 
        summary(match4.weights)
        
        # get paired data, with weights, as a dataset
        match4.pairs <- setDT(getPairs(match4.weights, single.rows = TRUE))
        match4.pairs.long <- setDT(getPairs(match4.weights, single.rows = FALSE)) # easier to compare when long, but need wide to extract the id pairs
        
        # classify pairs using a threshhold
        summary(epiClassify(match4.weights, threshold.upper = 0.46)) # Visually confirmed that matches above threshhold are strongly plausible
        
        # get linked pairs
        match4.match <- get.linked.pairs.mcare.pha(match4.pairs.long, 0.46)       
        
        # remove the linked data from the two parent datasets so we don't try to link them again
        mcare <- mcare[!(id_mcare %in% match4.match$id_mcare)]
        pha <- pha[!(pid %in% match4.match$pid)]
        
        # clean objects in memory
        rm(match4, match4.weights, match4.pairs, match4.pairs.long) # drop tables only needed to form the linkage      
      
      # Match  5 - Probabilistic: Block on SSN, string compare middle initial, gender_me, dob, soundex on first & last name ----
        match5 <- compare.linkage(mcare, pha, blockfld = c("ssn"),
                                strcmp = c("name_mdl", "gender_me", "dob.year", "dob.month", "dob.day"),
                                phonetic = c("name_srnm", "name_gvn"), phonfun = soundex)
        
        # get summary of potential pairs
        summary(match5) 
        
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
        match5.weights <- epiWeights(match5) 
        summary(match5.weights)
        
        # get paired data, with weights, as a dataset
        match5.pairs <- setDT(getPairs(match5.weights, single.rows = TRUE))
        match5.pairs.long <- setDT(getPairs(match5.weights, single.rows = FALSE)) # easier to compare when long, but need wide to extract the id pairs
        
        # classify pairs using a threshhold
        summary(epiClassify(match5.weights, threshold.upper = 0.39)) # Visually confirmed that matches above threshhold are strongly plausible
        
        # Have dupliacate IDs ... so, keep the one with higher probability weight
        match5.pairs[, dup.mcaid := 1:.N, by = id_mcare.1] # already sorted by Weight
        match5.pairs[, dup.pid := 1:.N, by = pid.2] # already sorted by Weight
        match5.pairs <- match5.pairs[dup.mcaid==1 & dup.pid == 1] # many if not all of these are a single person with different ids 
        
        # get linked pairs
        match5.match <- match5.pairs[Weight >= 0.39]
        setnames(match5.match, c("pid.2", "id_mcare.1"), c("pid", "id_mcare"))
        match5.match <- match5.match[, c("pid", "id_mcare")]
        
        # remove the linked data from the two parent datasets so we don't try to link them again
        pha <- pha[!(pid %in% match5.match$pid)]
        mcare <- mcare[!(id_mcare %in% match5.match$id_mcare)]
        
        # clean objects in memory
        rm(match5, match5.weights, match5.pairs, match5.pairs.long) # drop tables only needed to form the linkage   
      
      # Match  6 - Probabilistic: Block on DOB, string compare for SSN, gender_me, & middle initial, soundex for first and last name ####
        match6 <- compare.linkage(mcare, pha, 
                                blockfld = c("dob.year", "dob.month", "dob.day"), # blocking
                                strcmp = c("ssn", "gender_me","name_mdl"), # computer similarity between two
                                phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
        
        # get summary of potential pairs
        summary(match6) 
        
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
        match6.weights <- epiWeights(match6) 
        summary(match6.weights)
        
        # get paired data, with weights, as a dataset
        match6.pairs <- setDT(getPairs(match6.weights, single.rows = TRUE))
        match6.pairs.long <- setDT(getPairs(match6.weights, single.rows = FALSE)) # easier to compare when long, but need wide to extract the id pairs
        
        # classify pairs using a threshhold
        summary(epiClassify(match6.weights, threshold.upper = 0.605)) # Visually confirmed that matches above threshhold are strongly plausible
        
        # get linked pairs
        match6.match <- get.linked.pairs.mcare.pha(match6.pairs.long, 0.605)       
        
        # remove the linked data from the two parent datasets so we don't try to link them again
        pha <- pha[!(pid %in% match6.match$pid)]
        mcare <- mcare[!(id_mcare %in% match6.match$id_mcare)]
        
        # clean objects in memory
        rm(match6, match6.weights, match6.pairs, match6.pairs.long) # drop tables only needed to form the linkage   
      
      # Match  7 - Probabilistic: Block on DOB + last name + gender_me, string compare for first name, exclude SSN ... when PHA missing SSN ----
        pha.mi.ssn <- pha[is.na(ssn)] # try linkage with pha data missing SSN
        
        match7 <- compare.linkage(mcare, pha.mi.ssn, 
                                blockfld = c("dob.year", "dob.month", "dob.day", "name_srnm", "gender_me"), # blocking
                                strcmp = c("name_mdl", "name_gvn"), # compare similarity between two
                                exclude = c("ssn") )
        
        # get summary of potential pairs
        summary(match7) 
        
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
        match7.weights <- epiWeights(match7) 
        summary(match7.weights)
        
        # get paired data, with weights, as a dataset
        match7.pairs <- setDT(getPairs(match7.weights, single.rows = TRUE))
        match7.pairs.long <- setDT(getPairs(match7.weights, single.rows = FALSE))

        # classify pairs using a threshhold
        summary(epiClassify(match7.weights, threshold.upper = 0.62)) # based on visual inspection of curve and dataset with weights     
        
        # get linked pairs
        match7.match <- get.linked.pairs.mcare.pha(match7.pairs.long, 0.62)            
        
        # drop the linked data from the two parent datasets so we don't try to link them again
        pha <- pha[!(pid %in% match7.match$pid)]
        mcare <- mcare[!(id_mcare %in% match7.match$id_mcare)]            
        
        # clean objects in memory
        rm(match7, match7.weights, match7.pairs, match7.pairs.long, pha.mi.ssn) # drop tables only needed to form the linkage        
      
      # Match  8 - Probabilistic: Block on DOB + last name + gender_me, string compare for first name, exclude SSN ... when Mcare missing SSN ----
        mcare.mi.ssn <- mcare[is.na(ssn)] # try linkage with pha data missing SSN
        
        match8 <- compare.linkage(mcare.mi.ssn, pha, 
                                blockfld = c("dob.year", "dob.month", "dob.day", "name_srnm", "gender_me"), # blocking
                                strcmp = c("name_mdl", "name_gvn"), # compare similarity between two
                                exclude = c("ssn") )
        
        # get summary of potential pairs
        summary(match8) 
        
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
        match8.weights <- epiWeights(match8) 
        summary(match8.weights)
        
        # get paired data, with weights, as a dataset
        match8.pairs <- setDT(getPairs(match8.weights, single.rows = TRUE))
        match8.pairs.long <- setDT(getPairs(match8.weights, single.rows = FALSE))

        # classify pairs using a threshhold
        summary(epiClassify(match8.weights, threshold.upper = 0.62)) # based on visual inspection of curve and dataset with weights     
        
        # Have dupliacate IDs ... so, keep the one with higher probability weight
        match8.pairs[, dup.mcaid := 1:.N, by = id_mcare.1] # already sorted by Weight
        match8.pairs[, dup.pid := 1:.N, by = pid.2] # already sorted by Weight
        match8.pairs <- match8.pairs[dup.mcaid==1 & dup.pid == 1] # many if not all of these are a single person with different ids 
        
        # get linked pairs
        match8.match <- match8.pairs[Weight >= 0.62]
        setnames(match8.match, c("pid.2", "id_mcare.1"), c("pid", "id_mcare"))
        match8.match <- match8.match[, c("pid", "id_mcare")]          
        
        # drop the linked data from the two parent datasets so we don't try to link them again
        pha <- pha[!(pid %in% match8.match$pid)]
        mcare <- mcare[!(id_mcare %in% match8.match$id_mcare)]            
        
        # clean objects in memory
        rm(match8, match8.weights, match8.pairs, match8.pairs.long, mcare.mi.ssn) # drop tables only needed to form the linkage       
        
      # Match  9 - Probabilistic: Block year + mo + all names + gender_me, string compare SSN + day ----      
        match9 <- compare.linkage(mcare, pha, 
                                blockfld = c("dob.year", "dob.month", "name_mdl", "name_gvn", "name_srnm", "gender_me"), # blocking
                                strcmp = c("ssn", "dob.day")) # computer similarity between two
        
        # get summary of potential pairs
        summary(match9) 
        
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
        match9.weights <- epiWeights(match9) 
        summary(match9.weights)
        
        # get paired data, with weights, as a dataset
        match9.pairs <- setDT(getPairs(match9.weights, single.rows = TRUE))
        match9.pairs.long <- setDT(getPairs(match9.weights, single.rows = FALSE))
        
        # classify pairs using a threshhold
        summary(epiClassify(match9.weights, threshold.upper = 0.67)) # based on visual inspection of curve and dataset with weights     
        
        # get linked pairs
        match9.match <- get.linked.pairs.mcare.pha(match9.pairs.long, 0.67)            
        
        # drop the linked data from the two parent datasets so we don't try to link them again
        pha <- pha[!(pid %in% match9.match$pid)]
        mcare <- mcare[!(id_mcare %in% match9.match$id_mcare)]            
        
        # clean objects in memory
        rm(match9, match9.pairs, match9.pairs.long, match9.weights)      
      
      # Match  10 - Probabilistic: Block year + mo + all names + gender_me, string compare SSN + day, names use exact spelling ... NOT USEFUL----    
        
        # This model did not generate any potential pairs ... 
        
        # match10 <- compare.linkage(mcare, pha, 
        #                         blockfld = c("dob.year", "dob.month", "name_mdl", "name_gvn", "name_srnm", "gender_me"), # blocking
        #                         strcmp = c("ssn", "dob.day"), strcmpfun = levenshteinSim)  # computer similarity between two
        # # used levenshtein distance because it is more conserviative and causes greater separation with more than minor SSN deviations
        # 
        # # get summary of potential pairs
        # summary(match10) 
        # 
        # # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
        # match10.weights <- epiWeights(match10) 
        # summary(match10.weights)
        # 
        # # get paired data, with weights, as a dataset
        # match10.pairs <- setDT(getPairs(match10.weights, single.rows = TRUE))
        # match10.pairs.long <- setDT(getPairs(match10.weights, single.rows = FALSE))
        # 
        # # classify pairs using a threshhold
        #  summary(epiClassify(match10.weights, threshold.upper = 0.99)) # based on visual inspection of curve and dataset with weights
        # 
        # # get linked pairs
        #  match10.match <- get.linked.pairs.mcare.pha(match10.pairs.long, 0.99)     
        # 
        # # remove the linked data from the two parent datasets so we don't try to link them again
        #  pha <- pha[!(pid %in% match10.match$pid)]
        #  mcare <- mcare[!(id_mcare %in% match10.match$id_mcare)]
        # 
        # # clean objects in memory
        # rm(match10, match10.weights, match10.pairs, match10.pairs.long) # drop tables only needed to form the linkage        
      
      # Match  11 - Probabilistic: Block DOB + gender_me, string compare SSN + names ----    
        match11 <- compare.linkage(mcare, pha, 
                                 blockfld = c("dob.year", "dob.month", "dob.day", "gender_me"), # blocking
                                 strcmp = c("ssn", "name_mdl", "name_gvn", "name_srnm"), strcmpfun = levenshteinSim) # computer similarity between strings
        #phonetic = c("name_srnm", "name_gvn"), phonfun = soundex) # use phonetics for names, rather than spelling
        
        # get summary of potential pairs
        summary(match11) 
        
        # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
        match11.weights <- epiWeights(match11) 
        summary(match11.weights)
        
        # get paired data, with weights, as a dataset
        match11.pairs <- setDT(getPairs(match11.weights, single.rows = TRUE))
        match11.pairs.long <- setDT(getPairs(match11.weights, single.rows = FALSE))

        # classify pairs using a threshhold
        summary(epiClassify(match11.weights, threshold.upper = 0.537)) # based on visual inspection of curve and dataset with weights
        
        # Have dupliacate IDs ... so, keep the one with higher probability weight
        match11.pairs[, dup.mcaid := 1:.N, by = id_mcare.1] # already sorted by Weight
        match11.pairs[, dup.pid := 1:.N, by = pid.2] # already sorted by Weight
        match11.pairs <- match11.pairs[dup.mcaid==1 & dup.pid == 1] # many if not all of these are a single person with different ids 
        
        # get linked pairs
        match11.match <- match11.pairs[Weight >= 0.537]
        setnames(match11.match, c("pid.2", "id_mcare.1"), c("pid", "id_mcare"))
        match11.match <- match11.match[, c("pid", "id_mcare")]            
        
        # remove the linked data from the two parent datasets so we don't try to link them again
        pha <- pha[!(pid %in% match11.match$pid)]
        mcare <- mcare[!(id_mcare %in% match11.match$id_mcare)]
        
        # clean objects in memory
        rm(match11, match11.weights, match11.pairs.long, match11.pairs) # drop tables only needed to form the linkage                   
        
        # there were other fairly certain matches that remained using this method, but a probability cut-off woudln't work. Woudl need some kind of machine learning.
      
      # Match  12 - Probabilistic: MEMORY OVERLOADED ... Block DOB year + DOB mon + gender_me, string compare DOB day + SSN + middle initial, soundex first & last name ----
        # match12 <- compare.linkage(mcare, pha, 
        #                         blockfld = c("dob.year", "dob.month", "gender_me"), # blocking
        #                         strcmp = c("ssn", "dob.day", "name_mdl"),  # computer similarity between two
        #                         phonetic = c("name_srnm", "name_gvn"), phonfun = soundex)
        # 
        # # get summary of potential pairs
        # summary(match12) 
        # 
        # # calculate EpiLink weights (https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-0038-1633924)
        # match12.weights <- epiWeights(match12) 
        # summary(match12.weights)
        # gc()
        # 
        # # get paired data, with weights, as a dataset
        # # match12.pairs <- setDT(getPairs(match12.weights, single.rows = TRUE))
        # match12.pairs.long <- setDT(getPairs(match12.weights, single.rows = FALSE))
        # 
        # # classify pairs using a threshhold
        # summary(epiClassify(match12.weights, threshold.upper = 0.72)) # based on visual inspection of curve and dataset with weights     
        # 
        # # get linked pairs
        # match12.match <- get.linked.pairs.mcare.pha(match12.pairs.long, 0.72)            
        # 
        # # drop the linked data from the two parent datasets so we don't try to link them again
        # pha <- pha[!(pid %in% match12.match$pid)]
        # mcare <- mcare[!(id_mcare %in% match12.match$id_mcare)]            
        # 
        # # clean objects in memory
        # rm(match12, match12.pairs.long, match12.weights)      
      
  ## (3) Combine linked ID pairs ####
      # identify tables of matched pairs
      xwalk.list <- as.list(mget(grep("match", ls(), value = TRUE)))
      
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
      
  ## (4) Load xwalk table to SQL ----
      # create last_run timestamp
      xwalk[, last_run := Sys.time()]
      
      # create table ID for SQL
      tbl_id <- DBI::Id(schema = "tmp", 
                        table = "xwalk_mcare_pha")  
      
      # Identify the column types to be created in SQL
      sql.columns <- c("pid" = "integer", "id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "last_run" = "datetime")  
      
      # ensure column order in R is the same as that in SQL
      setcolorder(xwalk, names(sql.columns))
      
      # Write table to SQL
      dbWriteTable(db_apde51, 
                   tbl_id, 
                   value = as.data.frame(xwalk),
                   overwrite = T, append = F, 
                   field.types = sql.columns)
      
      # Confirm that all rows were loaded to sql
      stage.count <- as.numeric(odbc::dbGetQuery(db_apde51, 
                                                 "SELECT COUNT (*) FROM tmp.xwalk_mcare_pha"))
      if(stage.count != nrow(xwalk))
        stop("Mismatching row count, error writing or reading data")      
      
      
  ## (5) Close ODBC connection and drop temporary files ----    
      dbDisconnect(db_apde51) 
      dbDisconnect(db_claims51)      
      rm(list=(setdiff(ls(), keep.me)))
      gc()
      
#### ----------------- ####
#### LINK MCAID-PHA    ####   
#### ----------------- #### 
  ## (1) Load data from SQL ----
      # Medicaid ----
      db_claims51 <- dbConnect(odbc(), "PHClaims51")
      mcaid <- setDT(odbc::dbGetQuery(db_claims51, "SELECT 
                                      [id_mcaid], [ssn], [dob.year], [dob.month], [dob.day], [name_srnm], [name_gvn], [name_mdl], [gender_me] 
                                      FROM [PHClaims].[tmp].[xwalk_mcaid_prepped]"))
      
      # Housing ----
      db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
      pha <- setDT(odbc::dbGetQuery(db_apde51, "SELECT 
                                      [pid], [ssn], [dob.year], [dob.month], [dob.day], [name_srnm], [name_gvn], [name_mdl], [gender_me] 
                                      FROM [PH_APDEStore].[tmp].[xwalk_pha_prepped]"))
      
  ## (2) ---------- LINK DATA ---------- ----
      # MATCH 1: Determinist: Perfect ####
        match1 <- merge(mcaid, pha, by = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me", "name_srnm", "name_gvn", "name_mdl"), all=FALSE) 
        match1 <- match1[, .(pid, id_mcaid)] # keep the paired ids only
        
        # drop if there duplicate ids (keeping largest ID, presuming it is newer)
        setorder(match1, pid)
        match1[, dup := 1:.N, by = "id_mcaid"] 
        match1 <- match1[dup==1]
        
        setorder(match1, id_mcaid)
        match1[, dup := 1:.N, by = "pid"] 
        match1 <- match1[dup==1]
        
        match1[, dup := NULL]
        
        #removing the perfectly linked data from the Mcaid & PHA datasets because there is no need to perform additional record linkage functions on them
        mcaid <- mcaid[!(id_mcaid %in% match1$id_mcaid)]
        pha <- pha[!(pid %in% match1$pid)]
      
      # MATCH 2: Deterministic: Almost perfect (ignore middle initial) ####
        match2 <- merge(mcaid, pha, by = c("ssn", "dob.year", "dob.month", "dob.day", "gender_me", "name_srnm", "name_gvn"), all=FALSE) 
  
        match2 <- match2[, .(pid, id_mcaid)] # keep the paired ids only
  
        #removing the (near) perfectly linked data from the Mcaid & PHA datasets because there is no need to perform additional record linkage functions on them
        mcaid <- mcaid[!(id_mcaid %in% match2$id_mcaid)]
        pha <- pha[!(pid %in% match2$pid)]
      
      # MATCH 3: Deterministic: Perfect if swap first and last names (happens often with Asian names) ####
        mcaid.alt <- copy(mcaid)
        setnames(mcaid.alt, c("name_gvn", "name_srnm"), c("name_gvn.orig", "name_srnm.orig"))
        mcaid.alt[, name_gvn := name_srnm.orig][, name_srnm := name_gvn.orig]
        
        match3 <- merge(pha, mcaid.alt, by = c("dob.year", "dob.month", "dob.day", "gender_me","name_srnm", "name_gvn"), all = FALSE)
        match3 <- match3[(is.na(ssn.x) | is.na(ssn.y)) | ssn.x == ssn.y, ] # keep when SSN matches or >= 1 SSN is missing
        
        match3 <- match3[, .(pid, id_mcaid)]
        
        pha <- pha[!(pid %in% match3$pid)] # remove the matched rows from the parent pha dataset
        mcaid <- mcaid[!(id_mcaid %in% match3$id_mcaid)] # remove the matched rows from the parent mcaid dataset
        rm(mcaid.alt)
      
      # MATCH 4: Probabilistic: Block on SSN, match on other vars ####
           match4 <- compare.linkage(mcaid, pha, blockfld = c("ssn"),
                                    strcmp = c("name_mdl", "gender_me", "dob.year", "dob.month", "dob.day"),
                                    phonetic = c("name_srnm", "name_gvn"), phonfun = soundex)

          # Using EpiLink approach
          match4.weights <- epiWeights(match4)
          summary(match4.weights)
          
          # browse potential matches to identify a cutpoint
          match4.pairs <- setDT(getPairs(match4.weights, single.rows = TRUE))
          # View(match4.pairs[as.numeric(as.character(Weight)) > 0.45, 
          #                   .(Weight, name_srnm.1,name_srnm.2, name_gvn.1, name_gvn.2, name_mdl.1, name_mdl.2, 
          #                     gender_me.1, gender_me.2, dob.year.1, dob.year.2, dob.month.1, dob.month.2, dob.day.1, dob.day.2)])
          
          classify2 <- epiClassify(match4.weights, threshold.upper = 0.45)
          summary(classify2)
          match4 <- getPairs(classify2, single.rows = TRUE)
          
          # select the rows with matches that we trust 
          match4 <- setDT(match4 %>%
                            filter(
                              # Looks like 0.45 is a good cutoff when SSN and DOBs match exactly
                              (Weight >= 0.45 & dob.year.1 == dob.year.2 & dob.month.1 == dob.month.2 & dob.day.1 == dob.day.2) |
                                # Can use 0.49 when SSN and YOB match
                                (Weight >= 0.49 & dob.year.1 == dob.year.2) |
                                # When SSN, MOB, and DOB match but YOB is 1-2 years off
                                (Weight <= 0.49 & dob.year.1 != dob.year.2 & 
                                   dob.month.1 == dob.month.2 & dob.day.1 == dob.day.2 &
                                   abs(as.numeric(dob.year.1) - as.numeric(dob.year.2)) <= 2)
                            )) 
          
          # When an id matched > 1x, keep the row with the higher weight / probability 
          match4[, dup.mcaid := 1:.N, by = id_mcaid.1] # already sorted by Weight
          match4[, dup.pid := 1:.N, by = pid.2] # already sorted by Weight
          match4 <- match4[dup.mcaid==1 & dup.pid == 1] # many if not all of these are a single person with different ids
          
          # keep and standardize id pairs
          setnames(match4, c("pid.2", "id_mcaid.1"), c("pid", "id_mcaid"))
          match4 <- match4[, c("pid", "id_mcaid")]
          
          # remove pairs just matched from the universe of possible matching data
          mcaid <- mcaid[!(id_mcaid %in% match4$id_mcaid)]
          pha <- pha[!(pid %in% match4$pid)]
          
          # drop temporary objects
          rm(match4.pairs, match4.weights, classify2)
        
      # MATCH 5: Probabilistic: Block on soundex last name, match other vars #####
        match5 <- compare.linkage(pha, mcaid, blockfld = c("name_srnm"),
                                  strcmp = c("name_mdl", "gender_me", "dob.year", "dob.month", "dob.day"),
                                  phonetic = c("name_gvn"), phonfun = soundex,
                                  exclude = c("ssn"))
        
        # Using EpiLink approach
        match5.weights <- epiWeights(match5)
        summary(match5.weights)
        
        # browse potential matches to identify a cutpoint
        match5.pairs <- setDT(getPairs(match5.weights, single.rows = TRUE))
        # View(match5.pairs[as.numeric(as.character(Weight)) > 0.719, 
                          # .(Weight, name_gvn.1, name_gvn.2, name_mdl.1, name_mdl.2, gender_me.1, gender_me.2, ssn.1, ssn.2, 
                          #   dob.year.1, dob.year.2, dob.month.1, dob.month.2, dob.day.1, dob.day.2)])

        classify2 <- epiClassify(match5.weights, threshold.upper = 0.719)
        summary(classify2)

        # Looks like 0.719 is a good cutoff here
        match5 <- match5.pairs[Weight >=0.719]
        
        # When an id matched > 1x, keep the row with the higher weight / probability 
        match5[, dup.mcaid := 1:.N, by = id_mcaid.2] # already sorted by Weight
        match5[, dup.pid := 1:.N, by = pid.1] # already sorted by Weight
        match5 <- match5[dup.mcaid==1 & dup.pid == 1] # many if not all of these are a single person with different ids
        
        # keep and standardize id pairs
        setnames(match5, c("pid.1", "id_mcaid.2"), c("pid", "id_mcaid"))
        match5 <- match5[, c("pid", "id_mcaid")]
        
        # remove pairs just matched from the universe of possible matching data
        mcaid <- mcaid[!(id_mcaid %in% match5$id_mcaid)]
        pha <- pha[!(pid %in% match5$pid)]
        
        # drop temporary objects
        rm(match5.pairs, match5.weights, classify2)
      
  ## (3) Combine linked ID pairs ----
      # identify tables of matched pairs
      xwalk.list <- as.list(mget(grep("^match", ls(), value = TRUE)))
      
      # combine tables of matched pairs
      xwalk <- rbindlist(xwalk.list, use.names = TRUE)       
      
      # deduplicate entire rows (just in case, should not be needed)
      xwalk <- unique(xwalk)     
      
  ## (4) Load xwalk table to SQL ----
      # create last_run timestamp
      xwalk[, last_run := Sys.time()]
      
      # create table ID for SQL
      tbl_id <- DBI::Id(schema = "tmp", table = "xwalk_mcaid_pha")  
      
      # Identify the column types to be created in SQL
      sql.columns <- c("pid" = "integer", "id_mcaid" = "char(11)", "last_run" = "datetime")  
      
      # ensure column order in R is the same as that in SQL
      setcolorder(xwalk, names(sql.columns))
      
      # Write table to SQL
      dbWriteTable(db_apde51, 
                   tbl_id, 
                   value = as.data.frame(xwalk),
                   overwrite = T, append = F, 
                   field.types = sql.columns)
      
      # Confirm that all rows were loaded to sql
      stage.count <- as.numeric(odbc::dbGetQuery(db_apde51, 
                                                 "SELECT COUNT (*) FROM tmp.xwalk_mcaid_pha"))
      if(stage.count != nrow(xwalk))
        stop("Mismatching row count, error writing or reading data")      
      
  ## (5) Close ODBC connection and drop temporary files ----    
      dbDisconnect(db_apde51) 
      dbDisconnect(db_claims51)      
      rm(list=(setdiff(ls(), keep.me)))
      gc()
      
#### ------------------------ ####
#### LINK ID_APDE - MCAID - MCARE - PHA ####   
#### ------------------------ #### 
  ## (1) Load data from SQL ----
      # PHClaims51
        db_claims51 <- dbConnect(odbc(), "PHClaims51")
        
        mcare.mcaid <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT id_mcare, id_mcaid FROM [PHClaims].[tmp].[xwalk_mcaid_mcare]"))    
        
        mcaid.only <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT id_mcaid FROM final.mcaid_elig_demo")) # go back to elig_demo because it has an ALMOST complete list of all Mcaid ID
        
        mcare.only <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT id_mcare FROM final.mcare_elig_demo")) # go back to elig_demo because it has an ALMOST complete list of all Mcare ID
        
      # PH_APDEStore51
        db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")      
      
        pha.only <- setDT(odbc::dbGetQuery(db_apde51, "SELECT pid, enddate FROM stage.pha")) # did not filter to keep just >=2012
        pha.only <- pha.only[year(enddate) >= 2012, ][, enddate := NULL]
        pha.only <- unique(pha.only)
        
        mcaid.pha <- setDT(odbc::dbGetQuery(db_apde51, "SELECT DISTINCT pid, id_mcaid FROM [PH_APDEStore].[tmp].[xwalk_mcaid_pha]"))
        
        mcare.pha <- setDT(odbc::dbGetQuery(db_apde51, "SELECT DISTINCT pid, id_mcare FROM [PH_APDEStore].[tmp].[xwalk_mcare_pha]"))
        
  ## (2) Ensure there are no duplicate IDs ----        
      # check for duplicate IDs in mcare.mcaid ----
          if(length(unique(mcare.mcaid$id_mcare)) != nrow(mcare.mcaid)){
            stop("id_mcare in mcare.mcaid is not unique")
          }
          if(length(unique(mcare.mcaid$id_mcaid)) != nrow(mcare.mcaid)){
            stop("id_mcaid in mcare.mcaid is not unique")
        }
      
      # check for duplicate IDs in mcare.only ----
          if(length(unique(mcare.only$id_mcare)) != nrow(mcare.only)){
            stop("id_mcare in mcare.only is not unique")
          }
      
      # check for duplicate IDs in mcaid.only ----
          if(length(unique(mcaid.only$id_mcaid)) != nrow(mcaid.only)){
            stop("id_mcaid in mcaid.only is not unique")
          }    
      
      # check for duplicate IDs in pha.only ----
          if(length(unique(pha.only$pid)) != nrow(pha.only)){
            stop("pid in pha.only is not unique")
          }    
        
      # check for duplicate IDs in mcare.pha ----
          if(length(unique(mcare.pha$id_mcare)) != nrow(mcare.pha)){
            stop("id_mcare in mcare.pha is not unique")
          }
          if(length(unique(mcare.pha$pid)) != nrow(mcare.pha)){
            stop("pid in mcare.pha is not unique")
          }
        
      # check for duplicate IDs in mcaid.pha ----
        if(length(unique(mcaid.pha$id_mcaid)) != nrow(mcaid.pha)){
          stop("id_mcaid in mcaid.pha is not unique")
        }
        if(length(unique(mcaid.pha$pid)) != nrow(mcaid.pha)){
          stop("pid in mcaid.pha is not unique")
        }
      
  ## (3) Combined the three linked data files using mcare.mcaid as backbone ----
      # Merge mcare.mcaid with mcare.pha ----
        mcare.mcaid_mcare.pha <- merge(mcare.mcaid, mcare.pha, by = "id_mcare", all.x = TRUE, all.y = FALSE)
        
      # Merge mcare.mcaid with mcaid.pha ----
        mcare.mcaid_mcaid.pha <- merge(mcare.mcaid, mcaid.pha, by = "id_mcaid", all.x = TRUE, all.y = FALSE)
        
      # Append mcare.mcaid_mcare.pha & mcare.mcaid_mcaid.pha ----
        mcaid.mcare.pha <- unique(rbind(mcare.mcaid_mcare.pha, mcare.mcaid_mcaid.pha))
        
      # Clean up / deduplicate mcaid.mcare.pha ----
       # drop if mcare/mcaid combo exists already and missing PID 
        mcaid.mcare.pha[, dup.mcare.mcaid := .N, by = c("id_mcaid", "id_mcare")]
        mcaid.mcare.pha <- mcaid.mcare.pha[!(dup.mcare.mcaid !=1 & is.na(pid))] 
        mcaid.mcare.pha[, dup.mcare.mcaid := NULL]
        
        # keeper larger PID if >1 PID matches with a unique mcare/mcaid combination 
        setorder(mcaid.mcare.pha, -pid) 
        mcaid.mcare.pha[, dup.mcare.mcaid := 1:.N, by = c("id_mcare", "id_mcaid")]
        mcaid.mcare.pha <- mcaid.mcare.pha[dup.mcare.mcaid==1]
        mcaid.mcare.pha[, dup.mcare.mcaid := NULL]
        
        # check if there are remaining duplicate IDs (to assess whether can progress) 
        if(sum(duplicated(mcaid.mcare.pha[!is.na(id_mcaid)]$id_mcaid)) > 0) 
          stop("Fix duplicate id_mcaid")
        
        if(sum(duplicated(mcaid.mcare.pha[!is.na(id_mcare)]$id_mcare)) > 0)
          stop("Fix duplicate id_mcare")

        if(sum(duplicated(mcaid.mcare.pha[!is.na(pid)]$pid)) > 0)
          stop("Fix duplicate pid")

  ## (4) Add mcare.mcaid that are not already in mcaid.mcare.pha  ----
        # don't expect anything here, but added for sake of completeness
        mcaid.mcare.pha <- rbind(
          mcaid.mcare.pha,
          mcare.mcaid[!id_mcare %in% mcaid.mcare.pha$id_mcare & !id_mcaid %in% mcaid.mcare.pha$id_mcaid, ], 
          fill = TRUE
        )
        
  ## (5) Add mcare.pha that are not already in mcaid.mcare.pha ----
        mcaid.mcare.pha <- rbind(
          mcaid.mcare.pha,
          mcare.pha[!id_mcare %in% mcaid.mcare.pha$id_mcare & !pid %in% mcaid.mcare.pha$pid],
          fill = TRUE
        )
        
  ## (6) Add mcaid.pha that are not already in mcaid.mcare.pha ----
        mcaid.mcare.pha <- rbind(
          mcaid.mcare.pha,
          mcaid.pha[!id_mcaid %in% mcaid.mcare.pha$id_mcaid & !pid %in% mcaid.mcare.pha$pid],
          fill = TRUE
        )     
        
  ## (7) Add mcare.only that are not already in mcaid.mcare.pha ----
        mcaid.mcare.pha <- rbind(
          mcaid.mcare.pha,
          mcare.only[!id_mcare %in% mcaid.mcare.pha$id_mcare],
          fill = TRUE
        )  
        
  ## (8) Add mcaid.only that are not already in mcaid.mcare.pha ----
        mcaid.mcare.pha <- rbind(
          mcaid.mcare.pha,
          mcaid.only[!id_mcaid %in% mcaid.mcare.pha$id_mcaid],
          fill = TRUE
        )  
        
  ## (9) Add pha.only that are not already in mcaid.mcare.pha ----
        mcaid.mcare.pha <- rbind(
          mcaid.mcare.pha,
          pha.only[!pid %in% mcaid.mcare.pha$pid],
          fill = TRUE
        )  
        
  ## (10) Confirm that every ID is accounted for ----
        # Check Mcare
        extra.mcare <- setdiff(mcaid.mcare.pha[!is.na(id_mcare)]$id_mcare, mcare.only[!is.na(id_mcare)]$id_mcare) 
        missing.mcare <- setdiff(mcare.only[!is.na(id_mcare)]$id_mcare, mcaid.mcare.pha[!is.na(id_mcare)]$id_mcare) 
        length(extra.mcare) # Expect there will be extra b/c there are Mcare ids in SSN and Names files that are not in MBSF
        length(missing.mcare) # should be zero in length

        # Check Mcaid
        extra.mcaid <- setdiff(mcaid.mcare.pha[!is.na(id_mcaid)]$id_mcaid, mcaid.only[!is.na(id_mcaid)]$id_mcaid)
        missing.mcaid <- setdiff(mcaid.only[!is.na(id_mcaid)]$id_mcaid, mcaid.mcare.pha[!is.na(id_mcaid)]$id_mcaid)
        length(extra.mcaid) # Expect will be more than zero because there were id_mcaid in stage.mcaid_elig that were not in the elig_demo
        length(missing.mcaid) # should be zero

        # Check PHA
        extra.pid <- setdiff(mcaid.mcare.pha[!is.na(pid)]$pid, pha.only[!is.na(pid)]$pid)
        missing.pid <- setdiff(pha.only[!is.na(pid)]$pid, mcaid.mcare.pha[!is.na(pid)]$pid)
        length(extra.pid) # should be zero
        length(missing.pid) # should be zero

  ## (11) Confirm that there are no duplicates in the final mcaid.mcare.pha linkage ----      
        if(
          sum(duplicated(mcaid.mcare.pha[!is.na(id_mcare)]$id_mcare)) + 
          sum(duplicated(mcaid.mcare.pha[!is.na(id_mcaid)]$id_mcaid)) + 
          sum(duplicated(mcaid.mcare.pha[!is.na(pid)]$pid)) > 0)
          stop("There should be no duplicates in this final linked data.table")
        
  ## (12) Generate id_apde ----
        set.seed(98104) # set starting point for randomization of ordering
        mcaid.mcare.pha[, random.number := runif(nrow(mcaid.mcare.pha))] # create column of random numbers to be used for sorting
        setorder(mcaid.mcare.pha, random.number)
        mcaid.mcare.pha[, random.number := NULL]
        mcaid.mcare.pha[, id_apde := .I]
        setcolorder(mcaid.mcare.pha, c("id_apde", "id_mcare", "id_mcaid", "pid"))

  ## (13) Load mcaid.mcare.pha table to SQL ----
      # create last_run timestamp
      mcaid.mcare.pha[, last_run := Sys.time()]
      
      # create table ID for SQL
      tbl_id <- DBI::Id(schema = "stage", 
                        table = "xwalk_apde_mcaid_mcare_pha")  
      
      # identify the column types to be created in SQL
      sql.columns <- c("id_apde" = "integer", "id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "id_mcaid" = "char(11)", "pid" = "integer", "last_run" = "datetime")  
      
      # ensure column order in R is the same as that in SQL
      setcolorder(mcaid.mcare.pha, names(sql.columns))
      
      # Write table to SQL
      dbWriteTable(db_claims51, 
                   tbl_id, 
                   value = as.data.frame(mcaid.mcare.pha),
                   overwrite = T, append = F, 
                   field.types = sql.columns)
      
      # Confirm that all rows were loaded to sql
      stage.count <- as.numeric(odbc::dbGetQuery(db_claims51, 
                                                 "SELECT COUNT (*) FROM stage.xwalk_apde_mcaid_mcare_pha"))
      if(stage.count != nrow(mcaid.mcare.pha))
        stop("Mismatching row count, error writing or reading data")      
      
      # close database connections    
      dbDisconnect(db_claims51)  
      dbDisconnect(db_apde51)  
  
#### ---------------------- ####
#### DROP TMP SQL TABLES    ####   
#### ---------------------- #### 
  ## Drop tables from PhClaims51 ----
      db_claims51 <- dbConnect(odbc(), "PHClaims51")
      dbGetQuery(db_claims51, "DROP TABLE [PHClaims].[tmp].[xwalk_mcaid_mcare]")
      dbGetQuery(db_claims51, "DROP TABLE [PHClaims].[tmp].[xwalk_mcaid_prepped]")
      dbGetQuery(db_claims51, "DROP TABLE [PHClaims].[tmp].[xwalk_mcare_prepped]")
      dbDisconnect(db_claims51)  
    
  ## Drop tables from PH_APDEStore51 ----   
      db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
      dbGetQuery(db_apde51, "DROP TABLE [PH_APDEStore].[tmp].[xwalk_mcaid_pha]")
      dbGetQuery(db_apde51, "DROP TABLE [PH_APDEStore].[tmp].[xwalk_mcare_pha]")
      dbGetQuery(db_apde51, "DROP TABLE [PH_APDEStore].[tmp].[xwalk_pha_prepped]")
      dbDisconnect(db_apde51) 

## The end! ----      
    run.time <- Sys.time() - start.time  
    print(run.time)
    
    Sys.time() - start.time
    