# Header ####
  # Author: Danny Colombara
  # Date: August 28, 2019
  # Purpose: Create file with essential Medicaid data for linkage
  #
  # Notes: When there is conflicting information, will assume the most recent data is correct because it reflects a correction
  #        The exception is when the newer data is missing, in which case it will be filled with the most recent non-missing

## Set up R Environment ----
    rm(list=ls())  # clear memory
    pacman::p_load(data.table, odbc, DBI, tidyr, RecordLinkage) # load packages
    options("scipen"=999) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    
    start.time <- Sys.time()
  
## (1) Connect to SQL Server ----    
    db_claims <- dbConnect(odbc(), "PHClaims51")   
  
## (2) Load data from SQL ----  
    elig <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcaid, dob, gender_me, gender_female, gender_male FROM stage.mcaid_elig_demo"))

    name <- setDT(odbc::dbGetQuery(db_claims, "SELECT MEDICAID_RECIPIENT_ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, CLNDR_YEAR_MNTH FROM stage.mcaid_elig"))
    setnames(name, names(name), c("id_mcaid", "name_gvn", "name_mdl", "name_srnm", "date"))
    
    ssn <- setDT(odbc::dbGetQuery(db_claims, "SELECT MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, CLNDR_YEAR_MNTH FROM stage.mcaid_elig"))
    setnames(ssn, names(ssn), c("id_mcaid", "ssn", "date"))
    
## (3) Tidy individual data files before merging ----
    # elig ----
      elig <- unique(elig)
      if(nrow(elig) - length(unique(elig$id_mcaid)) != 0){
        stop('non-unique id_mcaid in elig')
      }
    
    # ssn ----
      # sort data
        setkey(ssn, id_mcaid, date)
        
      # identify enrollees with a SSN and keep the newest SSN
        ssn.ok <- ssn[!is.na(ssn)]
        ssn.ok <- ssn.ok[, rank := 1:.N, by = "id_mcaid"] 
        ssn.ok <- ssn.ok[rank == 1][, c("date", "rank") := NULL] 
        
      # identify enrollees who are missing SSN
        ssn.na <- unique(ssn[!id_mcaid %in% ssn.ok$id_mcaid][, c("date") := NULL]) 
        
      # append the those without SSN to those with SSN
        ssn <- rbind(ssn.ok, ssn.na)
        setkey(ssn, id_mcaid)
        rm(ssn.ok, ssn.na)
        
      # ensure there is only one row per id
        if(nrow(ssn) - length(unique(ssn$id_mcaid)) != 0){
          stop('non-unique id_mcaid in ssn')
        }
        
    # name ----  
      # sort data
        setkey(name, id_mcaid, date)
        
      # Remove any extraneous spaces at the beginning or end of a name
        name[, name_gvn := gsub("^ ", "", name_gvn)]
        name[, name_mdl := gsub("^ ", "", name_mdl)]
        name[, name_srnm := gsub("^ ", "", name_srnm)]
        name[, name_gvn := gsub(" $", "", name_gvn)]
        name[, name_mdl := gsub(" $", "", name_mdl)]
        name[, name_srnm := gsub(" $", "", name_srnm)]
        name <- unique(name)
        
      # Split those with consistent data from the others
        name <- name[, rank := 1:.N, by = c("id_mcaid", "name_gvn", "name_mdl", "name_srnm")] # rank for each set of unique data 
        name <- name[rank == 1][, c("rank") := NULL] # keep only most recent set of unique data rows
        name[, dup := .N, by = id_mcaid]  # identify duplicates by id
        name.ok <- name[dup == 1][, c("dup", "date") := NULL]
        
        name.dups <- name[dup > 1][, dup := NULL]
        
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
        name <- rbind(name.ok, name.dups)
        setkey(name, id_mcaid)
        rm(mi.na, mi.ok, mi.ok.copy, name.dups, name.ok)
        
      # ensure there is only one row per id
        if(nrow(name) - length(unique(name$id_mcaid)) != 0){
          stop('non-unique id_mcaid in ssn')
        }


## (4) Merge the Mcaid datasets ----
    id.key <- merge(elig, name, by = "id_mcaid", all=TRUE)
    id.key <- merge(id.key, ssn, by = "id_mcaid", all=TRUE)
  
## (5) Clean merged dataset ----
  # without ssn, dob, and last name, there is no hope of matching
    id.key <- id.key[!(is.na(ssn) & is.na(dob) & is.na(name_srnm) ), ] 
    
  # fill in missing SSN when dob, sex, and complete name are an exact match
    ssn.ok <- id.key[!is.na(ssn), ]
    ssn.na <- id.key[is.na(ssn), ]
    ssn.na[, c("ssn") := NULL]
    ssn.na <- merge(ssn.na, ssn.ok, by = c("dob", "gender_me", "gender_female", "gender_male", "name_mdl", "name_srnm", "name_gvn"), all.x = TRUE, all.y = FALSE)
    ssn.na[, id_mcaid.y := NULL]
    setnames(ssn.na, "id_mcaid.x", "id_mcaid")
    id.key <- rbind(ssn.ok, ssn.na)
    rm(ssn.ok, ssn.na)
  
  # deduplicate when all information is exactly the same except for id_mcaid
    setorder(id.key, -id_mcaid) # sort with largest ID at top
    id.key <- id.key[, dup := 1:.N, by = c("dob", "gender_me", "gender_female", "gender_male", "name_mdl", "name_srnm", "name_gvn", "ssn")]
    id.key <- id.key[dup == 1, ] # when all data duplicate, keep only the most recent, i.e., the largest id_mcaid
    id.key[, dup := NULL]
    
  # deduplicate when ssn appears more than once (this shouldn't happen because of the processing above)
    setorder(id.key, ssn, -id_mcaid) # order by ssn & id so that can identify the max id. 
    id.key[!is.na(ssn), dup := 1:.N, by="ssn"] # identify when when there are duplicate ssn (and ssn is not missing)
    id.key <- id.key[is.na(dup) | dup == 1, ][, dup := NULL] # drop when N > 1, this will keep the max mcaid id only, which is what we agreed to with Eli and Alastair
    
  # deduplicate when all data are the same except for the meciaid ID & SSN
    id.key[, dup := .N, by = c("dob", "gender_me", "name_mdl", "name_srnm", "name_gvn")]
    dup.ok <- id.key[dup == 1][, dup := NULL] # not duplicated
    dup <- id.key[dup > 1][, dup := NULL] # has duplicates
    setorder(dup, dob, name_gvn, name_mdl, name_srnm, -id_mcaid) # sort duplicated data, with largest (newest) id_mcaid first for for each person
    dup[, rank := 1:.N, by = c("dob", "gender_me", "name_mdl", "name_srnm", "name_gvn")]
    dup <- dup[rank==1, ][, rank := NULL] # keep only the most recent for each set of duplicates
    id.key <- rbind(dup.ok, dup) # combine unduplicated and de-duplicated data
    rm(dup.ok, dup)
    
## (6) Load Medicaid id table to SQL ----
  # create last_run timestamp
    id.key[, last_run := Sys.time()]
  
  # create table ID for SQL
    tbl_id <- DBI::Id(schema = "stage", 
                      table = "xwalk_01_linkage_prep_mcaid")  
  
  # column types for SQL
    sql.columns <- c("id_mcaid" = "CHAR(11)", "ssn" = "char(9)", "dob" = "date", "name_srnm" = "varchar(255)", 
                     "name_gvn" = "varchar(255)", "name_mdl" = "varchar(255)", 
                     "gender_me" = "varchar(255)", "gender_female" = "integer", "gender_male" = "integer", "last_run" = "datetime")  
  
  # ensure column order in R is the same as that in SQL
    setcolorder(id.key, names(sql.columns))
  
  # Write table to SQL
    dbWriteTable(db_claims, 
                 tbl_id, 
                 value = as.data.frame(id.key),
                 overwrite = T, append = F, 
                 field.types = sql.columns)
  
  # Confirm that all rows were loaded to sql
    stage.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                               "SELECT COUNT (*) FROM stage.xwalk_01_linkage_prep_mcaid"))
    if(stage.count != nrow(id.key))
      stop("Mismatching row count, error writing or reading data")
  
## (7) Close ODBC ----
      dbDisconnect(db_claims)        
      
## The end! ----      
    run.time <- Sys.time() - start.time  
    print(run.time)
    