# Header ####
  # Author: Danny Colombara
  # Date: August 28, 2019
  # Purpose: Create stage.mcaid_mcare_elig_demo for SQL
  #
  # This code is designed to be run as part of the master Medicaid/Medicare script:
  # https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_mcare_analytic.R
  #

## Set up R Environment ----
  # rm(list=ls())  # clear memory
  # pacman::p_load(data.table, odbc, DBI, lubridate) # load packages
  # options("scipen"=999) # turn off scientific notation  
  # options(warning.length = 8170) # get lengthy warnings, needed for SQL
  
  start.time <- Sys.time()
  
  yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_demo.yaml"
  
## (1) Connect to SQL Server ----    
  # db_claims <- dbConnect(odbc(), "PHClaims51")   
  
## (2) Load data from SQL ----  
  apde <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_apde, id_mcare, id_mcaid 
                                 FROM PHClaims.final.xwalk_apde_mcaid_mcare_pha"))
  
  mcare <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcare, dob, death_dt, geo_kc_ever, gender_female, gender_male, gender_me, gender_recent, race_eth_recent, race_recent,
                                  race_white, race_black, race_other, race_asian, race_asian_pi, race_aian, race_nhpi, race_latino, race_unk, race_eth_me, race_me 
                                  FROM PHClaims.final.mcare_elig_demo"))

  mcaid <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcaid, dob, gender_female, gender_male, gender_me, gender_recent, 
                                  race_eth_recent, race_recent, race_me, race_eth_me, 
                                  race_aian, race_asian, race_black, race_latino, race_nhpi, race_white, race_unk, race_eth_unk,  
                                  lang_max, lang_amharic, lang_arabic, lang_chinese, lang_korean, lang_english,
                                  lang_russian, lang_somali, lang_spanish, lang_ukrainian, lang_vietnamese 
                                  FROM PHClaims.final.mcaid_elig_demo"))

## (3) Merge on apde id ----
  mcare <- merge(apde[, .(id_apde, id_mcare)], mcare, by = "id_mcare", all.x = FALSE, all.y = TRUE)
  mcare[, id_mcare := NULL] # no longer needed now that have id_apde
  
  mcaid <- merge(apde[, .(id_apde, id_mcaid)], mcaid, by = "id_mcaid", all.x = FALSE, all.y = TRUE)
  mcaid[, id_mcaid := NULL] # no longer needed now that have id_apde
  
## (4) Identify the duals and split from non-duals ----
  dual.id <- intersect(mcaid$id_apde, mcare$id_apde)
  
  mcare.solo <- mcare[!id_apde %in% dual.id]
  mcaid.solo <- mcaid[!id_apde %in% dual.id]  
  
  mcare.dual <- mcare[id_apde %in% dual.id]
  mcaid.dual <- mcaid[id_apde %in% dual.id]

## (5) Combine the data for duals ----
  # some data is assumed to be more reliable in one dataset compared to the other
  dual <- merge(x = mcaid.dual, y = mcare.dual, by = "id_apde")
  setnames(dual, names(dual), gsub("\\.x$", ".mcaid", names(dual))) # clean up suffixes to eliminate confusion
  setnames(dual, names(dual), gsub("\\.y$", ".mcare", names(dual))) # clean up suffixes to eliminate confusion
  
  # ascribe MCARE data to duals
  dual[, dob := dob.mcaid] # default date of birth from Mcaid
  dual[!is.na(dob.mcare), dob := dob.mcare][, c("dob.mcaid", "dob.mcare") := NULL] # replace with Mcare when possible
    # race_asian_pi, death_dt, kc are only in Mcare 
  
  # loop to ascribe MCAID data to duals
  for(i in c("gender_me", "gender_female", "gender_male", "gender_recent", "race_eth_recent", "race_recent",
             "race_me", "race_eth_me", "race_aian", "race_asian", "race_black", "race_nhpi", "race_white", "race_latino")){
    dual[, paste0(i) := get(paste0(i, ".mcaid"))] # fill with Mcaid data
    dual[is.na(get(paste0(i))), paste0(i) := get(paste0(i, ".mcare"))] # If NA b/c missing Mcaid data, then fill with Mcare data
    dual[, paste0(i, ".mcaid") := NULL][, paste0(i, ".mcare") := NULL]
  }

  # add dual flag
    dual[, apde_dual := 1]
  
## (6) Append the duals to the non-duals ----
    elig <- rbindlist(list(dual, mcaid.solo, mcare.solo), use.names = TRUE, fill = TRUE)
    elig[is.na(apde_dual), apde_dual := 0] # fill in duals flag
    
## (7) Prep for pushing to SQL ----
    # set dates
      elig[, dob := as.Date(dob)]
      elig[, death_dt := as.Date(death_dt)]
    
    # recreate race unknown indicator
      elig[, race_unk := 0]
      elig[race_aian==0 & race_asian==0 & race_asian_pi==0 & race_black==0 & race_latino==0 & race_nhpi==0 & race_white==0, race_unk := 1] 
  
    # create time stamp
      elig[, last_run := Sys.time()] 
      
## (8) Write to SQL ----              
  # Pull YAML from GitHub
    table_config <- yaml::yaml.load(httr::GET(yaml.url))
  
  # Ensure columns are in same order in R & SQL
    setcolorder(elig, names(table_config$vars))
  
  # Write table to SQL
    dbWriteTable(db_claims, 
                 DBI::Id(schema = table_config$schema, table = table_config$table), 
                 value = as.data.frame(elig),
                 overwrite = T, append = F, 
                 field.types = unlist(table_config$vars))

## (9) Simple QA ----
    # Confirm that all rows were loaded to SQL ----
      stage.count <- as.numeric(odbc::dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.mcaid_mcare_elig_demo"))
      if(stage.count != nrow(elig)){stop("Mismatching row count, error reading or writing data")} else{print("All data appear to have been successfully loaded to SQL...")}    
    
    # More elaborate QA has it's own script! ----
    
## (10) Clean up ----
    rm(apde, mcaid, mcaid.dual, mcaid.solo, mcare, mcare.dual, mcare.solo, dual.id)
    rm(i, yaml.url)
    rm(dual, elig)
    rm(table_config)
    rm(stage.count, last_run, previous_rows, row_diff)
    rm(stage.count.unique)
    rm(qa.values)
    rm(problem.row_diff, problem.ids, problems)

## The end! ----
    run.time <- Sys.time() - start.time
    print(run.time)
    