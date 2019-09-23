## Header ----
  # Author: Danny Colombara
  # Date: September 11, 2019
  # R version: 3.5.3
  # Puprpose: Create a unique id_apde that will identify people across administrative datasets
  #           - Medicaid: mcaid
  #           - Medicare: mcare
  #           - King County Housing Authority: kcha
  #           - Seattle Housing Authority: sha
  #
  # Notes:  In order to ensure that each person receives a unique ID only one time, need to combine 
  #         and deduplicate the following data that were prepared previously
  #         pid is a unique id for those in KCHA &/| SHA 
  #
  #         The xwalk_05_linkage_mcaid_mcare_pha table contains all PIDs with any identified linkages to 
  #         id_mcaid and or id_mcare. This will be the foundational table to which we will add
  #         the following: 
  #           SQL.table								                  id_mcare	id_mcaid		pid
  #           [PHClaims].[stage].[mcare_elig_demo]		      x		
  #           [PHClaims].[stage].[mcaid_elig_demo]					          x	
  #           [PHClaims].[stage].[xwalk_03_linkage_mcaid_mcare]		    x			    x	
  # 

## Set up R environment #####
    rm(list = ls())
    options(max.print = 350, tibble.print_max = 30, scipen = 999)
    
    library(odbc) # Used to connect to SQL server
    library(data.table) # used to manipulate data
    
    start.time <- Sys.time()
    
## Connect to the servers #####
    db_claims <- dbConnect(odbc(), "PHClaims51")
    db.apde51 <- dbConnect(odbc(), "PH_APDEStore51")

## Load in data ----
    pha.mcare.mcaid <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT pid, id_mcaid, id_mcare 
                                              FROM stage.xwalk_05_linkage_mcaid_mcare_pha"))

    mcare.mcaid <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcare, id_mcaid 
                                          FROM stage.xwalk_03_linkage_mcaid_mcare"))    
    
    mcaid.only <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT id_mcaid 
                                       FROM stage.mcaid_elig_demo 
                                       WHERE id_mcaid IS NOT NULL"))
    
    mcare.only <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT id_mcare 
                                       FROM stage.mcare_elig_demo 
                                       WHERE id_mcare IS NOT NULL"))

    pid.dups <- setDT(odbc::dbGetQuery(db.apde51, "SELECT DISTINCT pid, alt_pid
                                FROM tmp.pid_dups
                                WHERE pid IS NOT NULL"))
  
## Clean the imported data (if needed) ----
    # check for duplicate IDs in pha.mcare.mcaid ----
        if(length(unique(pha.mcare.mcaid[!is.na(id_mcare)]$id_mcare)) != nrow(pha.mcare.mcaid[!is.na(id_mcare)])){
          stop("id_mcare in pha.mcare.mcaid is not unique")
        }
        if(length(unique(pha.mcare.mcaid[!is.na(id_mcaid)]$id_mcaid)) != nrow(pha.mcare.mcaid[!is.na(id_mcaid)])){
          stop("id_mcaid in pha.mcare.mcaid is not unique")
        }
        if(length(unique(pha.mcare.mcaid$pid)) != nrow(pha.mcare.mcaid)){
          stop("pid in pha.mcare.mcaid is not unique")
        }    
    
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
    
    # check for duplicate IDs in mcare.only ----
        if(length(unique(mcaid.only$id_mcaid)) != nrow(mcaid.only)){
          stop("id_mcaid in mcaid.only is not unique")
        }    
    
## Create combined file where each ID is matched if possible and only appears once ----
    # start with pha.mcare.mcaid ----
        # this has all pid, not just the ones that linked
        xwalk <- copy(pha.mcare.mcaid) 

    # append the mcare.mcaid pairs IF they don't already exist in 'xwalk' ----
        # first confirm that there are no cases where one of the mcare.mcaid ids are in the xwalk without it's partner
        if(nrow(mcare.mcaid[!id_mcare %in% xwalk$id_mcare]) != nrow(mcare.mcaid[!id_mcaid %in% xwalk$id_mcaid])){
          stop("The 'xwalk' data has an id_mcare or id_mcaid that is missing it's partner!
               If one of these ids appears in mcare.mcaid, it should never be without it's linked id in 'xwalk'")
        }
        
        xwalk <- rbind(xwalk, mcare.mcaid[!(id_mcare %in% xwalk$id_mcare & id_mcaid %in% xwalk$id_mcaid), ], fill = TRUE)
        
    # append mcare ids that never linked ----     
        xwalk <- rbind(xwalk, mcare.only[!(id_mcare %in% xwalk$id_mcare), ], fill = TRUE)
        # Possible that id_mcare matched but are not in the elig_demo because don't have MBSF data, but do have SSN and Names
        # So, the number of id_mcare in 'xwalk' can be >= the number in mcare.only, but the reverse is not true
        if(length(unique(xwalk[!is.na(id_mcare)]$id_mcare)) <  length(unique(mcare.only[!is.na(id_mcare)]$id_mcare))) {
          stop("There are fewer unique id_mcare in mcare.only vs 'xwalk' ... this should never be the case")
        }
        
    # append mcaid ids that never linked ----
        xwalk <- rbind(xwalk, mcaid.only[!(id_mcaid %in% xwalk$id_mcaid), ], fill = TRUE)        
        if(length(unique(xwalk[!is.na(id_mcaid)]$id_mcaid)) !=  length(unique(mcaid.only[!is.na(id_mcaid)]$id_mcaid))) {
          stop("The number of unique id_mcaid in mcaid.only vs 'xwalk' are not the same  ... this should never be the case")
        }

## Clean up xwalk data.table ----
    # Ensure there are no duplicate ids
        if( sum(duplicated(xwalk[!is.na(pid), ]$pid)) > 0){ stop("Duplicates in PID")}
        if( sum(duplicated(xwalk[!is.na(id_mcare), ]$id_mcare)) > 0){ stop("Duplicates in ID_MCARE")}
        if( sum(duplicated(xwalk[!is.na(id_mcaid), ]$id_mcaid)) > 0){ stop("Duplicates in ID_MCAID")}
    
    # Ensure there are no blank rows
        if ( nrow(xwalk[is.na(pid) & is.na(id_mcare) & is.na(id_mcaid), ]) >0 ){stop("Remove blank rows before proceding")}
        
## Generate id_apde ----
        set.seed(98104) # set starting point for randomization of ordering
        xwalk[, random.number := runif(nrow(xwalk))] # create column of random numbers to be used for sorting
        setorder(xwalk, random.number)
        xwalk[, random.number := NULL]
        xwalk[, id_apde := .I]
        
## Add previously identified duplicates back into the data to give them the same id_apde ----
        pid.dups.xwalk <- merge(pid.dups, xwalk[, .(id_apde, pid)], by = "pid", all.x = TRUE, all.y = FALSE)
        pid.dups.xwalk[, pid := NULL]
        setnames(pid.dups.xwalk, "alt_pid", "pid")
        xwalk <- rbind(xwalk, pid.dups.xwalk, fill = TRUE)
        
## Final tweaks before pushing to SQL ----
        setcolorder(xwalk, c("id_apde", "id_mcare", "id_mcaid", "pid"))
        setorder(xwalk, id_apde)

## Load xwalk table to SQL ----
    # create last_run timestamp
        xwalk[, last_run := Sys.time()]
        
    # create table ID for SQL
        tbl_id <- DBI::Id(schema = "stage", 
                          table = "xwalk_06_linkage_xwalk_apde_mcaid_mcare_pha")  
        
    # identify the column types to be created in SQL
        sql.columns <- c("id_apde" = "integer", "id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "id_mcaid" = "char(11)", "pid" = "integer", "last_run" = "datetime")  
        
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
                                                   "SELECT COUNT (*) FROM stage.xwalk_06_linkage_xwalk_apde_mcaid_mcare_pha"))
        if(stage.count != nrow(xwalk))
          stop("Mismatching row count, error writing or reading data")      
        
    # close database connections    
        dbDisconnect(db_claims)  
        dbDisconnect(db.apde51)  
        
## The end ----      