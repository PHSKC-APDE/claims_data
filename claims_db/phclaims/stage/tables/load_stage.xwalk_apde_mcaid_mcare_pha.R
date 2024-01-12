## HEADER ----
# Author: Danny Colombara
# Date: October 25, 2019
# R version: 4.3.1
# Updated by Alastair Matheson, 2021-06-27
# updated by Danny Colombara, 2024-01-04
#
# Purpose: Create ID linkage file, identifying people across IDH, Mcaid, Mcare, and PHA with a NEW ID_APDE
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
#        this will allow us to have sex== unknown, male, female, multiple
#
#

## OVERVIEW ----
# 1) Create simple functions
# 2) Load and prepare IDH identifiers (has KCMASTER_ID, id_mcaid, and phhousing_id)
# 3) Load and prepare Medicare identifiers
# 4) Order columns of IDH and Mcare data for visual comparison before linkage  
# 5) 
# 6) 
# 7) 
# 8) 

## Set up ----
  options(error = NULL, scipen = 999)
  library(lubridate) # manipulate dates / times
  library(DBI) # database connections
  library(rads) # apde package with nifty tools, including connecting to databases
  library(data.table) # data manipulation
  library(reclin2) # yet another record linkage package

  rm(list=ls())

  set.seed(98104)

  start.time <- Sys.time()

  ## Set up ODBC connections
  db_hhsaw <- rads::validate_hhsaw_key() # connects to Azure 16 HHSAW
  
  db_idh <- DBI::dbConnect(odbc::odbc(), driver = "ODBC Driver 17 for SQL Server", 
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433", 
                           database = "inthealth_dwhealth", 
                           uid = keyring::key_list("hhsaw")[["username"]], 
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]), 
                           Encrypt = "yes", TrustServerCertificate = "yes", 
                           Authentication = "ActiveDirectoryPassword")
  
## FUNCTIONS ... general data cleaning / prep procedures ----
  ## chunk_loader() ----
  #' Function to load big data to SQL (chunk.loader()) ... copied from housing repo
  #' 
  #' \code{chunk_loader} divides a data.frame/ data.table into smaller tables
  #' so it can be easily loaded to SQL. Experience has shows that loading large 
  #' tables in 'chunks' is less likely to cause errors. 
  #' 
  #' 
  #' @param DTx A data.table/data.frame
  #' @param connx The name of the relevant database connection that you have open
  #' @param chunk.size The number of rows that you desire to have per chunk
  #' @param schemax The name of the schema where you want to write the data
  #' @param tablex The name of the table where you want to write the data
  #' @param overwritex Do you want to overwrite the existing tables? Logical (T|F).
  #' @param appendx Do you want to append to an existing table? Logical (T|F). 
  #'  
  #' intentionally redundant with \code{overwritex} to ensure that tables are not 
  #' accidentally overwritten.
  #' @param field.typesx Optional ability to specify the fieldtype, e.g., INT, 
  #' VARCHAR(32), etc. 
  #' 
  #' @name chunk_loader
  #' 
  #' @export
  #' @rdname chunk_loader
  #' 
  
  chunk_loader <- function(DTx, # R data.frame/data.table
                           connx, # connection name
                           chunk.size = 1000, 
                           schemax = NULL, # schema name
                           tablex = NULL, # table name
                           overwritex = F, # overwrite?
                           appendx = T, # append?
                           field.typesx = NULL){ # want to specify specific field types?
    # save the name of the data.table being loaded
    DTxname <- deparse(substitute(DTx))
    
    # set initial values
    max.row.num <- nrow(DTx)
    number.chunks <-  ceiling(max.row.num/chunk.size) # number of chunks to be uploaded
    starting.row <- 1 # the starting row number for each chunk to be uploaded. Will begin with 1 for the first chunk
    ending.row <- chunk.size  # the final row number for each chunk to be uploaded. Will begin with the overall chunk size for the first chunk
    
    # If asked to overwrite, will DROP the table first (if it exists) and then just append
    if(overwritex == T){
      DBI::dbGetQuery(conn = connx, 
                      statement = paste0("IF OBJECT_ID('", schemax, ".", tablex, "', 'U') IS NOT NULL ", 
                                         "DROP TABLE ", schemax, ".", tablex))
      overwritex = F
      appendx = T
    }
    
    # Create loop for appending new data
    for(i in 1:number.chunks){
      # counter so we know it is not stuck
      message(paste0(DTxname, " ", Sys.time(), ": Loading chunk ", format(i, big.mark = ','), " of ", format(number.chunks, big.mark = ','), ": rows ", format(starting.row, big.mark = ','), "-", format(ending.row, big.mark = ',')))  
      
      # subset the data (i.e., create a data 'chunk')
      temp.DTx <- setDF(copy(DTx[starting.row:ending.row,])) 
      
      # load the data chunk into SQL
      if(is.null(field.typesx)){
        DBI::dbWriteTable(conn = connx, 
                          name = DBI::Id(schema = schemax, table = tablex), 
                          value = temp.DTx, 
                          append = appendx,
                          row.names = F)} 
      if(!is.null(field.typesx)){
        DBI::dbWriteTable(conn = connx, 
                          name = DBI::Id(schema = schemax, table = tablex), 
                          value = temp.DTx, 
                          append = F, # set to false so can use field types
                          row.names = F, 
                          field.types = field.typesx)
        field.typesx = NULL # because only use once, does not make sense to have it when appending      
      }
      
      # set the starting and ending rows for the next chunk to be uploaded
      starting.row <- starting.row + chunk.size
      ifelse(ending.row + chunk.size < max.row.num, 
             ending.row <- ending.row + chunk.size,
             ending.row <- max.row.num)
    } 
  }
  
  ## prep.dob() ----
    prep.dob <- function(dt){
      # Extract date components
      dt[, dob.year := as.integer(lubridate::year(dob))] # extract year
      dt[, dob.month := as.integer(lubridate::month(dob))] # extract month
      dt[, dob.day := as.integer(lubridate::day(dob))] # extract day
      dt[, c("dob") := NULL] # drop vars that are not needed
      
      return(dt)
    }
  
  ## prep.names() ----
    prep.names <- function(dt) {
      # All caps
      dt[, c("name_gvn", "name_mdl", "name_srnm") := lapply(.SD, toupper), .SDcols = c("name_gvn", "name_mdl", "name_srnm")]
      
      # Remove extraneous spaces at the beginning or end of a name
      dt[, c("name_gvn", "name_mdl", "name_srnm") := lapply(.SD, function(x) gsub("^\\s+|\\s+$", "", x)), .SDcols = c("name_gvn", "name_mdl", "name_srnm")]
      
      # Remove suffixes from names (e.g., I, II, III, I I, I I I, IV)
      dt[, c("name_gvn", "name_srnm") := lapply(.SD, function(x) gsub(" \\s*I{1,3}\\b| IV\\b", "", x)), .SDcols = c("name_gvn", "name_srnm")]
      
      # Remove suffixes from names (i.e., JR or SR)
      dt[, c("name_gvn", "name_srnm") := lapply(.SD, function(x) gsub(" JR$| SR$", "", x)), .SDcols = c("name_gvn", "name_srnm")]
      
      # Standardize middle names
      dt[is.na(name_gvn) & !is.na(name_mdl), `:=`(name_gvn = name_mdl, name_mdl = NA_character_)]
      dt[, name_mdl := substr(name_mdl, 1, 1)] # limit to a single character bc Mcare has only one character
      dt[grepl(" [A-Z]$", name_gvn) & is.na(name_mdl), `:=` (name_mdl = rads::substrRight(name_gvn, 1, 1), name_gvn = substr(name_gvn, 1, nchar(name_gvn)-2))] # get middle initial when added to first name
      
      # Only keep letters and white spaces
      dt[, c("name_gvn", "name_mdl", "name_srnm") := lapply(.SD, function(x) gsub("\\s+", "", gsub("[^A-Z ]", "", x))), .SDcols = c("name_gvn", "name_mdl", "name_srnm")]
      
      return(dt)
    }

  ## id_nodups() ... create vector of unique IDs ----
    # This function creates a vector of unique IDs of any length
    # id_n = how many unique IDs you want generated
    # id_length = how long do you want the ID to get (too short and you'll be stuck in a loop)
    id_nodups <- function(id_n, id_length, seed = 98104) {
      set.seed(seed)
      id_list <- stringi::stri_rand_strings(n = id_n, length = id_length, pattern = "[a-z0-9]")
      
      # If any IDs were duplicated (very unlikely), overwrite them with new IDs
      iteration <- 1
      while(any(duplicated(id_list)) & iteration <= 50) {
        id_list[which(duplicated(id_list))] <- stringi::stri_rand_strings(n = sum(duplicated(id_list), na.rm = TRUE),
                                                                          length = id_length,
                                                                          pattern = "[a-z0-9]")
        iteration <<- iteration + 1
      }
      
      if (iteration == 50) {
        stop("After 50 iterations there are still duplicate IDs. ",
             "Either decrease id_n or increase id_length")
      } else {
        return(id_list)
      }
    }

  ## match_process() ... Consolidate cluster IDs across identities ----
    # Adaptation of Carolina's code
    # From here: https://github.com/DCHS-PME/PMEtools/blob/main/R/idm_dedup.R
    # pairs_input = Output from a RecordLinkage getPairs function
    # df = The data frame that was fed into the matching process. 
    #      Must have rowid and id_hash fields
    # iteration = What match cycle this is (affects cluster ID suffix)
    
    match_process <- function(pairs_input, df, iteration) {
      ### Attach ids for each individual pairwise link found ----
      pairs <- pairs_input %>%
        distinct(id1, id2) %>%
        left_join(df, ., by = c(rowid = "id1"))
      pairs <- setDF(pairs)
      
      ### Roll up pair combinations ----
      # self-join to consolidate all pair combinations for clusters with > 2 identities linked 
      # roll up cluster id correctly with coalesce
      # formula for how many other_pair2 records should exist for n number of matching records: 
      #   (n*(n-1)/2) + 1 - e.g. 3 carolina johnsons will have 4  records (3*2/2+1)
      remaining_dupes <- sum(!is.na(pairs$id2))
      
      # while loop self-joining pairs until no more open pairs remain
      recursion_level <- 0
      recursion_df <- pairs %>% rename(id2_recur0 = id2)
      while (remaining_dupes > 0) {
        recursion_level <- recursion_level + 1
        print(paste0(remaining_dupes, " remaining duplicated rows. Starting recursion iteration ", recursion_level))
        recursion_df <- pairs %>%
          self_join_dups(base_df = ., iterate_df = recursion_df, iteration = recursion_level)
        remaining_dupes <- sum(!is.na(recursion_df[ , paste0("id2_recur", recursion_level)]))
      }
      
      # identify full list of id columns to coalesce after recursion
      recurcols <- tidyselect::vars_select(names(recursion_df), matches("_recur\\d")) %>%
        sort(decreasing = T)
      coalesce_cols <- c(recurcols, "rowid")
      coalesce_cols <- rlang::syms(coalesce_cols)
      
      # coalesce recursive id columns in sequence to generate single common cluster ID
      pairsend <- recursion_df %>%
        mutate(clusterid = coalesce(!!!coalesce_cols)) %>%
        rename(id2 = id2_recur0) %>%
        select(-contains("_recur")) %>%
        distinct()
      
      # identify any unclosed cluster groups (open triangle problem), resulting in duplicated cluster
      double_dups <- setDT(pairsend %>% select(rowid, clusterid))
      double_dups <- unique(double_dups)
      double_dups <- double_dups[, if(.N > 1) .SD, by = "rowid"]
      double_dups[, row_min := min(rowid), by = "clusterid"]
      # See if there are still any open triangles
      double_dups[, rows_per_id := uniqueN(row_min), by = "rowid"]
      
      if (max(double_dups$rows_per_id) > 2) {
        stop("More than 2 levels of open triangles, need to rework function")
      } else if (max(double_dups$rows_per_id) == 2) {
        double_dups[, back_join_id := min(row_min), by = "rowid"]
        double_dups[, row_min := NULL]
      } else {
        setnames(double_dups, "row_min", "back_join_id")
      }
      double_dups[, rowid := NULL]
      double_dups[, rows_per_id := NULL]
      double_dups <- unique(double_dups)
      double_dups <- setDF(double_dups)
      
      
      # collapse duplicate partial clusters to one cluster
      # error checking to make sure that correct total clusters are maintained
      if (nrow(double_dups) > 0) {
        pairsend <- left_join(pairsend, double_dups, by = c(clusterid = "clusterid")) %>%
          mutate(clusterid2 = coalesce(back_join_id, clusterid))
        
        message("There are ", sum(pairsend$clusterid != pairsend$clusterid2), 
                " mismatched clusterid/clusterid2 combos and at least ",
                nrow(double_dups)*2, " expected")
        
        pairsend <- pairsend %>%
          mutate(clusterid = clusterid2) %>%
          select(-clusterid2, -back_join_id)
      }
      
      ### Add identifiers/unique ids for paired records ----
      # overwrite the original pairs with the consolidated & informed dataframe
      pairs_final <- df %>%
        rename_all(~ paste0(., "_b")) %>%
        left_join(pairsend, ., by = c(id2 = "rowid_b"))
      
      ### Take the union of all unique ids with their cluster ids ----
      # (swinging links from _b cols to unioned rows, and taking distinct)
      # create cluster index
      cluster_index <- select(pairs_final, clusterid, id_hash = id_hash_b) %>%
        drop_na() %>%
        bind_rows(select(pairs_final, clusterid, id_hash)) %>%
        distinct()
      
      ### Check that each personal id only in one cluster ----
      n_pi_split <- setDT(pairs_final %>% select(id_hash, clusterid))
      n_pi_split <- unique(n_pi_split)
      n_pi_split <- n_pi_split[, if(.N > 1) .SD, by = "id_hash"]
      
      if (nrow(n_pi_split)) {
        stop(glue::glue("Deduplication processing error: {nrow(n_pi_split)} ",
                        "clients sorted into more than one cluster. ", 
                        "This is an internal failure in the function and will require debugging. ", 
                        "Talk to package maintainer)"))
      }
      
      ### Report results ----
      n_orig_ids <- df %>% select(id_hash) %>% n_distinct()
      n_cluster_ids <- n_distinct(cluster_index$clusterid)
      
      message("Number of unique clients prior to deduplication: ", n_orig_ids, 
              ". Number of deduplicated clients: ", n_cluster_ids)
      
      
      ### Attach cluster IDS back to base file ----
      output <- left_join(df, 
                          # Set up iteration name
                          rename(cluster_index, 
                                 !!quo_name(paste0("clusterid_", iteration)) := clusterid), 
                          by = "id_hash")
      output
    }  
  
  ## identify functions to be kept for user later on ----    
    keep.me <- copy(c(ls(), 'keep.me')) # everything created above should be permanent

## PREP RAW DATA ----
  # Load & prep IDH data ----
    # Pull data and clean white spaces ----
      idh <- setDT(odbc::dbGetQuery(db_idh, 
                                    "SELECT DISTINCT 
                                      KCMASTER_ID, 
                                      id_mcaid = MEDICAID_ID, 
                                      phousing_id = PHOUSING_ID,
                                      ssn = SSN,
                                      name_gvn = UPPER(FIRST_NAME_ORIG),
                                      name_mdl = UPPER(MIDDLE_NAME_ORIG),
                                      name_srnm = UPPER(LAST_NAME_ORIG),
                                      dob = CAST(DOB AS DATE),
                                      gender_me = GENDER
                                    FROM [IDMatch].[IM_HISTORY_TABLE]
                                    WHERE IS_HISTORICAL = 'N' AND KCMASTER_ID IS NOT NULL")) # did not download the name suffixes because cleaning code would eliminate them
      rads::sql_clean(idh)
  
      
    # Prep the names ----
      idh = prep.names(idh)
    
    # There are duplicate KCMASTER_ID, so try to fill in missing data when possible and then deduplicate ----
      # identify duplicates and split them off for processing
        idh_rows_orig <- nrow(idh)
        idh_ids_orig <- uniqueN(idh$KCMASTER_ID)
        idh[, dupcount := .N, KCMASTER_ID]
        idh.backfill <- setorder(idh[dupcount > 1][, dupcount := NULL], KCMASTER_ID) # separate out the duplicates
        idh <- idh[dupcount == 1][, c('dupcount') := NULL] 
    
      # Fill in blank data within a given ID using existing data
        idh.backfill[, orig_order := .I]
        for(myvar in c('id_mcaid', 'phousing_id', 'name_gvn', 'name_mdl', 'name_srnm', 'dob', 'gender_me')){
          idh.backfill[, paste0(myvar) := get(myvar)[1], by= .(KCMASTER_ID, cumsum(!is.na(get(myvar))) ) ] # fill forward / downward
          setorder(idh.backfill, -orig_order) # reverse order so will fill if the last cell for an ID has information
          idh.backfill[, paste0(myvar) := get(myvar)[1], by= .(KCMASTER_ID, cumsum(!is.na(get(myvar))) ) ] # fill forward / downward\
          setorder(idh.backfill, orig_order) # return to starting order
        }
        idh.backfill[, orig_order := NULL]
        
        idh.backfill[, name_gvn := gsub(paste0(" ", name_mdl, "$"), "", name_gvn), 1:nrow(idh.backfill)] # when middle initial was appended to the first name and is present in name_mdl, drop it from name_gvn
  
        # When there is more than one name per ID, use the longer one because it has more information. E.g., Robert F Smith >> Robert Smith
          idh.backfill[, namechars := rowSums(sapply(.SD, function(col) ifelse(is.na(col), 0, nchar(col))), na.rm = TRUE), .SDcols = c('name_gvn', 'name_mdl', 'name_srnm')]
          idh.backfill[, maxnamechars := max(namechars), KCMASTER_ID]
          idh.longnames <- idh.backfill[namechars == maxnamechars] # subset to rows with just the longest possible names
          setorder(idh.longnames[, random := runif(.N)], KCMASTER_ID, -random) # sort long names randomly within the KCMASTER_ID
          idh.longnames <- idh.longnames[, .SD[1], .(KCMASTER_ID)][, random := NULL][, .(KCMASTER_ID, name_gvn, name_mdl, name_srnm)] # randomly keep 1 row for each KCMASTER_ID
          
          idh.backfill <- merge(idh.backfill[, c('namechars', 'maxnamechars', 'name_gvn', 'name_mdl', 'name_srnm') := NULL], # replace the names with the long names
                                 idh.longnames, by = 'KCMASTER_ID', 
                                 all.x = T, all.y = F) 
          
        # Now randomly select one row from each KCMASTER_ID
          setorder(idh.backfill[, random := runif(.N)], KCMASTER_ID, -random)
          idh.backfill <- idh.backfill[, .SD[1], .(KCMASTER_ID)][, random := NULL]
          
          
       # Now append the deduplicated rows to the IDH rows that did not need deduplication
          idh <- rbind(idh, idh.backfill)
          
      if(idh_ids_orig == nrow(idh)){
        message("\U0001f642 You've successfully deduplicated the IDH data.")
      } else {stop("\n\U0001f47f There was a problem deduplicating KCMASTER_ID in the IDH data.")}
          
      # remove temporary objects
          rm(idh.backfill, idh.longnames, idh_ids_orig, idh_rows_orig, myvar)
  
    # Prep the dob ----
      prep.dob(idh)
          
    # Prep the SSN ----  
      idh <- housing::validate_ssn(idh, 'ssn')     
          
    # Drop rows that can never be matched ----
      idh <- idh[!(is.na(ssn) & is.na(name_srnm))]
      idh <- idh[!(is.na(ssn) & is.na(dob.year) & is.na(dob.month))]
      idh <- idh[!name_srnm %in% c('SKELETALREMAINS')]
      idh <- idh[!name_gvn %in% c('SKELETALREMAINS')]
      idh <- idh[!paste0(name_srnm, name_gvn) %in% c('SKELETALREMAINS', 'UNIDENTIFIEDSKELETON', 'NONOSSEOUSSKELETALMODEL')]
      
    # Load to personal repo on IDH ----  
      chunk_loader(DTx = idh, # R data.frame/data.table
                   connx = db_hhsaw, # connection name
                   chunk.size = 10000, 
                   schemax = Sys.getenv("USERNAME"), # schema name
                   tablex =  'link_prep_idh', # table name
                   overwritex = T, # overwrite?
                   appendx = F, # append?
                   field.typesx = NULL)  
      
  # Load & prep Mcare data ----
    # Load & prep SSN ----
      mcare.ssn <- rads::sql_clean(unique(setDT(odbc::dbGetQuery(db_hhsaw, "SELECT bene_id AS id_mcare, ssn FROM [claims].[stage_mcare_bene_ssn] WHERE ssn IS NOT NULL"))))
      mcare.ssn <- housing::validate_ssn(mcare.ssn, 'ssn') # clean garbage SSN
      mcare.ssn <- mcare.ssn[!is.na(ssn)]
      # There are duplicate id_mcare, meaning there were bene_id that matched with more than one ssn. I will randomly / arbitrarily keep only one value
        setorder(mcare.ssn[, random := runif(.N)], id_mcare, -random)
        mcare.ssn <- mcare.ssn[, .SD[1], .(id_mcare)][, random := NULL]
        
  
    # Load & prep Names ----
      mcare.names <- rads::sql_clean(unique(setDT(odbc::dbGetQuery(db_hhsaw, "SELECT bene_id AS id_mcare, bene_srnm_name AS name_srnm, 
                                              bene_gvn_name AS name_gvn, bene_mdl_name AS name_mdl FROM [claims].[stage_mcare_bene_names] WHERE bene_srnm_name IS NOT NULL"))))
        
      prep.names(mcare.names)
      
      # There are duplicate id_mcare, meaning there were bene_id that matched with more than one name. As with the IDH names, let's keep the names for a given KCMASTER_ID with more characters
      mcare.names[, namechars := rowSums(sapply(.SD, function(col) ifelse(is.na(col), 0, nchar(col))), na.rm = TRUE), .SDcols = c('name_gvn', 'name_mdl', 'name_srnm')]
      mcare.names[, maxnamechars := max(namechars), id_mcare]
      mcare.names <- mcare.names[namechars == maxnamechars] # subset to rows with just the longest possible names
      setorder(mcare.names[, random := runif(.N)], id_mcare, -random) # sort long names randomly within the KCMASTER_ID
      mcare.names <- mcare.names[, .SD[1], .(id_mcare)][, random := NULL] # randomly keep 1 row for each KCMASTER_ID
      mcare.names[, c('namechars', 'maxnamechars') := NULL]
    
    # Load & prep DOB & Gender ----
      mcare.elig <- rads::sql_clean(unique(setDT(odbc::dbGetQuery(db_hhsaw, "SELECT bene_id AS id_mcare, bene_birth_dt as dob, sex_ident_cd as gender_me from [claims].[stage_mcare_bene_enrollment]"))))
      mcare.elig[nchar(dob) <10, dob := NA] # Integer dob is useless in this data. E.g., one ID had dob 1966-07-21 and another row with 48. The 48 doesn't make sense with origin date in R, SQL, Excel, or SAS
      mcare.elig <- mcare.elig[!(is.na(dob) & is.na(gender_me))]
      # There are duplicate id_mcare, meaning there were bene_id that matched with more than one set of dob and gender. I will randomly / arbitrarily keep only one name
      setorder(mcare.elig[, random := runif(.N)], id_mcare, -random)
      mcare.elig <- mcare.elig[, .SD[1], .(id_mcare)][, random := NULL]
      # Normalize gender values with those from IDH
      mcare.elig[, gender_me := fcase(gender_me == 1, 'M', 
                                      gender_me == 2, 'F', 
                                      default = NA_character_)]
      
      prep.dob(mcare.elig)
    
    # Combine identifiers ----
      mcare <- merge(merge(mcare.ssn, mcare.names, all = T), mcare.elig, all = T)
      
      if(nrow(mcare) == uniqueN(mcare$id_mcare)){
        message("\U0001f642 You've successfully deduplicated the Medicare data.")
      } else {stop("\n\U0001f47f There was a problem deduplicating id_mcare in the Medicare data.")}
      
    # Drop rows that can never be matched ----
      mcare <- mcare[!(is.na(ssn) & is.na(name_srnm))]
      mcare <- mcare[!(is.na(ssn) & is.na(dob.year) & is.na(dob.month))]
      
    # Load to personal repo on IDH ----  
      common <- intersect(names(mcare), names(idh))
      setcolorder(idh, c('KCMASTER_ID', common))
      setcolorder(mcare, c('id_mcare', common))
      
      chunk_loader(DTx = mcare, # R data.frame/data.table
                   connx = db_hhsaw, # connection name
                   chunk.size = 10000, 
                   schemax = Sys.getenv("USERNAME"), # schema name
                   tablex =  'link_prep_mcare', # table name
                   overwritex = T, # overwrite?
                   appendx = F, # append?
                   field.typesx = NULL)  
      
## LINK PREPPED DATA ----
  # Load prepped data from personal schema on HHSAW into R if needed ----
      if(exists('idh')){message("\U0001f642 A data.table named 'idh' already exists and will not be reloaded from your HHSAW personal schema")}else{
        if(DBI::dbExistsTable(conn = db_hhsaw, DBI::Id(schema = Sys.getenv("USERNAME"), table = 'link_prep_idh'))){
            message('\U023F3 Be patient while R downloads ', paste0(Sys.getenv('USERNAME'), '.link_prep_idh'))
            idh <- setDT(DBI::dbGetQuery(conn = db_hhsaw, paste0("SELECT * from ", Sys.getenv('USERNAME'), '.link_prep_idh')))}else{
              message(paste0('\U0001f47f ', Sys.getenv('USERNAME'), '.link_prep_idh does not currently exist. Run the code above to (re)create it.'))
            }
      }
  
      if(exists('mcare')){message("\U0001f642 A data.table named 'mcare' already exists and will not be reloaded from your HHSAW personal schema")}else{
        if(DBI::dbExistsTable(conn = db_hhsaw, DBI::Id(schema = Sys.getenv("USERNAME"), table = 'link_prep_mcare'))){
          message('\U023F3 Be patient while R downloads ', paste0(Sys.getenv('USERNAME'), '.link_prep_mcare'))
          mcare <- setDT(DBI::dbGetQuery(conn = db_hhsaw, paste0("SELECT * from ", Sys.getenv('USERNAME'), '.link_prep_mcare')))}else{
            message(paste0('\U0001f47f ', Sys.getenv('USERNAME'), '.link_prep_mcare does not currently exist. Run the code above to (re)create it.'))
          }
      }
      
  # Linkage using reclin2 package ----
    # Save the linkage between KCMASTER_ID, id_mcaid, and phousing_id for later ----
      idh_mcaid_pha <- idh[!(is.na(id_mcaid) & is.na(phousing_id)), .(KCMASTER_ID, id_mcaid, phousing_id)]
      idh <- idh[, c('id_mcaid', 'phousing_id') := NULL] # drop the mcaid and pha ids because not needed for linkage
      
    # Split off the perfect deterministic matches (if any) ----
      deterministic1 <- merge(idh, mcare, by = c('ssn', 'name_srnm', 'name_mdl', 'name_gvn', 'gender_me', 'dob.year', 'dob.month', 'dob.day'), all = F)
      deterministic1 <- deterministic1[, .(KCMASTER_ID, id_mcare)]
      mcare <- mcare[!id_mcare %in% deterministic1$id_mcare]
      idh <- idh[!KCMASTER_ID %in% deterministic1$KCMASTER_ID]
      
    # Split off other deterministic matches ----
      deterministic2 <- rbind(
        merge(idh[is.na(name_srnm)], mcare, by = c('ssn', 'gender_me', 'dob.year', 'dob.month', 'dob.day'), all = F),
        merge(idh, mcare[is.na(name_srnm)], by = c('ssn', 'gender_me', 'dob.year', 'dob.month', 'dob.day'), all = F))
        deterministic2 <- deterministic2[, .(KCMASTER_ID, id_mcare)]
        mcare <- mcare[!id_mcare %in% deterministic2$id_mcare]      
        idh <- idh[!KCMASTER_ID %in% deterministic2$KCMASTER_ID]
      
    # Linkage attempt #1: blocking on SSN ----  
      # Linkage attempt #1: no blocking ----  
        # create pairs
          pairs <- pair_blocking(x = idh[!is.na(ssn)], y = mcare[!is.na(ssn)], on = "ssn")
      
        # get metrics for each variable pair
          compare_pairs(pairs, on = c('name_srnm', 'name_gvn', 'name_mdl', 'gender_me', 
                                      'dob.year', 'dob.month', 'dob.day'), 
                        comparators = list(name_srnm = cmp_jarowinkler(),
                                           name_gvn = cmp_jarowinkler(), 
                                           name_mdl = cmp_identical(), 
                                           gender_me = cmp_identical(), 
                                           dob.year = cmp_jarowinkler(), 
                                           dob.month = cmp_jarowinkler(), 
                                           dob.day = cmp_jarowinkler()), 
                        inplace = TRUE)
          
        # Score the pairs (to identify some true pairs for a training set)
          # mod1 <- problink_em(~name_srnm + name_gvn + name_mdl + gender_me +
          #                       dob.year + dob.month + dob.day, data = pairs)
          # pairs <- predict(mod1, pairs = pairs, add = TRUE)
          pairs <- score_simple(pairs, 'score', 
                                on = c('name_srnm', 'name_gvn', 'name_mdl', 'gender_me', 
                                       'dob.year', 'dob.month', 'dob.day'), 
                                w1 = c(name_srnm = 2, name_gvn = 1.5,  name_mdl = 1, gender_me = 0.5, 
                                       dob.year = 1.75, dob.month = 1.5, dob.day = 1.75), 
                                w0 = c(name_srnm = -1, name_gvn = -1,  name_mdl = -.5, gender_me = -.5, 
                                       dob.year = -1.5, dob.month = -.5, dob.day =-1), 
                                wna = 0)
        
        checklinks <- merge(link(pairs), pairs[, .(`.x`, `.y`, score)], by = c('.x', '.y'))
        setorder(setcolorder(checklinks, sort(names(checklinks))), -score)
        
        
                

      

      