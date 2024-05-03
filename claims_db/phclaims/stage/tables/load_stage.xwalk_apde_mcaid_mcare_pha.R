# Header ----
# Author: Danny Colombara
# Date: January 12, 2024
# R version: 4.3.1
# Purpose: Create an id_apde that maps to each KCMASTER_ID AND each id_mcare that
#          could not be linked to a KCMASTER_ID
#
# Notes: Via the KCMASTER_ID, we can also link back to the id_mcaid and id_pha
#
#        Agreed with Alastair 2024/01/12 that we should have a 1:1 KCMASTER <> id_apde
#        and that there can be multiple rows for each KCMASTER_ID
#
# This code is designed to be run as part of the master Medicaid/Medicare script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid_mcare/master_mcaid_mcare_analytic.R
#

# Set up ----
  options(error = NULL, scipen = 999)
  # rm(list=ls())
  library(data.table)
  library(rads)
  library(odbc)
  
  set.seed(98104)
  
  db_hhsaw <- rads::validate_hhsaw_key() # connects to Azure 16 HHSAW
  
  db_idh <- DBI::dbConnect(odbc::odbc(), driver = "ODBC Driver 17 for SQL Server", 
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433", 
                           database = "inthealth_dwhealth", 
                           uid = keyring::key_list("hhsaw")[["username"]], 
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]), 
                           Encrypt = "yes", TrustServerCertificate = "yes", 
                           Authentication = "ActiveDirectoryPassword")

# Load functions ----
  ## id_nodups(): generate unique ids ----
      # This function creates a vector of unique IDs of any length
      # id_n = how many unique IDs you want generated
      # id_length = how long do you want the ID to get (too short and you'll be stuck in a loop)
      id_nodups <- function(id_n, id_length, seed = 98104) {
        # Set seed for reproducibility
        set.seed(seed)
        
        # Generate initial random strings
        id_list <- stringi::stri_rand_strings(n = id_n, length = id_length, pattern = "[a-z0-9]")
        
        # Check for duplicates and regenerate if necessary
        iteration <- 1
        while (any(duplicated(id_list)) & iteration <= 50) {
          # Identify duplicated IDs
          duplicated_ids <- which(duplicated(id_list))
          
          # Regenerate new IDs for the duplicated ones
          id_list[duplicated_ids] <- stringi::stri_rand_strings(
            n = sum(duplicated(id_list), na.rm = TRUE),
            length = id_length,
            pattern = "[a-z0-9]"
          )
          
          # Increment iteration counter
          iteration <- iteration + 1
        }
        
        # Check if 50 iterations were reached without resolving duplicates
        if (iteration == 50) {
          stop("After 50 iterations there are still duplicate IDs. ",
               "Either decrease id_n or increase id_length")
        } else {
          # Return the final list of unique IDs
          return(id_list)
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
  
  
  
# Load & prep IDH data ----
  ## load from SQL ----
    idh <- setDT(odbc::dbGetQuery(db_idh, 
                                  "SELECT DISTINCT KCMASTER_ID
                                            FROM [IDMatch].[IM_HISTORY_TABLE]
                                            WHERE IS_HISTORICAL = 'N' AND KCMASTER_ID IS NOT NULL")) 
    mcaid <- setDT(odbc::dbGetQuery(db_idh, 
                                    "SELECT DISTINCT 
                                              KCMASTER_ID, 
                                              id_mcaid = MEDICAID_ID, 
                                              touched = CAST(LAST_TOUCHED AS DATE)
                                            FROM [IDMatch].[IM_HISTORY_TABLE]
                                            WHERE IS_HISTORICAL = 'N' AND SOURCE_SYSTEM = 'MEDICAID' AND KCMASTER_ID IS NOT NULL")) 
    
    pha <- setDT(odbc::dbGetQuery(db_idh, 
                                  "SELECT DISTINCT 
                                        KCMASTER_ID, 
                                        phousing_id = PHOUSING_ID,
                                        touched = CAST(LAST_TOUCHED AS DATE)
                                      FROM [IDMatch].[IM_HISTORY_TABLE]
                                      WHERE IS_HISTORICAL = 'N' AND phousing_id IS NOT NULL AND KCMASTER_ID IS NOT NULL")) 


  ## remove random white spaces ----
    rads::string_clean(idh)
    rads::string_clean(mcaid)
    rads::string_clean(pha)
    
  ## ensure data follow proper patterns ----
    idh <- unique(idh[grepl('^[0-9]{9}KC$', KCMASTER_ID), .(KCMASTER_ID)])
    
    mcaid <- unique(mcaid[grepl('^[0-9]{9}WA$', id_mcaid), .(KCMASTER_ID, id_mcaid, touched)])
      
    pha <- unique(pha[nchar(phousing_id) == 64, .(KCMASTER_ID, phousing_id, touched)])
    
  ## keep the most recent KCMASTER_ID for each mcaid/pha id ----
    setorder(mcaid, id_mcaid, -touched)
    mcaid <- mcaid[, .SD[1], id_mcaid][, touched := NULL]
    if(uniqueN(mcaid$id_mcaid) == nrow(mcaid)){message('\U0001f642 mcaid deduplicated!')} else {message('\U0001f47f mcaid not properly deduplicated')}
    
    setorder(pha, phousing_id, -touched)
    pha <- pha[, .SD[1], phousing_id][, touched := NULL]
    if(uniqueN(pha$phousing_id) == nrow(pha)){message('\U0001f642 pha deduplicated!')} else {message('\U0001f47f pha not properly deduplicated')}
  
# Add id_apde to KCMASTER_ID ----
    idh[, id_apde := id_nodups(id_n = nrow(idh), id_length = 10)]
    
# Merge on Medicaid IDs ----
    idh <- merge(idh, mcaid, by = c('KCMASTER_ID'), all = T)
    
# Merge on phousing_id ----
    idh <- merge(idh, pha, by = c('KCMASTER_ID'), all = T)
    
# Merge on Medicare IDs ----
    # in the future may have actual medicare IDs linked via the IDH or from
    # within APDE. Regardless, for now, we don't have a linkage so just add
    # a placeholder
    idh <- idh[, .(id_apde, KCMASTER_ID, id_mcaid, id_mcare = NA_character_, phousing_id, last_run = Sys.time())]
    setorder(idh, id_apde)
    
# Write to SQL ----
    idh_field_types <- c("id_apde" = "NVARCHAR(10)", 
                         "KCMASTER_ID" = 'NVARCHAR(11)', 
                         "id_mcaid" = 'NVARCHAR(11)', 
                         "id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", 
                         "phousing_id" = "NVARCHAR(64)",
                         "last_run" = "datetime") 
    
    rads::tsql_chunk_loader(ph.data = idh,
                            db_conn = rads::validate_hhsaw_key(),
                            chunk_size = 5000,
                            schema_name = 'claims',
                            table_name = 'stage_xwalk_apde_mcaid_mcare_pha',
                            overwrite = TRUE,
                            append = FALSE,
                            field_types = idh_field_types,
                            validate_field_types = FALSE,
                            validate_upload = TRUE)
    
# The end ----