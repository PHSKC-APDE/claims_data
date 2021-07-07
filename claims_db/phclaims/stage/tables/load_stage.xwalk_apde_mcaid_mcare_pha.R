# HEADER ----
# Author: Danny Colombara
# Date: October 25, 2019
# Updated by Alastair Matheson, 2021-06-27
#
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
#        this will allow us to have sex== unknown, male, female, multiple
#
#
#
# This code is designed to be run as part of the master Medicaid/Medicare script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_mcare_analytic.R
#

# OVERVIEW ----
# 1) Prepare file with all Mcaid Identifiers
# 2) Prepare file with all MCARE Identifiers
# 3) Prepare file with all PHA Identifiers
# 4) Link Mcaid-Mcare 
# 5) Link Mcare-PHA
# 6) Link Mcaid-PHA
# 7) Create 4-way linkage (APDE-MCARE-MCAID-PHA)
# 8) Drop all temporary SQL tables 

start.time <- Sys.time()

# FUNCTIONS ... general data cleaning / prep procedures ----
## Data prep ... clean names ----
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

## Data prep ... clean dob ----
prep.dob <- function(dt){
  # Extract date components
  dt[, dob.year := as.integer(lubridate::year(dob))] # extract year
  dt[, dob.month := as.integer(lubridate::month(dob))] # extract month
  dt[, dob.day := as.integer(lubridate::day(dob))] # extract day
  dt[, c("dob") := NULL] # drop vars that are not needed
  
  return(dt)
}

## Data prep .... clean sex ----
prep.sex <- function(dt){
  # Change sex to numeric for improved strcmp function
  dt[gender_me == "Multiple", gender_me := 0L]
  dt[gender_me == "Male", gender_me := 1L]
  dt[gender_me == "Female", gender_me := 2L]
  dt[gender_me == "Unknown", gender_me := NA_integer_]
  dt[, gender_me := as.integer(gender_me)]
  
  return(dt)
}


## Consolidate cluster IDs across identities ----
# Adaptation of Carolina's code
# From here: https://github.com/DCHS-PME/PMEtools/blob/master/R/idm_dedup.R
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


## Helper functions specifically for client deduplication
#' Function for joining duplicated records to base pair, used in recursive deduplication
#' @param base_df The starting dataframe with initial duplicated pair ids
#' @param iterate_df The df with iterated rowid joins - what is continually updated during recursive pair closing
#' @param iteration Numeric counter indicating which recursion iteration the self-joining loop is on. Used for column name suffixes
self_join_dups <- function(base_df, iterate_df, iteration) {
  joinby <- paste0("rowid_recur", iteration)
  names(joinby) <- paste0("id2_recur", iteration-1)
  
  base_df %>%
    select(rowid, id2) %>%
    rename_all(~paste0(., "_recur", iteration)) %>%
    left_join(iterate_df, ., by = joinby)
}


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



## Identify objects/function to keep throughout entire process ----    
keep.me <- c(ls(), "keep.me") # everything created above should be permanent



# PREP MCAID DATA ----
## NOTE TO PREVENT FUTURE INSANITY ----
# There are id_mcaid in stage.mcaid_elig that never appear in our elig_demo, 
# so it possible for people to match with Mcare or PHA and not appear in the elig_demo file

## (1) Load Mcaid data from SQL ----  
db_claims51 <- dbConnect(odbc(), "PHClaims51")

mcaid.elig <- setDT(odbc::dbGetQuery(db_claims51, "SELECT id_mcaid, dob, gender_me, gender_female, gender_male FROM final.mcaid_elig_demo"))

mcaid.names <- setDT(odbc::dbGetQuery(db_claims51, "SELECT MEDICAID_RECIPIENT_ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, CLNDR_YEAR_MNTH FROM stage.mcaid_elig"))
setnames(mcaid.names, names(mcaid.names), c("id_mcaid", "name_gvn", "name_mdl", "name_srnm", "date"))

mcaid.ssn <- setDT(odbc::dbGetQuery(db_claims51, "SELECT MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, CLNDR_YEAR_MNTH FROM stage.mcaid_elig"))
setnames(mcaid.ssn, names(mcaid.ssn), c("id_mcaid", "ssn", "date"))


## (2) Tidy individual Mcaid data files before merging ----
# elig
mcaid.elig <- unique(mcaid.elig)
if(nrow(mcaid.elig) - length(unique(mcaid.elig$id_mcaid)) != 0){
  stop('non-unique id_mcaid in elig')
}

# ssn
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

# name
# clean names using function
mcaid.names <- prep.names(mcaid.names)
mcaid.names <- prep.names(mcaid.names) # run second time because some people have suffixes like "blah JR IV", so first removes IV and second removes JR

# sort data
setkey(mcaid.names, id_mcaid, date) # sort from oldest to newest 

# Split those with consistent data from the others
mcaid.names <- mcaid.names[, rank := 1:.N, by = c("id_mcaid", "name_gvn", "name_mdl", "name_srnm")] # rank for each set of unique data 
mcaid.names <- mcaid.names[rank == 1][, c("rank") := NULL] # keep only most recent set of unique data rows
mcaid.names[, dup := .N, by = id_mcaid]  # identify duplicates by id
name.ok <- mcaid.names[dup == 1][, c("dup", "date") := NULL]

# For duplicate IDs, fill in missing middle initial when possible & keep most recent name
name.dups <- mcaid.names[dup > 1][, dup := NULL]
name.dups[, name_mdl  := name_mdl[1], by= .( id_mcaid , cumsum(!is.na(name_mdl)) ) ] # fill middle initial forward / downward
name.dups <- name.dups[name.dups[, .I[which.max(date)], by = 'id_mcaid'][,V1], .(id_mcaid, name_gvn, name_mdl, name_srnm)] # keep the row for the max year

# Append those with unique obs and those with deduplicated obs
mcaid.names <- rbind(name.ok, name.dups)
setkey(mcaid.names, id_mcaid)
rm(name.dups, name.ok)

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
mcaid.dt[, group := .GRP, by = c("dob", "gender_me", "gender_female", "gender_male", "name_mdl", "name_srnm", "name_gvn")] 
setorder(mcaid.dt, group, id_mcaid)
mcaid.dt[, ssn  := ssn[1], by= .(group , cumsum(!is.na(ssn)) ) ] # fill ssn forward / downward
setorder(mcaid.dt, group, -id_mcaid) # reverse order to fill SSN the other way
mcaid.dt[, ssn  := ssn[1], by= .(group , cumsum(!is.na(ssn)) ) ] # fill ssn forward / downward
mcaid.dt[, group := NULL]

# Look for when on person has multiple id_mcaids
# Keep all for now
setorder(mcaid.dt, -id_mcaid) # sort with largest ID at top
mcaid.dt <- mcaid.dt[, dup := 1:.N, by = c("dob", "gender_me", "gender_female", "gender_male", "name_mdl", "name_srnm", "name_gvn", "ssn")]
# mcaid.dt <- mcaid.dt[dup == 1, ] # when all data duplicate, keep only the most recent, i.e., the largest id_mcaid
mcaid.dt[, dup := NULL]


# See when an SSN appears more than once 
# Keep all for now
mcaid.dt[ssn=="123456789", ssn := NA] # this is a garbage SSN code
setorder(mcaid.dt, ssn, -id_mcaid) # order by ssn & id so that can identify the max id. 
mcaid.dt[!is.na(ssn), dup := 1:.N, by = "ssn"] # identify when when there are duplicate ssn (and ssn is not missing)
# mcaid.dt <- mcaid.dt[is.na(dup) | dup == 1, ][, dup := NULL] # drop when N > 1, this will keep the max mcaid id only, which is what we agreed to with Eli and Alastair

# Prep sex for linkage
mcaid.dt <- prep.sex(mcaid.dt)   

# Prep dob for linkage
mcaid.dt <- prep.dob(mcaid.dt)

## (5) Load Medicaid id table to SQL ----
# create last_run timestamp
mcaid.dt[, id_hash := as.character(toupper(openssl::sha256(paste(str_replace_na(ssn, ''),
                                                                 str_replace_na(id_mcaid, ''),
                                                                 str_replace_na(name_srnm, ''),
                                                                 str_replace_na(name_gvn, ''),
                                                                 str_replace_na(name_mdl, ''),
                                                                 str_replace_na(dob.year, ''),
                                                                 str_replace_na(dob.month, ''),
                                                                 str_replace_na(dob.day, ''),
                                                                 str_replace_na(gender_me, ''),
                                                                 sep = "|"))))]
mcaid.dt[, source := 'mcaid']
mcaid.dt[, last_run := Sys.time()]

# column types for SQL
sql.columns <- c("id_mcaid" = "CHAR(11)", "ssn" = "char(9)", 
                 "dob.year" = "INT", "dob.month" = "INT", "dob.day" = "INT",
                 "name_srnm" = "varchar(255)", "name_gvn" = "varchar(255)", "name_mdl" = "varchar(255)", 
                 "gender_me" = "INT", "gender_female" = "INT", "gender_male" = "INT", 
                 "id_hash" = "CHAR(64)", "source" = "VARCHAR(255)",
                 "last_run" = "datetime")  

# ensure column order in R is the same as that in SQL
setcolorder(mcaid.dt, names(sql.columns))


# Write table to SQL
# Split into smaller tables to avoid SQL connection issues
start <- 1L
max_rows <- 100000L
cycles <- ceiling(nrow(mcaid.dt)/max_rows)

lapply(seq(start, cycles), function(i) {
  start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
  end_row <- min(nrow(mcaid.dt), max_rows * i)
  
  message("Loading cycle ", i, " of ", cycles)
  if (i == 1) {
    dbWriteTable(db_claims51,
                 DBI::Id(schema = "tmp", table = "xwalk_mcaid_prepped"),
                 value = as.data.frame(mcaid.dt[start_row:end_row]),
                 overwrite = T, append = F,
                 field.types = sql.columns)
  } else {
    dbWriteTable(db_claims51,
                 DBI::Id(schema = "tmp", table = "xwalk_mcaid_prepped"),
                 value = as.data.frame(mcaid.dt[start_row:end_row]),
                 overwrite = F, append = T)
  }
})

# Confirm that all rows were loaded to sql
stage.count <- as.numeric(odbc::dbGetQuery(db_claims51, 
                                           "SELECT COUNT (*) FROM tmp.xwalk_mcaid_prepped"))
if(stage.count != nrow(mcaid.dt)) {
  stop("Mismatching row count, error writing or reading data")
}


## (6) Close ODBC connection & drop temporary files ----
rm(mcaid.elig, mcaid.names, mcaid.ssn)
gc()


# Prep MCARE DATA ----
## NOTE TO PREVENT FUTURE INSANITY ----
# There are id_mcare in names and ssn files that never appear in our MBSS, so it possible for people to match with Mcaid or PHA and not
# appear in the elig_demo file

## (1) Load data from SQL ----  
db_claims51 <- dbConnect(odbc(), "PHClaims51")   

mcare.elig <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT id_mcare, dob, gender_me, gender_female, gender_male FROM final.mcare_elig_demo"))

mcare.names <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT bene_id AS id_mcare, bene_srnm_name AS name_srnm, 
                                              bene_gvn_name AS name_gvn, bene_mdl_name AS name_mdl FROM stage.mcare_xwalk_edb_user_view"))

mcare.ssn <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT bene_id AS id_mcare, ssn FROM stage.mcare_xwalk_bene_ssn"))

## (2) Tidy individual data files before merging ----
# Keep only unique rows of identifiers within a file
if(nrow(mcare.elig) - length(unique(mcare.elig$id_mcare)) != 0){
  stop('non-unique id_mcare in mcare.elig')
} # confirm all ids are unique in elig data

mcare.names <- unique(mcare.names)
if(nrow(mcare.names) - length(unique(mcare.names$id_mcare)) != 0){
  stop('non-unique id_mcare in mcare.names')
} # confirm all ids are unique in names data

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
# But keep in so all id_mcares are present
# mcare.dt <- mcare.dt[!(is.na(ssn) & is.na(dob.year) & is.na(name_srnm) ), ] 

## (5) Deduplicate when all information is the same (name, SSN, dob, & gender) except id_mcare ----
# Keep all for now

## Identify the duplicates
mcare.dt[, dup := .N, by = c("name_srnm", "name_gvn", "name_mdl", "ssn", "dob.year", "dob.month", "dob.day", "gender_me")]
# mcare.dups <- mcare.dt[dup != 1 & !is.na(name_srnm), ]
# mcare.nondup <- mcare.dt[!id_mcare %in% mcare.dups$id_mcare ]
# 
# # choose the one to keep by the most recent enrollment year for each potential duplicate (from MBSF)
# mbsf <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT [bene_id] AS id_mcare, [bene_enrollmt_ref_yr] AS year FROM [PHClaims].[stage].[mcare_mbsf]"))
# mbsf <- mbsf[id_mcare %in% mcare.dups$id_mcare] # limit to ids that identify a duplicate in mcare.dups
# mbsf <- unique(mbsf[, .(maxyear = max(year)), by = "id_mcare"])
# 
# # merge MBSF max date back onto potential duplicates
# mcare.dups <- merge(mcare.dups, mbsf, by = "id_mcare")
# 
# # keep the most recent year for each set of duplicates
# mcare.dups[, group := .GRP, by = .(ssn, name_srnm, name_gvn, name_mdl, dob.year, dob.month, dob.day, gender_me)]
# mcare.dups <- mcare.dups[mcare.dups[, .I[which.max(maxyear)], by = 'group'][,V1], ] 
# 
# # combine non-duplicate and deduplicated data
# mcare.dt <- rbind(mcare.nondup, mcare.dups, fill = T)[, c("dup", "maxyear", "group") := NULL]
# rm(mcare.dups, mcare.nondup, mbsf)



## (6) Load Medicare id table to SQL ----
# create last_run timestamp and other variables
mcare.dt[, id_hash := as.character(toupper(openssl::sha256(paste(str_replace_na(ssn, ''),
                                                                 str_replace_na(id_mcare, ''),
                                                                 str_replace_na(name_srnm, ''),
                                                                 str_replace_na(name_gvn, ''),
                                                                 str_replace_na(name_mdl, ''),
                                                                 str_replace_na(dob.year, ''),
                                                                 str_replace_na(dob.month, ''),
                                                                 str_replace_na(dob.day, ''),
                                                                 str_replace_na(gender_me, ''),
                                                                 sep = "|"))))]
mcare.dt[, source := 'mcare']
mcare.dt[, last_run := Sys.time()]

# column types for SQL
sql.columns <- c("id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "ssn" = "char(9)", 
                 "dob.year" = "INT", "dob.month" = "INT", "dob.day" = "INT",
                 "name_srnm" = "varchar(255)", "name_gvn" = "varchar(255)", "name_mdl" = "varchar(255)", 
                 "gender_me" = "INT", "gender_female" = "INT", "gender_male" = "INT", 
                 "id_hash" = "CHAR(64)", "source" = "VARCHAR(255)",
                 "last_run" = "datetime")  

# ensure column order in R is the same as that in SQL
setcolorder(mcare.dt, names(sql.columns))

# Write table to SQL
# Split into smaller tables to avoid SQL connection issues
start <- 1L
max_rows <- 100000L
cycles <- ceiling(nrow(mcare.dt)/max_rows)

lapply(seq(start, cycles), function(i) {
  start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
  end_row <- min(nrow(mcare.dt), max_rows * i)
  
  message("Loading cycle ", i, " of ", cycles)
  if (i == 1) {
    dbWriteTable(db_claims51,
                 DBI::Id(schema = "tmp", table = "xwalk_mcare_prepped"),
                 value = as.data.frame(mcare.dt[start_row:end_row]),
                 overwrite = T, append = F,
                 field.types = sql.columns)
  } else {
    dbWriteTable(db_claims51,
                 DBI::Id(schema = "tmp", table = "xwalk_mcare_prepped"),
                 value = as.data.frame(mcare.dt[start_row:end_row]),
                 overwrite = F, append = T)
  }
})


# Confirm that all rows were loaded to sql
stage.count <- as.numeric(odbc::dbGetQuery(db_claims51, 
                                           "SELECT COUNT (*) FROM tmp.xwalk_mcare_prepped"))
if(stage.count != nrow(mcare.dt)) {
  stop("Mismatching row count, error writing or reading data")
}


## (7) Close ODBC connection and drop temporary files ----
rm(mcare.elig, mcare.names, mcare.ssn)
gc()



# PREP PHA DATA ----
## (1) Load data from SQL ----
# NB. As of 2021-06-27, PHA data are on the dev Azure HHSAW. Eventually update to prod
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqldev20.database.windows.net,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

pha.dt <- odbc::dbGetQuery(db_hhsaw, "SELECT * FROM pha.final_identities")


## (2) Tidy PHA data ----
# Align fields with earlier prep
pha.dt <- setDT(pha.dt %>%
                  rename(name_srnm = lname,
                         name_gvn = fname,
                         name_mdl = mname) %>%
                  mutate(gender_me = case_when(female == 1 ~ 2L,
                                               female == 0 ~ 1L),
                         dob.year = as.integer(lubridate::year(dob)),
                         dob.month = as.integer(lubridate::month(dob)),
                         dob.day = as.integer(lubridate::day(dob)),
                         # Redo id-hash based on new gender field
                         id_hash = as.character(toupper(openssl::sha256(paste(str_replace_na(ssn, ''),
                                                                              str_replace_na(pha_id, ''),
                                                                              str_replace_na(name_srnm, ''),
                                                                              str_replace_na(name_gvn, ''),
                                                                              str_replace_na(name_mdl, ''),
                                                                              str_replace_na(dob, ''),
                                                                              str_replace_na(gender_me, ''),
                                                                              sep = "|")))),
                         source = "pha"
                  ))

# Clean up names that same way as other data
pha.dt <- prep.names(pha.dt)



# COMBINE ALL SOURCES INTO ONE PLACE ----
## Bring in/fix data (if needed) ----


input <- bind_rows(mcaid.dt,
                   mcare.dt,
                   pha.dt) %>%
  select(-gender_female, -gender_male, -female, -dob, -last_run, -pha_id, -dup) %>%
  # Add phonics and set up a rowid for self-joining later
  mutate(name_srnm_phon = RecordLinkage::soundex(name_srnm),
         name_gvn_phon = RecordLinkage::soundex(name_gvn),
         rowid = row_number())



# FIRST PASS: BLOCK ON SSN ----
## Run deduplication ----
# Blocking on SSN or PHA ID and string compare names
st <- Sys.time()
match_01 <- RecordLinkage::compare.dedup(
  input, 
  blockfld = "ssn", 
  strcmp = c("name_srnm", "name_gvn", "name_mdl", "dob.year", "dob.month", "dob.day", "gender_me"), 
  exclude = c("id_mcaid", "id_mcare", "id_kc_pha", "name_srnm_phon", 
              "name_gvn_phon", "rowid", "id_hash", "source"))
message("Pairwise comparisons complete. Total run time: ", round(Sys.time() - st, 2), " ", units(Sys.time()-st))

summary(match_01)


## Add weights and extract pairs ----
# Using EpiLink approach
match_01 <- epiWeights(match_01)
classify_01 <- epiClassify(match_01, threshold.upper = 0.6)
summary(classify_01)
pairs_01 <- getPairs(classify_01, single.rows = TRUE) %>%
  mutate(across(contains("dob."), ~ str_squish(.)))

## Review output and select cutoff point(s) ----
pairs_01 %>% 
  filter(Weight >= 0.83) %>% 
  filter(!((dob.month.1 == "1" & dob.day.1 == "1") | (dob.month.2 == "1" & dob.day.2 == "1"))) %>%
  # filter(!(dob.month.1 == "1" & dob.day.1 == "1" & dob.month.2 == "1" & dob.day.2 == "1")) %>%
  # filter(dob.month.1 == "1" & dob.day.1 == "1" & dob.month.2 == "1" & dob.day.2 == "1") %>%
  filter(dob.year.1 != dob.year.2) %>%
  # filter(dob.month.1 == dob.month.2 & dob.day.1 == dob.day.2) %>%
  # filter(dob.month.1 == dob.day.2 & dob.day.1 == dob.month.2) %>%
  # filter(is.na(name_srnm.1) | is.na(name_srnm.2)) %>%
  # filter(name_srnm.1 == name_gvn.2 & name_gvn.1 == name_srnm.2) %>%
  filter(gender_me.1 != gender_me.2) %>%
  filter(source.1 != source.2) %>%
  select(id1, ssn.1, name_srnm.1, name_gvn.1, name_mdl.1, dob.year.1, 
         dob.month.1, dob.day.1, gender_me.1, source.1,
         id2, ssn.2, name_srnm.2, name_gvn.2, name_mdl.2, dob.year.2, 
         dob.month.2, dob.day.2, gender_me.2, source.2,
         Weight) %>%
  tail()


# Make truncated version
pairs_01_trunc <- pairs_01 %>%
  # Avoid matching all the PHA IDs again
  filter(source.1 != source.2) %>%
  filter(
    # SECTION FOR NON-JAN 1 BIRTH DATES
    (
      !((dob.month.1 == "1" & dob.day.1 == "1") | (dob.month.2 == "1" & dob.day.2 == "1")) &
        (
          # Can take quite a low score when SSN matches, names are transposed, and YOB is the same
          (Weight >= 0.4 & dob.year.1 == dob.year.2 & name_srnm.1 == name_gvn.2 & name_gvn.1 == name_srnm.2) |
            # Higher score when SSN matches, names are transposed, and YOB is different
            (Weight >= 0.65 & dob.year.1 != dob.year.2 & name_srnm.1 == name_gvn.2 & name_gvn.1 == name_srnm.2) |
            # Same month and day of birth but different year, no name checks
            (Weight >= 0.72 & dob.year.1 != dob.year.2 & dob.month.1 == dob.month.2 & dob.day.1 == dob.day.2) |
            # Transposed month and day of birth but no name checks
            (Weight >= 0.63 & dob.year.1 == dob.year.2 & dob.month.1 == dob.day.2 & dob.day.1 == dob.month.2) |
            # Mismatched gender but same YOB
            (Weight >= 0.73 & dob.year.1 == dob.year.2 & gender_me.1 != gender_me.2) |
            # Higher threshold if mismatched gender and YOB
            (Weight >= 0.844 & dob.year.1 != dob.year.2 & gender_me.1 != gender_me.2) | 
            # Catch everything else
            (Weight >= 0.74 & gender_me.1 == gender_me.2)
            
        )
    ) |
      # SECTION FOR WHEN THERE IS A JAN 1 BIRTRH DATE INVOLVED
    (Weight >= 0.75 & dob.month.1 == "1" & dob.day.1 == "1" & dob.month.2 == "1" & dob.day.2 == "1") |
      (Weight >= 0.77 & (dob.month.1 == "1" & dob.day.1 == "1") | (dob.month.2 == "1" & dob.day.2 == "1")) |
      # SECTION FOR MISSING GENDER AND/OR DOB
      (
        (is.na(gender_me.1) | is.na(gender_me.2) | is.na(dob.year.1) | is.na(dob.year.2)) &
          (
            # First names match
            (Weight > 0.45 & name_gvn.1 == name_gvn.2) |
              # Higher threshold first names don't match
              (Weight > 0.54 & name_gvn.1 != name_gvn.2)
          )
      )
  )

## Collapse IDs ----
match_01_dedup <- match_process(pairs_input = pairs_01_trunc, df = input, iteration = 1)

## Error check ----
match_01_chk <- setDT(match_01_dedup %>% distinct(id_hash, clusterid_1))
match_01_chk[, cnt := .N, by = "id_hash"]
match_01_chk %>% count(cnt)
if (max(match_01_chk$cnt) > 1) {
  stop("Some id_hash values are associated with multiple clusterid_1 values. ",
       "Check what went wrong.")
}
rm(match_01_chk)


# SECOND PASS: BLOCK ON PHONETIC LNAME, FNAME AND DOB ----
## Run deduplication ----
st <- Sys.time()
match_02 <- RecordLinkage::compare.dedup(
  input, 
  blockfld = c("name_srnm_phon", "name_gvn_phon", "dob.year", "dob.month", "dob.day"), 
  strcmp = c("ssn", "name_srnm", "name_gvn", "name_mdl", "gender_me"), 
  exclude = c("id_mcaid", "id_mcare", "id_kc_pha", "rowid", "id_hash", "source"))
message("Pairwise comparisons complete. Total run time: ", round(Sys.time() - st, 2), " ", units(Sys.time()-st))

summary(match_02)


## Add weights and extract pairs ----
# Using EpiLink approach
match_02 <- epiWeights(match_02)
classify_02 <- epiClassify(match_02, threshold.upper = 0.6)
summary(classify_02)
pairs_02 <- getPairs(classify_02, single.rows = TRUE) %>%
  mutate(across(contains("dob."), ~ str_squish(.)))


## Review output and select cutoff point(s) ----
pairs_02 %>% filter(Weight <= 0.8 & ssn.1 != ssn.2) %>% select(-contains("id_hash")) %>% head()

pairs_02 %>% 
  filter(source.1 != source.2) %>%
  # select(-contains("id_hash")) %>% 
  filter(Weight >= 0.82) %>% 
  # filter(ssn.1 != ssn.2) %>%
  # filter(!is.na(ssn.1) & !is.na(ssn.2)) %>%
  filter(is.na(ssn.1) | is.na(ssn.2)) %>%
  # filter(!(dob.month.1 == "1" & dob.day.1 == "1")) %>%
  filter(dob.month.1 == "1" & dob.day.1 == "1") %>%
  select(id1, ssn.1, name_srnm.1, name_gvn.1, name_mdl.1, dob.year.1, 
         dob.month.1, dob.day.1, gender_me.1, source.1,
         id2, ssn.2, name_srnm.2, name_gvn.2, name_mdl.2, dob.year.2, 
         dob.month.2, dob.day.2, gender_me.2, source.2,
         Weight) %>%
  tail()


# Make truncated data frame
pairs_02_trunc <- pairs_02 %>%
  # Avoid matching all the PHA IDs again
  filter(source.1 != source.2) %>%
  filter(
    # Matching SSN all have high weights and look good
    ssn.1 == ssn.2 |
      # SECTION WHERE SSNs DO NOT MATCH
    (Weight >= 0.88 & ssn.1 != ssn.2 & !(dob.month.1 == "1" & dob.day.1 == "1")) |
      (Weight >= 0.90 & ssn.1 != ssn.2 & dob.month.1 == "1" & dob.day.1 == "1") |
     # SECTION WHERE AN SSN IS MISSING
     ((is.na(ssn.1) | is.na(ssn.2)) &
       (
         (Weight >= 0.69 & !(dob.month.1 == "1" & dob.day.1 == "1")) |
           (Weight >= 0.85 & dob.month.1 == "1" & dob.day.1 == "1")
       )
     )
  )


## Collapse IDs ----
match_02_dedup <- match_process(pairs_input = pairs_02_trunc, df = input, iteration = 2) %>%
  mutate(clusterid_2 = clusterid_2 + max(match_01_dedup$clusterid_1))

## Error check ----
match_02_chk <- setDT(match_02_dedup %>% distinct(id_hash, clusterid_2))
match_02_chk[, cnt := .N, by = "id_hash"]
match_02_chk %>% count(cnt)
if (max(match_02_chk$cnt) > 1) {
  stop("Some id_hash values are associated with multiple clusterid_2 values. ",
       "Check what went wrong.")
}

rm(match_02_chk)


# BRING MATCHING ROUNDS TOGETHER ----
# Use clusterid_1 as the starting point, find where one clusterid_2 value
# is associated with multiple clusterid_1 values, then take the min of the latter.
# This would need to made iterative if there is more than two matching processes.

ids_dedup <- setDT(full_join(select(match_01_dedup, id_hash, clusterid_1), 
                             select(match_02_dedup, id_hash, clusterid_2),
                             by = "id_hash"))

ids_dedup[, clusterid := min(clusterid_1), by = "clusterid_2"]


## Error check ----
ids_dedup_chk <- unique(ids_dedup[, c("id_hash", "clusterid")])
ids_dedup_chk[, cnt_id := .N, by = "id_hash"]
ids_dedup_chk[, cnt_hash := .N, by = "clusterid"]
# cnt_id should = 1 and cnt_hash should be >= 1
ids_dedup_chk %>% count(cnt_id, cnt_hash)
if (max(ids_dedup_chk$cnt_id) > 1) {
  stop("There is more than one cluster ID for a given id_has. Investigate why.")
}


## Now make an alpha-numeric ID that will be stored in a table ----

# NB. This will need to be reworked when there is an existing table with APDE IDs
#  Likely make twice as many IDs as needed then weed out the ones already in
#    the master list, before trimming to the actual number needed.

ids_final <- id_nodups(id_n = n_distinct(ids_dedup$clusterid),
                       id_length = 10)
ids_final <- ids_dedup %>%
  distinct(clusterid) %>%
  arrange(clusterid) %>%
  bind_cols(., id_apde = ids_final)

mcaid_mcare_pha <- input %>%
  select(ssn, id_mcaid, id_mcare, id_kc_pha, name_srnm, name_gvn, name_mdl, 
         dob.year, dob.month, dob.day, gender_me, id_hash) %>%
  left_join(., select(ids_dedup, id_hash, clusterid), by = "id_hash") %>%
  left_join(., ids_final, by = "clusterid") %>%
  select(id_apde, id_mcaid, id_mcare, id_kc_pha, id_hash) %>%
  distinct() %>%
  mutate(last_run = Sys.time())



# QA FINAL DATA ----
### REVIEW POINT ----
# Number of id_hashes compared to the number of id_apdes
message("There are ", n_distinct(mcaid_mcare_pha$id_hash), " IDs and ", 
        n_distinct(mcaid_mcare_pha$id_apde), " id_apde IDs")

db_claims51 <- dbConnect(odbc(), "PHClaims51")

mcaid.only <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT id_mcaid FROM final.mcaid_elig_demo")) # go back to elig_demo because it has an ALMOST complete list of all Mcaid ID

mcare.only <- setDT(odbc::dbGetQuery(db_claims51, "SELECT DISTINCT id_mcare FROM final.mcare_elig_demo")) # go back to elig_demo because it has an ALMOST complete list of all Mcare ID


## Confirm that every ID is accounted for ----
# Check Mcare
extra.mcare <- setdiff(mcaid_mcare_pha[!is.na(id_mcare)]$id_mcare, mcare.only[!is.na(id_mcare)]$id_mcare) 
missing.mcare <- setdiff(mcare.only[!is.na(id_mcare)]$id_mcare, mcaid_mcare_pha[!is.na(id_mcare)]$id_mcare) 
length(extra.mcare) # Expect there will be extra b/c there are Mcare ids in SSN and Names files that are not in MBSF
length(missing.mcare) # should be zero in length

# Check Mcaid
extra.mcaid <- setdiff(mcaid_mcare_pha[!is.na(id_mcaid)]$id_mcaid, mcaid.only[!is.na(id_mcaid)]$id_mcaid)
missing.mcaid <- setdiff(mcaid.only[!is.na(id_mcaid)]$id_mcaid, mcaid_mcare_pha[!is.na(id_mcaid)]$id_mcaid)
length(extra.mcaid) # Expect will be more than zero because there were id_mcaid in stage.mcaid_elig that were not in the elig_demo
length(missing.mcaid) # should be zero


## Confirm that there are no duplicates in the final mcaid_mcare_pha linkage ----      
if(
  sum(duplicated(mcaid_mcare_pha[!is.na(id_mcare)]$id_mcare)) + 
  sum(duplicated(mcaid_mcare_pha[!is.na(id_mcaid)]$id_mcaid)) + 
  sum(duplicated(mcaid_mcare_pha[!is.na(id_kc_pha)]$id_kc_pha)) > 0)
  stop("There should be no duplicates in this final linked data.table")


# LOAD TO SQL ----
## identify the column types to be created in SQL ----
sql.columns <- c("id_apde" = "char(10)", 
                 "id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", 
                 "id_mcaid" = "char(11)", 
                 "id_kc_pha" = "char(10)", 
                 "id_hash" = "char(64)",
                 "last_run" = "datetime")  

# ensure column order in R is the same as that in SQL
setcolorder(mcaid_mcare_pha, names(sql.columns))


## Write table to SQL ----
# Split into smaller tables to avoid SQL connection issues
start <- 1L
max_rows <- 100000L
cycles <- ceiling(nrow(mcaid_mcare_pha)/max_rows)

lapply(seq(start, cycles), function(i) {
  start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
  end_row <- min(nrow(mcaid_mcare_pha), max_rows * i)
  
  message("Loading cycle ", i, " of ", cycles)
  if (i == 1) {
    dbWriteTable(db_claims51,
                 DBI::Id(schema = "stage", table = "xwalk_apde_mcaid_mcare_pha"),
                 value = as.data.frame(mcaid_mcare_pha[start_row:end_row]),
                 overwrite = T, append = F,
                 field.types = sql.columns)
  } else {
    dbWriteTable(db_claims51,
                 DBI::Id(schema = "stage", table = "xwalk_apde_mcaid_mcare_pha"),
                 value = as.data.frame(mcaid_mcare_pha[start_row:end_row]),
                 overwrite = F, append = T)
  }
})


## Confirm that all rows were loaded to sql ----
stage.count <- as.numeric(odbc::dbGetQuery(db_claims51, 
                                           "SELECT COUNT (*) FROM stage.xwalk_apde_mcaid_mcare_pha"))
if(stage.count != nrow(mcaid_mcare_pha))
  stop("Mismatching row count, error writing or reading data")      

# close database connections    
dbDisconnect(db_claims51)  
dbDisconnect(db_apde51)  


# CLEAN UP ----
## Remove data
rm(list = ls(pattern = "match"))
rm(list = ls(pattern = "pairs"))
rm(list = ls(pattern = "classify"))
rm(list = ls(pattern = "ids_"))
rm(mcaid.dt, mcare.dt, pha.dt)
rm(input)


## DROP TMP SQL TABLES ----
## Drop tables from PhClaims51 ----
db_claims51 <- dbConnect(odbc(), "PHClaims51")
dbExecute(db_claims51, "DROP TABLE [PHClaims].[tmp].[xwalk_mcaid_mcare]")
dbExecute(db_claims51, "DROP TABLE [PHClaims].[tmp].[xwalk_mcaid_prepped]")
dbExecute(db_claims51, "DROP TABLE [PHClaims].[tmp].[xwalk_mcare_prepped]")
dbDisconnect(db_claims51)  

## Drop tables from PH_APDEStore51 ----   
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
dbExecute(db_apde51, "DROP TABLE [PH_APDEStore].[tmp].[xwalk_mcaid_pha]")
dbExecute(db_apde51, "DROP TABLE [PH_APDEStore].[tmp].[xwalk_mcare_pha]")
dbExecute(db_apde51, "DROP TABLE [PH_APDEStore].[tmp].[xwalk_pha_prepped]")
dbDisconnect(db_apde51) 

## The end! ----      
run.time <- Sys.time() - start.time  
print(run.time)

Sys.time() - start.time
