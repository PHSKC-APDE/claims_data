# HEADER ----
# Author: Danny Colombara
# Date: October 25, 2019
# Updated by Alastair Matheson, 2021-06-27
#
# Purpose: Create master ID linkage file, identifying people across Mcaid, Mcare, and PHA with a NEW ID_APDE
#
# Notes: Will force one id_apde per id_mcaid and id_mcare. Even though there appear to be a few id_mcaid values
#         that map to >1 person, there is no simple way to tease apart their claims.
# 
#        All values of DOB, gender, etc. are included in the matching process. However, a single, best value will
#         be selected for DOB is an elig_demo table.
#
# This code is designed to be run as part of the master Medicaid/Medicare script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_mcare_analytic.R
#

# OVERVIEW ----
# 1) Prepare Medicaid identifiers
# 2) Prepare Medicare identifiers
# 3) Prepare PHA and PHA waitlist identifiers
# 5) Link IDs
# 6) Retain history of linkages

start.time <- Sys.time()

# FUNCTIONS ... general data cleaning / prep procedures ----
## Data prep ... clean names ----
prep.names <- function(dt){
  # Remove any extraneous spaces at the beginning or end of a name
  dt[, name_gvn := str_squish(name_gvn)]
  dt[, name_mdl := str_squish(name_mdl)]
  dt[, name_srnm := str_squish(name_srnm)]
  dt <- unique(dt)
  
  # Set up suffixes to remove
  suffix <- c(" SR", " JR", "-JR", "JR", "JR I", "JR II", "JR III", " II", " III", " IV")
  
  # Last names
  dt[, name_srnm := gsub("[0-9]", "", name_srnm)]  # remove any numbers that may be present in last name
  dt[, name_srnm := gsub("'", "", name_srnm)]  # remove apostrophes from last name, e.g., O'BRIEN >> OBRIEN
  dt[, name_srnm := gsub("\\.", "", name_srnm)]  # remove periods from last name, e.g., JONES JR. >> JONES JR
  dt[, name_srnm := gsub(paste(suffix, "$", collapse="|", sep = ""), "", name_srnm)]  # remove all suffixes from last name
  dt[, name_srnm := gsub("_|-", " ", name_srnm)]  # commented out but left here to document that this made the matching worse
  dt[, name_srnm := ifelse(name_srnm == "", NA, name_srnm)]
  
  # First names
  dt[, name_gvn := gsub("[0-9]", "", name_gvn)]  # remove any numbers that may be present in first names
  dt[, name_gvn := gsub("\\.", "", name_gvn)]  # remove periods from first name, e.g., JONES JR. >> JONES JR
  dt[, name_gvn := gsub("_|-", " ", name_gvn)]  # remove all hyphens from first names because use is inconsistent. Found that replacing with a space was worse
  dt[name_gvn != "JR", name_gvn := gsub(paste(suffix, "$", collapse="|", sep = ""), "", name_gvn)] # remove all suffixes from first name unless full name is JR
  dt[, name_gvn := ifelse(name_gvn == "", NA, name_gvn)]
  
  # Middle initials
  dt[, name_mdl := gsub("[0-9]", "", name_mdl)]  # remove any numbers that may be present in middle initial
  dt[!(grep("[A-Z]", name_mdl)), name_mdl := NA] # when middle initial is not a letter, replace it with NA
  dt[, name_mdl := ifelse(name_mdl == "", NA, name_mdl)]
  
  return(dt)
}

## Data prep ... clean dob ----
prep.dob <- function(dt){
  # Extract date components
  dt[, dob_year := as.integer(lubridate::year(dob))] # extract year
  dt[, dob_month := as.integer(lubridate::month(dob))] # extract month
  dt[, dob_day := as.integer(lubridate::day(dob))] # extract day
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
  
  if (nrow(double_dups) > 0) {
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
  }
  
  # collapse duplicate partial clusters to one cluster
  # error checking to make sure that correct total clusters are maintained
  if (nrow(double_dups) > 0) {
    message("Collapsing partial clusters")
    pairsend <- left_join(pairsend, double_dups, by = c("clusterid" = "clusterid")) %>%
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


# PREP MCAID DATA ----
## NOTE TO PREVENT FUTURE INSANITY ----
# There are id_mcaid in stage.mcaid_elig that never appear in our elig_demo, 
# so it possible for people to match with Mcare or PHA and not appear in the elig_demo file

## (1) Load Mcaid data from SQL ----  
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

mcaid <- setDT(odbc::dbGetQuery(
  db_hhsaw, "SELECT DISTINCT MEDICAID_RECIPIENT_ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, 
  GENDER, BIRTH_DATE, SOCIAL_SECURITY_NMBR 
  FROM claims.mcaid_elig"))
setnames(mcaid, names(mcaid), 
         c("id_mcaid", "name_gvn", "name_mdl", "name_srnm", "gender_me", "dob", "ssn"))


## (2) Tidy Mcaid data ----
# Names
mcaid <- prep.names(mcaid)
mcaid <- prep.names(mcaid) # run second time because some people have suffixes like "blah JR IV", so first removes IV and second removes JR

# Gender
mcaid <- prep.sex(mcaid)

# DOB
mcaid <- prep.dob(mcaid)

# SSN
# Find junk SSNs and set to NA 
mcaid <- housing::junk_ssn_all(mcaid, id = ssn)
mcaid[, temp_num := NULL] # This will be unnecessary once the housing package is updated
mcaid[ssn_junk == 1, ssn := NA]
mcaid[, ssn_junk := NULL]

# Source and id_hash
mcaid[, source := "mcaid"]
mcaid[, id_hash := as.character(toupper(openssl::sha256(paste(str_replace_na(ssn, ''),
                                                              str_replace_na(id_mcaid, ''),
                                                              str_replace_na(name_srnm, ''),
                                                              str_replace_na(name_gvn, ''),
                                                              str_replace_na(name_mdl, ''),
                                                              str_replace_na(dob_year, ''),
                                                              str_replace_na(dob_month, ''),
                                                              str_replace_na(dob_day, ''),
                                                              str_replace_na(gender_me, ''),
                                                              sep = "|"))))]
mcaid <- unique(mcaid)

## (3) Compare to existing IDs ----
# Placeholder until a table of IDs exists and a history table is set up


# Prep MCARE DATA ----

## NOT RUNNING UNTIL WE RECEIVE MCARE DATA AGAIN ----
# WILL THEN NEED TO OVERHAUL THE PROCESSES BELOW
# 
# ## NOTE TO PREVENT FUTURE INSANITY ----
# # There are id_mcare in names and ssn files that never appear in our MBSS, so it possible for people to match with Mcaid or PHA and not
# # appear in the elig_demo file
# 
# ## (1) Load data from SQL ----  
# db_claims <- dbConnect(odbc(), "PHClaims51")   
# 
# mcare.elig <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT id_mcare, dob, gender_me, gender_female, gender_male FROM final.mcare_elig_demo"))
# 
# mcare.names <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT bene_id AS id_mcare, bene_srnm_name AS name_srnm, 
#                                               bene_gvn_name AS name_gvn, bene_mdl_name AS name_mdl FROM stage.mcare_xwalk_edb_user_view"))
# 
# mcare.ssn <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT bene_id AS id_mcare, ssn FROM stage.mcare_xwalk_bene_ssn"))
# 
# ## (2) Tidy individual data files before merging ----
# # Keep only unique rows of identifiers within a file
# if(nrow(mcare.elig) - length(unique(mcare.elig$id_mcare)) != 0){
#   stop('non-unique id_mcare in mcare.elig')
# } # confirm all ids are unique in elig data
# 
# mcare.names <- unique(mcare.names)
# if(nrow(mcare.names) - length(unique(mcare.names$id_mcare)) != 0){
#   stop('non-unique id_mcare in mcare.names')
# } # confirm all ids are unique in names data
# 
# if(nrow(mcare.ssn) - length(unique(mcare.ssn$id_mcare)) >0){
#   stop('non-unique id_mcare in mcare.ssn')
# } # confirm all id and ssn are unique
# 
# 
# ## (3) Merge Mcare identifiers together ----
# # for all of WA state, want the most complete dataset possible, regardless of whether missing SSN or any other bit of information
# mcare.dt <- merge(mcare.ssn, mcare.names, by = "id_mcare", all.x=T, all.y = T)  
# if(nrow(mcare.dt) - length(unique(mcare.dt$id_mcare)) != 0){
#   stop('non-unique id_mcare!')
# }
# 
# mcare.dt <- merge(mcare.dt, mcare.elig, by = "id_mcare",  all.x=T, all.y = T)
# if(nrow(mcare.dt) - length(unique(mcare.dt$id_mcare)) != 0){
#   stop('non-unique id_mcare!')
# }
# 
# ## (4) Run cleaning functions on Medicare data ----
# mcare.dt <- prep.names(mcare.dt)
# mcare.dt <- prep.names(mcare.dt) # run second time because some people have suffixes like "blah JR IV", so first removes IV and second removes JR
# mcare.dt <- prep.dob(mcare.dt)
# mcare.dt <- prep.sex(mcare.dt)
# 
# # without ssn, dob, and last name, there is no hope of matching
# # But keep in so all id_mcares are present
# # mcare.dt <- mcare.dt[!(is.na(ssn) & is.na(dob_year) & is.na(name_srnm) ), ] 
# 
# ## (5) Deduplicate when all information is the same (name, SSN, dob, & gender) except id_mcare ----
# # Keep all for now
# 
# ## Identify the duplicates
# mcare.dt[, dup := .N, by = c("name_srnm", "name_gvn", "name_mdl", "ssn", "dob_year", "dob_month", "dob_day", "gender_me")]
# # mcare.dups <- mcare.dt[dup != 1 & !is.na(name_srnm), ]
# # mcare.nondup <- mcare.dt[!id_mcare %in% mcare.dups$id_mcare ]
# # 
# # # choose the one to keep by the most recent enrollment year for each potential duplicate (from MBSF)
# # mbsf <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT [bene_id] AS id_mcare, [bene_enrollmt_ref_yr] AS year FROM [PHClaims].[stage].[mcare_mbsf]"))
# # mbsf <- mbsf[id_mcare %in% mcare.dups$id_mcare] # limit to ids that identify a duplicate in mcare.dups
# # mbsf <- unique(mbsf[, .(maxyear = max(year)), by = "id_mcare"])
# # 
# # # merge MBSF max date back onto potential duplicates
# # mcare.dups <- merge(mcare.dups, mbsf, by = "id_mcare")
# # 
# # # keep the most recent year for each set of duplicates
# # mcare.dups[, group := .GRP, by = .(ssn, name_srnm, name_gvn, name_mdl, dob_year, dob_month, dob_day, gender_me)]
# # mcare.dups <- mcare.dups[mcare.dups[, .I[which.max(maxyear)], by = 'group'][,V1], ] 
# # 
# # # combine non-duplicate and deduplicated data
# # mcare.dt <- rbind(mcare.nondup, mcare.dups, fill = T)[, c("dup", "maxyear", "group") := NULL]
# # rm(mcare.dups, mcare.nondup, mbsf)
# 
# 



# PREP PHA DATA ----
## (1) Load data from SQL ----
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

pha <- odbc::dbGetQuery(db_hhsaw, "SELECT * FROM pha.final_identities")


## (2) Tidy PHA data ----
# Align fields with earlier prep
pha <- setDT(pha %>%
                  rename(name_srnm = lname,
                         name_gvn = fname,
                         name_mdl = mname) %>%
                  mutate(gender_me = case_when(female == 1 ~ 2L,
                                               female == 0 ~ 1L),
                         dob_year = as.integer(lubridate::year(dob)),
                         dob_month = as.integer(lubridate::month(dob)),
                         dob_day = as.integer(lubridate::day(dob))))

# Clean up names that same way as other data
pha <- prep.names(pha)

# Add source and redo id_hash
pha[, source := "pha"]

# Keep only distinct values
pha <- unique(pha)

## (3) Compare to existing IDs ----
# Placeholder for now until there is a table of existing IDs and history


# COMBINE ALL SOURCES INTO ONE PLACE ----
input <- bind_rows(mcaid,
                   # mcare.dt,
                   pha) %>%
  select(-female, -dob, -last_run, -pha_id) %>%
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
  strcmp = c("name_srnm", "name_gvn", "name_mdl", "dob_year", "dob_month", "dob_day", "gender_me"), 
  exclude = c("id_mcaid", 
              # "id_mcare", 
              "id_kc_pha", "name_srnm_phon", 
              "name_gvn_phon", "rowid", "id_hash", "source"))
message("Pairwise comparisons complete. Total run time: ", round(Sys.time() - st, 2), " ", units(Sys.time()-st))

summary(match_01)


## Add weights and extract pairs ----
# Using EpiLink approach
match_01 <- epiWeights(match_01)
classify_01 <- epiClassify(match_01, threshold.upper = 0.6)
summary(classify_01)
pairs_01 <- getPairs(classify_01, single.rows = TRUE) %>%
  mutate(across(contains("dob_"), ~ str_squish(.)),
         across(contains("dob_"), ~ as.numeric(.)))

## Review output and select cutoff point(s) ----
# pairs_01 %>% 
#   filter(Weight >= 0.55) %>% 
#   # filter(source.1 == "mcaid" & source.2 == "mcaid") %>%
#   # filter(!((dob_month.1 == "1" & dob_day.1 == "1") | (dob_month.2 == "1" & dob_day.2 == "1"))) %>%
#   # filter(!(dob_month.1 == "1" & dob_day.1 == "1" & dob_month.2 == "1" & dob_day.2 == "1")) %>%
#   # filter(dob_month.1 == "1" & dob_day.1 == "1" & dob_month.2 == "1" & dob_day.2 == "1") %>%
#   # filter(dob_year.1 != dob_year.2) %>%
#   # filter(dob_year.1 == dob_year.2 | abs(dob_year.1 - dob_year.2) == 100) %>%
#   # filter(dob_month.1 == dob_month.2 & dob_day.1 == dob_day.2) %>%
#   # filter(dob_month.1 == dob_day.2 & dob_day.1 == dob_month.2) %>%
#   # filter(is.na(name_srnm.1) | is.na(name_srnm.2)) %>%
#   # filter(name_srnm.1 == name_gvn.2 & name_gvn.1 == name_srnm.2) %>%
#   # filter(gender_me.1 != gender_me.2) %>%
#   # filter(source.1 != source.2) %>%
#   filter(is.na(gender_me.1) | is.na(gender_me.2) | is.na(dob_year.1) | is.na(dob_year.2)) %>%
#   filter(name_gvn.1 == name_gvn.2) %>%
#   select(id1, ssn.1, name_srnm.1, name_gvn.1, name_mdl.1, dob_year.1, 
#          dob_month.1, dob_day.1, gender_me.1, source.1, id_mcaid.1, id_kc_pha.1,
#          id2, ssn.2, name_srnm.2, name_gvn.2, name_mdl.2, dob_year.2, 
#          dob_month.2, dob_day.2, gender_me.2, source.2, id_mcaid.2, id_kc_pha.2,
#          Weight) %>%
#   tail()

# Make truncated version
pairs_01_trunc <- pairs_01 %>%
  # Avoid matching all the PHA IDs again but allow Medicaid rows to match
  filter(!(source.1 == "pha" & source.2 == "pha")) %>%
  filter(
    # SECTION FOR NON-JAN 1 BIRTH DATES
    (
      !((dob_month.1 == "1" & dob_day.1 == "1") | (dob_month.2 == "1" & dob_day.2 == "1")) &
        (
          # Can take quite a low score when SSN matches, names are transposed, and YOB is the same or off by 100
          (Weight >= 0.4 & (dob_year.1 == dob_year.2 | abs(dob_year.1 - dob_year.2) == 100) & 
             name_srnm.1 == name_gvn.2 & name_gvn.1 == name_srnm.2) |
            # Higher score when SSN matches, names are transposed, and YOB is different
            (Weight >= 0.65 & dob_year.1 != dob_year.2 & name_srnm.1 == name_gvn.2 & name_gvn.1 == name_srnm.2) |
            # Same month and day of birth but different year, no name checks
            (Weight >= 0.72 & dob_year.1 != dob_year.2 & dob_month.1 == dob_month.2 & dob_day.1 == dob_day.2) |
            # Transposed month and day of birth but no name checks
            (Weight >= 0.63 & dob_year.1 == dob_year.2 & dob_month.1 == dob_day.2 & dob_day.1 == dob_month.2) |
            # Mismatched gender but same YOB
            (Weight >= 0.73 & dob_year.1 == dob_year.2 & gender_me.1 != gender_me.2) |
            # Higher threshold if mismatched gender and YOB
            (Weight >= 0.844 & dob_year.1 != dob_year.2 & gender_me.1 != gender_me.2) | 
            # Catch everything else
            (Weight >= 0.74 & gender_me.1 == gender_me.2)
        )
    ) |
      # SECTION FOR WHEN THERE IS A JAN 1 BIRTH DATE INVOLVED
    (Weight >= 0.75 & dob_month.1 == "1" & dob_day.1 == "1" & dob_month.2 == "1" & dob_day.2 == "1") |
      (Weight >= 0.77 & (dob_month.1 == "1" & dob_day.1 == "1") | (dob_month.2 == "1" & dob_day.2 == "1")) |
      # SECTION FOR MISSING GENDER AND/OR DOB
      (
        (is.na(gender_me.1) | is.na(gender_me.2) | is.na(dob_year.1) | is.na(dob_year.2)) &
          (
            # First names match
            (Weight > 0.55 & name_gvn.1 == name_gvn.2) |
              # Higher threshold first names don't match
              (Weight > 0.64 & name_gvn.1 != name_gvn.2)
          )
      )
  )

## Collapse IDs ----
match_01_dedup <- match_process(pairs_input = pairs_01_trunc, df = input, iteration = 1)

## Error check ----
match_01_chk <- setDT(match_01_dedup %>% distinct(id_hash, clusterid_1))
match_01_chk[, cnt := .N, by = "id_hash"]

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
  blockfld = c("name_srnm_phon", "name_gvn_phon", "dob_year", "dob_month", "dob_day"), 
  strcmp = c("ssn", "name_srnm", "name_gvn", "name_mdl", "gender_me"), 
  exclude = c("id_mcaid", 
              #"id_mcare", 
              "id_kc_pha", "rowid", "id_hash", "source"))
message("Pairwise comparisons complete. Total run time: ", round(Sys.time() - st, 2), " ", units(Sys.time()-st))

summary(match_02)

## Add weights and extract pairs ----
# Using EpiLink approach
match_02 <- epiWeights(match_02)
classify_02 <- epiClassify(match_02, threshold.upper = 0.6)
summary(classify_02)
pairs_02 <- getPairs(classify_02, single.rows = TRUE) %>%
  mutate(across(contains("dob_"), ~ str_squish(.)),
         across(contains("dob_"), ~ as.numeric(.)))


## Review output and select cutoff point(s) ----
# pairs_02 %>% 
#   filter(!(source.1 == "pha" & source.2 == "pha")) %>%
#   filter(!(source.1 == "mcaid" & source.2 == "mcaid" & id_mcaid.1 != id_mcaid.2)) %>%
#   filter(source.1 != source.2) %>%
#   # select(-contains("id_hash")) %>% 
#   filter(Weight >= 0.82) %>% 
#   # filter(ssn.1 != ssn.2) %>%
#   # filter(!is.na(ssn.1) & !is.na(ssn.2)) %>%
#   filter(is.na(ssn.1) | is.na(ssn.2)) %>%
#   # filter(!(dob_month.1 == "1" & dob_day.1 == "1")) %>%
#   filter(dob_month.1 == "1" & dob_day.1 == "1") %>%
#   # filter(id_mcaid.1 != id_mcaid.2) %>%
#   # filter(name_gvn.1 == name_gvn.2) %>%
#   select(id1, ssn.1, name_srnm.1, name_gvn.1, name_mdl.1, dob_year.1, 
#          dob_month.1, dob_day.1, gender_me.1, source.1, id_mcaid.1, id_kc_pha.1,
#          id2, ssn.2, name_srnm.2, name_gvn.2, name_mdl.2, dob_year.2, 
#          dob_month.2, dob_day.2, gender_me.2, source.2, id_mcaid.2, id_kc_pha.2,
#          Weight) %>%
#   tail()


# Make truncated data frame
pairs_02_trunc <- pairs_02 %>%
  # Avoid matching all the PHA IDs again but allow Medicaid rows to match
  filter(!(source.1 == "pha" & source.2 == "pha")) %>%
  # Don't include mismatching id_mcaids (spot checking a few indicated most (but not all) are different people)
  filter(!(source.1 == "mcaid" & source.2 == "mcaid" & id_mcaid.1 != id_mcaid.2)) %>%
  filter(
    # Matching SSN all have high weights and look good
    ssn.1 == ssn.2 |
    # id_mcaid matches are also good to include
    id_mcaid.1 == id_mcaid.2 |
      # SECTION WHERE SSNs DO NOT MATCH
      (Weight >= 0.88 & ssn.1 != ssn.2 & !(dob_month.1 == "1" & dob_day.1 == "1")) |
      (Weight >= 0.90 & ssn.1 != ssn.2 & dob_month.1 == "1" & dob_day.1 == "1") |
      # SECTION WHERE AN SSN IS MISSING
      (Weight >= 0.76 & (is.na(ssn.1) | is.na(ssn.2)) & !(dob_month.1 == "1" & dob_day.1 == "1")) |
      (Weight >= 0.82 & (is.na(ssn.1) | is.na(ssn.2)) & dob_month.1 == "1" & dob_day.1 == "1")
  )


## Collapse IDs ----
match_02_dedup <- match_process(pairs_input = pairs_02_trunc, df = input, iteration = 2) %>%
  mutate(clusterid_2 = clusterid_2 + max(match_01_dedup$clusterid_1))

## Error check ----
match_02_chk <- setDT(match_02_dedup %>% distinct(id_hash, clusterid_2))
match_02_chk[, cnt := .N, by = "id_hash"]

if (max(match_02_chk$cnt) > 1) {
  stop("Some id_hash values are associated with multiple clusterid_2 values. ",
       "Check what went wrong.")
}

rm(match_02_chk)


# THIRD PASS: BLOCK ON ID_MCAID ----
# This only applies to Medicaid sources but will capture variations within an ID
## Run deduplication ----
st <- Sys.time()
match_03 <- RecordLinkage::compare.dedup(
  input, 
  blockfld = c("id_mcaid"), 
  strcmp = c("ssn", "name_srnm", "name_gvn", "name_mdl", "gender_me", "dob_year", "dob_month", "dob_day"), 
  exclude = c(#"id_mcare", 
              "id_kc_pha", "name_srnm_phon", "name_gvn_phon", "rowid", "id_hash", "source"))
message("Pairwise comparisons complete. Total run time: ", round(Sys.time() - st, 2), " ", units(Sys.time()-st))

summary(match_03)

## Add weights and extract pairs ----
# Using EpiLink approach
match_03 <- epiWeights(match_03)
classify_03 <- epiClassify(match_03, threshold.upper = 0.6)
summary(classify_03)
pairs_03 <- getPairs(classify_03, single.rows = TRUE) %>%
  mutate(across(contains("dob_"), ~ str_squish(.)),
         across(contains("dob_"), ~ as.numeric(.)))

## Review output and select cutoff point(s) ----
# pairs_03 %>% 
#   filter(source.1 == source.2) %>%
#   filter(Weight >= 0.6) %>% 
#   filter(ssn.1 == ssn.2) %>%
#   # filter(!is.na(ssn.1) & !is.na(ssn.2)) %>%
#   # filter(is.na(ssn.1) | is.na(ssn.2)) %>%
#   # filter(!(dob_month.1 == "1" & dob_day.1 == "1")) %>%
#   # filter(dob_month.1 == "1" & dob_day.1 == "1") %>%
#   # filter(id_mcaid.1 != id_mcaid.2) %>%
#   filter(name_gvn.1 != name_gvn.2) %>%
#   select(id1, ssn.1, name_srnm.1, name_gvn.1, name_mdl.1, dob_year.1, 
#          dob_month.1, dob_day.1, gender_me.1, source.1, id_mcaid.1, id_kc_pha.1,
#          id2, ssn.2, name_srnm.2, name_gvn.2, name_mdl.2, dob_year.2, 
#          dob_month.2, dob_day.2, gender_me.2, source.2, id_mcaid.2, id_kc_pha.2,
#          Weight) %>%
#   tail()


# Make truncated data frame
pairs_03_trunc <- pairs_03 %>%
  filter(
    # Keep any matching id_mcaid (i.e. all pairs)
    id_mcaid.1 == id_mcaid.2
  )


## Collapse IDs ----
match_03_dedup <- match_process(pairs_input = pairs_03_trunc, df = input, iteration = 3) %>%
  mutate(clusterid_3 = clusterid_3 + max(match_02_dedup$clusterid_2))

## Error check ----
match_03_chk <- setDT(match_03_dedup %>% distinct(id_hash, clusterid_3))
match_03_chk[, cnt := .N, by = "id_hash"]

if (max(match_03_chk$cnt) > 1) {
  stop("Some id_hash values are associated with multiple clusterid_3 values. ",
       "Check what went wrong.")
}

rm(match_03_chk)


# FOURTH PASS: BLOCK ON ID_KC_PHA AND PHONETIC NAME ----
# This only applies to PHA sources but will capture variations within an ID
## Run deduplication ----
st <- Sys.time()
match_04 <- RecordLinkage::compare.dedup(
  input, 
  blockfld = c("id_kc_pha"), 
  strcmp = c("ssn", "name_srnm", "name_gvn", "name_mdl", "gender_me", "dob_year", "dob_month", "dob_day"), 
  exclude = c("id_mcaid", #"id_mcare", 
              "rowid", "id_hash", "source", "name_srnm_phon", "name_gvn_phon"))
message("Pairwise comparisons complete. Total run time: ", round(Sys.time() - st, 2), " ", units(Sys.time()-st))

summary(match_04)

## Add weights and extract pairs ----
# Using EpiLink approach
match_04 <- epiWeights(match_04)
classify_04 <- epiClassify(match_04, threshold.upper = 0.6)
summary(classify_04)
pairs_04 <- getPairs(classify_04, single.rows = TRUE) %>%
  mutate(across(contains("dob_"), ~ str_squish(.)),
         across(contains("dob_"), ~ as.numeric(.)))

## Review output and select cutoff point(s) ----
# pairs_04 %>% 
#   filter(source.1 == source.2) %>%
#   filter(Weight >= 0.7) %>% 
#   filter(ssn.1 == ssn.2) %>%
#   # filter(!is.na(ssn.1) & !is.na(ssn.2)) %>%
#   # filter(is.na(ssn.1) | is.na(ssn.2)) %>%
#   # filter(!(dob_month.1 == "1" & dob_day.1 == "1")) %>%
#   # filter(dob_month.1 == "1" & dob_day.1 == "1") %>%
#   # filter(id_mcaid.1 != id_mcaid.2) %>%
#   # filter(name_gvn.1 == name_gvn.2) %>%
#   select(id1, ssn.1, name_srnm.1, name_gvn.1, name_mdl.1, dob_year.1, 
#          dob_month.1, dob_day.1, gender_me.1, source.1, id_mcaid.1, id_kc_pha.1,
#          id2, ssn.2, name_srnm.2, name_gvn.2, name_mdl.2, dob_year.2, 
#          dob_month.2, dob_day.2, gender_me.2, source.2, id_mcaid.2, id_kc_pha.2,
#          Weight) %>%
#   tail()


# Make truncated data frame
pairs_04_trunc <- pairs_04 %>%
  filter(
    # Keep any matching id_mcaid (i.e. all pairs)
    id_kc_pha.1 == id_kc_pha.2
  )

## Collapse IDs ----
match_04_dedup <- match_process(pairs_input = pairs_04_trunc, df = input, iteration = 4) %>%
  mutate(clusterid_4 = clusterid_4 + max(match_03_dedup$clusterid_3))

## Error check ----
match_04_chk <- setDT(match_04_dedup %>% distinct(id_hash, clusterid_4))
match_04_chk[, cnt := .N, by = "id_hash"]

if (max(match_04_chk$cnt) > 1) {
  stop("Some id_hash values are associated with multiple clusterid_4 values. ",
       "Check what went wrong.")
}

rm(match_04_chk)


# BRING MATCHING ROUNDS TOGETHER ----
# Use clusterid_1 as the starting point, find where one clusterid_2 value
# is associated with multiple clusterid_1 values, then take the min of the latter.
# Repeat using clusterid_2 until there is a 1:1 match between clusterid_1 and _2
# For now, need to do this iterative for passes 3 and 4. Eventually come up with a better
# scaleable solution

final_dedup <- function(df1, df2, id1, id2) {
  ## Make joint data
  ids_join <- setDT(full_join(select(df1, id_hash, id1) %>%
                                rename(clusterid_1 = id1), 
                               select(df2, id_hash, id2) %>%
                                rename(clusterid_2 = id2),
                               by = "id_hash"))
  
  # Count how much consolidation is required
  ids_join[, clusterid_1_cnt := uniqueN(clusterid_1), by = "clusterid_2"]
  ids_join[, clusterid_2_cnt := uniqueN(clusterid_2), by = "clusterid_1"]
  remaining_dupes <- ids_join %>% 
    count(clusterid_1_cnt, clusterid_2_cnt) %>%
    filter(clusterid_1_cnt != clusterid_2_cnt) %>%
    summarise(dups = sum(n))
  remaining_dupes <- remaining_dupes$dups[1]
  
  if (remaining_dupes > 0) {
    # Keep deduplicating until there are no more open triangles
    recursion_level <- 0
    recursion_dt <- copy(ids_join)
    setnames(recursion_dt, old = c("clusterid_1", "clusterid_2"), new = c("id_1_recur0", "id_2_recur0"))
    
    while (remaining_dupes > 0) {
      recursion_level <- recursion_level + 1
      message(remaining_dupes, " remaining duplicated rows. Starting recursion iteration ", recursion_level)
      
      recursion_dt <- final_dedup_collapse(iterate_dt = recursion_dt, iteration = recursion_level)
      
      # Check how many duplicates remain
      remaining_dupes <- recursion_dt %>% count(clusterid_1_cnt, clusterid_2_cnt) %>%
        filter(clusterid_1_cnt != clusterid_2_cnt) %>%
        summarise(dups = sum(n))
      remaining_dupes <- remaining_dupes$dups[1]
    }
    
    # Get final ID to use
    ids_join <- recursion_dt[, cluster_final := get(paste0("id_1_recur", recursion_level))]
  } else {
    message("No duplicates to resolve")
    ids_join[, cluster_final := clusterid_1]
  }
  
  # Keep only relevant columns
  ids_join <- ids_join[, .(id_hash, cluster_final)]
  
  ids_join
}

final_dedup_collapse <- function(iterate_dt, iteration) {
  # Set up all the new variables needed
  # Try and make this dynamic later
  id_1_old <- paste0("id_1_recur", iteration-1)
  id_1_min <- paste0("id_1_recur", iteration, "_min")
  id_1_new <- paste0("id_1_recur", iteration)
  id_1_cnt <- paste0("id_1_recur", iteration, "_cnt")
  
  id_2_old <- paste0("id_2_recur", iteration-1)
  id_2_min <- paste0("id_2_recur", iteration, "_min")
  id_2_new <- paste0("id_2_recur", iteration)
  id_2_cnt <- paste0("id_2_recur", iteration, "_cnt")
  
  # Any rows with blank IDs those that didn't match at all. Just bring over non-NA
  iterate_dt[is.na(iterate_dt[[id_1_old]]), (id_1_old) := get(id_2_old)]
  iterate_dt[is.na(iterate_dt[[id_2_old]]), (id_2_old) := get(id_1_old)]
  # First find the existing min value for IDs 1 and 2
  iterate_dt[, (id_1_min) := min(get(id_1_old), na.rm = T), by = id_2_old]
  iterate_dt[, (id_2_min) := min(get(id_2_old), na.rm = T), by = id_1_old]
  # Then set the new ID 1 and 2 based on the min
  iterate_dt[, (id_1_new) := min(get(id_1_min), na.rm = T), by = id_2_min]
  iterate_dt[, (id_2_new) := min(get(id_2_min), na.rm = T), by = id_1_min]
  # Set up a count of remaining duplicates
  iterate_dt[, clusterid_1_cnt := uniqueN(get(id_1_new)), by = id_2_new]
  iterate_dt[, clusterid_2_cnt := uniqueN(get(id_2_new)), by = id_1_new]
  print(head(iterate_dt))
  iterate_dt
}


## Make joint data with pass 1 plus other passes ----
combine_1_2 <- final_dedup(df1 = match_01_dedup, id1 = "clusterid_1",
                           df2 = match_02_dedup, id2 = "clusterid_2")

combine_1_3 <- final_dedup(df1 = match_01_dedup, id1 = "clusterid_1",
                           df2 = match_03_dedup, id2 = "clusterid_3")

combine_1_4 <- final_dedup(df1 = match_01_dedup, id1 = "clusterid_1",
                           df2 = match_04_dedup, id2 = "clusterid_4")

## Make joint data with pass 2 plus other passes ----
combine_2_3 <- final_dedup(df1 = match_02_dedup, id1 = "clusterid_2",
                           df2 = match_03_dedup, id2 = "clusterid_3")

combine_2_4 <- final_dedup(df1 = match_02_dedup, id1 = "clusterid_2",
                           df2 = match_04_dedup, id2 = "clusterid_4")

## Make joint data with pass 2 plus other passes ----
combine_3_4 <- final_dedup(df1 = match_03_dedup, id1 = "clusterid_3",
                           df2 = match_04_dedup, id2 = "clusterid_4")


## Now bring the combinations together ----
combine_1_2_3 <- final_dedup(df1 = combine_1_2, id1 = "cluster_final",
                             df2 = combine_1_3, id2 = "cluster_final")

combine_1_2_4 <- final_dedup(df1 = combine_1_2, id1 = "cluster_final",
                             df2 = combine_1_4, id2 = "cluster_final")

combine_all_1 <- final_dedup(df1 = combine_1_2_3, id1 = "cluster_final",
                             df2 = combine_1_2_4, id2 = "cluster_final")

combine_all_2 <- final_dedup(df1 = combine_2_3, id1 = "cluster_final",
                             df2 = combine_2_4, id2 = "cluster_final")

combine_all_1_2 <- final_dedup(df1 = combine_all_1, id1 = "cluster_final",
                               df2 = combine_all_2, id2 = "cluster_final")

combine_all <- final_dedup(df1 = combine_all_1_2, id1 = "cluster_final",
                           df2 = combine_3_4, id2 = "cluster_final")


## Error check ----
combine_all_chk <- unique(combine_all[, c("id_hash", "cluster_final")])
combine_all_chk[, cnt_id := .N, by = "id_hash"]
combine_all_chk[, cnt_hash := .N, by = "cluster_final"]
# cnt_id should = 1 and cnt_hash should be >= 1
combine_all_chk %>% count(cnt_id, cnt_hash)
if (max(combine_all_chk$cnt_id) > 1) {
  stop("There is more than one cluster ID for a given id_hash. Investigate why.")
}


## Now make an alpha-numeric ID that will be stored in a table ----
# NB. This will need to be reworked when there is an existing table with APDE IDs
#  Likely make twice as many IDs as needed then weed out the ones already in
#    the master list, before trimming to the actual number needed.

ids_final <- id_nodups(id_n = n_distinct(combine_all$cluster_final),
                       id_length = 10)
ids_final <- combine_all %>%
  distinct(cluster_final) %>%
  arrange(cluster_final) %>%
  bind_cols(., id_apde = ids_final)

names_final <- input %>%
  select(ssn, id_mcaid, 
         #id_mcare, 
         id_kc_pha, name_srnm, name_gvn, name_mdl, 
         dob_year, dob_month, dob_day, gender_me, id_hash) %>%
  left_join(., select(combine_all, id_hash, cluster_final), by = "id_hash") %>%
  left_join(., ids_final, by = "cluster_final") %>%
  # select(id_apde, id_mcaid, 
  #        #id_mcare, 
  #        id_kc_pha, id_hash) %>%
  distinct() %>%
  mutate(last_run = Sys.time())



# QA FINAL DATA ----
### REVIEW POINT ----
# Number of id_hashes compared to the number of id_apdes
message("There are ", n_distinct(names_final$id_hash), " IDs and ", 
        n_distinct(names_final$id_apde), " id_apde IDs")

db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

mcaid_elig_demo <- setDT(odbc::dbGetQuery(db_hhsaw, "SELECT DISTINCT id_mcaid FROM claims.final_mcaid_elig_demo")) # go back to elig_demo because it has an ALMOST complete list of all Mcaid ID

# mcare.only <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT id_mcare FROM final.mcare_elig_demo")) # go back to elig_demo because it has an ALMOST complete list of all Mcare ID


## Confirm that every ID is accounted for ----
# Check Mcaid
extra_mcaid <- setdiff(names_final[!is.na(id_mcaid)]$id_mcaid, mcaid_elig_demo[!is.na(id_mcaid)]$id_mcaid)
missing_mcaid <- setdiff(mcaid_elig_demo[!is.na(id_mcaid)]$id_mcaid, names_final[!is.na(id_mcaid)]$id_mcaid)
length(extra_mcaid) # Might be > zero if there are id_mcaid in stage.mcaid_elig that were not in the elig_demo
length(missing_mcaid) # should be zero

# # Check Mcare
# extra.mcare <- setdiff(names_final[!is.na(id_mcare)]$id_mcare, mcare.only[!is.na(id_mcare)]$id_mcare) 
# missing.mcare <- setdiff(mcare.only[!is.na(id_mcare)]$id_mcare, names_final[!is.na(id_mcare)]$id_mcare) 
# length(extra.mcare) # Expect there will be extra b/c there are Mcare ids in SSN and Names files that are not in MBSF
# length(missing.mcare) # should be zero in length

## Confirm that there are no duplicates in the final names_final linkage ----      
# Mcaid - id_apde per id_mcaid
id_dup_chk <- copy(names_final)
id_dup_chk[!is.na(id_mcaid), dup_mcaid := uniqueN(id_apde, na.rm = T), by = "id_mcaid"]
id_dup_chk %>% count(dup_mcaid)
if (nrow(id_dup_chk[!is.na(id_mcaid) & dup_mcaid > 1]) == 0) {
  message("There were no Medicaid IDs allocated to >1 APDE IDs (as expected)")
} else {
  stop("Some Medicaid IDs have been assigned to multiple APDE IDs. Investigate why.")
}

# Mcaid - id_mcaid per id_apde
id_dup_chk[!is.na(id_mcaid), dup_apde_mcaid := uniqueN(id_mcaid, na.rm = T), by = "id_apde"]
id_dup_chk %>% count(dup_apde_mcaid)
id_dup_chk[dup_apde_mcaid > 1]

# PHA
id_dup_chk[!is.na(id_kc_pha), dup_pha := uniqueN(id_apde, na.rm = T), by = "id_kc_pha"]
id_dup_chk %>% count(dup_pha)
if (nrow(id_dup_chk[!is.na(id_kc_pha) & dup_pha > 1]) == 0) {
  message("There were no PHA IDs allocated to >1 APDE IDs (as expected)")
} else {
  stop("Some PHA IDs have been assigned to multiple APDE IDs. Investigate why.")
}

# PHA - id_kc_pha per id_apde
id_dup_chk[!is.na(id_kc_pha), dup_apde_pha := uniqueN(id_kc_pha, na.rm = T), by = "id_apde"]
id_dup_chk %>% count(dup_apde_pha)
id_dup_chk[dup_apde_pha > 1]


## Check there aren't a crazy high number of rows per ID ----
id_dup_chk[, id_cnt := .N, by = "id_apde"]
id_dup_chk %>% count(id_cnt)


## Check that some mcaid and PHA IDs matched ----
id_dup_chk[, id_mcaid_present := ifelse(!is.na(dup_apde_mcaid), 1L, 0L)]
id_dup_chk[, id_pha_present := ifelse(!is.na(dup_apde_pha), 1L, 0L)]
id_dup_chk[, id_mcaid_pha := max(id_mcaid_present) + max(id_pha_present), by = "id_apde"]
id_dup_chk %>% distinct(id_apde, id_mcaid_pha) %>% count(id_mcaid_pha)


# LOAD TO SQL ----
## identify the column types to be created in SQL ----
sql_columns <- c("id_apde" = "char(10)", 
                 #"id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", 
                 "id_mcaid" = "char(11)", 
                 "id_kc_pha" = "char(10)", 
                 "id_hash" = "char(64)",
                 "last_run" = "datetime")  

# ensure column order in R is the same as that in SQL
setcolorder(names_final, names(sql_columns))
names_final <- names_final[, names(sql_columns), with = F]


## Write table to SQL ----
# Split into smaller tables to avoid SQL connection issues
start <- 1L
max_rows <- 100000L
cycles <- ceiling(nrow(names_final)/max_rows)

lapply(seq(start, cycles), function(i) {
  start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
  end_row <- min(nrow(names_final), max_rows * i)
  
  message("Loading cycle ", i, " of ", cycles)
  if (i == 1) {
    dbWriteTable(db_hhsaw,
                 DBI::Id(schema = "claims", table = "stage_xwalk_apde_ids"),
                 value = as.data.frame(names_final[start_row:end_row]),
                 overwrite = T, append = F,
                 field.types = sql_columns)
  } else {
    dbWriteTable(db_hhsaw,
                 DBI::Id(schema = "claims", table = "stage_xwalk_apde_ids"),
                 value = as.data.frame(names_final[start_row:end_row]),
                 overwrite = F, append = T)
  }
})


## Confirm that all rows were loaded to sql ----
stage.count <- as.numeric(odbc::dbGetQuery(db_hhsaw, 
                                           "SELECT COUNT (*) FROM claims.stage_xwalk_apde_ids"))
if(stage.count != nrow(names_final))
  stop("Mismatching row count, error writing or reading data")      

# close database connections    
dbDisconnect(db_claims)  
dbDisconnect(db_apde51)  


# CLEAN UP ----
## Remove data
rm(list = ls(pattern = "match"))
rm(list = ls(pattern = "pairs"))
rm(list = ls(pattern = "classify"))
rm(list = ls(pattern = "ids_"))
rm(list = ls(pattern = "combine_"))
rm(mcaid, mcare.dt, pha)
rm(input)


## The end! ----      
run.time <- Sys.time() - start.time  
print(run.time)

Sys.time() - start.time
