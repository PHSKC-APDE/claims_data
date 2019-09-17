# Header ####
  # Author: Danny Colombara
  # Date: August 28, 2019
  # Purpose: Create stage.mcare_elig_demo for SQL

## Set up R Environment ----
  rm(list=ls())  # clear memory
  pacman::p_load(data.table, odbc, DBI, tidyr, lubridate) # load packages
  options("scipen"=999) # turn off scientific notation  
  options(warning.length = 8170) # get lengthy warnings, needed for SQL
  
  start.time <- Sys.time()
  
  yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage_mcare_elig_demo.yaml"
  
## (1) Connect to SQL Server ----    
  db_claims <- dbConnect(odbc(), "PHClaims51")   
  
## (2) Load data from SQL ----  
  mbsf <- setDT(odbc::dbGetQuery(db_claims, "SELECT bene_id, bene_birth_dt, bene_death_dt, sex_ident_cd, bene_enrollmt_ref_yr, rti_race_cd,zip_cd FROM PHClaims.stage.mcare_mbsf"))
  setnames(mbsf, names(mbsf), c("id_mcare", "dob", "death_dt", "sex", "year", "race", "zip_code"))  
  
## (3) Create date of birth indicator as a data.table ----
  # Want to keep the most recent date b/c assume it is a correction in Medicare registration ... discussed with Eli/Alastair
  dob <- unique(mbsf[, c("id_mcare", "dob", "year")])
  setkey(dob, id_mcare, year) # sorts by these columns and indicates these are key columns
  dob[!is.na(dob), maxyear := max(year), by=id_mcare] # identify the max year for each ID's date of birth
  dob <- dob[year==maxyear,] # keep data for the most recent (max) year
  dob <- dob[, c("maxyear", "year"):=NULL] # delete columns no longer of use   
  if(nrow(dob) - length(unique(dob$id_mcare)) != 0){
    stop('non-unique id_mcare in dob')
  } # confirm all id_mcare are unique
  
## (4) Create King County ever indicator (moment by moment KC status will be in timevar table) ----
  kc.zips <- fread("https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/zip_admin.csv") #github file with KC zipcodes
  kc <- copy(mbsf[, c("id_mcare", "zip_code", "year")])
  kc[, geo_kc := as.numeric(as.integer(zip_code) %in% kc.zips$zip)] #creating the new var kc (a 0/1 var) 
  kc <- unique(kc[, c("id_mcare", "geo_kc", "year")])
  kc <- kc[, lapply(.SD, sum, na.rm = TRUE), by = id_mcare, .SDcols = c("geo_kc")] # sum/collapse/aggregate the kc variables by ID
  kc[geo_kc>1, geo_kc := 1]# geo_kc should be either 0 | 1, so if >1, replace with 1
  if(nrow(kc) - length(unique(kc$id_mcare)) != 0){
    stop('non-unique id_mcare in kc')
  }  # confirm all id_mcare are unique
  rm(kc.zips)

## (5) Create sex indicator as a data.table ----
  sex <- unique(mbsf[, .(id_mcare, sex, year)])
  # identify the most recent gender for gender_recent
      setorder(sex, id_mcare, -year) # sort so most recent year is first for each id
      sex.recent <- copy(sex)
      sex.recent[, ordering := 1:.N, by = id_mcare] # identify the most recent row for each id  
      sex.recent <- sex.recent[ordering == 1, ]
      sex.recent[sex==0, gender_recent := "Unknown"][sex==1, gender_recent := "Male"][sex==2, gender_recent := "Female"]
      sex.recent[, c("sex", "year", "ordering") := NULL]
  # back to processing all the sex data
      sex[, gender_female:=0][sex==2, gender_female := 1] # identify females
      sex[, gender_male:=0][sex==1, gender_male := 1] # identify males
      sex <- sex[, lapply(.SD, sum, na.rm = TRUE), by = id_mcare, .SDcols = c("gender_female", "gender_male")] # sum/collapse/aggregate the sex variables by ID
      sex[gender_female>1, gender_female := 1][gender_male>1, gender_male := 1] # female and male should be either 0 | 1, so if >1, replace with 1.
      sex[gender_female==1 & gender_male == 0, gender_me := "Female"][gender_female==0 & gender_male == 1, gender_me := "Male"]
      sex[gender_female==0 & gender_male == 0, gender_me := "Unknown"][gender_female==1 & gender_male == 1, gender_me := "Multiple"]
      sex[, gender_female_t := NA_integer_] # in mcaid, this is the percent of time data show female. However, non-sensical for mcare
      sex[, gender_male_t := NA_integer_] # in mcaid, this is the percent of time data show male. We decided not to do this for Medicare.
  # merge on gender_recent
      sex <- merge(sex, sex.recent, by = "id_mcare", all = TRUE)
      rm(sex.recent)
  # confirm all id_mcare are unique
      if(nrow(sex) - length(unique(sex$id_mcare)) != 0){
        stop('non-unique id_mcare in sex')
      } 

## (6) Create race indicator as a data.table ----
  #For race, use RTI_RACE_CD rather than BENE_RACE_CD because better allocation of Hipanic and Asian
  # Prep race 
      race <- unique(mbsf[, c("id_mcare", "race", "year")])
      race <- race[!is.na(id_mcare)]
      race[race==0, race_unk := 1] 
      race[race==1, race_white := 1] 
      race[race==2, race_black := 1] 
      race[race==3, race_other := 1] 
      race[, race_asian:=0] # RTI method cannot be used to identify Asian
      race[race==4, race_asian_pi := 1] # RTI method groups asian and pacific islander, so there is no way to split them in our data 
      race[race==5, race_latino := 1] 
      race[race==6, race_aian := 1] 
      race[, race_nhpi:=0] # RTI method cannot be used to identify NHPI
  # identify the most recent race for race_recent
      setorder(race, id_mcare, -year) # sort so most recent year is first for each id
      race.recent <- copy(race)
      race.recent[, ordering := 1:.N, by = id_mcare] # identify the most recent row for each id  
      race.recent <- race.recent[ordering == 1, ]
      race.recent[, race_eth_recent := as.character(factor(race, levels = c(0:6), labels = c("Unknown", "White", "Black", "Other", "Asian_PI", "Latino", "AIAN")))]
      race.recent <- race.recent[, .(id_mcare, race_eth_recent)]
      race.recent[, race_recent := NA_character_] # for medicare cannot separate Latino from race
  # back to processing all the race data
    # sum/collapse/aggregate the race variables by ID
      race.vars <- paste0("race_", c("white", "black", "other", "asian", "asian_pi", "aian", "nhpi", "latino", "unk"))
      race <- race[, lapply(.SD, sum, na.rm = TRUE), by = id_mcare, .SDcols = race.vars] 
    # if collapsed data sums >1, replace with 1
      race[race_white>1, race_white := 1][race_black>1, race_black := 1][race_other>1, race_other := 1]
      race[race_asian>1, race_asian := 1][race_asian_pi>1, race_asian_pi := 1]
      race[race_aian>1, race_aian := 1][race_nhpi>1, race_nhpi := 1][race_latino>1, race_latino := 1][race_unk>1, race_unk := 1]  
    # create race_ethnicity var
      race[race_unk==1, race_eth_me := "Unknown"]
      race[race_white==1, race_eth_me := "White"]
      race[race_black==1, race_eth_me := "Black"]
      race[race_other==1, race_eth_me := "Other"]
      race[race_asian_pi==1, race_eth_me := "Asian_PI"]
      race[race_latino==1, race_eth_me := "Latino"]
      race[race_aian==1, race_eth_me := "AIAN"]
      race[(race_white + race_black + race_other + race_asian_pi + race_latino + race_aian + race_unk) > 1, race_eth_me := "Multiple"]
    # create race_me
      race[, race_me := NA_character_] # this cannot be calculated b/c we only use RTI race coding, which includes Hispanic as a race  
  # merge on race_recent
      race <- merge(race, race.recent, by = "id_mcare", all = TRUE)
      rm(race.recent)      

  # create indicators for percent time as race (just to copy mcaid format)
    race[, c("race_white_t", "race_black_t", "race_asian_t", "race_aian_t", "race_asian_pi_t", "race_nhpi_t", "race_other_t", "race_latino_t", "race_unk_t") := NA_integer_] # Unlike Medicaid, we decided not to do this for Medicare
  # confirm all id_mcare are unique
      if(nrow(race) - length(unique(race$id_mcare)) != 0){
        stop('non-unique id_mcare in race')
      } 

## (7) Create date of death as a data.table ----
  death <- unique(mbsf[!is.na(death_dt) & death_dt != "1900-01-01", .(id_mcare, death_dt, year)]) # copy only death data
  setorder(death, id_mcare, -year) # order by year so that when drop duplicate id_mcare below, will keep the most recent death date, which will be assumed to be a correction
  death[, dup := 1:.N, by = "id_mcare"] # identify duplicates
  death <- death[dup == 1, ] # drop duplicates
  death <- death[, .(id_mcare, death_dt)]
  death[, death_dt := as.Date(death_dt)]
  if(nrow(death) != length(unique(death$id_mcare))){stop("Repeated id_mcare in death data ... FIX before moving on!")}
  
## (8) Merge all data.tables ----
  # tidy mbsf
  elig <- unique(mbsf[, "id_mcare"])
  # add on dob
  elig <- merge(elig, dob, by = "id_mcare", all = TRUE)
  # add on King County ever indicator
  elig <- merge(elig, kc, by = "id_mcare", all = TRUE)  
  # add on gender
  elig <- merge(elig, sex, by = "id_mcare", all = TRUE)
  # add on race
  elig <- merge(elig, race, by = "id_mcare", all = TRUE)
  # add on death 
  elig <- merge(elig, death, by = "id_mcare", all = TRUE)
  
## (9) Add time stamp ----
  elig[, last_run := Sys.time()]  

## (10) Write to SQL ----              
  # set elig columns to proper type 
    elig[, dob := as.Date(dob)]

  # Pull YAML from GitHub
    table_config <- yaml::yaml.load(RCurl::getURL(yaml.url))
  
  # Create table ID
    tbl_id <- DBI::Id(schema = table_config$schema, 
                      table = table_config$table)  
  
  # Ensure columns are in same order in R & SQL
    setcolorder(elig, names(table_config$vars))
  
  # Write table to SQL
    dbWriteTable(db_claims, 
                 tbl_id, 
                 value = as.data.frame(elig),
                 overwrite = T, append = F, 
                 field.types = unlist(table_config$vars))

## (11) Simple QA ----
    # Confirm that all rows were loaded to SQL ----
      stage.count <- as.numeric(odbc::dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.mcare_elig_demo"))
      if(stage.count != nrow(elig))
        stop("Mismatching row count, error reading in data")    
    
    # check that rows in stage are not less than the last time that it was created ----
      last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcare_elig_demo")[[1]])
    
      # count number of rows
      previous_rows <- as.numeric(
        odbc::dbGetQuery(db_claims, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcare_elig_demo' AND
                         qa_item = 'row_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcare_elig_demo' AND
                         qa_item = 'row_count') b
                         ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous_rows)){previous_rows = 0}
      
      row_diff <- stage.count - previous_rows
      
      if (row_diff < 0) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcare_elig_demo',
                         'Number new rows compared to most recent run', 
                         'FAIL', 
                         {Sys.time()}, 
                         'There were {row_diff} fewer rows in the most recent table 
                         ({stage.count} vs. {previous_rows})')",
                         .con = db_claims))
        
        problem.row_diff <- glue::glue("Fewer rows than found last time.  
                                       Check metadata.qa_mcare for details (last_run = {last_run})
                                       \n")
      } else {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcare_elig_demo',
                         'Number new rows compared to most recent run', 
                         'PASS', 
                         {Sys.time()}, 
                         'There were {row_diff} more rows in the most recent table 
                         ({stage.count} vs. {previous_rows})')",
                         .con = db_claims))
        
        problem.row_diff <- glue::glue(" ") # no problem, so empty error message
        
      }
    
    # check that there are no duplicates ----
      # get count of unique id (each id should only appear once)
      stage.count.unique <- as.numeric(odbc::dbGetQuery(
        db_claims, "SELECT COUNT (*) 
        FROM (Select id_mcare 
        FROM stage.mcare_elig_demo
        GROUP BY id_mcare
        )t;"
            ))
      
      if (stage.count.unique != stage.count) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES (
                         {last_run}, 
                         'stage.mcare_elig_demo',
                         'Number distinct IDs', 
                         'FAIL', 
                         {Sys.time()}, 
                         'There were {stage.count.unique} distinct IDs but {stage.count} rows overall (should be the same)'
                         )
                         ",
                         .con = db_claims))
        
        problem.ids  <- glue::glue("Number of distinct IDs doesn't match the number of rows. 
                                   Check metadata.qa_mcare for details (last_run = {last_run})
                                   \n")
      } else {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcare_elig_demo',
                         'Number distinct IDs', 
                         'PASS', 
                         {Sys.time()}, 
                         'The number of distinct IDs matched number of overall rows ({stage.count.unique})')",
                         .con = db_claims))
        
        problem.ids  <- glue::glue(" ") # no problem
      }
    
    # create summary of errors ---- 
      problems <- glue::glue(
        problem.ids, "\n",
        problem.row_diff)
    
    
    
    
## (12) Fill qa_mcare_values table ----
    qa.values <- glue::glue_sql("INSERT INTO metadata.qa_mcare_values
                                (table_name, qa_item, qa_value, qa_date, note) 
                                VALUES ('stage.mcare_elig_demo',
                                'row_count', 
                                {stage.count}, 
                                {Sys.time()}, 
                                '')",
                                .con = db_claims)
    
    odbc::dbGetQuery(conn = db_claims, qa.values)


## (13) Print error messages ----
    if(problems >1){
      message(glue::glue("WARNING ... MCARE_ELIG_DEMO FAILED AT LEAST ONE QA TEST", "\n",
                         "Summary of problems in MCARE_ELIG_DEMO: ", "\n", 
                         problems))
    }else{message("Staged MCARE_ELIG_DEMO passed all QA tests")}

## The end! ----
    run.time <- Sys.time() - start.time
    print(run.time)
    