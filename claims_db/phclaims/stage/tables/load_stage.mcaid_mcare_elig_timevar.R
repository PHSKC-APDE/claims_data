# Header ####
  # Author: Danny Colombara
  # Date: September 16, 2019
  # Purpose: Create stage.mcaid_mcare_elig_timevar for SQL
  #
  # Notes: BEFORE RUNNING THIS CODE, PLEASE BE SURE THE FOLLOWING ARE UP TO DATE ... 
  #       - [PHClaims].[stage].[mcaid_elig_timevar]
  #       - [PHClaims].[stage].[mcare_elig_timevar]
  #       - [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha]

## Set up R Environment ----
  rm(list=ls())  # clear memory
  start.time <- Sys.time()
  pacman::p_load(data.table, dplyr, odbc, DBI, lubridate) # load packages
  options("scipen"=999) # turn off scientific notation  
  options(warning.length = 8170) # get lengthy warnings, needed for SQL
  
  start.time <- Sys.time()
  
  kc.zips.url <- "https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/zip_admin.csv"
  
  yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_timevar.yaml"
  
## (1) Connect to SQL Server ----    
  db_claims <- dbConnect(odbc(), "PHClaims51")   
  
## (2) Load data from SQL ----  
  apde <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_apde, id_mcare, id_mcaid
                                 FROM PHClaims.final.xwalk_apde_mcaid_mcare_pha"))
  
  mcare <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcare, from_date, to_date, part_a, part_b, part_c, partial, buy_in, geo_zip 
                                  FROM PHClaims.final.mcare_elig_timevar"))
           mcare[, from_date := as.integer(as.Date(from_date))] # convert date string to a real date
           mcare[, to_date := as.integer(as.Date(to_date))] # convert date to an integer (temporarily for finding intersections)
           
  mcaid <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcaid, from_date, to_date, tpl, bsp_group_cid, full_benefit, cov_type, mco_id, 
                                  geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip, geo_zip_centroid, 
                                  geo_street_centroid, geo_county_code, geo_tractce10, geo_hra_code, geo_school_code
                                  FROM PHClaims.final.mcaid_elig_timevar"))
          mcaid[, from_date := as.integer(as.Date(from_date))] # convert date string to a real date
          mcaid[, to_date := as.integer(as.Date(to_date))] # convert date to an integer (temporarily for finding intersections)
          # Ensure new geography naming conventions are followed ----
          setnames(mcaid, grep("_clean$", names(mcaid), value = T), gsub("_clean", "", grep("_clean$", names(mcaid), value = T)) )
          setnames(mcaid, "geo_tractce10", "geo_tract_code")
  
## (3) Merge on dual status ----
  mcare <- merge(apde[, .(id_apde, id_mcare)], mcare, by = "id_mcare", all.x = FALSE, all.y = TRUE)
  mcare[, id_mcare := NULL] # no longer needed now that have id_apde
  
  mcaid <- merge(apde[, .(id_apde, id_mcaid)], mcaid, by = "id_mcaid", all.x = FALSE, all.y = TRUE)
  mcaid[, id_mcaid := NULL] # no longer needed now that have id_apde
  
## (4) Identify the duals and split from non-duals for additional processing ----
  dual.id <- intersect(mcaid$id_apde, mcare$id_apde)
  
  mcaid.solo <- mcaid[!id_apde %in% dual.id]  
  mcare.solo <- mcare[!id_apde %in% dual.id]
  
  mcaid.dual <- mcaid[id_apde %in% dual.id]
  mcare.dual <- mcare[id_apde %in% dual.id]
  
  # for the duals, add _mcare suffix to differentiate common varnames from Mcaid data
    setnames(mcare.dual, "geo_zip", "geo_zip_mcare")

  # drop main original datasets that are no longer needed
    rm(mcaid, mcare)
    gc()

## (5) Duals Part 1: Create master list of time intervals by ID ----
    # Originally from ... 
    # https://github.com/PHSKC-APDE/Housing/blob/master/processing/09_pha_mcaid_join.R
    # confirmed on 11/4/2019 that the results are 100% the same as an alternate method where
    # a giant table is made with every possible day for each ID, followed by checking for the intersection
    # of the data with each individual day, followed by collapsing the data for contiguous time periods
    # The comparison method is 100% guaranteed to be accurate, but is much slower (~12 minutes vs 39 seconds)
    # so we decided to stick with Alastair Matheson's method

  #-- create all possible permutations of date interval combinations from mcare and mcaid for each id ----
    duals <- merge(mcare.dual[, .(id_apde, from_date, to_date)], mcaid.dual[, .(id_apde, from_date, to_date)], by = "id_apde", allow.cartesian = TRUE)
    
    setnames(duals, grep("\\.x$", names(duals), value = T), gsub(".x", "_mcare", grep("\\.x$", names(duals), value = T)))
    setnames(duals, grep("\\.y$", names(duals), value = T), gsub(".y", "_mcaid", grep("\\.y$", names(duals), value = T)))
    
	#-- Identify the type of overlaps & number of duplicate rows needed ----
		temp <- duals %>%
		  mutate(overlap_type = case_when(
		    # First ID the non-matches
		    is.na(from_date_mcare) | is.na(from_date_mcaid) ~ 0,
		    # Then figure out which overlapping date comes first
		    # Exactly the same dates
		    from_date_mcare == from_date_mcaid & to_date_mcare == to_date_mcaid ~ 1,
		    # mcare before mcaid (or exactly the same dates)
		    from_date_mcare <= from_date_mcaid & from_date_mcaid <= to_date_mcare & 
		      to_date_mcare <= to_date_mcaid ~ 2,
		    # mcaid before mcare
		    from_date_mcaid <= from_date_mcare & from_date_mcare <= to_date_mcaid & 
		      to_date_mcaid <= to_date_mcare ~ 3,
		    # mcaid dates competely within mcare dates or vice versa
		    from_date_mcaid >= from_date_mcare & to_date_mcaid <= to_date_mcare ~ 4,
		    from_date_mcare >= from_date_mcaid & to_date_mcare <= to_date_mcaid ~ 5,
		    # mcare coverage only before mcaid (or mcaid only after mcare)
		    from_date_mcare < from_date_mcaid & to_date_mcare < from_date_mcaid ~ 6,
		    # mcare coverage only after mcaid (or mcaid only before mcare)
		    from_date_mcare > to_date_mcaid & to_date_mcare > to_date_mcaid ~ 7,
		    # Anyone rows that are left
		    TRUE ~ 8),
		    # Calculate overlapping dates
		    from_date_o = as.Date(case_when(
		      overlap_type %in% c(1, 2, 4) ~ from_date_mcaid,
		      overlap_type %in% c(3, 5) ~ from_date_mcare), origin = "1970-01-01"),
		    to_date_o = as.Date(ifelse(overlap_type %in% c(1:5),
		                               pmin(to_date_mcaid, to_date_mcare),
		                               NA), origin = "1970-01-01"),
		    # Need to duplicate rows to separate out non-overlapping mcare and mcaid periods
		    repnum = case_when(
		      overlap_type %in% c(2:5) ~ 3,
		      overlap_type %in% c(6:7) ~ 2,
		      TRUE ~ 1)
		  ) %>%
		  select(id_apde, from_date_mcare, to_date_mcare, from_date_mcaid, to_date_mcaid, 
		         from_date_o, to_date_o, overlap_type, repnum) %>%
		  arrange(id_apde, from_date_mcare, from_date_mcaid, from_date_o, 
		          to_date_mcare, to_date_mcaid, to_date_o)

	#-- Expand out rows to separate out overlaps ----
		temp_ext <- temp[rep(seq(nrow(temp)), temp$repnum), 1:ncol(temp)]

	#-- Process the expanded data ----
		temp_ext <- temp_ext %>% 
		  group_by(id_apde, from_date_mcare, to_date_mcare, from_date_mcaid, to_date_mcaid) %>% 
		  mutate(rownum_temp = row_number()) %>%
		  ungroup() %>%
		  arrange(id_apde, from_date_mcare, to_date_mcare, from_date_mcaid, to_date_mcaid, from_date_o, 
		          to_date_o, overlap_type, rownum_temp) %>%
		  mutate(
		    # Remove non-overlapping dates
		    from_date_mcare = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | 
		                                   (overlap_type == 7 & rownum_temp == 1), 
		                                 NA, from_date_mcare), origin = "1970-01-01"), 
		    to_date_mcare = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | 
		                                 (overlap_type == 7 & rownum_temp == 1), 
		                               NA, to_date_mcare), origin = "1970-01-01"),
		    from_date_mcaid = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | 
		                                   (overlap_type == 7 & rownum_temp == 2), 
		                                 NA, from_date_mcaid), origin = "1970-01-01"), 
		    to_date_mcaid = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | 
		                                 (overlap_type == 7 & rownum_temp == 2), 
		                               NA, to_date_mcaid), origin = "1970-01-01")) %>%
		  distinct(id_apde, from_date_mcare, to_date_mcare, from_date_mcaid, to_date_mcaid, from_date_o, 
		           to_date_o, overlap_type, rownum_temp, .keep_all = TRUE) %>%
		  # Remove first row if start dates are the same or mcare is only one day
		  filter(!(overlap_type %in% c(2:5) & rownum_temp == 1 & 
		             (from_date_mcare == from_date_mcaid | from_date_mcare == to_date_mcare))) %>%
		  # Remove third row if to_dates are the same
		  filter(!(overlap_type %in% c(2:5) & rownum_temp == 3 & to_date_mcare == to_date_mcaid))

	#-- Calculate the finalized date columms----
		temp_ext <- temp_ext %>%
		  # Set up combined dates
		  mutate(
		    # Start with rows with only mcare or mcaid, or when both sets of dates are identical
		    from_date = as.Date(
		      case_when(
		        (!is.na(from_date_mcare) & is.na(from_date_mcaid)) | overlap_type == 1 ~ from_date_mcare,
		        !is.na(from_date_mcaid) & is.na(from_date_mcare) ~ from_date_mcaid), origin = "1970-01-01"),
		    to_date = as.Date(
		      case_when(
		        (!is.na(to_date_mcare) & is.na(to_date_mcaid)) | overlap_type == 1 ~ to_date_mcare,
		        !is.na(to_date_mcaid) & is.na(to_date_mcare) ~ to_date_mcaid), origin = "1970-01-01"),
		    # Now look at overlapping rows and rows completely contained within the other data's dates
		    from_date = as.Date(
		      case_when(
		        overlap_type %in% c(2, 4) & rownum_temp == 1 ~ from_date_mcare,
		        overlap_type %in% c(3, 5) & rownum_temp == 1 ~ from_date_mcaid,
		        overlap_type %in% c(2:5) & rownum_temp == 2 ~ from_date_o,
		        overlap_type %in% c(2:5) & rownum_temp == 3 ~ to_date_o + 1,
		        TRUE ~ from_date), origin = "1970-01-01"),
		    to_date = as.Date(
		      case_when(
		        overlap_type %in% c(2:5) & rownum_temp == 1 ~ lead(from_date_o, 1) - 1,
		        overlap_type %in% c(2:5) & rownum_temp == 2 ~ to_date_o,
		        overlap_type %in% c(2, 5) & rownum_temp == 3 ~ to_date_mcaid,
		        overlap_type %in% c(3, 4) & rownum_temp == 3 ~ to_date_mcare,
		        TRUE ~ to_date), origin = "1970-01-01"),
		    # Deal with the last line for each person if it's part of an overlap
		    from_date = as.Date(ifelse((id_apde != lead(id_apde, 1) | is.na(lead(id_apde, 1))) &
		                                   overlap_type %in% c(2:5) & 
		                                   to_date_mcare != to_date_mcaid, 
		                                 lag(to_date_o, 1) + 1, 
		                                 from_date), origin = "1970-01-01"),
		    to_date = as.Date(ifelse((id_apde != lead(id_apde, 1) | is.na(lead(id_apde, 1))) &
		                                 overlap_type %in% c(2:5), 
		                               pmax(to_date_mcare, to_date_mcaid, na.rm = TRUE), 
		                               to_date), origin = "1970-01-01")
		  ) %>%
		  arrange(id_apde, from_date, to_date, from_date_mcare, from_date_mcaid, 
		          to_date_mcare, to_date_mcaid, overlap_type)

	#-- Label and clean summary interval data ----
		temp_ext <- temp_ext %>%
		 mutate(
		    # Identify which type of enrollment this row represents
		    enroll_type = 
		      case_when(
		        (overlap_type == 2 & rownum_temp == 1) | 
		          (overlap_type == 3 & rownum_temp == 3) |
		          (overlap_type == 6 & rownum_temp == 1) | 
		          (overlap_type == 7 & rownum_temp == 2) |
		          (overlap_type == 4 & rownum_temp %in% c(1, 3)) |
		          (overlap_type == 0 & is.na(from_date_mcaid)) ~ "mcare",
		        (overlap_type == 3 & rownum_temp == 1) | 
		          (overlap_type == 2 & rownum_temp == 3) |
		          (overlap_type == 6 & rownum_temp == 2) | 
		          (overlap_type == 7 & rownum_temp == 1) | 
		          (overlap_type == 5 & rownum_temp %in% c(1, 3)) |
		          (overlap_type == 0 & is.na(from_date_mcare)) ~ "mcaid",
		        overlap_type == 1 | (overlap_type %in% c(2:5) & rownum_temp == 2) ~ "both",
		        TRUE ~ "x"
		      ),
		    # Drop rows from enroll_type == h/m when they are fully covered by an enroll_type == b
		    drop = 
		      case_when(
		        id_apde == lag(id_apde, 1) & !is.na(lag(id_apde, 1)) & 
		          from_date == lag(from_date, 1) & !is.na(lag(from_date, 1)) &
		          to_date >= lag(to_date, 1) & !is.na(lag(to_date, 1)) & 
		          # Fix up quirk from mcare data where two rows present for the same day
		          !(lag(enroll_type, 1) != "mcaid" & lag(to_date_mcare, 1) == lag(from_date_mcare, 1)) &
		          enroll_type != "both" ~ 1,
		        id_apde == lead(id_apde, 1) & !is.na(lead(id_apde, 1)) & 
		          from_date == lead(from_date, 1) & !is.na(lead(from_date, 1)) &
		          to_date <= lead(to_date, 1) & !is.na(lead(to_date, 1)) & 
		          # Fix up quirk from mcare data where two rows present for the same day
		          !(lead(enroll_type, 1) != "mcaid" & lead(to_date_mcare, 1) == lead(from_date_mcare, 1)) &
		          enroll_type != "both" & lead(enroll_type, 1) == "both" ~ 1,
		        # Fix up other oddities when the date range is only one day
		        id_apde == lag(id_apde, 1) & !is.na(lag(id_apde, 1)) & 
		          from_date == lag(from_date, 1) & !is.na(lag(from_date, 1)) &
		          from_date == to_date & !is.na(from_date) & 
		          ((enroll_type == "mcaid" & lag(enroll_type, 1) %in% c("both", "mcare")) |
		             (enroll_type == "mcare" & lag(enroll_type, 1) %in% c("both", "mcaid"))) ~ 1,
		        id_apde == lag(id_apde, 1) & !is.na(lag(id_apde, 1)) & 
		          from_date == lag(from_date, 1) & !is.na(lag(from_date, 1)) &
		          from_date == to_date & !is.na(from_date) &
		          from_date_mcare == lag(from_date_mcare, 1) & to_date_mcare == lag(to_date_mcare, 1) &
		          !is.na(from_date_mcare) & !is.na(lag(from_date_mcare, 1)) &
		          enroll_type != "both" ~ 1,
		        id_apde == lead(id_apde, 1) & !is.na(lead(id_apde, 1)) & 
		          from_date == lead(from_date, 1) & !is.na(lead(from_date, 1)) &
		          from_date == to_date & !is.na(from_date) &
		          ((enroll_type == "mcaid" & lead(enroll_type, 1) %in% c("both", "mcare")) |
		             (enroll_type == "mcare" & lead(enroll_type, 1) %in% c("both", "mcaid"))) ~ 1,
		        # Drop rows where the to_date < from_date due to 
		        # both data sources' dates ending at the same time
		        to_date < from_date ~ 1,
		        TRUE ~ 0
		      )
		  ) %>%
		  filter(drop == 0 | is.na(drop)) %>%
		  # Truncate remaining overlapping end dates
		  mutate(to_date = as.Date(
		    ifelse(id_apde == lead(id_apde, 1) & !is.na(lead(from_date, 1)) &
		             from_date < lead(from_date, 1) &
		             to_date >= lead(to_date, 1),
		           lead(from_date, 1) - 1,
		           to_date),
		    origin = "1970-01-01")
		  ) %>%
		  select(-drop, -repnum, -rownum_temp) %>%
		  # With rows truncated, now additional rows with enroll_type == h/m that 
		  # are fully covered by an enroll_type == b
		  # Also catches single day rows that now have to_date < from_date
		  mutate(
		    drop = case_when(
		      id_apde == lag(id_apde, 1) & from_date == lag(from_date, 1) &
		        to_date == lag(to_date, 1) & lag(enroll_type, 1) == "both" & 
		        enroll_type != "both" ~ 1,
		      id_apde == lead(id_apde, 1) & from_date == lead(from_date, 1) &
		        to_date <= lead(to_date, 1) & lead(enroll_type, 1) == "both" ~ 1,
		      id_apde == lag(id_apde, 1) & from_date >= lag(from_date, 1) &
		        to_date <= lag(to_date, 1) & enroll_type != "both" &
		        lag(enroll_type, 1) == "both" ~ 1,
		      id_apde == lead(id_apde, 1) & from_date >= lead(from_date, 1) &
		        to_date <= lead(to_date, 1) & enroll_type != "both" &
		        lead(enroll_type, 1) == "both" ~ 1,
		      TRUE ~ 0)
		  ) %>%
		  filter(drop == 0 | is.na(drop)) %>%
		  select(id_apde, from_date, to_date, enroll_type)
		
		duals <- setDT(copy(temp_ext))
		rm(temp, temp_ext)

## (6) Duals Part 2: join mcare/mcaid data based on ID & overlapping time periods ----      
      duals[, c("from_date", "to_date") := lapply(.SD, as.integer), .SDcols = c("from_date", "to_date")] # ensure type==integer for foverlaps()
      setkey(duals, id_apde, from_date, to_date)    
      
      mcare.dual[, c("from_date", "to_date") := lapply(.SD, as.integer), .SDcols = c("from_date", "to_date")] # ensure type==integer for foverlaps()
      setkey(mcare.dual, id_apde, from_date, to_date)
      
      mcaid.dual[, c("from_date", "to_date") := lapply(.SD, as.integer), .SDcols = c("from_date", "to_date")] # ensure type==integer for foverlaps()
      setkey(mcaid.dual, id_apde, from_date, to_date)
      
      # join on the Medicaid duals data (using foverlaps ... https://github.com/Rdatatable/data.table/blob/master/man/foverlaps.Rd)
      duals <- foverlaps(duals, mcaid.dual, type = "any", mult = "all")
      duals[, from_date := i.from_date] # the complete set of proper from_dates are in i.from_date
      duals[, to_date := i.to_date] # the complete set of proper to_dates are in i.to_date
      duals[, c("i.from_date", "i.to_date") := NULL] # no longer needed
      setkey(duals, id_apde, from_date, to_date)
      
      # join on the Medicare duals data
      duals <- foverlaps(duals, mcare.dual, type = "any", mult = "all")
      duals[, from_date := i.from_date] # the complete set of proper from_dates are in i.from_date
      duals[, to_date := i.to_date] # the complete set of proper to_dates are in i.to_date
      duals[, c("i.from_date", "i.to_date") := NULL] # no longer needed    

## (7) Append duals and non-duals data ----
      timevar <- rbindlist(list(duals, mcare.solo, mcaid.solo), use.names = TRUE, fill = TRUE)
      setkey(timevar, id_apde, from_date) # order dual data
      
## (8) Collapse data if dates are contiguous and all data is the same ----
    # Create unique ID for data chunks ----
      timevar.vars <- setdiff(names(timevar), c("from_date", "to_date")) # all vars except date vars
      timevar[, group := .GRP, by = timevar.vars] # create group id
      timevar[, group := cumsum( c(0, diff(group)!=0) )] # in situation like a:a:a:b:b:b:b:a:a:a, want to distinguish first set of "a" from second set of "a"
    
    # Create unique ID for contiguous times within a given data chunk ----
      setkey(timevar, id_apde, from_date)
      timevar[, prev_to_date := c(NA, to_date[-.N]), by = "group"] # create row with the previous 'to_date', MUCH faster than the shift "lag" function in data.table
      timevar[, diff.prev := from_date - prev_to_date] # difference between from_date & prev_to_date will be 1 (day) if they are contiguous
      timevar[diff.prev != 1, diff.prev := NA] # set to NA if difference is not 1 day, i.e., it is not contiguous, i.e., it starts a new contiguous chunk
      timevar[is.na(diff.prev), contig.id := .I] # Give a unique number for each start of a new contiguous chunk (i.e., section starts with NA)
      setkey(timevar, group, from_date) # need to order the data so the following line will work.
      timevar[, contig.id  := contig.id[1], by=  .( group , cumsum(!is.na(contig.id))) ] # fill forward by group
      timevar[, c("prev_to_date", "diff.prev") := NULL] # drop columns that were just intermediates
      
    # Collapse rows where data chunks are constant and time is contiguous ----      
      timevar[, from_date := min(from_date), by = c("group", "contig.id")]
      timevar[, to_date := max(to_date), by = c("group", "contig.id")]
      timevar[, c("group", "contig.id") := NULL]
      timevar <- unique(timevar)
    
## (9) Prep for pushing to SQL ----
    # Create mcare, mcaid, & dual flags ----
      timevar[, mcare := 0][part_a==1 | part_b == 1 | part_c==1, mcare := 1]
      timevar[, mcaid := 0][!is.na(cov_type), mcaid := 1]
      timevar[, apde_dual := 0][mcare == 1 & mcaid == 1, apde_dual := 1]
      timevar[, enroll_type := NULL] # kept until now for comparison with the dual flag
      timevar <- timevar[!(mcare==0 & mcaid==0)]

    # Create contiguous flag ----  
      # If contiguous with the PREVIOUS row, then it is marked as contiguous. This is the same as mcaid_elig_timevar
      timevar[, prev_to_date := c(NA, to_date[-.N]), by = "id_apde"] # MUCH faster than the shift "lag" function in data.table
      timevar[, contiguous := 0]
      timevar[from_date - prev_to_date == 1, contiguous := 1]
      timevar[, prev_to_date := NULL] # drop because no longer needed
      
    # Create cov_time_date ----
      timevar[, cov_time_day := as.integer(to_date - from_date)]
      
    # Set dates as.Date() ----
      timevar[, c("from_date", "to_date") := lapply(.SD, as.Date, origin = "1970-01-01"), .SDcols =  c("from_date", "to_date")] 

    # Select data from Medicare or Medicaid, as appropriate ----
      timevar[is.na(geo_zip) & !is.na(geo_zip_mcare), geo_zip := geo_zip_mcare]
      timevar[, geo_zip_mcare := NULL]
      
    # Add KC flag based on zip code ----  
      kc.zips <- fread(kc.zips.url)
      timevar[, geo_kc := 0]
      timevar[geo_zip %in% unique(as.character(kc.zips$zip)), geo_kc := 1]
      rm(kc.zips)
      
    # create time stamp ----
      timevar[, last_run := Sys.time()] 
      
## (10) Write to SQL ----              
  # Pull YAML from GitHub
    table_config <- yaml::yaml.load(RCurl::getURL(yaml.url))
  
  # Create table ID
    tbl_id <- DBI::Id(schema = table_config$schema, 
                      table = table_config$table)  
  
  # Ensure columns are in same order in R & SQL
    setcolorder(timevar, names(table_config$vars))
  
  # Write table to SQL
    dbWriteTable(db_claims, 
                 tbl_id, 
                 value = as.data.frame(timevar),
                 overwrite = T, append = F, 
                 field.types = unlist(table_config$vars))

## (11) Simple QA ----
    # Confirm that all rows were loaded to SQL ----
      stage.count <- as.numeric(odbc::dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.mcaid_mcare_elig_timevar"))
      if(stage.count != nrow(timevar))
        stop("Mismatching row count, error writing data")    
    
    # check that rows in stage are not less than the last time that it was created ----
      last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_mcare_elig_timevar")[[1]])
    
      # count number of rows
      previous_rows <- as.numeric(
        odbc::dbGetQuery(db_claims, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_timevar' AND
                         qa_item = 'row_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_timevar' AND
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
                         'stage.mcaid_mcare_elig_timevar',
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
                         'stage.mcaid_mcare_elig_timevar',
                         'Number new rows compared to most recent run', 
                         'PASS', 
                         {Sys.time()}, 
                         'There were {row_diff} more rows in the most recent table 
                         ({stage.count} vs. {previous_rows})')",
                         .con = db_claims))
        
        problem.row_diff <- glue::glue(" ") # no problem, so empty error message
        
      }
    
    # check that the number of distinct IDs not less than the last time that it was created ----
      # get count of unique id (each id should only appear once)
      current.unique.id <- as.numeric(odbc::dbGetQuery(
        db_claims, "SELECT COUNT (DISTINCT id_apde) 
        FROM stage.mcaid_mcare_elig_timevar"))
      
      previous.unique.id <- as.numeric(
        odbc::dbGetQuery(db_claims, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_timevar' AND
                         qa_item = 'id_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_timevar' AND
                         qa_item = 'id_count') b
                         ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous.unique.id)){previous.unique.id = 0}
      
      id_diff <- current.unique.id - previous.unique.id
      
      if (id_diff < 0) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_elig_timevar',
                         'Number distinct IDs compared to most recent run', 
                         'FAIL', 
                         {Sys.time()}, 
                         'There were {id_diff} fewer IDs in the most recent table 
                         ({current.unique.id} vs. {previous.unique.id})')",
                         .con = db_claims))
        
        problem.id_diff <- glue::glue("Fewer unique IDs than found last time.  
                                       Check metadata.qa_mcare for details (last_run = {last_run})
                                       \n")
      } else {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_elig_timevar',
                         'Number distinct IDs compared to most recent run', 
                         'PASS', 
                         {Sys.time()}, 
                         'There were {id_diff} more IDs in the most recent table 
                         ({current.unique.id} vs. {previous.unique.id})')",
                         .con = db_claims))
        
        problem.id_diff <- glue::glue(" ") # no problem, so empty error message
      }
    
    # create summary of errors ---- 
      problems <- glue::glue(
        problem.row_diff, "\n",
        problem.id_diff)

## (12) Fill qa_mcare_values table ----
    qa.values <- glue::glue_sql("INSERT INTO metadata.qa_mcare_values
                                (table_name, qa_item, qa_value, qa_date, note) 
                                VALUES ('stage.mcaid_mcare_elig_timevar',
                                'row_count', 
                                {stage.count}, 
                                {Sys.time()}, 
                                '')",
                                .con = db_claims)
    
    odbc::dbGetQuery(conn = db_claims, qa.values)
    
    qa.values2 <- glue::glue_sql("INSERT INTO metadata.qa_mcare_values
                                (table_name, qa_item, qa_value, qa_date, note) 
                                VALUES ('stage.mcaid_mcare_elig_timevar',
                                'id_count', 
                                {current.unique.id}, 
                                {Sys.time()}, 
                                '')",
                                .con = db_claims)
    
    odbc::dbGetQuery(conn = db_claims, qa.values2)
    

## (13) Print error messages ----
    if(problems >1){
      message(glue::glue("WARNING ... MCAID_MCARE_ELIG_TIMEVAR FAILED AT LEAST ONE QA TEST", "\n",
                         "Summary of problems in MCAID_MCARE_ELIG_TIMEVAR: ", "\n", 
                         problems))
    }else{message("Staged MCAID_MCARE_ELIG_TIMEVAR passed all QA tests")}

## The end! ----
    run.time <- Sys.time() - start.time
    print(run.time)
    