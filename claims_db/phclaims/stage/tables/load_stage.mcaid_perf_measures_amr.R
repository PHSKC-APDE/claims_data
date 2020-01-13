################################################################
#
# Calculate the ages 5-64 asthma medication ratio quality measure (HEDIS)
#
# Measure available here: 
# https://www.medicaid.gov/medicaid/quality-of-care/downloads/medicaid-adult-core-set-manual.pdf
# https://www.medicaid.gov/medicaid/quality-of-care/downloads/medicaid-and-chip-child-core-set-manual.pdf
#
# Alastair Matheson
# APDE, PHSKC
# 2019-04
#
################################################################

#### STEPS ####
# PART 1 - GENERATE DENOMINATOR POPULATION
# 1A - Find anyone who meets the asthma definition for a given 12 months
# 1B - find people who have exclusion criteria
# 1C - Bring populations together
# 1D - Check for persistent asthma (met the definition the previous year)

# PART 2 - GENERATE NUMERATOR DATA
# 2A - Calculate units of medication
# 2B - Calculate the medication ratio

# PART 3 - BRING NUMERATOR AND DENOMINATOR TOGETHER


#### BRING IN LIBRARIES AND SET OPTIONS ####
library(tidyverse)
library(lubridate)
library(odbc)

options(max.print = 600, tibble.print_max = 50, scipen = 999, warning.length = 5000)

db_claims <- dbConnect(odbc(), "PHClaims51")


#### SET UP PARAMETERS FOR TABLE CREATION ####
# Find the most recent month we have enrollment summaries for
# Comes in as year-month
max_month <- unlist(dbGetQuery(db_claims, "SELECT MAX(year_month) FROM stage.perf_enroll_denom"))
# Now find last day of the month for going forward a month then back a day
max_month <- as.Date(parse_date_time(max_month, "Ym") %m+% months(1) - days(1))

# Set up quarters to run over
months_list <- as.list(seq(as.Date("2013-01-01"), as.Date(max_month) + 1, by = "quarter") - 1)


##################################################
#### PART 1 - GENERATE DENOMINATOR POPULATION ####

#### Start with people who were enrolled for at least 11 months ####
# Currently making use of a temp table that will become permanent after QA processes

# For the first quarter, create the temp table. Need to set a counter
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT id_mcaid, year_month, end_month_age 
                      FROM [PHClaims].[stage].[perf_enroll_denom]
                      WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND 
                      end_month_age >= 5 AND end_month_age < 65) a 
                     LEFT JOIN
                     (SELECT year_month, end_month, beg_measure_year_month 
                       FROM [ref].[perf_year_month]) b 
                     ON a.year_month = b.year_month
                     WHERE b.end_month = '", x, "'")
  
  if (i == 1) {
    # Note: DBI package only supports the temporary option for dbRemoveTable
    # not for dbExistsTable. Use try() in the mean time so that the code
    # continues even if an error is thrown trying to remove a table that
    # doesn't exist
    try(dbRemoveTable(db_claims, "##asthma_pop", temporary = T), silent = T)

    DBI::dbExecute(db_claims, 
                paste0("SELECT a.id_mcaid, a.year_month, b.end_month, a.end_month_age, 
                        b.beg_measure_year_month, 'enroll_flag' = 1 
                       INTO ##asthma_pop FROM ",
                       sql_temp))
    i <<- i + 1
  } else {
    DBI::dbExecute(db_claims, 
                paste0("INSERT INTO ##asthma_pop 
                       SELECT a.id_mcaid, a.year_month, b.end_month, a.end_month_age, 
                        beg_measure_year_month, 'enroll_flag' = 1 
                       FROM ",
                       sql_temp))
  }
})

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count FROM ##asthma_pop GROUP BY end_month ORDER BY end_month")



#### Find events that define someone with asthma ####
### Make temp table of everyone with an asthma definition
try(dbRemoveTable(db_claims, "##asthma_dx", temporary = T))
dbGetQuery(db_claims,
           "SELECT a.id_mcaid, a.claim_header_id, a.first_service_date, b.icdcm_number, 'asthma' = 1
           INTO ##asthma_dx
           FROM 
           (SELECT id_mcaid, claim_header_id, first_service_date
             FROM [PHClaims].[final].[mcaid_claim_header]) a
           INNER JOIN
           (SELECT id_mcaid, claim_header_id, icdcm_norm, icdcm_version, icdcm_number
             FROM [PHClaims].[final].[mcaid_claim_icdcm_header]) b
           ON a.id_mcaid = b.id_mcaid AND a.claim_header_id = b.claim_header_id
           INNER JOIN
           (SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS dx_ver 
             FROM [PHClaims].[ref].[hedis_code_system] 
             WHERE value_set_name = 'Asthma') c 
           ON b.icdcm_norm = c.code AND b.icdcm_version = c.dx_ver")

# Check results
dbGetQuery(db_claims, "SELECT COUNT(*) AS count FROM ##asthma_dx")


#### 1+ ED or inpatient visits with primary asthma dx in the past 12 months ####
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT id, claim_header_id, from_date, to_date, ed, inpatient 
                      FROM [PHClaims].[dbo].[mcaid_claim_summary] 
                      WHERE (ed = 1 OR inpatient = 1) AND 
                      from_date <= '", x, "' AND 
                      from_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "'))) a 
                     INNER JOIN 
                     (SELECT id_mcaid, claim_header_id FROM ##asthma_dx WHERE dx_number = 1) b 
                       ON a.id = b.id AND a.claim_header_id = b.claim_header_id 
                       GROUP BY a.id")
  
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_ed_inpat", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      SUM(a.ed) AS ed_cnt, SUM(a.inpatient) AS inpat_cnt 
                      INTO ##asthma_ed_inpat FROM ",
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_ed_inpat 
                      SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      SUM(a.ed) AS ed_cnt, SUM(a.inpatient) AS inpat_cnt 
                      FROM ",
                      sql_temp))
  }
})

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count FROM ##asthma_ed_inpat GROUP BY end_month ORDER BY end_month")


#### 4+ outpatient visits with any asthma dx AND 2+ asthma med dispensing events ####
# Apply 2+ med events later when joined
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT id_mcaid, claim_header_id, from_date 
                      FROM [PHClaims].[dbo].[mcaid_claim_summary] 
                      WHERE from_date <= '", x, "' AND 
                      from_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "'))) a 
                     INNER JOIN 
                     (SELECT id_mcaid, claim_header_id FROM ##asthma_dx) b 
                       ON a.id_mcaid = b.id_mcaid AND a.claim_header_id = b.claim_header_id 
                       INNER JOIN 
                       (SELECT id_mcaid, claim_header_id, pcode FROM [PHClaims].[dbo].[mcaid_claim_proc]) c 
                       ON a.id_mcaid = c.id_mcaid AND a.claim_header_id = c.claim_header_id 
                       INNER JOIN 
                       (SELECT code FROM [PHClaims].[ref].[hedis_code_system] 
                         WHERE value_set_name = 'Outpatient') d 
                       ON c.pcode = d.code 
                       GROUP BY a.id_mcaid")
  
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_outpat", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      COUNT(DISTINCT a.from_date) AS outpat_cnt 
                      INTO ##asthma_outpat FROM ",
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_outpat 
                      SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      COUNT(DISTINCT a.from_date) AS outpat_cnt 
                      FROM ",
                      sql_temp))
  }
})

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count FROM ##asthma_outpat GROUP BY end_month ORDER BY end_month")


#### 4+ asthma dispensing events ####
### Set up code just for oral meds
# Need to sum up days prescribed for each drug type then calculate number of events (0-30 days = 1 event, each full 30 days beyond = another event)
# Also need to separate out luekotriene-only counts (part of inclusion criteria)
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT a.id_mcaid, a.drug_fill_date, b.generic_product_name, 
                      CASE WHEN SUM(a.drug_supply_d) <= 30 THEN 1
                      WHEN SUM(a.drug_supply_d) > 30 THEN FLOOR(SUM(a.drug_supply_d) / 30)
                      END AS drug_events
                      FROM 
                      (SELECT id_mcaid, ndc_code, drug_fill_date, drug_supply_d
                        FROM [PHClaims].[dbo].[mcaid_claim_pharm]
                        WHERE drug_fill_date <= '", x, "' AND 
                        drug_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "'))) a 
                      INNER JOIN
                      (SELECT medication_list_name, ndc_code, generic_product_name, [route], [description]
                        FROM [PHClaims].[ref].[hedis_ndc_code] 
                        WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications') 
                        AND [route] = 'oral' AND [description] = 'Leukotriene modifiers') b
                      ON a.ndc_code = b.ndc_code
                      GROUP BY a.id_mcaid, a.drug_fill_date, b.generic_product_name) c 
                     GROUP BY c.id_mcaid, c.drug_fill_date
                     ORDER BY c.id_mcaid, c.drug_fill_date")
  
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_rx_event_oral_lk", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT c.id_mcaid, 'end_month' = '", x, "', 
                      c.drug_fill_date, SUM(c.drug_events) AS events_oral_lk
                      INTO ##asthma_rx_event_oral_lk FROM ",
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_rx_event_oral_lk 
                      SELECT c.id_mcaid, 'end_month' = '", x, "', 
                      c.drug_fill_date, SUM(c.drug_events) AS events_oral_lk
                       FROM ",
                      sql_temp))
  }
  })

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_oral_lk GROUP BY end_month ORDER BY end_month")

# Non-luekotriene inhibitors
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT a.id_mcaid, a.drug_fill_date, b.generic_product_name, 
                      CASE WHEN SUM(a.drug_supply_d) <= 30 THEN 1
                      WHEN SUM(a.drug_supply_d) > 30 THEN FLOOR(SUM(a.drug_supply_d) / 30)
                      END AS drug_events
                      FROM 
                      (SELECT id_mcaid, ndc_code, drug_fill_date, drug_supply_d
                        FROM [PHClaims].[dbo].[mcaid_claim_pharm]
                        WHERE drug_fill_date <= '", x, "' AND 
                        drug_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "'))) a 
                      INNER JOIN
                      (SELECT medication_list_name, ndc_code, generic_product_name, [route], [description]
                        FROM [PHClaims].[ref].[hedis_ndc_code] 
                        WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications') 
                        AND [route] = 'oral' AND [description] <> 'Leukotriene modifiers') b
                      ON a.ndc_code = b.ndc_code
                      GROUP BY a.id_mcaid, a.drug_fill_date, b.generic_product_name) c 
                     GROUP BY c.id_mcaid, c.drug_fill_date
                     ORDER BY c.id_mcaid, c.drug_fill_date")
  
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_rx_event_oral_non_lk", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT c.id_mcaid, 'end_month' = '", x, "', 
                      c.drug_fill_date, SUM(c.drug_events) AS events_oral_non_lk
                      INTO ##asthma_rx_event_oral_non_lk FROM ",
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_rx_event_oral_non_lk 
                      SELECT c.id_mcaid, 'end_month' = '", x, "', 
                      c.drug_fill_date, SUM(c.drug_events) AS events_oral_non_lk
                      FROM ",
                      sql_temp))
  }
  })

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_oral_non_lk GROUP BY end_month ORDER BY end_month")


### Set up code just for inhalers
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT id_mcaid, ndc_code, drug_fill_date
                      FROM [PHClaims].[dbo].[mcaid_claim_pharm]
                      WHERE drug_fill_date <= '", x, "' AND 
                      drug_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "'))) a 
                     INNER JOIN
                     (SELECT medication_list_name, ndc_code, generic_product_name, [route]
                       FROM [PHClaims].[ref].[hedis_ndc_code]
                       WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications')
                       AND [route] = 'inhalation') b 
                     ON a.ndc_code = b.ndc_code
                     GROUP BY a.id_mcaid, a.drug_fill_date
                     ORDER BY a.id_mcaid, a.drug_fill_date")
    
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_rx_event_inhaler", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      a.drug_fill_date, COUNT (DISTINCT b.generic_product_name) AS events_inhaler
                      INTO ##asthma_rx_event_inhaler FROM ",
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_rx_event_inhaler
                      SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      a.drug_fill_date, COUNT (DISTINCT b.generic_product_name) AS events_inhaler
                      FROM ",
                      sql_temp))
  }
  })

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_inhaler GROUP BY end_month ORDER BY end_month")


### Set up code just for injections
# Need to separate out antibody inhibitor-only counts
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT id_mcaid, ndc_code, drug_fill_date
                      FROM [PHClaims].[dbo].[mcaid_claim_pharm]
                      WHERE drug_fill_date <= '", x, "' AND 
                      drug_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "'))) a 
                     INNER JOIN
                     (SELECT medication_list_name, ndc_code, [route], [description]
                       FROM [PHClaims].[ref].[hedis_ndc_code]
                       WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications')
                       AND [route] IN ('intravenous', 'subcutaneous') AND [description] = 'Antibody inhibitor') b
                     ON a.ndc_code = b.ndc_code
                     GROUP BY a.id_mcaid, a.drug_fill_date, a.ndc_code
                     ORDER BY a.id_mcaid, a.drug_fill_date")
  
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_rx_event_inject_antib", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      a.drug_fill_date, COUNT (*) AS events_inject_antib
                      INTO ##asthma_rx_event_inject_antib FROM ",
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_rx_event_inject_antib
                      SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      a.drug_fill_date, COUNT (*) AS events_inject_antib
                      FROM ",
                      sql_temp))
  }
  })

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_inject_antib GROUP BY end_month ORDER BY end_month")


# Non-antibody inhibitor counts
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT id_mcaid, ndc_code, drug_fill_date 
               FROM [PHClaims].[dbo].[mcaid_claim_pharm] 
               WHERE drug_fill_date <= '", x, "' AND 
               drug_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "'))) a 
              INNER JOIN
              (SELECT medication_list_name, ndc_code, [route], [description] 
              FROM [PHClaims].[ref].[hedis_ndc_code] 
              WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications') 
                AND [route] IN ('intravenous', 'subcutaneous') AND [description] <> 'Antibody inhibitor') b 
              ON a.ndc_code = b.ndc_code
              GROUP BY a.id_mcaid, a.drug_fill_date, a.ndc_code 
              ORDER BY a.id_mcaid, a.drug_fill_date")
  
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_rx_event_inject_non_antib", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      a.drug_fill_date, COUNT (*) AS events_inject_non_antib
                      INTO ##asthma_rx_event_inject_non_antib FROM ", 
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_rx_event_inject_non_antib
                      SELECT a.id_mcaid, 'end_month' = '", x, "', 
                      a.drug_fill_date, COUNT (*) AS events_inject_non_antib
                      FROM ",
                      sql_temp))
  }
  })

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_inject_non_antib GROUP BY end_month ORDER BY end_month")


### Combine rx temp tables and sum events
# Join to dx table to check if people meet the dx requirement

# Make collated table outside of loop to avoid recreating it
try(dbRemoveTable(db_claims, "##asthma_rx_event_temp", temporary = T))
dbGetQuery(db_claims,
           "SELECT g.id_mcaid, g.end_month, SUM(events_rx) AS events_rx,
           CASE WHEN SUM(events_rx) = SUM(dx_needed_cnt) THEN 1 ELSE 0 END AS dx_needed
           INTO ##asthma_rx_event_temp
           FROM
           (SELECT f.id_mcaid, f.end_month, f.drug_fill_date,
             f.events_oral_lk + f.events_oral_non_lk + f.events_inhaler + 
               f.events_inject_antib + f.events_inject_non_antib AS events_rx,
             f.events_oral_lk + f.events_inject_antib AS dx_needed_cnt
            FROM
            (SELECT COALESCE(a.id_mcaid, b.id_mcaid, c.id_mcaid, d.id_mcaid, e.id_mcaid) as id_mcaid,
               COALESCE(a.end_month, b.end_month, c.end_month, d.end_month, 
                        e.end_month) AS end_month,
               COALESCE(a.drug_fill_date, b.drug_fill_date, c.drug_fill_date, 
                        d.drug_fill_date, e.drug_fill_date) AS drug_fill_date,
               ISNULL(a.events_oral_lk, 0) AS events_oral_lk,
               ISNULL(b.events_oral_non_lk, 0) AS events_oral_non_lk,
               ISNULL(c.events_inhaler, 0) AS events_inhaler,
               ISNULL(d.events_inject_antib, 0) AS events_inject_antib,
               ISNULL(e.events_inject_non_antib, 0) AS events_inject_non_antib
            FROM 
               (SELECT id_mcaid, end_month, drug_fill_date, events_oral_lk
                FROM ##asthma_rx_event_oral_lk) a 
                FULL JOIN 
                (SELECT id_mcaid, end_month, drug_fill_date, events_oral_non_lk
                FROM ##asthma_rx_event_oral_non_lk) b 
                ON a.id_mcaid = b.id_mcaid AND a.end_month = b.end_month AND a.drug_fill_date = b.drug_fill_date
                FULL JOIN 
                (SELECT id_mcaid, end_month, drug_fill_date, events_inhaler
                FROM ##asthma_rx_event_inhaler) c 
                ON COALESCE(a.id_mcaid, b.id_mcaid) = c.id_mcaid AND COALESCE(a.end_month, b.end_month) = c.end_month AND 
                  COALESCE(a.drug_fill_date, b.drug_fill_date) = c.drug_fill_date
                FULL JOIN 
                (SELECT id_mcaid, end_month, drug_fill_date, events_inject_antib
                FROM ##asthma_rx_event_inject_antib) d 
                ON COALESCE(a.id_mcaid, b.id_mcaid, c.id_mcaid) = d.id_mcaid AND 
                  COALESCE(a.end_month, b.end_month, c.end_month) = d.end_month AND 
                  COALESCE(a.drug_fill_date, b.drug_fill_date, c.drug_fill_date) = d.drug_fill_date
                FULL JOIN 
                (SELECT id_mcaid, end_month, drug_fill_date, events_inject_non_antib
                FROM ##asthma_rx_event_inject_non_antib) e 
                ON COALESCE(a.id_mcaid, b.id_mcaid, c.id_mcaid, d.id_mcaid) = e.id_mcaid AND 
                  COALESCE(a.end_month, b.end_month, c.end_month, d.end_month) = e.end_month AND 
                  COALESCE(a.drug_fill_date, b.drug_fill_date, c.drug_fill_date, d.drug_fill_date) = e.drug_fill_date
                ) f 
              GROUP BY f.id_mcaid, f.end_month, f.drug_fill_date, f.events_oral_lk, f.events_oral_non_lk, 
                f.events_inhaler, f.events_inject_antib, f.events_inject_non_antib) g 
              GROUP BY g.id_mcaid, g.end_month")


i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT id_mcaid, end_month, events_rx, dx_needed
                      FROM ##asthma_rx_event_temp 
                      WHERE end_month = '", x, "') h 
                      LEFT JOIN 
                      (SELECT DISTINCT id_mcaid, 'dx_made' = 1
                      FROM ##asthma_dx WHERE from_date <= '", x, "' AND 
                        from_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "'))) i 
                      ON h.id_mcaid = i.id_mcaid,")
  
  
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_rx_event", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT h.id_mcaid, h.end_month, h.events_rx, h.dx_needed, ISNULL(i.dx_made, 0) AS dx_made
                      INTO ##asthma_rx_event FROM ",
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_rx_event
                      SELECT h.id_mcaid, h.end_month, h.events_rx, h.dx_needed, ISNULL(i.dx_made, 0) AS dx_made
                      FROM ",
                      sql_temp))
  }
  })

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event GROUP BY end_month ORDER BY end_month")


#### 1B - FIND PEOPLE WHO HAVE EXCLUSION CRITERIA ####
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT id_mcaid, claim_header_id
                      FROM [PHClaims].[dbo].[mcaid_claim_summary]
                      WHERE from_date <= '", x, "') a
                     LEFT JOIN
                     (SELECT id_mcaid, claim_header_id, dx_norm, dx_ver
                       FROM [PHClaims].[dbo].[mcaid_claim_dx]) b
                     ON a.id_mcaid = b.id_mcaid, AND a.claim_header_id = b.claim_header_id
                     INNER JOIN
                     (SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS dx_ver
                       FROM [PHClaims].[ref].[hedis_code_system]
                       WHERE value_set_name = 'Emphysema'
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS dx_ver
                       FROM [PHClaims].[ref].[hedis_code_system]
                       WHERE value_set_name = 'Other Emphysema'
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS dx_ver
                       FROM [PHClaims].[ref].[hedis_code_system]
                       WHERE value_set_name = 'COPD'
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS dx_ver
                       FROM [PHClaims].[ref].[hedis_code_system]
                       WHERE value_set_name = 'Obstructive Chronic Bronchitis' 
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS dx_ver
                       FROM [PHClaims].[ref].[hedis_code_system]
                       WHERE value_set_name = 'Chronic Respiratory Conditions Due To Fumes/Vapors' 
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS dx_ver
                       FROM [PHClaims].[ref].[hedis_code_system]
                       WHERE value_set_name = 'Cystic Fibrosis' 
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS dx_ver
                       FROM [PHClaims].[ref].[hedis_code_system]
                       WHERE value_set_name = 'Acute Respiratory Failure' 
                     ) c
                     ON b.dx_norm = c.code AND b.dx_ver = c.dx_ver")
  
  
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_excl", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT DISTINCT a.id_mcaid, 'end_month' = '", x, "'
                      INTO ##asthma_excl FROM ",
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_excl
                      SELECT DISTINCT a.id_mcaid, 'end_month' = '", x, "' 
                      FROM ",
                      sql_temp))
  }
})


# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_excl GROUP BY end_month ORDER BY end_month")


#### 1C - BRING POPULATIONS TOGETHER ####
### Quantify how many people at each stage by adding flags

### See how many met any of the asthma inclusion criteria for a single year
# Also include last year's date for later code
try(dbRemoveTable(db_claims, "##asthma_any", temporary = T))
dbGetQuery(db_claims,
           "SELECT f.id_mcaid, f.year_month, f.end_month, DATEADD(YEAR, -1, f.end_month) AS past_year, 
            f.end_month_age, f.beg_measure_year_month, f.enroll_flag, 
            f.ed_flag, f.inpat_flag, f.outpat_flag, f.rx_flag, f.rx_any
           INTO ##asthma_any
           FROM
            (SELECT e.id_mcaid, e.end_month, 
              e.year_month, e.end_month_age, e.beg_measure_year_month, e.enroll_flag, 
              CASE WHEN MAX(e.ed_cnt) > 0 THEN 1 ELSE 0 END AS ed_flag,
              CASE WHEN MAX(e.inpat_cnt) > 0 THEN 1 ELSE 0 END AS inpat_flag,
              CASE WHEN MAX(e.outpat_cnt) > 0 AND MAX(e.events_rx) > 2 THEN 1 ELSE 0 END AS outpat_flag,
              CASE WHEN MAX(e.events_rx) >= 4 AND MAX(e.dx_needed) = 0 THEN 1 
                WHEN MAX(e.events_rx) >= 4 AND MAX(e.dx_needed) = 1 AND MAX(dx_made) = 1 THEN 1 
                ELSE 0 END AS rx_flag,
              CASE WHEN MAX(e.events_rx) > 0 THEN 1 ELSE 0 END AS rx_any
            FROM
              (SELECT COALESCE(a.id_mcaid, b.id_mcaid, c.id_mcaid, d.id_mcaid) AS id_mcaid, 
                a.year_month, 
                COALESCE(a.end_month, b.end_month, c.end_month, d.end_month) AS end_month, 
                a.end_month_age, a.beg_measure_year_month, 
                ISNULL(a.enroll_flag, 0) AS enroll_flag, 
                ISNULL(b.ed_cnt, 0) AS ed_cnt, 
                ISNULL(b.inpat_cnt, 0) AS inpat_cnt, 
                ISNULL(c.outpat_cnt, 0) AS outpat_cnt, 
                ISNULL(d.events_rx, 0) AS events_rx, 
                ISNULL(d.dx_needed, 0) AS dx_needed, 
                ISNULL(d.dx_made, 0) AS dx_made 
              FROM
              (SELECT id_mcaid, year_month, end_month, end_month_age, 
                beg_measure_year_month,  enroll_flag 
              FROM ##asthma_pop) a
              FULL JOIN
              (SELECT id_mcaid, end_month, ed_cnt, inpat_cnt 
              FROM ##asthma_ed_inpat) b 
              ON a.id_mcaid = b.id_mcaid AND a.end_month = b.end_month 
              FULL JOIN 
              (SELECT id_mcaid, end_month, outpat_cnt 
              FROM ##asthma_outpat) c 
              ON COALESCE(a.id_mcaid, b.id_mcaid) = c.id_mcaid AND 
              COALESCE(a.end_month, b.end_month) = c.end_month
              FULL JOIN 
              (SELECT id_mcaid, end_month, events_rx, dx_needed, dx_made 
              FROM ##asthma_rx_event) d 
              ON COALESCE(a.id_mcaid, b.id_mcaid, c.id_mcaid) = d.id_mcaid AND 
                COALESCE(a.end_month, b.end_month, c.end_month) = d.end_month
              ) e 
              GROUP BY e.id_mcaid, e.year_month, e.end_month, e.end_month_age, 
                e.beg_measure_year_month, e.enroll_flag) f 
            WHERE NOT (f.ed_flag = 0 AND f.inpat_flag = 0 AND 
                       f.outpat_flag = 0 AND f.rx_flag = 0)
            GROUP BY f.id_mcaid, f.year_month, f.end_month, f.end_month_age, 
              f.beg_measure_year_month, f.enroll_flag, f.ed_flag, f.inpat_flag, 
              f.outpat_flag, f.rx_flag, f.rx_any 
            ORDER BY f.id_mcaid, f.end_month")

# Check counts
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_any GROUP BY end_month ORDER BY end_month")



### Apply check of persistent asthma (i.e., see if they had asthma the previous year)
try(dbRemoveTable(db_claims, "##asthma_persist", temporary = T))
dbGetQuery(db_claims,
           "SELECT a.id_mcaid, a.year_month, a.end_month, a.past_year, a.end_month_age, 
            a.beg_measure_year_month, a.enroll_flag, a.ed_flag, a.inpat_flag, 
            a.outpat_flag, a.rx_flag, a.rx_any, ISNULL(b.persistent, 0) AS persistent
           INTO ##asthma_persist FROM
            (SELECT id_mcaid, year_month, end_month, past_year, end_month_age, 
              beg_measure_year_month, enroll_flag, ed_flag, inpat_flag, 
              outpat_flag, rx_flag, rx_any
            FROM ##asthma_any) a
            LEFT JOIN
            (SELECT id_mcaid, end_month, 'persistent' = 1 FROM ##asthma_any) b
            ON a.id_mcaid = b.id_mcaid AND a.past_year = b.end_month")

# Check counts
dbGetQuery(db_claims, "SELECT end_month, persistent,  COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_persist GROUP BY end_month, persistent ORDER BY end_month, persistent")


### Remove people with exclusion critiera
try(dbRemoveTable(db_claims, "##asthma_denom", temporary = T))
dbGetQuery(db_claims,
           "SELECT a.id_mcaid, a.year_month, a.end_month, a.past_year, a.end_month_age, 
            a.beg_measure_year_month, a.enroll_flag, a.ed_flag, a.inpat_flag, 
            a.outpat_flag, a.rx_flag, a.rx_any, a.persistent, 
            ISNULL(b.dx_exclude, 0) AS dx_exclude
           INTO ##asthma_denom
           FROM
            (SELECT id_mcaid, year_month, end_month, past_year, beg_measure_year_month, end_month_age, 
              enroll_flag, ed_flag, inpat_flag, outpat_flag, rx_flag, rx_any, persistent
              FROM ##asthma_persist) a
            LEFT JOIN
            (SELECT id_mcaid, end_month, 'dx_exclude' = 1
            FROM ##asthma_excl) b
            ON a.id_mcaid = b.id_mcaid AND a.end_month = b.end_month")

# Check counts
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_denom GROUP BY end_month ORDER BY end_month")
dbGetQuery(db_claims, "SELECT end_month, persistent,  COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_denom GROUP BY end_month, persistent ORDER BY end_month, persistent")
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_denom WHERE enroll_flag = 1 AND rx_any = 1 AND persistent = 1 AND  dx_exclude = 0 
           GROUP BY end_month ORDER BY end_month")


# Pull into R for additional analyses
asthma_denom <- DBI::dbReadTable(db_claims, "##asthma_denom")


##########################################
#### PART 2 - GENERATE NUMERATOR DATA ####

#### 2A - CALCULATE UNITS OF MEDICATION ####
### Set up general linked claims and drug data to avoid rerunning it for each date
# NB. Calc for oral meds differs from how events are calculated 
#   (partial 30 days beyond the initial 30 count are included here).
# Using ceiling for inhaler because some claims had a dispensed amount of 
#   6.7 and a package size of 7.
try(dbRemoveTable(db_claims, "##asthma_rx_meds_temp", temporary = T))
dbGetQuery(db_claims,
           "SELECT c.id_mcaid, c.medication_list_name, c.drug_fill_date, c.[route], 
            c.generic_product_name, c.med_units
           INTO ##asthma_rx_meds_temp
           FROM
           (SELECT a.id_mcaid, b.medication_list_name, a.drug_fill_date, b.[route], b.generic_product_name, 
             CASE 
              WHEN b.[route] = 'oral' AND SUM(a.drug_supply_d) <= 30 THEN 1
              WHEN b.[route] = 'oral' AND SUM(a.drug_supply_d) > 30 THEN CEILING(SUM(a.drug_supply_d) / 30)
              WHEN b.[route] IN ('inhalation', 'intravenous', 'subcutaneous') THEN CEILING(a.drug_dispensed_amt/b.package_size)
              END AS med_units
            FROM 
            (SELECT id_mcaid, ndc_code, drug_fill_date, drug_supply_d, drug_dispensed_amt
            FROM [PHClaims].[dbo].[mcaid_claim_pharm]) a 
            INNER JOIN
            (SELECT medication_list_name, ndc_code, generic_product_name, [route], package_size
            FROM [PHClaims].[ref].[hedis_ndc_code] 
            WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications')) b
            ON a.ndc_code = b.ndc_code
            GROUP BY a.id_mcaid, b.medication_list_name, a.drug_fill_date, b.[route], 
              b.generic_product_name, a.drug_dispensed_amt, b.package_size) c")

# Check results
dbGetQuery(db_claims, "SELECT COUNT(*) AS count FROM ##asthma_rx_meds_temp")


#### 2A - CALCULATE AMR ####
i <- 1
lapply(months_list, function(x) {
  
  sql_temp <- paste0("(SELECT COALESCE(a.id_mcaid, b.id_mcaid) AS id_mcaid, 'end_month' = '", x, "', 
                     ISNULL(a.meds_control, 0) AS meds_control,
                     ISNULL(b.meds_relief, 0) AS meds_relief 
                     FROM
                        (SELECT id_mcaid, SUM(med_units) AS meds_control
                        FROM ##asthma_rx_meds_temp
                        WHERE drug_fill_date <= '", x, "' AND 
                          drug_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "')) AND 
                          medication_list_name = 'Asthma Controller Medications'
                        GROUP BY id_mcaid) a
                        FULL JOIN
                        (SELECT id_mcaid, SUM(med_units) AS meds_relief
                        FROM ##asthma_rx_meds_temp
                        WHERE drug_fill_date <= '", x, "' AND 
                          drug_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, '", x, "')) AND 
                          medication_list_name = 'Asthma Reliever Medications'
                        GROUP BY id_mcaid) b
                        ON a.id_mcaid = b.id_mcaid) c
                    ORDER BY id_mcaid")
  
  if (i == 1) {
    try(dbRemoveTable(db_claims, "##asthma_amr", temporary = T))
    dbGetQuery(db_claims, 
               paste0("SELECT c.id_mcaid, c.end_month, c.meds_control, c.meds_relief,
                          ISNULL(c.meds_control / (c.meds_control + c.meds_relief), 0) AS amr 
                      INTO ##asthma_amr FROM ",
                      sql_temp))
    i <<- i + 1
  } else {
    dbGetQuery(db_claims, 
               paste0("INSERT INTO ##asthma_amr 
                      SELECT c.id_mcaid, c.end_month, c.meds_control, c.meds_relief,
                      ISNULL(c.meds_control / (c.meds_control + c.meds_relief), 0) AS amr 
                      FROM ",
                      sql_temp))
  }
})

# Check results
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count, COUNT(DISTINCT id_mcaid) AS cnt_id 
           FROM ##asthma_amr GROUP BY end_month ORDER BY end_month")

dbGetQuery(db_claims, "SELECT TOP(20) * FROM ##asthma_amr ORDER BY id_mcaid, end_month")


###########################################################
#### PART 3 - BRING NUMERATOR AND DENOMINATOR TOGETHER ####
### For full HEDIS measure, require persistent asthma
try(dbRemoveTable(db_claims, "##asthma_final", temporary = T))
dbGetQuery(db_claims,
           "SELECT a.id_mcaid, a.end_month, beg_measure_year_month AS beg_year_month, 
            a.year_month AS end_year_month, 
            a.end_month_age, b.amr
           INTO ##asthma_final FROM
            (SELECT id_mcaid, year_month, end_month, past_year, end_month_age, beg_measure_year_month
              FROM ##asthma_denom 
              WHERE enroll_flag = 1 AND rx_any = 1 AND persistent = 1 AND dx_exclude = 0) a
            LEFT JOIN
            (SELECT id_mcaid, end_month, amr FROM ##asthma_amr) b
            ON a.id_mcaid = b.id_mcaid AND a.end_month = b.end_month
            ORDER BY a.id_mcaid, a.end_month")

# Check counts
dbGetQuery(db_claims, "SELECT COUNT(*) FROM ##asthma_final")
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_final GROUP BY end_month ORDER BY end_month")


### Add to performance measurement table
### Remove any existing rows for this measure
tbl_id_meta <- DBI::Id(catalog = "PHClaims", schema = "stage", table = "mcaid_perf_measure")
if (dbExistsTable(db_claims, tbl_id_meta) == T) {
  dbGetQuery(db_claims, "DELETE FROM stage.mcaid_perf_measure WITH (TABLOCK) 
             WHERE measure_id = 19;")
  
  dbGetQuery(db_claims,
             paste0("INSERT INTO stage.mcaid_perf_measure WITH (TABLOCK)
                    SELECT a.beg_year_month, a.end_year_month, 
                      a.id_mcaid, a.end_month_age, b.age_grp_10 AS age_grp, 
                      'measure_id' = 19, 'denominator' = 1, 
                      CASE WHEN a.amr >= 0.5 THEN 1 ELSE 0 END AS numerator,
                      load_date = '", Sys.Date() , "'
                    FROM
                      (SELECT beg_year_month, end_year_month, id_mcaid, end_month_age, amr
                      FROM ##asthma_final) a
                      LEFT JOIN
                      (SELECT age, age_grp_10 FROM ref.age_grp) b
                      ON a.end_month_age = b.age"))
} else if(dbExistsTable(db_claims, "stage.mcaid_perf_measure") == F) {
  dbGetQuery(db_claims,
             paste0("SELECT a.beg_year_month, a.end_year_month, 
                    a.id_mcaid, a.end_month_age, b.age_grp_10 AS age_grp, 
                    'measure_id' = 19, 'denominator' = 1, 
                    CASE WHEN a.amr >= 0.5 THEN 1 ELSE 0 END AS numerator,
                    load_date = '", Sys.Date() , "' 
                    INTO stage.mcaid_perf_measures
                    FROM
                    (SELECT beg_year_month, end_year_month, id_mcaid, end_month_age, amr
                    FROM ##asthma_final) a
                    LEFT JOIN
                    (SELECT age, age_grp_10 FROM ref.age_grp) b
                    ON a.end_month_age = b.age"))
}


### For more relaxed version of AMR measure, ignore asthma dx in prev year
try(dbRemoveTable(db_claims, "##asthma_final_1yr", temporary = T))
dbGetQuery(db_claims,
           "SELECT a.id_mcaid, a.end_month, beg_measure_year_month AS beg_year_month, 
            a.year_month AS end_year_month, 
            a.end_month_age, b.amr
           INTO ##asthma_final_1yr FROM
            (SELECT id_mcaid, year_month, end_month, past_year, end_month_age, beg_measure_year_month
              FROM ##asthma_denom 
              WHERE enroll_flag = 1 AND rx_any = 1 AND dx_exclude = 0) a
            LEFT JOIN
            (SELECT id_mcaid, end_month, amr FROM ##asthma_amr) b
            ON a.id_mcaid = b.id_mcaid AND a.end_month = b.end_month
            ORDER BY a.id_mcaid, a.end_month")

# Check counts
dbGetQuery(db_claims, "SELECT COUNT(*) FROM ##asthma_final_1yr")
dbGetQuery(db_claims, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_final_1yr GROUP BY end_month ORDER BY end_month")


### Add to performance measurement table
### Remove any existing rows for this measure
tbl_id_meta <- DBI::Id(catalog = "PHClaims", schema = "stage", table = "mcaid_perf_measure")
if (dbExistsTable(db_claims, tbl_id_meta) == T) {
  dbGetQuery(db_claims, "DELETE FROM stage.mcaid_perf_measure WITH (TABLOCK) 
             WHERE measure_id = 20;")
  
  dbGetQuery(db_claims,
             paste0("INSERT INTO stage.mcaid_perf_measure WITH (TABLOCK)
                    SELECT a.beg_year_month, a.end_year_month, 
                    a.id_mcaid, a.end_month_age, b.age_grp_10 AS age_grp, 
                    'measure_id' = 20, 'denominator' = 1, 
                    CASE WHEN a.amr >= 0.5 THEN 1 ELSE 0 END AS numerator,
                    load_date = '", Sys.Date() , "'
                    FROM
                    (SELECT beg_year_month, end_year_month, id_mcaid, end_month_age, amr
                    FROM ##asthma_final_1yr) a
                    LEFT JOIN
                    (SELECT age, age_grp_10 FROM ref.age_grp) b
                    ON a.end_month_age = b.age"))
} else if(dbExistsTable(db_claims, "stage.mcaid_perf_measure") == F) {
  dbGetQuery(db_claims,
             paste0("SELECT a.beg_year_month, a.end_year_month, 
                    a.id_mcaid, a.end_month_age, b.age_grp_10 AS age_grp, 
                    'measure_id' = 20, 'denominator' = 1, 
                    CASE WHEN a.amr >= 0.5 THEN 1 ELSE 0 END AS numerator,
                    load_date = '", Sys.Date() , "' 
                    INTO stage.mcaid_perf_measures
                    FROM
                    (SELECT beg_year_month, end_year_month, id_mcaid, end_month_age, amr
                    FROM ##asthma_final_1yr) a
                    LEFT JOIN
                    (SELECT age, age_grp_10 FROM ref.age_grp) b
                    ON a.end_month_age = b.age"))
}



#### See how many people are excluded at each step ####
### Eventually add this as a flag when this code is turned into a function
elig_asthma <- asthma_denom %>% group_by(end_month) %>% summarise(any_asthma = n()) %>% ungroup() %>%
  left_join(., asthma_denom %>% filter(rx_any == 1) %>%
              group_by(end_month) %>% summarise(had_rx = n_distinct(id_mcaid)) %>% ungroup(),
            by = "end_month") %>%
  left_join(., asthma_denom %>% filter(enroll_flag == 1) %>%
              group_by(end_month) %>% summarise(elig_enroll = n_distinct(id_mcaid)) %>% ungroup(),
            by = "end_month") %>%
  left_join(., asthma_denom %>% filter(persistent == 1) %>%
              group_by(end_month) %>% summarise(persistent = n_distinct(id_mcaid)) %>% ungroup(),
            by = "end_month") %>%
  left_join(., asthma_denom %>% filter(dx_exclude == 0) %>%
              group_by(end_month) %>% summarise(dx_exclude = n_distinct(id_mcaid)) %>% ungroup(),
            by = "end_month") %>%
  left_join(., asthma_denom %>% filter(enroll_flag == 1 & persistent == 1) %>%
              group_by(end_month) %>% summarise(elig_enroll_persistent = n_distinct(id_mcaid)) %>% ungroup(),
            by = "end_month") %>%
  left_join(., asthma_denom %>% filter(enroll_flag == 1 & rx_any == 1 & persistent == 1 & dx_exclude == 0) %>%
              group_by(end_month) %>% summarise(all_criteria = n_distinct(id_mcaid)) %>% ungroup(),
            by = "end_month")

write.csv(elig_asthma, file = "//dchs-shares01/dchsdata/DCHSPHClaimsData/Analyses/Alastair/asthma_amr_elig_numbers.csv",
          row.names = F)
