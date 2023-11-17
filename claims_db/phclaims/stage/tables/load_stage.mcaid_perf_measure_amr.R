# Calculate the ages 5-64 asthma medication ratio quality measure (HEDIS)
#
# Measure available here: 
# https://www.medicaid.gov/medicaid/quality-of-care/downloads/medicaid-adult-core-set-manual.pdf
# https://www.medicaid.gov/medicaid/quality-of-care/downloads/medicaid-and-chip-child-core-set-manual.pdf
#
# Alastair Matheson
# APDE, PHSKC
# 2019-04, turned into a function 2021-03
#

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

### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# max_month = date up to which analyses should be run (usually MAX(year_month) FROM stage.mcaid_perf_enroll_denom)
# return_data = returns the denominator data and # people excluded for additional analyses


stage_mcaid_perf_measure_amr_f <- function(conn = NULL,
                                           server = c("hhsaw", "phclaims"),
                                           max_month = NULL,
                                           return_data = F) {
  
  
  #### SET UP PARAMETERS FOR TABLE CREATION ####
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (server == "hhsaw") {
    final_schema <- "claims"
    final_table <- "final_"
    ref_schema <- "claims"
    ref_table <- "ref_"
    stage_schema <- "claims"
    stage_table <- "stage_"
    view_schema <- "claims"
  } else {
    final_schema <- "final"
    final_table <- ""
    ref_schema <- "ref"
    ref_table <- ""
    stage_schema <- "stage"
    stage_table <- ""
    view_schema <- "stage"
  }
  
  
  # Find the most recent month we have enrollment summaries for
  # Comes in as year-month
  if (is.null(max_month)) {
    max_month <- unlist(dbGetQuery(conn, 
                                   glue::glue_sql("SELECT MAX(year_month) 
                                                  FROM {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_enroll_denom",
                                                  .con = conn)))
  }
  
  # Now find last day of the month for going forward a month then back a day
  max_month <- as.Date(parse_date_time(max_month, "Ym") %m+% months(1) - days(1))
  
  # Set up years to run over
  months_list <- as.list(seq(as.Date("2013-01-01"), as.Date(max_month) + 1, by = "year") - 1)
  
  
  #### PART 1 - GENERATE DENOMINATOR POPULATION ####
  message("PArt 1 - Generate denominator population")
  
  #### Start with people who were enrolled for at least 11 months ####
  # Currently making use of a temp table that will become permanent after QA processes
  
  # For the first quarter, create the temp table. Need to set a counter
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT id_mcaid, year_month, end_month_age 
                      FROM {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_enroll_denom
                      WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND 
                      end_month_age >= 5 AND end_month_age < 65) a 
                     LEFT JOIN
                     (SELECT year_month, end_month, beg_measure_year_month 
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month) b 
                     ON a.year_month = b.year_month
                     WHERE b.end_month = {x}",
                               .con = conn)
    
    if (i == 1) {
      # Note: DBI package only supports the temporary option for dbRemoveTable
      # not for dbExistsTable. Use try() in the mean time so that the code
      # continues even if an error is thrown trying to remove a table that
      # doesn't exist
      try(dbRemoveTable(conn, "##asthma_pop", temporary = T), silent = T)
      
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT a.id_mcaid, a.year_month, b.end_month, a.end_month_age, 
                        b.beg_measure_year_month, 1 AS [enroll_flag]
                       INTO ##asthma_pop FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_pop 
                       SELECT a.id_mcaid, a.year_month, b.end_month, a.end_month_age, 
                        b.beg_measure_year_month, 1 AS [enroll_flag] 
                       FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count FROM ##asthma_pop GROUP BY end_month ORDER BY end_month"))
  
  
  #### Find events that define someone with asthma ####
  ### Make temp table of everyone with an asthma definition
  try(dbRemoveTable(conn, "##asthma_dx", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql(
                 "SELECT a.id_mcaid, a.claim_header_id, a.first_service_date, b.icdcm_number, 'asthma' = 1
           INTO ##asthma_dx
           FROM 
           (SELECT id_mcaid, claim_header_id, first_service_date
             FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_header) a
           INNER JOIN
           (SELECT id_mcaid, claim_header_id, icdcm_norm, icdcm_version, icdcm_number
             FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header) b
           ON a.id_mcaid = b.id_mcaid AND a.claim_header_id = b.claim_header_id
           INNER JOIN
           (SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS dx_ver 
             FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde 
             WHERE value_set_name = 'Asthma') c 
           ON b.icdcm_norm = c.code AND b.icdcm_version = c.dx_ver",
                 .con = conn))
  
  # Check results
  print(dbGetQuery(conn, "SELECT COUNT(*) AS count FROM ##asthma_dx"))
  
  
  #### 1+ ED or inpatient visits with primary asthma dx in the past 12 months ####
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT id_mcaid, claim_header_id, first_service_date, last_service_date, ed_perform, inpatient 
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_header 
                      WHERE (ed_perform = 1 OR inpatient = 1) AND 
                      first_service_date <= {x} AND 
                      first_service_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x}))) a 
                     INNER JOIN 
                     (SELECT id_mcaid, claim_header_id FROM ##asthma_dx WHERE icdcm_number = '01') b 
                       ON a.id_mcaid = b.id_mcaid AND a.claim_header_id = b.claim_header_id 
                       GROUP BY a.id_mcaid",
                               .con = conn)
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_ed_inpat", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT a.id_mcaid, 'end_month' = {x}, 
                      SUM(a.ed_perform) AS ed_cnt, SUM(a.inpatient) AS inpat_cnt 
                      INTO ##asthma_ed_inpat FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_ed_inpat 
                      SELECT a.id_mcaid, 'end_month' = {x}, 
                      SUM(a.ed_perform) AS ed_cnt, SUM(a.inpatient) AS inpat_cnt 
                      FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count FROM ##asthma_ed_inpat GROUP BY end_month ORDER BY end_month"))
  
  
  #### 4+ outpatient visits with any asthma dx AND 2+ asthma med dispensing events ####
  # Apply 2+ med events later when joined
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT id_mcaid, claim_header_id, first_service_date 
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_header 
                      WHERE first_service_date <= {x} AND 
                      first_service_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x}))) a 
                     INNER JOIN 
                     (SELECT id_mcaid, claim_header_id FROM ##asthma_dx) b 
                       ON a.id_mcaid = b.id_mcaid AND a.claim_header_id = b.claim_header_id 
                       INNER JOIN 
                       (SELECT id_mcaid, claim_header_id, procedure_code 
                       FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_procedure) c 
                       ON a.id_mcaid = c.id_mcaid AND a.claim_header_id = c.claim_header_id 
                       INNER JOIN 
                       (SELECT code FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde
                         WHERE value_set_name = 'Outpatient') d 
                       ON c.procedure_code = d.code 
                       GROUP BY a.id_mcaid",
                               .con = conn)
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_outpat", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT a.id_mcaid, 'end_month' = {x}, 
                      COUNT(DISTINCT a.first_service_date) AS outpat_cnt 
                      INTO ##asthma_outpat FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_outpat 
                      SELECT a.id_mcaid, 'end_month' = {x}, 
                      COUNT(DISTINCT a.first_service_date) AS outpat_cnt 
                      FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count FROM ##asthma_outpat GROUP BY end_month ORDER BY end_month"))
  
  
  #### 4+ asthma dispensing events ####
  #### Set up code just for oral meds ####
  # Need to sum up days prescribed for each drug type then calculate number of events (0-30 days = 1 event, each full 30 days beyond = another event)
  # Also need to separate out luekotriene-only counts (part of inclusion criteria)
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT a.id_mcaid, a.rx_fill_date, b.generic_product_name, 
                      CASE WHEN SUM(a.rx_days_supply) <= 30 THEN 1
                      WHEN SUM(a.rx_days_supply) > 30 THEN FLOOR(SUM(a.rx_days_supply) / 30)
                      END AS drug_events
                      FROM 
                      (SELECT id_mcaid, ndc, rx_fill_date, rx_days_supply
                        FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_pharm
                        WHERE rx_fill_date <= {x} AND 
                        rx_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x}))) a 
                      INNER JOIN
                      (SELECT medication_list_name, [code] AS 'ndc_code', generic_product_name, [route], drug_class AS 'description'
                        FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_medication_lists_apde 
                        WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications') 
                        AND [route] = 'oral' AND [drug_class] = 'Leukotriene modifiers' 
                        AND code_system = 'NDC') b
                      ON a.ndc = b.ndc_code
                      GROUP BY a.id_mcaid, a.rx_fill_date, b.generic_product_name) c 
                     GROUP BY c.id_mcaid, c.rx_fill_date
                     ORDER BY c.id_mcaid, c.rx_fill_date",
                               .con = conn)
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_rx_event_oral_lk", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT c.id_mcaid, 'end_month' = {x}, 
                      c.rx_fill_date, SUM(c.drug_events) AS events_oral_lk
                      INTO ##asthma_rx_event_oral_lk FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_rx_event_oral_lk 
                      SELECT c.id_mcaid, 'end_month' = {x}, 
                      c.rx_fill_date, SUM(c.drug_events) AS events_oral_lk
                       FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_oral_lk GROUP BY end_month ORDER BY end_month"))
  
  
  #### Non-luekotriene inhibitors ####
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT a.id_mcaid, a.rx_fill_date, b.generic_product_name, 
                      CASE WHEN SUM(a.rx_days_supply) <= 30 THEN 1
                      WHEN SUM(a.rx_days_supply) > 30 THEN FLOOR(SUM(a.rx_days_supply) / 30)
                      END AS drug_events
                      FROM 
                      (SELECT id_mcaid, ndc, rx_fill_date, rx_days_supply
                        FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_pharm
                        WHERE rx_fill_date <= {x} AND 
                        rx_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x}))) a 
                      INNER JOIN
                      (SELECT medication_list_name, [code] AS 'ndc_code', generic_product_name, [route], drug_class AS 'description'
                        FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_medication_lists_apde 
                        WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications') 
                        AND [route] = 'oral' AND [drug_class] <> 'Leukotriene modifiers'
                        AND code_system = 'NDC') b
                      ON a.ndc = b.ndc_code
                      GROUP BY a.id_mcaid, a.rx_fill_date, b.generic_product_name) c 
                     GROUP BY c.id_mcaid, c.rx_fill_date
                     ORDER BY c.id_mcaid, c.rx_fill_date",
                               .con = conn)
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_rx_event_oral_non_lk", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT c.id_mcaid, 'end_month' = {x}, 
                      c.rx_fill_date, SUM(c.drug_events) AS events_oral_non_lk
                      INTO ##asthma_rx_event_oral_non_lk FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_rx_event_oral_non_lk 
                      SELECT c.id_mcaid, 'end_month' = {x}, 
                      c.rx_fill_date, SUM(c.drug_events) AS events_oral_non_lk
                      FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_oral_non_lk GROUP BY end_month ORDER BY end_month"))
  
  
  #### Set up code just for inhalers ####
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT id_mcaid, ndc, rx_fill_date
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_pharm
                      WHERE rx_fill_date <= {x} AND 
                      rx_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x}))) a 
                     INNER JOIN
                     (SELECT medication_list_name, [code] AS 'ndc_code', generic_product_name, [route]
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_medication_lists_apde
                       WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications')
                       AND [route] = 'inhalation' AND code_system = 'NDC') b 
                     ON a.ndc = b.ndc_code
                     GROUP BY a.id_mcaid, a.rx_fill_date
                     ORDER BY a.id_mcaid, a.rx_fill_date",
                               .con = conn)
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_rx_event_inhaler", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT a.id_mcaid, 'end_month' = {x}, 
                      a.rx_fill_date, COUNT (DISTINCT b.generic_product_name) AS events_inhaler
                      INTO ##asthma_rx_event_inhaler FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_rx_event_inhaler
                      SELECT a.id_mcaid, 'end_month' = {x}, 
                      a.rx_fill_date, COUNT (DISTINCT b.generic_product_name) AS events_inhaler
                      FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_inhaler GROUP BY end_month ORDER BY end_month"))
  
  
  #### Set up code just for injections ####
  # Need to separate out antibody inhibitor-only counts
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT id_mcaid, ndc, rx_fill_date
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_pharm
                      WHERE rx_fill_date <= {x} AND 
                      rx_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x}))) a 
                     INNER JOIN
                     (SELECT medication_list_name, [code] AS 'ndc_code', [route], drug_class AS 'description'
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_medication_lists_apde
                       WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications')
                       AND [route] IN ('intravenous', 'subcutaneous') AND drug_class = 'Antibody inhibitor'
                       AND code_system = 'NDC') b
                     ON a.ndc = b.ndc_code
                     GROUP BY a.id_mcaid, a.rx_fill_date, a.ndc
                     ORDER BY a.id_mcaid, a.rx_fill_date",
                               .con = conn)
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_rx_event_inject_antib", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT a.id_mcaid, 'end_month' = {x}, 
                      a.rx_fill_date, COUNT (*) AS events_inject_antib
                      INTO ##asthma_rx_event_inject_antib FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_rx_event_inject_antib
                      SELECT a.id_mcaid, 'end_month' = {x}, 
                      a.rx_fill_date, COUNT (*) AS events_inject_antib
                      FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_inject_antib GROUP BY end_month ORDER BY end_month"))
  
  
  #### Non-antibody inhibitor counts ####
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT id_mcaid, ndc, rx_fill_date 
               FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_pharm 
               WHERE rx_fill_date <= {x} AND 
               rx_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x}))) a 
              INNER JOIN
              (SELECT medication_list_name, [code] AS 'ndc_code', [route], drug_class AS 'description'
              FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_medication_lists_apde
              WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications') 
                AND [route] IN ('intravenous', 'subcutaneous') AND drug_class <> 'Antibody inhibitor'
                AND code_system = 'NDC') b 
              ON a.ndc = b.ndc_code
              GROUP BY a.id_mcaid, a.rx_fill_date, a.ndc 
              ORDER BY a.id_mcaid, a.rx_fill_date",
                               .con = conn)
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_rx_event_inject_non_antib", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT a.id_mcaid, 'end_month' = {x}, 
                      a.rx_fill_date, COUNT (*) AS events_inject_non_antib
                      INTO ##asthma_rx_event_inject_non_antib FROM {sql_temp}", 
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_rx_event_inject_non_antib
                      SELECT a.id_mcaid, 'end_month' = {x}, 
                      a.rx_fill_date, COUNT (*) AS events_inject_non_antib
                      FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event_inject_non_antib GROUP BY end_month ORDER BY end_month"))
  
  
  #### Combine rx temp tables and sum events ####
  # Join to dx table to check if people meet the dx requirement
  
  # Make collated table outside of loop to avoid recreating it
  try(dbRemoveTable(conn, "##asthma_rx_event_temp", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 "SELECT g.id_mcaid, g.end_month, SUM(events_rx) AS events_rx,
           CASE WHEN SUM(events_rx) = SUM(dx_needed_cnt) THEN 1 ELSE 0 END AS dx_needed
           INTO ##asthma_rx_event_temp
           FROM
           (SELECT f.id_mcaid, f.end_month, f.rx_fill_date,
             f.events_oral_lk + f.events_oral_non_lk + f.events_inhaler + 
               f.events_inject_antib + f.events_inject_non_antib AS events_rx,
             f.events_oral_lk + f.events_inject_antib AS dx_needed_cnt
            FROM
            (SELECT COALESCE(a.id_mcaid, b.id_mcaid, c.id_mcaid, d.id_mcaid, e.id_mcaid) as id_mcaid,
               COALESCE(a.end_month, b.end_month, c.end_month, d.end_month, 
                        e.end_month) AS end_month,
               COALESCE(a.rx_fill_date, b.rx_fill_date, c.rx_fill_date, 
                        d.rx_fill_date, e.rx_fill_date) AS rx_fill_date,
               ISNULL(a.events_oral_lk, 0) AS events_oral_lk,
               ISNULL(b.events_oral_non_lk, 0) AS events_oral_non_lk,
               ISNULL(c.events_inhaler, 0) AS events_inhaler,
               ISNULL(d.events_inject_antib, 0) AS events_inject_antib,
               ISNULL(e.events_inject_non_antib, 0) AS events_inject_non_antib
            FROM 
               (SELECT id_mcaid, end_month, rx_fill_date, events_oral_lk
                FROM ##asthma_rx_event_oral_lk) a 
                FULL JOIN 
                (SELECT id_mcaid, end_month, rx_fill_date, events_oral_non_lk
                FROM ##asthma_rx_event_oral_non_lk) b 
                ON a.id_mcaid = b.id_mcaid AND a.end_month = b.end_month AND a.rx_fill_date = b.rx_fill_date
                FULL JOIN 
                (SELECT id_mcaid, end_month, rx_fill_date, events_inhaler
                FROM ##asthma_rx_event_inhaler) c 
                ON COALESCE(a.id_mcaid, b.id_mcaid) = c.id_mcaid AND COALESCE(a.end_month, b.end_month) = c.end_month AND 
                  COALESCE(a.rx_fill_date, b.rx_fill_date) = c.rx_fill_date
                FULL JOIN 
                (SELECT id_mcaid, end_month, rx_fill_date, events_inject_antib
                FROM ##asthma_rx_event_inject_antib) d 
                ON COALESCE(a.id_mcaid, b.id_mcaid, c.id_mcaid) = d.id_mcaid AND 
                  COALESCE(a.end_month, b.end_month, c.end_month) = d.end_month AND 
                  COALESCE(a.rx_fill_date, b.rx_fill_date, c.rx_fill_date) = d.rx_fill_date
                FULL JOIN 
                (SELECT id_mcaid, end_month, rx_fill_date, events_inject_non_antib
                FROM ##asthma_rx_event_inject_non_antib) e 
                ON COALESCE(a.id_mcaid, b.id_mcaid, c.id_mcaid, d.id_mcaid) = e.id_mcaid AND 
                  COALESCE(a.end_month, b.end_month, c.end_month, d.end_month) = e.end_month AND 
                  COALESCE(a.rx_fill_date, b.rx_fill_date, c.rx_fill_date, d.rx_fill_date) = e.rx_fill_date
                ) f 
              GROUP BY f.id_mcaid, f.end_month, f.rx_fill_date, f.events_oral_lk, f.events_oral_non_lk, 
                f.events_inhaler, f.events_inject_antib, f.events_inject_non_antib) g 
              GROUP BY g.id_mcaid, g.end_month")
  
  
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT id_mcaid, end_month, events_rx, dx_needed
                      FROM ##asthma_rx_event_temp 
                      WHERE end_month = {x}) h 
                      LEFT JOIN 
                      (SELECT DISTINCT id_mcaid, 'dx_made' = 1
                      FROM ##asthma_dx WHERE first_service_date <= {x} AND 
                        first_service_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x}))) i 
                      ON h.id_mcaid = i.id_mcaid",
                               .con = conn)
    
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_rx_event", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT h.id_mcaid, h.end_month, h.events_rx, h.dx_needed, ISNULL(i.dx_made, 0) AS dx_made
                      INTO ##asthma_rx_event FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_rx_event
                      SELECT h.id_mcaid, h.end_month, h.events_rx, h.dx_needed, ISNULL(i.dx_made, 0) AS dx_made
                      FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count FROM ##asthma_rx_event GROUP BY end_month ORDER BY end_month"))
  
  
  #### 1B - FIND PEOPLE WHO HAVE EXCLUSION CRITERIA ####
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT id_mcaid, claim_header_id
                      FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_header
                      WHERE first_service_date <= {x}) a
                     LEFT JOIN
                     (SELECT id_mcaid, claim_header_id, icdcm_norm, icdcm_version
                       FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_icdcm_header) b
                     ON a.id_mcaid = b.id_mcaid AND a.claim_header_id = b.claim_header_id
                     INNER JOIN
                     (SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS icdcm_version
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde
                       WHERE value_set_name = 'Emphysema'
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS icdcm_version
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde
                       WHERE value_set_name = 'Other Emphysema'
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS icdcm_version
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde
                       WHERE value_set_name = 'COPD'
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS icdcm_version
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde
                       WHERE value_set_name = 'Obstructive Chronic Bronchitis' 
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS icdcm_version
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde
                       WHERE value_set_name = 'Chronic Respiratory Conditions Due To Fumes/Vapors' 
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS icdcm_version
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde
                       WHERE value_set_name = 'Cystic Fibrosis' 
                       UNION
                       SELECT code, CASE WHEN SUBSTRING(code_system, 4, 1) = '9' THEN 9 ELSE 10 END AS icdcm_version
                       FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_value_sets_apde
                       WHERE value_set_name = 'Acute Respiratory Failure' 
                     ) c
                     ON b.icdcm_norm = c.code AND b.icdcm_version = c.icdcm_version",
                               .con = conn)
    
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_excl", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT DISTINCT a.id_mcaid, 'end_month' = {x}
                      INTO ##asthma_excl FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_excl
                      SELECT DISTINCT a.id_mcaid, 'end_month' = {x} 
                      FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_excl GROUP BY end_month ORDER BY end_month"))
  
  
  #### 1C - BRING POPULATIONS TOGETHER ####
  ### Quantify how many people at each stage by adding flags
  
  ### See how many met any of the asthma inclusion criteria for a single year
  # Also include last year's date for later code
  try(dbRemoveTable(conn, "##asthma_any", temporary = T), silent = T)
  DBI::dbExecute(conn,
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
                beg_measure_year_month, enroll_flag 
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
  dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_any GROUP BY end_month ORDER BY end_month")
  
  
  ### Apply check of persistent asthma (i.e., see if they had asthma the previous year)
  try(dbRemoveTable(conn, "##asthma_persist", temporary = T), silent = T)
  DBI::dbExecute(conn,
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
  dbGetQuery(conn, "SELECT end_month, persistent,  COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_persist GROUP BY end_month, persistent ORDER BY end_month, persistent")
  
  
  ### Remove people with exclusion criteria
  try(dbRemoveTable(conn, "##asthma_denom", temporary = T), silent = T)
  DBI::dbExecute(conn,
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
  dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_denom GROUP BY end_month ORDER BY end_month")
  dbGetQuery(conn, "SELECT end_month, persistent,  COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_denom GROUP BY end_month, persistent ORDER BY end_month, persistent")
  dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_denom WHERE enroll_flag = 1 AND rx_any = 1 AND persistent = 1 AND  dx_exclude = 0 
           GROUP BY end_month ORDER BY end_month")
  
  
  
  #### PART 2 - GENERATE NUMERATOR DATA ####
  message("Part 2 - Generate numerator data")
  
  #### 2A - CALCULATE UNITS OF MEDICATION ####
  ### Set up general linked claims and drug data to avoid rerunning it for each date
  # NB. Calc for oral meds differs from how events are calculated 
  #   (partial 30 days beyond the initial 30 count are included here).
  # Using ceiling for inhaler because some claims had a dispensed amount of 
  #   6.7 and a package size of 7.
  try(dbRemoveTable(conn, "##asthma_rx_meds_temp", temporary = T), silent = T)
  DBI::dbExecute(conn,
                 glue::glue_sql(
                 "SELECT c.id_mcaid, c.medication_list_name, c.rx_fill_date, c.[route], 
            c.generic_product_name, c.med_units
           INTO ##asthma_rx_meds_temp
           FROM
           (SELECT a.id_mcaid, b.medication_list_name, a.rx_fill_date, b.[route], b.generic_product_name, 
             CASE 
              WHEN b.[route] = 'oral' AND SUM(a.rx_days_supply) <= 30 THEN 1
              WHEN b.[route] = 'oral' AND SUM(a.rx_days_supply) > 30 THEN CEILING(SUM(a.rx_days_supply) / 30)
              WHEN b.[route] IN ('intravenous', 'subcutaneous') THEN CEILING(a.rx_quantity/b.package_size) 
              WHEN b.[route] = 'inhalation' THEN 1
              END AS med_units
            FROM 
            (SELECT id_mcaid, ndc, rx_fill_date, rx_days_supply, rx_quantity
            FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_claim_pharm) a 
            INNER JOIN
            (SELECT medication_list_name, [code] AS 'ndc_code', generic_product_name, [route], package_size
            FROM {`ref_schema`}.{DBI::SQL(ref_table)}hedis_medication_lists_apde
            WHERE  medication_list_name IN ('Asthma Controller Medications', 'Asthma Reliever Medications')
              AND code_system = 'NDC') b
            ON a.ndc = b.ndc_code
            GROUP BY a.id_mcaid, b.medication_list_name, a.rx_fill_date, b.[route], 
              b.generic_product_name, a.rx_quantity, b.package_size) c",
                 .con = conn))
  
  # Check results
  print(dbGetQuery(conn, "SELECT COUNT(*) AS count FROM ##asthma_rx_meds_temp"))
  
  
  #### 2A - CALCULATE AMR ####
  i <- 1
  lapply(months_list, function(x) {
    
    sql_temp <- glue::glue_sql("(SELECT COALESCE(a.id_mcaid, b.id_mcaid) AS id_mcaid, 'end_month' = {x}, 
                     ISNULL(a.meds_control, 0) AS meds_control,
                     ISNULL(b.meds_relief, 0) AS meds_relief 
                     FROM
                        (SELECT id_mcaid, SUM(med_units) AS meds_control
                        FROM ##asthma_rx_meds_temp
                        WHERE rx_fill_date <= {x} AND 
                          rx_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x})) AND 
                          medication_list_name = 'Asthma Controller Medications'
                        GROUP BY id_mcaid) a
                        FULL JOIN
                        (SELECT id_mcaid, SUM(med_units) AS meds_relief
                        FROM ##asthma_rx_meds_temp
                        WHERE rx_fill_date <= {x} AND 
                          rx_fill_date >= DATEADD(DAY, 1, DATEADD(YEAR, -1, {x})) AND 
                          medication_list_name = 'Asthma Reliever Medications'
                        GROUP BY id_mcaid) b
                        ON a.id_mcaid = b.id_mcaid) c
                    ORDER BY id_mcaid",
                               .con = conn)
    
    if (i == 1) {
      try(dbRemoveTable(conn, "##asthma_amr", temporary = T), silent = T)
      DBI::dbExecute(conn, 
                     glue::glue_sql("SELECT c.id_mcaid, c.end_month, c.meds_control, c.meds_relief,
                          ISNULL(c.meds_control / (c.meds_control + c.meds_relief), 0) AS amr 
                      INTO ##asthma_amr FROM {sql_temp}",
                                    .con = conn))
      i <<- i + 1
    } else {
      DBI::dbExecute(conn, 
                     glue::glue_sql("INSERT INTO ##asthma_amr 
                      SELECT c.id_mcaid, c.end_month, c.meds_control, c.meds_relief,
                      ISNULL(c.meds_control / (c.meds_control + c.meds_relief), 0) AS amr 
                      FROM {sql_temp}",
                                    .con = conn))
    }
  })
  
  # Check results
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count, COUNT(DISTINCT id_mcaid) AS cnt_id 
           FROM ##asthma_amr GROUP BY end_month ORDER BY end_month"))
  
  print(dbGetQuery(conn, "SELECT TOP(20) * FROM ##asthma_amr ORDER BY id_mcaid, end_month"))
  
  
  
  #### PART 3 - BRING NUMERATOR AND DENOMINATOR TOGETHER ####
  message("Part 3 - Bring numerator and denominator together")
  
  ### For full HEDIS measure, require persistent asthma
  try(dbRemoveTable(conn, "##asthma_final", temporary = T), silent = T)
  DBI::dbExecute(conn,
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
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_final GROUP BY end_month ORDER BY end_month"))
  
  
  #### PART 4 - ADD TO PERFORMANCE MEASUREMENT TABLE ####
  message("Part 4 - Add to performance measurement table")
  ### Remove any existing rows for this measure
  tbl_id_meta <- DBI::Id(schema = stage_schema, table = glue::glue_sql(stage_table, "mcaid_perf_measure"))
  if (dbExistsTable(conn, tbl_id_meta) == T) {
    dbGetQuery(conn, 
               glue::glue_sql("DELETE FROM {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_measure WITH (TABLOCK) 
                 WHERE measure_id = 19;",
                              .con = conn))
    
    DBI::dbExecute(conn,
                   glue::glue_sql("INSERT INTO {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_measure WITH (TABLOCK)
                    SELECT a.beg_year_month, a.end_year_month, 
                      a.id_mcaid, a.end_month_age, b.age_grp_10 AS age_grp, 
                      'measure_id' = 19, 'denominator' = 1, 
                      CASE WHEN a.amr >= 0.5 THEN 1 ELSE 0 END AS numerator,
                      load_date = {Sys.Date()}
                    FROM
                      (SELECT beg_year_month, end_year_month, id_mcaid, end_month_age, amr
                      FROM ##asthma_final) a
                      LEFT JOIN
                      (SELECT age, age_grp_10 FROM {`ref_schema`}.{DBI::SQL(ref_table)}age_grp) b
                      ON a.end_month_age = b.age",
                                  .con = conn))
  } else {
    DBI::dbExecute(conn,
                   glue::glue_sql("SELECT a.beg_year_month, a.end_year_month, 
                    a.id_mcaid, a.end_month_age, b.age_grp_10 AS age_grp, 
                    'measure_id' = 19, 'denominator' = 1, 
                    CASE WHEN a.amr >= 0.5 THEN 1 ELSE 0 END AS numerator,
                    load_date =  {Sys.Date()}
                    INTO {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_measure
                    FROM
                    (SELECT beg_year_month, end_year_month, id_mcaid, end_month_age, amr
                    FROM ##asthma_final) a
                    LEFT JOIN
                    (SELECT age, age_grp_10 FROM {`ref_schema`}.{DBI::SQL(ref_table)}age_grp) b
                    ON a.end_month_age = b.age",
                                  .con = conn))
  }
  
  
  ### For more relaxed version of AMR measure, ignore asthma dx in prev year
  try(dbRemoveTable(conn, "##asthma_final_1yr", temporary = T), silent = T)
  DBI::dbExecute(conn,
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
  print(dbGetQuery(conn, "SELECT end_month, COUNT(*) AS count, COUNT (DISTINCT id_mcaid) AS count_id 
           FROM ##asthma_final_1yr GROUP BY end_month ORDER BY end_month"))
  
  
  ### Add to performance measurement table
  ### Remove any existing rows for this measure
  if (dbExistsTable(conn, tbl_id_meta) == T) {
    DBI::dbExecute(conn, 
    glue::glue_sql("DELETE FROM {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_measure WITH (TABLOCK) 
             WHERE measure_id = 20;",
                   .con = conn))
    
    DBI::dbExecute(conn,
                   glue::glue_sql("INSERT INTO {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_measure WITH (TABLOCK)
                    SELECT a.beg_year_month, a.end_year_month, 
                    a.id_mcaid, a.end_month_age, b.age_grp_10 AS age_grp, 
                    'measure_id' = 20, 'denominator' = 1, 
                    CASE WHEN a.amr >= 0.5 THEN 1 ELSE 0 END AS numerator,
                    load_date = {Sys.Date()}
                    FROM
                    (SELECT beg_year_month, end_year_month, id_mcaid, end_month_age, amr
                    FROM ##asthma_final_1yr) a
                    LEFT JOIN
                    (SELECT age, age_grp_10 FROM {`ref_schema`}.{DBI::SQL(ref_table)}age_grp) b
                    ON a.end_month_age = b.age",
                                  .con = conn))
  } else {
    DBI::dbExecute(conn,
                   glue::glue_sql("SELECT a.beg_year_month, a.end_year_month, 
                    a.id_mcaid, a.end_month_age, b.age_grp_10 AS age_grp, 
                    'measure_id' = 20, 'denominator' = 1, 
                    CASE WHEN a.amr >= 0.5 THEN 1 ELSE 0 END AS numerator,
                    load_date = {Sys.Date()}
                    INTO {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_measure
                    FROM
                    (SELECT beg_year_month, end_year_month, id_mcaid, end_month_age, amr
                    FROM ##asthma_final_1yr) a
                    LEFT JOIN
                    (SELECT age, age_grp_10 FROM {`ref_schema`}.{DBI::SQL(ref_table)}age_grp) b
                    ON a.end_month_age = b.age",
                                  .con = conn))
  }
  
  
  #### RETURN DATA IF DESIRED ####
  if (return_data == T) {
    # Pull into R for additional analyses
    asthma_denom <- DBI::dbReadTable(conn, "##asthma_denom")
    
    # See how many people are excluded at each step
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
    
    output <- list(asthma_denom = asthma_denom,
                   elig_asthma = elig_asthma)
    
    return(output)
  }
  
  #### CLEAN UP ####
  try(dbRemoveTable(conn, "##asthma_pop", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_dx", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_ed_inpat", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_outpat", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_final_1yr", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_rx_event_oral_lk", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_rx_event_oral_non_lk", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_rx_event_inhaler", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_rx_event_inject_antib", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_rx_event_inject_non_antib", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_rx_event_temp", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_rx_event", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_excl", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_any", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_persist", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_denom", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_rx_meds_temp", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_amr", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_final", temporary = T), silent = T)
  try(dbRemoveTable(conn, "##asthma_final_1yr", temporary = T), silent = T)
  
  
  #### Return data after removing temp tables #####
  # Need to use this order because ##asthma_denom is used to create the output
  if (return_data == T) {
    return(output)
  }
}