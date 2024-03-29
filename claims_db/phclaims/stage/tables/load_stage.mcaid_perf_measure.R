### Function to generate performance measures
#
# Alastair Matheson, based on Philip Sylling's SQL code
# 2020-09
#

### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# measure = which performance measure to run

stage_mcaid_perf_measure_f <- function(conn = NULL,
                                       server = c("hhsaw", "phclaims"),
                                       measure = c("Acute Hospital Utilization",
                                                   "All-Cause ED Visits",
                                                   "Child and Adolescent Access to Primary Care",
                                                   "Follow-up ED visit for Alcohol/Drug Abuse",
                                                   "Follow-up ED visit for Mental Illness",
                                                   "Follow-up Hospitalization for Mental Illness",
                                                   "Mental Health Treatment Penetration",
                                                   "SUD Treatment Penetration",
                                                   "SUD Treatment Penetration (Opioid)",
                                                   "Plan All-Cause Readmissions (30 days)"),
                                       end_month = NULL,
                                       config = NULL,
                                       get_config = F) {
  
  
  
  #### SET VARIABLES ####
  server <- match.arg(server)
  measure <- match.arg(measure)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  stage_schema <- config[[server]][["stage_schema"]]
  stage_table <- ifelse(is.null(config[[server]][["stage_table"]]), '',
                        config[[server]][["stage_table"]])
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- ifelse(is.null(config[[server]][["ref_table"]]), '',
                      config[[server]][["ref_table"]])
  
  
  #### GENERAL MEASURES ####
  # Acute Hospital Utilization
  # All-Cause ED Visits
  # Child and Adolescent Access to Primary Care
  # Mental Health Treatment Penetration
  # SUD Treatment Penetration
  # SUD Treatment Penetration (Opioid)
  
  
  ### Set up SQL code based on the indicator
  # Age group
  if (measure %in% c("Acute Hospital Utilization",
                     "All-Cause ED Visits")) {
    age_grp <- DBI::SQL('')
  } else if (measure %in% c("Child and Adolescent Access to Primary Care",
                            "Mental Health Treatment Penetration",
                            "SUD Treatment Penetration",
                            "SUD Treatment Penetration (Opioid)")) {
    age_grp <- DBI::SQL(" WHEN ref.[age_group] = 'age_grp_9_months' THEN age.[age_grp_9_months] ")
  }
  
  
  # Prior 12m fields
  if (measure %in% c("Acute Hospital Utilization",
                     "All-Cause ED Visits",
                     "Mental Health Treatment Penetration",
                     "SUD Treatment Penetration",
                     "SUD Treatment Penetration (Opioid)")) {
    full_criteria_prior <- DBI::SQL('')
    hospice_prior <- DBI::SQL('')
    prior_where <- DBI::SQL('')
  } else if (measure %in% c("Child and Adolescent Access to Primary Care")) {
    full_criteria_prior <- DBI::SQL(",den.[full_criteria_prior_t_12_m] ")
    hospice_prior <- DBI::SQL(",den.[hospice_prior_t_12_m] ")
    prior_where <- DBI::SQL("AND CASE WHEN [age_grp] IN ('Age 7-11', 'Age 12-19') THEN [full_criteria_prior_t_12_m] 
                              ELSE 11 END >= 11
                            AND CASE WHEN [age_grp] IN ('Age 7-11', 'Age 12-19') THEN [hospice_prior_t_12_m] 
                              ELSE 0 END = 0 ")
  }
  
  
  # Denominator field
  if (measure %in% c("Acute Hospital Utilization",
                     "Child and Adolescent Access to Primary Care")) {
    denom_field <- DBI::SQL(",1 AS [denominator]")
  } else if (measure %in% c("All-Cause ED Visits")) {
    denom_field <- DBI::SQL(",den.[full_criteria_t_12_m] AS [denominator]")
  } else if (measure %in% c("Mental Health Treatment Penetration",
                            "SUD Treatment Penetration",
                            "SUD Treatment Penetration (Opioid)")) {
    denom_field <- DBI::SQL(",stg_den.[measure_value] AS [denominator]")
  }
  
  # Denominator join
  if (measure %in% c("All-Cause ED Visits",
                     "Acute Hospital Utilization",
                     "Child and Adolescent Access to Primary Care")) {
    denom_join <- DBI::SQL('')
    denom_where <- DBI::SQL('')
  } else if (measure %in% c("Mental Health Treatment Penetration",
                            "SUD Treatment Penetration",
                            "SUD Treatment Penetration (Opioid)")) {
    denom_join <- glue::glue_sql("LEFT JOIN {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_staging AS stg_den
                  ON mem.[id_mcaid] = stg_den.[id_mcaid]
                  AND ym.[year_month] = stg_den.[year_month]
                  --- This JOIN condition gets only utilization rows for the relevant measure
                  AND ref.[measure_id] = stg_den.[measure_id]
                  AND stg_den.[num_denom] = 'D'",
                                 .con = conn)
    denom_where <- DBI::SQL("AND [denominator] = 1")
  }
  
  # Numerator
  if (measure %in% c("All-Cause ED Visits",
                     "Acute Hospital Utilization")) {
     num_field <- DBI::SQL(",SUM(ISNULL(stg_num.[measure_value], 0)) 
                  OVER(PARTITION BY mem.[id_mcaid] 
                       ORDER BY ym.[year_month] 
                       ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]")
     num_load <- DBI::SQL(",[numerator] ")
  } else if (measure %in% c("Child and Adolescent Access to Primary Care")) {
    num_field <- DBI::SQL(",CASE WHEN MAX(ISNULL(stg_num.[measure_value], 0)) 
                              OVER(PARTITION BY mem.[id_mcaid] 
                                   ORDER BY ym.[year_month] 
                                   ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) > 0 THEN 1 
                              ELSE 0 END AS [numerator_t_12_m]
                          ,CASE WHEN MAX(ISNULL(stg_num.[measure_value], 0)) 
                              OVER(PARTITION BY mem.[id_mcaid] 
                                   ORDER BY ym.[year_month] 
                                   ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) > 0 THEN 1 
                              ELSE 0 END AS [numerator_t_24_m]")
    num_load <- DBI::SQL(",CASE WHEN [age_grp] IN ('Age 12-24 Months', 'Age 25 Months-6') THEN [numerator_t_12_m] 
                              WHEN [age_grp] IN ('Age 7-11', 'Age 12-19') THEN [numerator_t_24_m] 
                              END AS [numerator] ")
  } else if (measure %in% c("Mental Health Treatment Penetration",
                            "SUD Treatment Penetration",
                            "SUD Treatment Penetration (Opioid)")) {
    num_field <- DBI::SQL(",stg_num.[measure_value] AS [numerator]")
    num_load <- DBI::SQL(",[numerator] ")
  }
  
  # Num denom
  if (measure %in% c("All-Cause ED Visits",
                     "Acute Hospital Utilization")) {
    num_join <- DBI::SQL('')
    num_where <- DBI::SQL('')
  } else if (measure %in% c("Child and Adolescent Access to Primary Care")) {
    num_join <- DBI::SQL('')
    # [beg_measure_year_month] - 100 denotes 24-month identification period for denominator
    num_where <- DBI::SQL(" - 100")
  } else if (measure %in% c("Mental Health Treatment Penetration",
                            "SUD Treatment Penetration",
                            "SUD Treatment Penetration (Opioid)")) {
    num_join <- glue::glue_sql("AND stg_num.[year_month] >= (
                    SELECT [beg_measure_year_month] 
                    FROM {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month 
                    WHERE [year_month] = {end_month})", .con = conn)
    # [beg_measure_year_month] - 100 denotes 24-month identification period for denominator
    num_where <- DBI::SQL(" - 100")
  }
  
  # Outlier
  if (measure %in% c("Acute Hospital Utilization")) {
    outlier_field <- DBI::SQL(",CASE WHEN SUM(ISNULL(stg_num.[measure_value], 0)) 
                      OVER(PARTITION BY mem.[id_mcaid] ORDER BY ym.[year_month] 
                           ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) >= 3 THEN 1 ELSE 0 END AS [outlier]")
    outlier_where <- DBI::SQL("AND [outlier] = 0")
  } else if (measure %in% c("All-Cause ED Visits",
                            "Child and Adolescent Access to Primary Care",
                            "Mental Health Treatment Penetration",
                            "SUD Treatment Penetration",
                            "SUD Treatment Penetration (Opioid)")) {
    outlier_field <- DBI::SQL('')
    outlier_where <- DBI::SQL('')
  } 
  
  # Temp step
  if (measure %in% c("All-Cause ED Visits",
                     "Acute Hospital Utilization",
                     "Child and Adolescent Access to Primary Care")) {
    temp_step <- DBI::SQL('')
  } else if (measure %in% c("Mental Health Treatment Penetration",
                            "SUD Treatment Penetration",
                            "SUD Treatment Penetration (Opioid)")) {
    temp_step <- DBI::SQL("SELECT * INTO #temp
                  FROM CTE;
                  
                  CREATE CLUSTERED INDEX idx_cl_#temp ON #temp([id_mcaid], [end_year_month]);
                  
                  WITH CTE AS
                  (
                    SELECT
                    [beg_year_month]
                    ,[end_year_month]
                    ,[id_mcaid]
                    ,[end_month_age]
                    ,[age_grp]
                    ,[measure_id]
                    ,[full_criteria_t_12_m]
                    -- 24-month identification period for denominator
                    ,MAX(ISNULL([denominator], 0)) OVER(
                      PARTITION BY [id_mcaid] 
                      ORDER BY [end_year_month] 
                      ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS [denominator]
                    -- 12-month identification period for numerator
                    ,MAX(ISNULL([numerator], 0)) OVER(
                      PARTITION BY [id_mcaid] 
                      ORDER BY [end_year_month] 
                      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]
                    FROM #temp
                    )")
  }
  
  # End month age
  if (measure %in% c("Acute Hospital Utilization",
                     "SUD Treatment Penetration (Opioid)")) {
    end_month_age <- DBI::SQL("18")
  } else if (measure %in% c("All-Cause ED Visits")) {
    end_month_age <- DBI::SQL("0")
  } else if (measure %in% c("Child and Adolescent Access to Primary Care")) {
    end_month_age <- DBI::SQL("12 AND [end_month_age] <= 19")
  } else if (measure %in% c("Mental Health Treatment Penetration")) {
    end_month_age <- DBI::SQL("6")
  } else if (measure %in% c("SUD Treatment Penetration")) {
    end_month_age <- DBI::SQL("12")
  } 
  
  # Time requirement
  if (measure %in% c("Acute Hospital Utilization",
                     "Child and Adolescent Access to Primary Care",
                     "Mental Health Treatment Penetration",
                     "SUD Treatment Penetration",
                     "SUD Treatment Penetration (Opioid)")) {
    full_criteria_t_12_m <- 11
  } else if (measure %in% c("All-Cause ED Visits")) {
    full_criteria_t_12_m <- 7
  } 
  
  # Hopsice
  if (measure %in% c("Acute Hospital Utilization",
                     "All-Cause ED Visits",
                     "Child and Adolescent Access to Primary Care")) {
    hospice_where <- DBI::SQL("AND [hospice_t_12_m] = 0 ")
  } else if (measure %in% c("Mental Health Treatment Penetration",
                            "SUD Treatment Penetration",
                            "SUD Treatment Penetration (Opioid)")) {
    hospice_where <- DBI::SQL('')
  }
  
  
  #### Set up SQL ####
  if (measure %in% c("Acute Hospital Utilization",
                     "All-Cause ED Visits",
                     "Child and Adolescent Access to Primary Care",
                     "Mental Health Treatment Penetration",
                     "SUD Treatment Penetration",
                     "SUD Treatment Penetration (Opioid)")) {
    load_sql <- glue::glue_sql("WITH CTE AS
                                (
                                  SELECT
                                  ym.[beg_measure_year_month] AS [beg_year_month]
                                  ,ym.[year_month] AS [end_year_month]
                                  ,den.[end_quarter]
                                  ,mem.[id_mcaid]
                                  ,den.[end_month_age]
                                  ,CASE WHEN ref.[age_group] = 'age_grp_1' THEN age.[age_grp_1]
                                    WHEN ref.[age_group] = 'age_grp_2' THEN age.[age_grp_2]
                                    WHEN ref.[age_group] = 'age_grp_3' THEN age.[age_grp_3]
                                    WHEN ref.[age_group] = 'age_grp_4' THEN age.[age_grp_4]
                                    WHEN ref.[age_group] = 'age_grp_5' THEN age.[age_grp_5]
                                    WHEN ref.[age_group] = 'age_grp_6' THEN age.[age_grp_6]
                                    WHEN ref.[age_group] = 'age_grp_7' THEN age.[age_grp_7]
                                    WHEN ref.[age_group] = 'age_grp_8' THEN age.[age_grp_8]
                                    {age_grp}
                                  END AS [age_grp]
                                  
                                  ,ref.[measure_id]
                                  ,den.[full_criteria_t_12_m]
                                {full_criteria_prior}
                                  ,den.[hospice_t_12_m]
                                {hospice_prior}
                                {denom_field}
                                {num_field}
                                {outlier_field}
                                  
                                  FROM {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month AS ym
                                  
                                  CROSS JOIN {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_distinct_member AS mem
                                  
                                  LEFT JOIN {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_enroll_denom AS den
                                    ON mem.[id_mcaid] = den.[id_mcaid]
                                    AND ym.[year_month] = den.[year_month]
                                    AND den.[year_month] = {end_month}
                                  
                                  LEFT JOIN {`ref_schema`}.{DBI::SQL(ref_table)}perf_measure AS ref
                                  ON ref.[measure_name] = {measure}
                                  
                                  LEFT JOIN {`ref_schema`}.{DBI::SQL(ref_table)}age_grp AS age
                                  ON den.[end_month_age] = age.[age]
                                  
                                {denom_join}
                                  
                                  LEFT JOIN {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_staging AS stg_num
                                  ON mem.[id_mcaid] = stg_num.[id_mcaid]
                                  AND ym.[year_month] = stg_num.[year_month]
                                  
                                {num_join}
                                  --  This JOIN condition gets only utilization rows for the relevant measure
                                  AND ref.[measure_id] = stg_num.[measure_id]
                                  AND stg_num.[num_denom] = 'N'
                                  
                                  WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] {num_where} 
                                                            FROM {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month 
                                                            WHERE [year_month] = {end_month})
                                  AND ym.[year_month] <= {end_month}
                                )
                                
                                -- Might need temp step depending on measure
                             {temp_step}
                                
                                INSERT INTO {`to_schema`}.{`to_table`}
                                ([beg_year_month]
                                  ,[end_year_month]
                                  ,[id_mcaid]
                                  ,[end_month_age]
                                  ,[age_grp]
                                  ,[measure_id]
                                  ,[denominator]
                                  ,[numerator]
                                  ,[load_date])
                                
                                SELECT
                                [beg_year_month]
                                ,[end_year_month]
                                ,[id_mcaid]
                                ,[end_month_age]
                                ,[age_grp]
                                ,[measure_id]
                                ,[denominator]
                               {num_load}
                                ,CAST(GETDATE() AS DATE) AS [load_date]
                                
                                FROM CTE
                                
                                WHERE 1 = 1
                                AND [end_month_age] >= {end_month_age}
                                -- [full_criteria_t_12_m] will be NULL in all rows except were [end_year_month] = @end_month_int
                                AND [full_criteria_t_12_m] >= {full_criteria_t_12_m}
                              {denom_where}
                              {hospice_where} 
                              {outlier_where} 
                              {prior_where}",
                               .con = conn)
  }
  
  
  
  
  #### EVENT-BASED MEASURES ####
  # Follow-up ED visit for Alcohol/Drug Abuse
  # Follow-up ED visit for Mental Illness
  # Follow-up Hospitalization for Mental Illness
  # Plan All-Cause Readmissions (30 days)
  
  
  ### Set up SQL code based on the indicator
  # Age group
  if (measure %in% c("Follow-up ED visit for Alcohol/Drug Abuse",
                     "Follow-up ED visit for Mental Illness",
                     "Follow-up Hospitalization for Mental Illness",
                     "Plan All-Cause Readmissions (30 days)")) {
    age_grp <- DBI::SQL('')
  }
  
  # Residency/coverage
  if (measure %in% c("Follow-up ED visit for Alcohol/Drug Abuse",
                     "Follow-up ED visit for Mental Illness",
                     "Follow-up Hospitalization for Mental Illness")) {
    res_cov <- DBI::SQL("--- Members need King County residency for 11+ months during measurement year
              ,res.[enrolled_any_t_12_m] ")
  } else if (measure %in% c("Plan All-Cause Readmissions (30 days)")) {
    res_cov <- DBI::SQL("--Members need coverage in 11/12 months prior to index event
                ,den.[full_criteria_t_12_m]
                ,den.[hospice_t_12_m] ")
  } 
  
  
  # stage join
  if (measure %in% c("Follow-up ED visit for Alcohol/Drug Abuse",
                     "Follow-up ED visit for Mental Illness",
                     "Follow-up Hospitalization for Mental Illness")) {
    stage_join <- glue::glue_sql("LEFT JOIN {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_enroll_denom AS res
                                 ON stg.[id_mcaid] = res.[id_mcaid]
                                 AND res.[year_month] = {end_month}",
                                 .con = conn)
  } else if (measure %in% c("Plan All-Cause Readmissions (30 days)")) {
    stage_join <- DBI::SQL('')
  }
  
  
  
  # event date age
  if (measure %in% "Follow-up ED visit for Alcohol/Drug Abuse") {
    event_date_age <- glue::glue_sql("AND [event_date_age] >= 13", .con = conn)
  } else if (measure %in% c("Follow-up ED visit for Mental Illness",
                            "Follow-up Hospitalization for Mental Illness")) {
    event_date_age <- glue::glue_sql("AND [event_date_age] >= 6 ", .con = conn)
  } else if (measure %in% c("Plan All-Cause Readmissions (30 days)")) {
    event_date_age <- glue::glue_sql("AND [event_date_age] BETWEEN 18 AND 64
                                     AND [end_month_age] BETWEEN 18 AND 64 ", 
                                     .con = conn)
  }
  
  # enrollment coverage
  if (measure %in% c("Follow-up ED visit for Alcohol/Drug Abuse",
                     "Follow-up ED visit for Mental Illness",
                     "Follow-up Hospitalization for Mental Illness")) {
    enrollment_where <- DBI::SQL("-- For ACH regional attribution, ANY enrollment is used as a proxy for King County residence
                                     AND [enrolled_any_t_12_m] >= 11 ")
  } else if (measure %in% c("Plan All-Cause Readmissions (30 days)")) {
    enrollment_where <- DBI::SQL("AND [full_criteria_t_12_m] >= 11
                                 AND [hospice_t_12_m] = 0 "
    )
  }
  
  
  
  
  #### Set up SQL ####
  if (measure %in% c("Follow-up ED visit for Alcohol/Drug Abuse",
                     "Follow-up ED visit for Mental Illness",
                     "Follow-up Hospitalization for Mental Illness",
                     "Plan All-Cause Readmissions (30 days)")) {
    load_sql <- glue::glue_sql("WITH CTE AS
                               (
                                 SELECT
                                 ym.[beg_measure_year_month] AS [beg_year_month]
                                 ,stg.[year_month] AS [end_year_month]
                                 ,den.[end_quarter]
                                 ,stg.[id_mcaid]
                                 
                                 /*
                                   [stage].[mcaid_perf_measure] requires one row per person per measurement year. 
                                 However, for event-based measures, a person may have two different ages at 
                                 different index events during the same measurement year. Thus, insert age at 
                                 last index event [end_month_age] into [stage].[mcaid_perf_measure] BUT filter 
                                 for inclusion below by age at each index event [event_date_age].
                                 */
                                   ,MAX(DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
                                          CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
                                          stg.[event_date] THEN 1 ELSE 0 END) OVER(PARTITION BY stg.[id_mcaid]) AS [end_month_age]
                                 
                                 ,DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
                                   CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
                                   stg.[event_date] THEN 1 ELSE 0 END AS [event_date_age]
                                 
                                 ,ref.[measure_id]
                                 ,den.[full_criteria]
                                 ,den.[hospice]
                                 /*
                                   Members need coverage in month following index event.
                                 */
                                  ,den.[full_criteria_p_2_m]
                                 ,den.[hospice_p_2_m]
                                 
                                {res_cov}
                                 
                                 ,stg.[event_date]
                                 /*
                                   If index visit occurs on 1st of month, then 31-day follow-up period contained
                                 within calendar month.
                                 Then, [full_criteria_p_2_m], [hospice_p_2_m] are not used
                                 */
                                   ,CASE WHEN DAY(stg.[event_date]) = 1 AND MONTH([event_date]) IN (1, 3, 5, 7, 8, 10, 12)
                                 THEN 1 ELSE 0 END AS [need_1_month_coverage]
                                 
                                 ,stg.[denominator]
                                 ,stg.[numerator]
                                 
                                 FROM {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_staging_event_date AS stg
                                 
                                 INNER JOIN {`ref_schema`}.{DBI::SQL(ref_table)}perf_measure AS ref
                                 ON stg.[measure_id] = ref.[measure_id]
                                 AND ref.[measure_name] LIKE {glue::glue(measure, '%')}
                                 
                                 INNER JOIN {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month AS ym
                                 ON stg.[year_month] = ym.[year_month]
                                 
                                 /*
                                   [stage].[mcaid_perf_enroll_denom] must be joined TWICE for some measures
                                 (1) Member must have comprehensive, non-dual, non-tpl, no-hospice coverage from 
                                 [event_date] through 30 days after [event_date]
                                 (2) Member must have residence in the ACH region for 11 out of 12 months in the
                                 measurement year. This is proxied by [enrolled_any_t_12_m]
                                 */
                                   
                                   LEFT JOIN {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_enroll_denom AS den
                                 ON stg.[id_mcaid] = den.[id_mcaid]
                                 AND stg.[year_month] = den.[year_month]
                                 
                               {stage_join}
                                 
                                 WHERE stg.[event_date] >= (SELECT [12_month_prior] 
                                                            FROM {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month 
                                                            WHERE [year_month] = {end_month})
                                 
                                   -- Cut off index visits during last 31-day period because of insufficient follow-up period
                                   AND stg.[event_date] <= (SELECT DATEADD(DAY, -30, [end_month]) 
                                                            
                                                            FROM {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month
                                                            WHERE [year_month] = {end_month})
                               )

                               INSERT INTO {`to_schema`}.{`to_table`}
                               ([beg_year_month]
                                 ,[end_year_month]
                                 ,[id_mcaid]
                                 ,[end_month_age]
                                 ,[age_grp]
                                 ,[measure_id]
                                 ,[denominator]
                                 ,[numerator]
                                 ,[load_date])
                               
                               SELECT
                               (SELECT [beg_measure_year_month] 
                                 FROM {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month 
                                 WHERE [year_month] = {end_month}) AS [beg_year_month]
                               ,{end_month} AS [end_year_month]
                               ,[id_mcaid]
                               ,[end_month_age]
                               ,CASE WHEN ref.[age_group] = 'age_grp_1' THEN age.[age_grp_1]
                               WHEN ref.[age_group] = 'age_grp_2' THEN age.[age_grp_2]
                               WHEN ref.[age_group] = 'age_grp_3' THEN age.[age_grp_3]
                               WHEN ref.[age_group] = 'age_grp_4' THEN age.[age_grp_4]
                               WHEN ref.[age_group] = 'age_grp_5' THEN age.[age_grp_5]
                               WHEN ref.[age_group] = 'age_grp_6' THEN age.[age_grp_6]
                               WHEN ref.[age_group] = 'age_grp_7' THEN age.[age_grp_7]
                               WHEN ref.[age_group] = 'age_grp_8' THEN age.[age_grp_8]
                               WHEN ref.[age_group] = 'age_grp_9_months' THEN age.[age_grp_9_months]
                               END AS [age_grp]
                               ,a.[measure_id]
                               ,SUM([denominator]) AS [denominator]
                               ,SUM([numerator]) AS [numerator]
                               ,CAST(GETDATE() AS DATE) AS [load_date]
                               
                               FROM [CTE] AS a
                               
                               INNER JOIN {`ref_schema`}.{DBI::SQL(ref_table)}perf_measure AS ref
                               ON a.[measure_id] = ref.[measure_id]
                               
                               /*
                                 Join age_grp columns here, use CASE above to select age_grp_x from ref.perf_measure
                               */
                                 LEFT JOIN {`ref_schema`}.{DBI::SQL(ref_table)}age_grp AS age
                               ON a.[end_month_age] = age.[age]
                               
                               WHERE 1 = 1
                               /*
                                 Filter by age at time of index event
                               */
                                 {event_date_age}
                               -- For follow-up measures, enrollment is required at time of index event
                               AND [full_criteria] = 1
                               AND [hospice] = 0
                               AND (([need_1_month_coverage] = 1) OR ([full_criteria_p_2_m] = 2 AND [hospice_p_2_m] = 0))
                               
                               {enrollment_where}
                               
                               GROUP BY 
                               [id_mcaid]
                               ,[end_month_age]
                               ,CASE WHEN ref.[age_group] = 'age_grp_1' THEN age.[age_grp_1]
                               WHEN ref.[age_group] = 'age_grp_2' THEN age.[age_grp_2]
                               WHEN ref.[age_group] = 'age_grp_3' THEN age.[age_grp_3]
                               WHEN ref.[age_group] = 'age_grp_4' THEN age.[age_grp_4]
                               WHEN ref.[age_group] = 'age_grp_5' THEN age.[age_grp_5]
                               WHEN ref.[age_group] = 'age_grp_6' THEN age.[age_grp_6]
                               WHEN ref.[age_group] = 'age_grp_7' THEN age.[age_grp_7]
                               WHEN ref.[age_group] = 'age_grp_8' THEN age.[age_grp_8]
                               WHEN ref.[age_group] = 'age_grp_9_months' THEN age.[age_grp_9_months]
                               END
                               ,a.[measure_id]",
                               .con = conn)
  }
 

  #### REMOVE EXISTING DATA ####
  try(DBI::dbExecute(conn,
                 glue::glue_sql("DELETE FROM {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_measure
                                FROM {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_measure AS a
                                INNER JOIN {`ref_schema`}.{DBI::SQL(ref_table)}perf_measure AS b
                                ON a.measure_id = b.measure_id
                                WHERE b.measure_name = {measure}
                                AND end_year_month = {end_month}",
                                .con = conn)))


  #### CREATE TABLE IF IT DOESN'T EXIST ####
  if (dbExistsTable(conn = conn, DBI::Id(schema = to_schema, table = to_table)) == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
    create_table_f(conn = conn, server = server, config = config)
  }

  #### ADD NEW DATA ####
  DBI::dbExecute(conn, load_sql)
}