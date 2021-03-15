### Function to generate a table of people with full benefits for a given time period
# Adapated from a SQL function created by Philip Sylling ([stage].[fn_mcaid_perf_enroll_member_month])
# Repurposed to work in on-prem and Azure servers
#
# Alastair Matheson, 2021-03
#
# Table names still currently hard-coded (specific to each server) but could
# be made more generic and fed by a YAML config file
#
# Notes from Philip's code:
# 1. Create Age at beginning of month and end of month. This would correspond to age
#    at Beginning of Measurement Year or End of Measurement Year (typical)
# 2. Create enrollment gaps as ZERO rows by the following join
#    [stage].[mcaid_elig_demo] CROSS JOIN [ref].[perf_year_month] LEFT JOIN [stage].[mcaid_perf_elig_member_month]
#    The ZERO rows are used to track changing enrollment threshold over time.
#
# conn = name of odbc connection
# server = specify if we are working in HHSAW or PHClaims
# start_date_int = the year and month to begin calculating enrollment for (integer of YYYYMM)
# end_date_int = the year and month to end calculating enrollment for (integer of YYYYMM)
# output_table = the name of the table to output results to
# output_temp = if the output table should be a local temp table
#
# Note: if output_temp = FALSE, the table will be stored in the following schema:
#    HHSAW = claims.tmp_<table_name>
#    PHClaims = tmp.<table_name>


mcaid_perf_enroll_member_month_f <- function(conn = NULL,
                                             server = c("hhsaw", "phclaims"),
                                             start_date_int = NULL,
                                             end_date_int = NULL,
                                             output_table = NULL,
                                             output_temp = TRUE) {
  
  
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
  
  
  # Set up table name
  if (output_temp == TRUE) {
    table_schema <- ""
    table_name <- paste0("##", output_table)
  } else {
    if (server == "hhsaw") {
      table_schema <- "claims."
      table_name <- paste0("tmp_", output_table)
    } else if (server == "phclaims") {
      table_schema <- "tmp."
      table_name <- output_table
    }
    
  }
  
  # Set up SQL code
  sql <- glue::glue_sql("SELECT 
                        b.[year_month], 
                        b.[month], 
                        a.[id_mcaid], 
                        a.[dob],
                        DATEDIFF(YEAR, a.[dob], b.[end_month]) - 
                          CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, a.[dob], b.[end_month]), a.[dob]) > b.[end_month] THEN 1 
                          ELSE 0 
                          END AS [end_month_age],
                        DATEDIFF(MONTH, a.[dob], b.[end_month]) - 
                          CASE WHEN DATEADD(MONTH, DATEDIFF(MONTH, a.[dob], b.[end_month]), a.[dob]) > b.[end_month] THEN 1 
                          ELSE 0 
                          END AS [age_in_months],
                        CASE WHEN c.[MEDICAID_RECIPIENT_ID] IS NOT NULL THEN 1 ELSE 0 END AS [enrolled_any],
                        -- Use BSP Group Full Benefit Methodology from HCA/Providence CORE
                        CASE WHEN d.[full_benefit] = 'Y' THEN 1 ELSE 0 END AS [full_benefit],
                        CASE WHEN c.[DUAL_ELIG] = 'Y' THEN 1 ELSE 0 END AS [dual],
                        CASE WHEN c.[TPL_FULL_FLAG] = 'Y' THEN 1 ELSE 0 END AS [tpl],
                        ISNULL(e.[hospice_flag], 0) AS [hospice],
                        CASE WHEN c.[MEDICAID_RECIPIENT_ID] IS NOT NULL AND d.[full_benefit] = 'Y' AND 
                            c.[DUAL_ELIG] = 'N' AND c.[TPL_FULL_FLAG] = ' ' THEN 1 
                            ELSE 0 
                            END AS [full_criteria],
                        c.[RSDNTL_POSTAL_CODE] AS [zip_code],
                        b.[row_num]
                        INTO {DBI::SQL(table_schema)}{`table_name`}
                        
                        FROM {`final_schema`}.{DBI::SQL(final_table)}mcaid_elig_demo AS a
                        
                        CROSS JOIN 
                        (
                          SELECT *, ROW_NUMBER() OVER(ORDER BY [year_month]) AS [row_num]
                          FROM {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month
                          WHERE [year_month] BETWEEN {start_date_int} AND {end_date_int}
                        ) AS b
                        
                        LEFT JOIN {`stage_schema`}.{DBI::SQL(stage_table)}mcaid_perf_elig_member_month AS c
                        ON a.[id_mcaid] = c.[MEDICAID_RECIPIENT_ID]
                        AND b.[year_month] = c.[CLNDR_YEAR_MNTH]
                        
                        LEFT JOIN {`ref_schema`}.{DBI::SQL(ref_table)}mcaid_rac_code AS d
                        ON c.[RPRTBL_RAC_CODE] = d.[rac_code]
                        
                        LEFT JOIN {`view_schema`}.[v_mcaid_perf_hospice_member_month] AS e
                        ON a.[id_mcaid] = e.[id_mcaid]
                        AND b.[year_month] = e.[year_month];",
                        .con = conn)
  
  # Execute query
  DBI::dbExecute(conn = conn, sql)
}









