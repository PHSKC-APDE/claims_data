# This code creates the the mcaid performance measures elig member month table
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# R script developed by Alastair Matheson based on Philip Sylling's stored procedure
#
### NB. There is no accompanying YAML file as everything is set in this script

### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims

load_stage_mcaid_perf_elig_member_month_f <- function(conn = NULL,
                                                      server = c("hhsaw", "phclaims")) {
  
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (server == "hhsaw") {
    to_schema <- "claims"
    to_table <- "stage_"
    from_schema <- "claims"
    from_table <- "stage"
    ref_schema <- "claims"
    ref_table <- "ref_"
  } else if (server == "phclaims") {
    to_schema <- "stage"
    to_table <- ""
    from_schema <- "stage"
    from_table <- ""
    ref_schema <- "ref"
    ref_table <- ""
  }
  
  
  ### Create table for temporary work
  # Drop existing table
  DBI::dbExecute(conn, "IF OBJECT_ID('tempdb..#temp') IS NOT NULL 
                 DROP TABLE #temp;")
  
  # Make table
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT
                 [CLNDR_YEAR_MNTH]
                 ,[MEDICAID_RECIPIENT_ID]
                 ,[RPRTBL_RAC_CODE]
                 ,[FROM_DATE]
                 ,[TO_DATE]
                 ,[COVERAGE_TYPE_IND]
                 ,CASE WHEN ([COVERAGE_TYPE_IND] = 'MC' AND [MC_PRVDR_NAME] = 'Amerigroup Washington Inc') THEN 'AGP'
                    WHEN ([COVERAGE_TYPE_IND] = 'MC' AND [MC_PRVDR_NAME] = 'Community Health Plan of Washington') THEN 'CHP'
                    WHEN ([COVERAGE_TYPE_IND] = 'MC' AND [MC_PRVDR_NAME] IN ('Coordinated Care Corporation', 'Coordinated Care of Washington')) THEN 'CCW'
                    WHEN ([COVERAGE_TYPE_IND] = 'MC' AND [MC_PRVDR_NAME] = 'Molina Healthcare of Washington Inc') THEN 'MHW'
                    WHEN ([COVERAGE_TYPE_IND] = 'MC' AND [MC_PRVDR_NAME] = 'United Health Care Community Plan') THEN 'UHC'
                    WHEN ([COVERAGE_TYPE_IND] = 'MC') THEN NULL
                    ELSE NULL END AS [MC_PRVDR_NAME]
                 ,[DUAL_ELIG]
                 ,[TPL_FULL_FLAG]
                 ,[RSDNTL_POSTAL_CODE]
                 INTO #temp
                 FROM {`from_schema`}.{DBI::SQL(from_table)}mcaid_elig;",
                                .con = conn))
  
  ### Make index
  DBI::dbExecute(conn, "CREATE CLUSTERED INDEX [idx_cl_#temp] 
                 ON #temp([MEDICAID_RECIPIENT_ID], [CLNDR_YEAR_MNTH]);")
  
  
  ### Clear out stage table
  DBI::dbExecute(conn, 
  glue::glue_sql("IF OBJECT_ID('{`to_schema`}.{DBI::SQL(to_table)}mcaid_perf_elig_member_month', 'U') IS NOT NULL
                 DROP TABLE {`to_schema`}.{DBI::SQL(to_table)}mcaid_perf_elig_member_month;",
                 .con = conn))
                            
  
  ### Load new table
  DBI::dbExecute(conn,
                 glue::glue_sql("WITH CTE AS
                 (
                   SELECT
                   CAST([CLNDR_YEAR_MNTH]) AS INT AS CLNDR_YEAR_MNTH
                   ,[MEDICAID_RECIPIENT_ID]
                   ,[RPRTBL_RAC_CODE]
                   ,[FROM_DATE]
                   ,[TO_DATE]
                   ,[COVERAGE_TYPE_IND]
                   ,[MC_PRVDR_NAME]
                   ,[DUAL_ELIG]
                   ,[TPL_FULL_FLAG]
                   ,[RSDNTL_POSTAL_CODE]
                   ,ROW_NUMBER() OVER(PARTITION BY [MEDICAID_RECIPIENT_ID], [CLNDR_YEAR_MNTH] 
                                      ORDER BY DATEDIFF(DAY, [FROM_DATE], [TO_DATE]) DESC) AS [row_num]
                   FROM #temp AS a
                   INNER JOIN {`ref_schema`}.{DBI::SQL(ref_table)}apcd_zip AS b
                   ON a.[RSDNTL_POSTAL_CODE] = b.[zip_code]
                   WHERE b.[state] = 'WA' AND b.[county_name] = 'King'
                 )
                 
                 SELECT
                 [CLNDR_YEAR_MNTH]
                 ,[MEDICAID_RECIPIENT_ID]
                 ,[RPRTBL_RAC_CODE]
                 ,[FROM_DATE]
                 ,[TO_DATE]
                 ,[COVERAGE_TYPE_IND]
                 ,[MC_PRVDR_NAME]
                 ,[DUAL_ELIG]
                 ,[TPL_FULL_FLAG]
                 ,[RSDNTL_POSTAL_CODE]
                 ,CAST(GETDATE() AS DATE) AS [load_date]
                 
                 INTO {`to_schema`}.{DBI::SQL(to_table)}mcaid_perf_elig_member_month
                 FROM CTE
                 WHERE 1 = 1
                 AND [row_num] = 1;",
                                .con = conn))
                            
                  
  # Make primary key
  DBI::dbExecute(conn,
                 glue::glue_sql("ALTER TABLE {`to_schema`}.{DBI::SQL(to_table)}mcaid_perf_elig_member_month]
                                ADD CONSTRAINT [pk_mcaid_perf_elig_member_month_MEDICAID_RECIPIENT_ID_CLNDR_YEAR_MNTH] 
                                PRIMARY KEY ([MEDICAID_RECIPIENT_ID], [CLNDR_YEAR_MNTH]);",
                                .con = conn))
  
}