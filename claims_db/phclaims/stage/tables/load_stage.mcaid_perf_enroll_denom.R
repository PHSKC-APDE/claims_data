# This code creates the the mcaid performance measures enrollment denominator table
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# R script developed by Alastair Matheson based on Philip Sylling's stored procedure


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_perf_enroll_denom_f <- function(conn = NULL,
                                                 server = c("hhsaw", "phclaims"),
                                                 start_date_int = NULL,
                                                 end_date_int = NULL,
                                                 config = NULL,
                                                 get_config = F) {
  
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
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
  
  
  
  #### Remove indices ####
  DBI::dbExecute(conn,
                 glue::glue_sql("IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'idx_nc_mcaid_perf_enroll_denom_age_in_months')
                                DROP INDEX [idx_nc_mcaid_perf_enroll_denom_age_in_months] ON {`to_schema`}.{`to_table`};",
                                .con = conn))
  DBI::dbExecute(conn,
                 glue::glue_sql("IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'idx_nc_mcaid_perf_enroll_denom_end_month_age')
                                DROP INDEX [idx_nc_mcaid_perf_enroll_denom_end_month_age] ON {`to_schema`}.{`to_table`};",
                                .con = conn))
  DBI::dbExecute(conn,
                 glue::glue_sql("IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'idx_cl_mcaid_perf_enroll_denom_id_mcaid_year_month')
                                DROP INDEX [idx_cl_mcaid_perf_enroll_denom_id_mcaid_year_month] ON {`to_schema`}.{`to_table`};",
                                .con = conn))
  
  
  #### Clear out existing data based on dates ####
  if (DBI::dbExistsTable(conn, 
                         name = glue::glue_sql("{`to_schema`}.{`to_table`}", .con = conn))) {
    DBI::dbExecute(conn,
                   glue::glue_sql("DELETE FROM {`to_schema`}.{`to_table`}
                                WHERE year_month >= {start_date_int}
                                AND year_month <= {end_date_int}",
                                  .con = conn))
  }

  
  look_back_date_int <- odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT YEAR(24_month_prior) * 100 + MONTH(24_month_prior) FROM 
                         {`ref_schema`}.{DBI::SQL(ref_table)}perf_year_month 
                         WHERE year_month = {start_date_int}",
                         .con = conn))
  
  
  #### Set up initial temp table ####
  # Delete existing table
  DBI::dbExecute(conn, "IF OBJECT_ID('tempdb..##temp', 'U') IS NOT NULL DROP TABLE ##temp")
  
  
  # Load into temp table
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT * INTO ##temp
                                FROM {`stage_schema`}.{DBI::SQL(stage_table)}_mcaid_perf_enroll_member_month
                                (CAST({look_back_date_int} AS VARCHAR(20)), CAST({end_date_int} AS VARCHAR(20)))",
                                .con = conn))
  
  # Set up an index
  DBI::dbExecute(conn,
                 "CREATE CLUSTERED INDEX [idx_cl_#temp_id_mcaid_year_month] ON ##temp([id_mcaid], [year_month])")



  #### Set up second temp table ####
  # Delete existing table
  DBI::dbExecute(conn, "IF OBJECT_ID('tempdb..##mcaid_perf_enroll_denom', 'U') IS NOT NULL
                 DROP TABLE ##mcaid_perf_enroll_denom")
  
  # Load into temp table
  DBI::dbExecute(conn,
                 "SELECT
                 [year_month]
                 ,[month]
                 ,[id_mcaid]
                 ,[dob]
                 ,[end_month_age]
                 ,CASE WHEN [end_month_age] BETWEEN 0 AND 20 THEN [age_in_months] ELSE NULL END AS [age_in_months]
                 ,[enrolled_any]
                 ,SUM([enrolled_any]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [enrolled_any_t_12_m]
                 ,[full_benefit]
                 ,SUM([full_benefit]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [full_benefit_t_12_m]
                 ,[dual]
                 ,SUM([dual]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [dual_t_12_m]
                 ,[tpl]
                 ,SUM([tpl]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [tpl_t_12_m]
                 ,[hospice]
                 ,SUM([hospice]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [hospice_t_12_m]
                 ,SUM([hospice]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN 23 PRECEDING AND 12 PRECEDING) AS [hospice_prior_t_12_m]
                 ,SUM([hospice]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS [hospice_p_2_m]
                 ,[full_criteria]
                 ,SUM([full_criteria]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [full_criteria_t_12_m]
                 ,SUM([full_criteria]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN 23 PRECEDING AND 12 PRECEDING) AS [full_criteria_prior_t_12_m]
                 ,SUM([full_criteria]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS [full_criteria_p_2_m]
                 ,[zip_code]
                 ,[row_num]
                 INTO ##mcaid_perf_enroll_denom
                 FROM ##temp")
  
  # Set up an index
  DBI::dbExecute(conn,
                 "CREATE CLUSTERED INDEX [idx_cl_#mcaid_perf_enroll_denom_id_mcaid_year_month] 
                                         ON ##mcaid_perf_enroll_denom([id_mcaid], [year_month])")
 
  
  #### Set up third temp table ####
  # Delete existing table
  DBI::dbExecute(conn, "IF OBJECT_ID('tempdb..##last_year_month', 'U') IS NOT NULL
                 DROP TABLE ##last_year_month")
  
  # Load into temp table
  DBI::dbExecute(conn,
                 "SELECT
                 [year_month]
                 ,[month]
                 ,[id_mcaid]
                 ,[dob]
                 ,[end_month_age]
                 ,[age_in_months]
                 ,[enrolled_any]
                 ,[enrolled_any_t_12_m]
                 ,[full_benefit]
                 ,[full_benefit_t_12_m]
                 ,[dual]
                 ,[dual_t_12_m]
                 ,[tpl]
                 ,[tpl_t_12_m]
                 ,[hospice]
                 ,[hospice_t_12_m]
                 ,[hospice_prior_t_12_m]
                 ,[hospice_p_2_m]
                 ,[full_criteria]
                 ,[full_criteria_t_12_m]
                 ,[full_criteria_prior_t_12_m]
                 ,[full_criteria_p_2_m]
                 ,[zip_code]
                 ,[relevant_year_month]
                 ,MAX([relevant_year_month]) OVER(
                   PARTITION BY [id_mcaid] 
                   ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [last_year_month]
                 ,[row_num]
                 
                 INTO ##last_year_month
                 FROM ##mcaid_perf_enroll_denom
                 CROSS APPLY(VALUES(CASE WHEN [zip_code] IS NOT NULL 
                                    THEN [year_month] END)) AS a([relevant_year_month])")
  
  # Set up an index
  DBI::dbExecute(conn,
                 "CREATE CLUSTERED INDEX idx_cl_#last_year_month ON ##last_year_month([id_mcaid], [last_year_month])")
  

  
  #### Make stage table ####
  DBI::dbExecute(conn,
                 glue::glue_sql(
                   "WITH CTE AS
                   (
                     SELECT
                     [year_month]
                     ,CASE WHEN [month] IN (3, 6, 9, 12) THEN 1 ELSE 0 END AS [end_quarter]
                     ,[id_mcaid]
                     ,[dob]
                     ,[end_month_age]
                     ,[age_in_months]
                     ,MAX([zip_code]) OVER(PARTITION BY [id_mcaid], [last_year_month]) AS [last_zip_code]
                     ,[enrolled_any]
                     ,[enrolled_any_t_12_m]
                     ,[full_benefit]
                     ,[full_benefit_t_12_m]
                     ,[dual]
                     ,[dual_t_12_m]
                     ,[tpl]
                     ,[tpl_t_12_m]
                     ,[hospice]
                     ,[hospice_t_12_m]
                     ,[hospice_prior_t_12_m]
                     ,[hospice_p_2_m]
                     ,[full_criteria]
                     ,[full_criteria_t_12_m]
                     ,[full_criteria_prior_t_12_m]
                     ,[full_criteria_p_2_m]
                     ,CAST(GETDATE() AS DATE) AS [load_date]
                     FROM ##last_year_month
                   )
                   INSERT INTO {`to_schema`}.{`to_table`}
                   SELECT *
                     FROM CTE
                   WHERE 1 = 1
                   AND [year_month] >= {start_date_int}
                   AND [year_month] <= {end_date_int}
                   AND [enrolled_any_t_12_m] >= 1
                   ORDER BY [id_mcaid], [year_month]",
                   .con = conn))

  
  
  ### Add new indices ####
  DBI::dbExecute(conn, 
                 glue::glue_sql("CREATE CLUSTERED INDEX [idx_cl_mcaid_perf_enroll_denom_id_mcaid_year_month] ON {`to_schema`}.{`to_table`}([id_mcaid], [year_month])",
                                .con = conn))
  DBI::dbExecute(conn, 
                 glue::glue_sql("CREATE NONCLUSTERED INDEX [idx_nc_mcaid_perf_enroll_denom_end_month_age] ON {`to_schema`}.{`to_table`}([end_month_age])",
                                .con = conn))
  DBI::dbExecute(conn, 
                 glue::glue_sql("CREATE NONCLUSTERED INDEX [idx_nc_mcaid_perf_enroll_denom_age_in_months] ON {`to_schema`}.{`to_table`}([age_in_months])",
                                .con = conn))
  
  message("Performance measure enrollment denominator table created")
  
  
  #### Clean up temp tables ####
  DBI::dbExecute(conn, "IF OBJECT_ID('tempdb..##temp', 'U') IS NOT NULL DROP TABLE ##temp")
  DBI::dbExecute(conn, "IF OBJECT_ID('tempdb..##mcaid_perf_enroll_denom', 'U') IS NOT NULL
                 DROP TABLE ##mcaid_perf_enroll_denom")
  DBI::dbExecute(conn, "IF OBJECT_ID('tempdb..##last_year_month', 'U') IS NOT NULL
                 DROP TABLE ##last_year_month")
  
}