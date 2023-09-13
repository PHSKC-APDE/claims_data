# This code creates the the mcaid performance measures distinct member table
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# R script developed by Alastair Matheson based on Philip Sylling's stored procedure
#
### NB. There is no accompanying YAML file as everything is set in this script

### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims

load_stage_mcaid_perf_distinct_member_f <- function(conn = NULL,
                                                    server = c("hhsaw", "phclaims")) {
  
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (server == "hhsaw") {
    to_schema <- "claims"
    to_table <- "stage_"
  } else if (server == "phclaims") {
    to_schema <- "stage"
    to_table <- ""
  }
  
  
  #### Drop existing table ####
  DBI::dbExecute(conn,
                 glue::glue_sql("IF OBJECT_ID('{`to_schema`}.{DBI::SQL(to_table)}mcaid_perf_distinct_member','U') IS NOT NULL
                                DROP TABLE {`to_schema`}.{DBI::SQL(to_table)}mcaid_perf_distinct_member;",
                                .con = conn))
  
  
  #### Make table ####
  DBI::dbExecute(conn,
                 glue::glue_sql("SELECT DISTINCT
                                [id_mcaid]
                                ,CAST(GETDATE() AS DATE) AS [load_date]
                                INTO {`to_schema`}.{DBI::SQL(to_table)}mcaid_perf_distinct_member
                                FROM {`to_schema`}.{DBI::SQL(to_table)}mcaid_perf_enroll_denom",
                                .con = conn))
  
  
  #### Make index ####
  DBI::dbExecute(conn, 
                 glue::glue_sql("CREATE CLUSTERED INDEX [idx_cl_mcaid_perf_distinct_member_id_mcaid] 
                                ON {`to_schema`}.{DBI::SQL(to_table)}mcaid_perf_distinct_member([id_mcaid])",
                                .con = conn))
}