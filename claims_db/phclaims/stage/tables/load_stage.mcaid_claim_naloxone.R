# This code creates the the mcaid claim naloxone table
# Create a reference table for naloxone distributed in mcaid claims
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# R script developed by Jeremy Whitehurst using SQL scripts from Eli Kern, Jennifer Liu and Spencer Hensley
#
## 

## Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims

load_stage_mcaid_claim_naloxone_f <- function(conn = NULL,
                                          server = c("hhsaw", "phclaims"),
										                      config = NULL,
                                          get_config = F) {
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  # READ IN CONFIG FILE ----
  # NOTE: The mcaid_mcare YAML files still have the older structure (no server, etc.)
  # If the format is updated, need to add in more robust code here to identify schema and table names
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  # VARIABLES ----
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  final_schema <- config[[server]][["final_schema"]]
  final_table <- ifelse(is.null(config[[server]][["final_table"]]), '',
                        config[[server]][["final_table"]])
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- ifelse(is.null(config[[server]][["ref_table"]]), '',
                      config[[server]][["ref_table"]])
  stage_schema <- config[[server]][["stage_schema"]]
  
  message("Creating ", to_schema, ".", to_table, ".")
  time_start <- Sys.time()
  
  #### DROP EXISTING TABLE TO USE SELECT INTO ####
  try(DBI::dbRemoveTable(conn, DBI::Id(schema = to_schema, table = to_table)), silent = T)
  
  #### LOAD TABLE ####
  message("STEP 1: CREATE TABLE TO HOLD NDC CODES IDENTIFYING naloxone FOR LIKE JOIN")
  step1_sql <- glue::glue_sql("
  --First, create a table holding all NDC codes identifying naloxone
    --created actual reference table
    
--Second, add a column with % that can be used for a LIKE join
IF OBJECT_ID(N'tempdb..#naloxone_ndc_list') IS NOT NULL DROP TABLE #naloxone_ndc_list;
select *, '%' + ndc + '%' as ndc_like
into #naloxone_ndc_list
from {`stage_schema`}.{`paste0(ref_table, 'naxolone_ndc')`};

--Third, LIKE join all distinct NDC codes to the list of naloxone NDC codes to create a data source-specific reference table
--Then, use this custom reference table down below for an exact join
IF OBJECT_ID(N'tempdb..#naloxone_ndc_ref_table') IS NOT NULL DROP TABLE #naloxone_ndc_ref_table;
select a.ndc, 1 as naloxone_flag
into #naloxone_ndc_ref_table
from (
	select distinct ndc from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_pharm')`}
) as a
inner join #naloxone_ndc_list as b
on a.ndc like b.ndc_like;", 
	  .con = conn)
  DBI::dbExecute(conn = conn, step1_sql)
    
  message("STEP 2: CREATE TABLE OF naloxone EVENTS")
  try(odbc::dbRemoveTable(conn, "#mcaid_moud_proc_2", temporary = T), silent = T)
  step2_sql <- glue::glue_sql("
	 SELECT 
	 a.id_mcaid
	,a.claim_header_id
	,a.ndc as code
	,UPPER(B.PROPRIETARYNAME) AS description
	,a.rx_fill_date AS date
	,A.rx_quantity AS quantity, 
	case when DOSAGEFORMNAME LIKE '%SPRAY%' or A.NDC = '00093216519' THEN 'SPRAY' 
		WHEN dosageformname LIKE '%INJECTION%' or A.NDC in ('55150034510', '55150032710', '00409121525')  THEN 'INJECTION' 
		ELSE NULL END 
		AS form,
	CASE WHEN A.NDC = '00093216519' THEN 40.00
	WHEN A.NDC = '55150034510' THEN 1
	WHEN A.ndc = '55150032710' THEN 0.4
	ELSE ACTIVE_NUMERATOR_STRENGTH / 
		(case when ACTIVE_INGRED_UNIT = 'mg/.1mL' THEN .1 
			WHEN  ACTIVE_INGRED_UNIT = 'mg/mL' THEN 1 ELSE NULL END) 
			END
			AS dosage_per_ml,
	'PHARMACY' AS location,
	getdate() as last_run

INTO {`to_schema`}.{`to_table`}

FROM {`final_schema`}.{`paste0(final_table, 'mcaid_claim_pharm')`} as a
	left join {`ref_schema`}.{`paste0(ref_table, 'ndc_codes')`} as b
	on a.ndc = b.ndc

	inner join #naloxone_ndc_ref_table as c
	on a.ndc = c.ndc

WHERE year(a.rx_fill_date) >= 2016
	AND rx_quantity >= 1.00


-- Next get naloxone distributed as part of procedure codes

UNION


SELECT
	id_mcaid,
	claim_header_id,
	a.procedure_code AS code, 
	UPPER(procedure_long_desc) as description,
	last_service_date AS [date],
	CASE WHEN a.procedure_code IN ('G1028', 'G2215') THEN 2
		WHEN a.procedure_code IN ('G2216', 'J2310', 'J2311', 'J3490') THEN 1
		ELSE NULL
		END
		AS quantity,
	CASE WHEN a.procedure_code IN ('G1028', 'G2215') THEN 'SPRAY'
		WHEN a.procedure_code IN ('G2216', 'J2310', 'J2311') THEN 'INJECTION'
		WHEN a.procedure_code IN ('J3490') THEN 'UNKNOWN'
		ELSE NULL
		END
		AS form,
	CASE WHEN a.procedure_code IN ('G1028') THEN 80
		WHEN a.procedure_code IN ('G2215') THEN 40
		ELSE NULL
		END
		AS DOSAGE_IN_ML,
		'PROCEDURE' AS location,
		getdate() as last_run

FROM {`final_schema`}.{`paste0(final_table, 'mcaid_claim_procedure')`} AS A
	LEFT JOIN {`ref_schema`}.{`paste0(ref_table, 'apcd_procedure_code')`} AS B
	ON A.procedure_code = B.procedure_code

WHERE year(last_service_date) >= 2016 
	and 
	(a.procedure_code in ('G1028', 'G2215', 'G2216 ', 'J2310', 'J2311') 
		or 
	a.procedure_code = 'J3490' and (modifier_1 in ('HG', 'TG') 
	    or modifier_2 in ('HG', 'TG')
		or modifier_3 in ('HG', 'TG') or modifier_4 in ('HG', 'TG')));",
	  .con = conn)
  DBI::dbExecute(conn = conn, step2_sql)
  
  time_end <- Sys.time()
  message("Loading took ", round(difftime(time_end, time_start, units = "secs"), 2), 
          " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
          " mins)")
  
  
  #### ADD INDEX ####
  #add_index_f(conn, server = server, table_config = config)
}
