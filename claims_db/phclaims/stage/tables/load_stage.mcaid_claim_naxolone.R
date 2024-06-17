# This code creates the the mcaid claim naxolone table
# Create a reference table for naxolone distributed in mcaid claims
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# R script developed by Jeremy Whitehurst using SQL scripts from Eli Kern, Jennifer Liu and Spencer Hensley
#
### 

### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims

load_stage_mcaid_claim_naxolone_f <- function(conn = NULL,
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

  message("Creating ", to_schema, ".", to_table, ".")
  time_start <- Sys.time()
  
  #### DROP EXISTING TABLE TO USE SELECT INTO ####
  try(DBI::dbRemoveTable(conn, DBI::Id(schema = to_schema, table = to_table)))
  
  #### LOAD TABLE ####
  message("STEP 1: CREATE TABLE TO HOLD NDC CODES IDENTIFYING NALOXONE FOR LIKE JOIN")
  try(odbc::dbRemoveTable(conn, "##naloxone_ndc_list_prep", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##naloxone_ndc_list", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##naloxone_ndc_ref_table", temporary = T), silent = T)
  step1_sql <- glue::glue_sql("
  --First, create a table holding all NDC codes identifying naloxone
	create table ##naloxone_ndc_list_prep (ndc varchar(255));

	insert into ##naloxone_ndc_list_prep
	values 
('00932165'),
('04049921'),
('04049923'),
('04091782'),
('06416132'),
('06416260'),
('360000310'),
('435980750'),
('458020811'),
('500903294'),
('500905908'),
('500906710'),
('516621238'),
('516621240'),
('516621385'),
('516621495'),
('516621544'),
('516621620'),
('525840120'),
('551500327'),
('551500345'),
('594670679'),
('636299321'),
('674570299'),
('674570645'),
('674570992'),
('695470353'),
('700690071'),
('712050528'),
('718727009'),
('718727198'),
('718727219'),
('718727297'),
('725720450'),
('763293369'),
('786700140'),
('829540100'),
('04049920'),
('04049922'),
('04091215'),
('05912971'),
('06416205'),
('360000308'),
('420230224'),
('458020578'),
('500902422'),
('500905427'),
('500906491'),
('500906963'),
('516621239'),
('516621242'),
('516621426'),
('516621529'),
('516621586'),
('516621642'),
('542880124'),
('551500328'),
('557000985'),
('608420002'),
('674570292'),
('674570599'),
('674570987'),
('695470212'),
('695470627'),
('700690072'),
('712050707'),
('718727177'),
('718727215'),
('718727294'),
('718727299'),
('763291469'),
('763293469'),
('804250259'),
('830080007');

--Second, add a column with % that can be used for a LIKE join
select *, '%' + ndc + '%' as ndc_like
into ##naloxone_ndc_list
from ##naloxone_ndc_list_prep;

--Third, LIKE join all distinct NDC codes to the list of naloxone NDC codes to create a data source-specific reference table
--Then, use this custom reference table down below for an exact join
select a.ndc, 1 as naloxone_flag
into ##naloxone_ndc_ref_table
from (
	select distinct ndc from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_pharm')`}
) as a
inner join ##naloxone_ndc_list as b
on a.ndc like b.ndc_like;", 
	  .con = conn)
  DBI::dbExecute(conn = conn, step1_sql)
    
  message("STEP 2: CREATE TABLE OF NALOXONE EVENTS")
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_proc_2", temporary = T), silent = T)
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
	left join ref.ndc_codes as b
	on a.ndc = b.ndc

	inner join ##naloxone_ndc_ref_table as c
	on a.ndc = c.ndc

WHERE year(a.rx_fill_date) >= 2016
	AND rx_quantity >= 1.00


-- Next get Naloxone distributed as part of procedure codes

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
  add_index_f(conn, server = server, table_config = config)
}