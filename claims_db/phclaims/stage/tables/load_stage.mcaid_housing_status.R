#' @title load_stage.mcaid_housing_status
#' 
#' @description TBD
#' 
#' @details TBD
#' 
#' Note: 
#' 

## Set up constants and dbs ----
pacman::p_load(apde.etl, caret, data.table, glue, here, irr, lubridate, odbc, openxlsx, rads)

load_stage_mcaid_housing_status <- function(conn = NULL,
                                               server = c("hhsaw", "phclaims"),
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
  schema <- config[[server]][["schema"]]
  to_table <- config[[server]][["to_table"]]
  timevar_table <- config[[server]][["timevar_table"]]
  month_table <- config[[server]][["month_table"]]
  icdcm_table <- config[[server]][["icdcm_header_table"]]
  kcids_table <- config[[server]][["kcids_table"]]
  z_code_crosswalk_path <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/refs/heads/main/claims_db/db_loader/mcaid/mcaid_housing_status_crosswalk.xlsx"

  
  ## Medaid data load ----
  message("Loading data to ", schema, ".", to_table)
  # Load Medicaid data
  load_sql <- glue::glue_sql("
-----------------------------------------
--Proposed revised script to create mcaid_housing_status table with following changes
--Eli Kern, PHSKC-APDE, March 2026

--Updates:
	--Switch to using elig_month in place of elig_timevar table
	--Add to_date to supplement from_date to implement time period approach instead of point-in-time housing status
	--Change search from HOMELESS to LIKE and search second line of street address as well
-----------------------------------------

if object_id(N'{`schema`}.{`to_table`}',N'U') is not null drop table {`schema`}.{`to_table`};
--Pull out ICD-CM codes associated with housing status
WITH zcodes AS (
	SELECT id_mcaid, first_service_date, icdcm_norm
    FROM {`schema`}.{`icdcm_table`} -- replace with YAML config ref
    WHERE icdcm_norm  IN ('Z590', 'Z5900','Z5901', 'Z5902', 'Z591', 'Z5910', 'Z5919') OR icdcm_norm LIKE 'Z5981%'
),
--Flag address-based housing status and combine with Z codes
temp1 as (
	SELECT
	a.id_mcaid, a.from_date, a.to_date,
	CASE
		WHEN a.geo_add1 LIKE '%HOMELESS%' OR a.geo_add2 LIKE '%HOMELESS%' THEN 1
		ELSE 0
	END AS is_homeless_addr,
	CASE
		WHEN b.icdcm_norm in ('Z590', 'Z5900','Z5901', 'Z5902') THEN 'homeless'
		WHEN b.icdcm_norm in ('Z591', 'Z5910', 'Z5919') or b.icdcm_norm like 'Z5981%' THEN 'unstably housed'
	END AS zcode_status
	FROM {`schema`}.{`month_table`} AS a -- replace with YAML config ref
	LEFT JOIN zcodes AS b
	ON a.id_mcaid = b.id_mcaid AND b.first_service_date BETWEEN a.from_date AND a.to_date
),
temp2 AS (
	--Assign housing status and subset to those with non-null housing status
	SELECT
	id_mcaid,
	from_date,
	to_date,
	CASE
		WHEN zcode_status IS NOT NULL THEN zcode_status
		WHEN is_homeless_addr = 1 THEN 'homeless'
		ELSE NULL
	END AS housing_status,
	CASE
		WHEN is_homeless_addr = 1 AND zcode_status IS NOT NULL THEN 'multiple'
		WHEN is_homeless_addr = 1 AND zcode_status IS NULL then 'homeless_address'
		WHEN is_homeless_addr = 0 AND zcode_status IS NOT NULL THEN 'z_codes'
		ELSE NULL
	END AS housing_status_source
	FROM temp1
	WHERE is_homeless_addr = 1 OR zcode_status IS NOT NULL
),
--flag time periods that have more than 1 housing_status value or housing_status_source
temp3 AS (
	SELECT id_mcaid, from_date, to_date,
	COUNT(DISTINCT housing_status) AS housing_status_dcount,
	COUNT(DISTINCT housing_status_source) AS housing_status_source_dcount
	FROM temp2
	GROUP BY id_mcaid, from_date, to_date
)
--where multiple housing_status values exist, set to homeless; for multiple sources, set to multiple
SELECT DISTINCT
a.id_mcaid,
a.from_date,
a.to_date,
CASE
	WHEN b.housing_status_dcount > 1 THEN 'homeless'
	ELSE a.housing_status
END AS housing_status,
CASE
	WHEN b.housing_status_source_dcount > 1 THEN 'multiple'
	ELSE a.housing_status_source
END AS housing_status_source,
GETDATE() as last_run
INTO {`schema`}.{`to_table`}
FROM temp2 AS a
LEFT JOIN temp3 AS b
ON (a.id_mcaid = b.id_mcaid) and (a.from_date = b.from_date) and (a.to_date = b.to_date);",
                             .con = conn);

  DBI::dbExecute(conn, load_sql);
  
}
