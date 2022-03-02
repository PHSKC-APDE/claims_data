# This code loads table ([stage].[mcaid_claim_value_set]) to hold DISTINCT 
# claim headers that meet RDA and/or HEDIS value set definitions.
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
# 
# Created by: Philip Sylling, 2019-11-14
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-12
# 


message("Loading stage.mcaid_claim_value_set")

#### SET UP FUNCTIONS, ETC. ####
if (!exists("db_claims")) {
        db_claims <- DBI::dbConnect(odbc(), "PHClaims")  
}

if (!exists("add_index")) {
        devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")
}


#### TRUNCATE TABLE AND REMOVE ANY INDICES ####
DBI::dbGetQuery(db_claims, "TRUNCATE TABLE [stage].[mcaid_claim_value_set]")

# This code pulls out the index name
existing_index <- dbGetQuery(db_claims, 
glue::glue_sql("SELECT DISTINCT a.existing_index
  FROM
  (SELECT ind.name AS existing_index
    FROM
    (SELECT object_id, name, type_desc FROM sys.indexes
    WHERE type_desc LIKE 'CLUSTERED%') ind
    JOIN
    (SELECT name, schema_id, object_id FROM sys.tables
    WHERE name = 'mcaid_claim_value_set') t
    ON ind.object_id = t.object_id
    INNER JOIN
    (SELECT name, schema_id FROM sys.schemas
    WHERE name = 'stage') s
    ON t.schema_id = s.schema_id) a", 
               .con = db_claims))[[1]]

if (length(existing_index) != 0) {
        message("Removing existing clustered/clustered columnstore index")
        dbGetQuery(db_claims,
                   glue::glue_sql("DROP INDEX {`existing_index`} ON 
                                  stage.mcaid_claim_value_set", .con = db_claims))
}


#### LOAD DATA IN ####
DBI::dbGetQuery(db_claims,
"INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 rda.[value_set_group]
,rda.[value_set_name]
,rda.[data_source_type]
,rda.[sub_group]
,rda.[code_set]
,NULL AS [primary_dx_only]
,pr.[id_mcaid]
,pr.[claim_header_id]
,pr.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN [ref].[rda_value_set] AS rda
ON rda.[code_set] IN ('CPT', 'HCPCS', 'ICD10PCS', 'ICD9PCS')
AND pr.[procedure_code] = rda.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 rda.[value_set_group]
,rda.[value_set_name]
,rda.[data_source_type]
,rda.[sub_group]
,rda.[code_set]
,NULL AS [primary_dx_only]
,hd.[id_mcaid]
,hd.[claim_header_id]
,hd.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_header] AS hd
INNER JOIN [ref].[rda_value_set] AS rda
ON rda.[code_set] IN ('DRG')
AND hd.[drvd_drg_code] = rda.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 rda.[value_set_group]
,rda.[value_set_name]
,rda.[data_source_type]
,rda.[sub_group]
,rda.[code_set]
,'Y' AS [primary_dx_only]
,dx.[id_mcaid]
,dx.[claim_header_id]
,dx.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_icdcm_header] AS dx
INNER JOIN [ref].[rda_value_set] AS rda
ON rda.[code_set] IN ('ICD10CM')
AND dx.[icdcm_version] = 10
AND dx.[icdcm_number] = '01'
AND dx.[icdcm_norm] = rda.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 rda.[value_set_group]
,rda.[value_set_name]
,rda.[data_source_type]
,rda.[sub_group]
,rda.[code_set]
,'Y' AS [primary_dx_only]
,dx.[id_mcaid]
,dx.[claim_header_id]
,dx.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_icdcm_header] AS dx
INNER JOIN [ref].[rda_value_set] AS rda
ON rda.[code_set] IN ('ICD9CM')
AND dx.[icdcm_version] = 9
AND dx.[icdcm_number] = '01'
AND dx.[icdcm_norm] = rda.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 rda.[value_set_group]
,rda.[value_set_name]
,rda.[data_source_type]
,rda.[sub_group]
,rda.[code_set]
,'N' AS [primary_dx_only]
,dx.[id_mcaid]
,dx.[claim_header_id]
,dx.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_icdcm_header] AS dx
INNER JOIN [ref].[rda_value_set] AS rda
ON rda.[code_set] IN ('ICD10CM')
AND dx.[icdcm_version] = 10
--AND dx.[icdcm_number] = '01'
AND dx.[icdcm_norm] = rda.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 rda.[value_set_group]
,rda.[value_set_name]
,rda.[data_source_type]
,rda.[sub_group]
,rda.[code_set]
,'N' AS [primary_dx_only]
,dx.[id_mcaid]
,dx.[claim_header_id]
,dx.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_icdcm_header] AS dx
INNER JOIN [ref].[rda_value_set] AS rda
ON rda.[code_set] IN ('ICD9CM')
AND dx.[icdcm_version] = 9
--AND dx.[icdcm_number] = '01'
AND dx.[icdcm_norm] = rda.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 rda.[value_set_group]
,rda.[value_set_name]
,rda.[data_source_type]
,rda.[sub_group]
,rda.[code_set]
,NULL AS [primary_dx_only]
,ph.[id_mcaid]
,ph.[claim_header_id]
,ph.[rx_fill_date] AS [service_date]
FROM [final].[mcaid_claim_pharm] AS ph
INNER JOIN [ref].[rda_value_set] AS rda
ON rda.[code_set] IN ('NDC')
AND rda.[active] = 'Y'
AND ph.[ndc] = rda.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 rda.[value_set_group]
,rda.[value_set_name]
,rda.[data_source_type]
,rda.[sub_group]
,rda.[code_set]
,NULL AS [primary_dx_only]
,ln.[id_mcaid]
,ln.[claim_header_id]
,ln.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_line] AS ln
INNER JOIN [ref].[rda_value_set] AS rda
ON rda.[code_set] IN ('UBREV')
AND ln.[rev_code] = rda.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 'HEDIS' AS [value_set_group]
,hed.[value_set_name]
,NULL AS [data_source_type]
,NULL AS [sub_group]
,hed.[code_system]
,NULL AS [primary_dx_only]
,pr.[id_mcaid]
,pr.[claim_header_id]
,pr.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
('FUH Stand Alone Visits'
,'FUH Visits Group 1'
,'FUH Visits Group 2'
,'TCM 7 Day'
,'TCM 14 Day')
AND hed.[code_system] IN ('CPT', 'HCPCS')
AND pr.[procedure_code] = hed.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 'HEDIS' AS [value_set_group]
,hed.[value_set_name]
,NULL AS [data_source_type]
,NULL AS [sub_group]
,hed.[code_system]
,NULL AS [primary_dx_only]
,ln.[id_mcaid]
,ln.[claim_header_id]
,ln.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_line] AS ln
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Inpatient Stay'
,'Nonacute Inpatient Stay'
,'FUH RevCodes Group 1'
,'FUH RevCodes Group 2')
AND hed.[code_system] IN ('UBREV')
AND ln.[rev_code] = hed.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 'HEDIS' AS [value_set_group]
,hed.[value_set_name]
,NULL AS [data_source_type]
,NULL AS [sub_group]
,hed.[code_system]
,NULL AS [primary_dx_only]
,hd.[id_mcaid]
,hd.[claim_header_id]
,hd.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_header] AS hd
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Nonacute Inpatient Stay')
AND hed.[code_system] = 'UBTOB' 
AND hd.[type_of_bill_code] = hed.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 'HEDIS' AS [value_set_group]
,hed.[value_set_name]
,NULL AS [data_source_type]
,NULL AS [sub_group]
,hed.[code_system]
,NULL AS [primary_dx_only]
,hd.[id_mcaid]
,hd.[claim_header_id]
,hd.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_header] AS hd
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
('FUH POS Group 1'
,'FUH POS Group 2')
AND hed.[code_system] = 'POS' 
AND hd.[place_of_service_code] = hed.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 'HEDIS' AS [value_set_group]
,hed.[value_set_name]
,NULL AS [data_source_type]
,NULL AS [sub_group]
,hed.[code_system]
,'Y' AS [primary_dx_only]
,dx.[id_mcaid]
,dx.[claim_header_id]
,dx.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_icdcm_header] AS dx
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN
('Mental Health Diagnosis'
,'Mental Illness')
AND hed.[code_system] = 'ICD10CM'
AND dx.[icdcm_version] = 10
-- Principal Diagnosis
AND dx.[icdcm_number] = '01'
AND dx.[icdcm_norm] = hed.[code];

INSERT INTO [stage].[mcaid_claim_value_set] WITH (TABLOCK)
SELECT DISTINCT
--TOP(100)
 'HEDIS' AS [value_set_group]
,hed.[value_set_name]
,NULL AS [data_source_type]
,NULL AS [sub_group]
,hed.[code_system]
,'N' AS [primary_dx_only]
,dx.[id_mcaid]
,dx.[claim_header_id]
,dx.[first_service_date] AS [service_date]
FROM [final].[mcaid_claim_icdcm_header] AS dx
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN
('Mental Health Diagnosis'
,'Mental Illness')
AND hed.[code_system] = 'ICD10CM'
AND dx.[icdcm_version] = 10
--AND dx.[icdcm_number] = '01'
AND dx.[icdcm_norm] = hed.[code];")


#### ADD NEW INDEX ####
DBI::dbGetQuery(db_claims,
                "CREATE CLUSTERED COLUMNSTORE INDEX [idx_ccs_mcaid_claim_value_set]
                ON [stage].[mcaid_claim_value_set]")
