
/*
This table documents changes to table names/column names/data types in creating
the analytic tables from the new Medicaid Extracts compared to the old [dbo] 
schema analytic tables.

Created by: Philip Sylling, 2019-06-07
Modified by:

Returns
 prior_table_name
,prior_column_name
,prior_data_type
,new_table_name
,new_column_name
,new_data_type
,any_change, Yes if any change occurred
*/

IF OBJECT_ID('[metadata].[mcaid_format_changes]', 'U') IS NOT NULL
DROP TABLE [metadata].[mcaid_format_changes];
CREATE TABLE [metadata].[mcaid_format_changes]
(prior_table_name VARCHAR(200)
,prior_column_name VARCHAR(200)
,prior_ordinal_position VARCHAR(200)
,prior_data_type VARCHAR(200)
,new_table_name VARCHAR(200)
,new_column_name VARCHAR(200)
,new_ordinal_position VARCHAR(200)
,new_data_type VARCHAR(200)
,any_change VARCHAR(200)
,CONSTRAINT [PK_metadata_mcaid_format_changes] PRIMARY KEY CLUSTERED([new_table_name], [new_ordinal_position], [prior_ordinal_position]));
GO

INSERT INTO [metadata].[mcaid_format_changes]
VALUES
 ('dbo.mcaid_claim_dx', 'id', '1', 'varchar(200)', 'final.mcaid_claim_icdcm_header', 'id_mcaid', '1', 'varchar(200)', 'Yes')
,('dbo.mcaid_claim_dx', 'tcn', '2', 'varchar(200)', 'final.mcaid_claim_icdcm_header', 'claim_header_id', '2', 'bigint', 'Yes')
,('dbo.mcaid_claim_dx', 'dx_raw', '3', 'varchar(200)', 'final.mcaid_claim_icdcm_header', 'icdcm_raw', '3', 'varchar(200)', 'Yes')
,('dbo.mcaid_claim_dx', 'dx_norm', '4', 'varchar(200)', 'final.mcaid_claim_icdcm_header', 'icdcm_norm', '4', 'varchar(200)', 'Yes')
,('dbo.mcaid_claim_dx', 'dx_ver', '5', 'tinyint', 'final.mcaid_claim_icdcm_header', 'icdcm_version', '5', 'tinyint', 'Yes')
,('dbo.mcaid_claim_dx', 'dx_number', '6', 'tinyint', 'final.mcaid_claim_icdcm_header', 'icdcm_number', '6', 'varchar(5)', 'Yes')

,('dbo.mcaid_claim_line', 'id', '1', 'varchar(200)', 'final.mcaid_claim_line', 'id_mcaid', '1', 'varchar(200)', 'Yes')
,('dbo.mcaid_claim_line', 'tcn', '2', 'varchar(200)', 'final.mcaid_claim_line', 'claim_header_id', '2', 'bigint', 'Yes')
,('dbo.mcaid_claim_line', 'tcn_line', '3', 'varchar(200)', 'final.mcaid_claim_line', 'claim_line_id', '3', 'bigint', 'Yes')
,('dbo.mcaid_claim_line', 'rcode', '4', 'varchar(200)', 'final.mcaid_claim_line', 'rev_code', '4', 'varchar(200)', 'Yes')
,('dbo.mcaid_claim_line', 'rac_code_l', '5', 'varchar(200)', 'final.mcaid_claim_line', 'rac_code_line', '5', 'int', 'Yes')

,('dbo.mcaid_claim_pharm', 'id', '1', 'varchar(200)', 'final.mcaid_claim_pharm', 'id_mcaid', '1', 'varchar(200)', 'Yes')
,('dbo.mcaid_claim_pharm', 'tcn', '2', 'varchar(200)', 'final.mcaid_claim_pharm', 'claim_header_id', '2', 'bigint', 'Yes')
,('dbo.mcaid_claim_pharm', 'ndc_code', '3', 'varchar(200)', 'final.mcaid_claim_pharm', 'ndc', '3', 'varchar(200)', 'Yes')
,('dbo.mcaid_claim_pharm', 'drug_strength', '4', 'varchar(200)', 'final.mcaid_claim_pharm', 'Column Dropped', 'NULL', 'NULL', 'Yes')
,('dbo.mcaid_claim_pharm', 'drug_supply_d', '5', 'smallint', 'final.mcaid_claim_pharm', 'rx_days_supply', '4', 'smallint', 'Yes')
,('dbo.mcaid_claim_pharm', 'drug_dosage', '6', 'varchar(200)', 'final.mcaid_claim_pharm', 'Column Dropped', 'NULL', 'NULL', 'Yes')
,('dbo.mcaid_claim_pharm', 'drug_dispensed_amt', '7', 'numeric', 'final.mcaid_claim_pharm', 'rx_quantity', '5', 'numeric', 'Yes')
,('dbo.mcaid_claim_pharm', 'drug_fill_date', '8', 'date', 'final.mcaid_claim_pharm', 'rx_fill_date', '6', 'date', 'Yes')
,('dbo.mcaid_claim_pharm', 'prescriber_id', '9', 'varchar(200)', 'final.mcaid_claim_pharm', 'pharmacy_npi', '7', 'bigint', 'Yes')

SELECT * FROM [metadata].[mcaid_format_changes];

/*
SELECT 
 TABLE_SCHEMA + '.' + TABLE_NAME AS prior_table_name
,COLUMN_NAME AS prior_column_name
,CAST(ORDINAL_POSITION AS VARCHAR(200)) AS prior_ordinal_position
,CASE WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN DATA_TYPE + '(' +  CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(100)) + ')' ELSE DATA_TYPE END AS prior_data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
AND TABLE_NAME IN
('mcaid_claim_dx'
,'mcaid_claim_header'
,'mcaid_claim_line'
,'mcaid_claim_pharm'
,'mcaid_claim_proc'
,'mcaid_claim_summary')
ORDER BY TABLE_NAME, ORDINAL_POSITION;
*/