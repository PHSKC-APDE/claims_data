
/*
This table documents changes to table names/column names/data types in creating
the analytic tables from the new Medicaid Extracts compared to the old [dbo] 
schema analytic tables.

Created by: Philip Sylling, 2019-06-07
Modified by: Philip Sylling, 2019-06-11

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
,CONSTRAINT [PK_metadata_mcaid_format_changes] PRIMARY KEY CLUSTERED([new_table_name], [new_ordinal_position], [prior_ordinal_position]));
GO

INSERT INTO [metadata].[mcaid_format_changes]
VALUES
 ('dbo.mcaid_claim_line', 'id', '1', 'varchar(200)', 'final.mcaid_claim_line', 'id_mcaid', '1', 'varchar(200)')
,('dbo.mcaid_claim_line', 'tcn', '2', 'varchar(200)', 'final.mcaid_claim_line', 'claim_header_id', '2', 'bigint')
,('dbo.mcaid_claim_line', 'tcn_line', '3', 'varchar(200)', 'final.mcaid_claim_line', 'claim_line_id', '3', 'bigint')
,('dbo.mcaid_claim_line', 'rcode', '4', 'varchar(200)', 'final.mcaid_claim_line', 'rev_code', '4', 'varchar(200)')
,('dbo.mcaid_claim_line', 'rac_code_l', '5', 'varchar(200)', 'final.mcaid_claim_line', 'rac_code_line', '5', 'int')
,('dbo.mcaid_claim_proc', 'id', '1', 'varchar(200)', 'mcaid_claim_procedure', 'id_mcaid', '1', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'tcn', '2', 'varchar(200)', 'mcaid_claim_procedure', 'claim_header_id', '2', 'bigint')
,('dbo.mcaid_claim_proc', 'pcode', '3', 'varchar(200)', 'mcaid_claim_procedure', 'procedure_code', '3', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'proc_number', '4', 'varchar(4)', 'mcaid_claim_procedure', 'procedure_code_number', '4', 'varchar(4)')
,('dbo.mcaid_claim_proc', 'pcode_mod_1', '5', 'varchar(200)', 'mcaid_claim_procedure', 'modifier_1', '5', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'pcode_mod_2', '6', 'varchar(200)', 'mcaid_claim_procedure', 'modifier_2', '6', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'pcode_mod_3', '7', 'varchar(200)', 'mcaid_claim_procedure', 'modifier_3', '7', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'pcode_mod_4', '8', 'varchar(200)', 'mcaid_claim_procedure', 'modifier_4', '8', 'varchar(200)')

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