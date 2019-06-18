
/*
This table documents changes to table names/column names/data types in creating
the analytic tables from the new Medicaid Extracts compared to the old [dbo] 
schema analytic tables.

Created by: Philip Sylling, 2019-06-07
Modified by: Philip Sylling, 2019-06-17

Returns
 mcaid_format_change_id
,prior_table_name
,prior_column_name
,prior_ordinal_position
,prior_data_type
,new_table_name
,new_column_name
,new_ordinal_position
,new_data_type
*/

IF OBJECT_ID('[metadata].[mcaid_format_change]', 'U') IS NOT NULL
DROP TABLE [metadata].[mcaid_format_change];
CREATE TABLE [metadata].[mcaid_format_change]
(mcaid_format_change_id SMALLINT IDENTITY
,prior_table_name VARCHAR(200)
,prior_column_name VARCHAR(200)
,prior_ordinal_position VARCHAR(200)
,prior_data_type VARCHAR(200)
,new_table_name VARCHAR(200)
,new_column_name VARCHAR(200)
,new_ordinal_position VARCHAR(200)
,new_data_type VARCHAR(200)
,CONSTRAINT [PK_metadata_mcaid_format_change] PRIMARY KEY CLUSTERED([mcaid_format_change_id]));
GO

INSERT INTO [metadata].[mcaid_format_change]
(prior_table_name
,prior_column_name
,prior_ordinal_position
,prior_data_type
,new_table_name
,new_column_name
,new_ordinal_position
,new_data_type)

VALUES

 ('dbo.mcaid_claim_dx', 'id', '1', 'varchar(200)', 'final.mcaid_claim_icdcm_header', 'id_mcaid', '1', 'varchar(255)')
,('dbo.mcaid_claim_dx', 'tcn', '2', 'varchar(200)', 'final.mcaid_claim_icdcm_header', 'claim_header_id', '2', 'bigint')
,('dbo.mcaid_claim_dx', 'dx_raw', '3', 'varchar(200)', 'final.mcaid_claim_icdcm_header', 'icdcm_raw', '3', 'varchar(255)')
,('dbo.mcaid_claim_dx', 'dx_norm', '4', 'varchar(200)', 'final.mcaid_claim_icdcm_header', 'icdcm_norm', '4', 'varchar(255)')
,('dbo.mcaid_claim_dx', 'dx_ver', '5', 'tinyint', 'final.mcaid_claim_icdcm_header', 'icdcm_version', '5', 'tinyint')
,('dbo.mcaid_claim_dx', 'dx_number', '6', 'tinyint', 'final.mcaid_claim_icdcm_header', 'icdcm_number', '6', 'varchar(5)')
,('NULL', 'NULL', 'NULL', 'NULL', 'final.mcaid_claim_icdcm_header', 'last_run', '7', 'datetime')

,('dbo.mcaid_claim_header', 'id', '1', 'varchar(200)', 'mcaid_claim_header', 'id_mcaid', '', 'varchar(255)')
,('dbo.mcaid_claim_header', 'tcn', '2', 'varchar(200)', 'mcaid_claim_header', 'claim_header_id', '', 'bigint')
,('dbo.mcaid_claim_header', 'clm_type_code', '3', 'varchar(200)', 'mcaid_claim_header', 'clm_type_mcaid_id', '', 'varchar(20)')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'claim_type_id', '', 'tinyint')
,('dbo.mcaid_claim_header', 'from_date', '4', 'date', 'mcaid_claim_header', 'first_service_date', '', 'date')
,('dbo.mcaid_claim_header', 'to_date', '5', 'date', 'mcaid_claim_header', 'last_service_date', '', 'date')
,('dbo.mcaid_claim_header', 'patient_status', '6', 'varchar(200)', 'mcaid_claim_header', 'patient_status', '', 'varchar(255)')
,('dbo.mcaid_claim_header', 'adm_source', '7', 'varchar(200)', 'mcaid_claim_header', 'admsn_source', '', 'varchar(255)')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'admsn_date', '', 'date')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'admsn_time', '', 'time(0)')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'dschrg_date', '', 'date')
,('dbo.mcaid_claim_header', 'pos_code', '8', 'varchar(200)', 'mcaid_claim_header', 'place_of_service_code', '', 'varchar(255)')
,('dbo.mcaid_claim_header', 'clm_cat_code', '9', 'varchar(200)', 'NULL', 'NULL', 'NULL', 'NULL')
,('dbo.mcaid_claim_header', 'bill_type_code', '10', 'varchar(200)', 'mcaid_claim_header', 'type_of_bill_code', '', 'varchar(255)')
,('dbo.mcaid_claim_header', 'clm_status_code', '11', 'varchar(200)', 'mcaid_claim_header', 'clm_status_code', '', 'tinyint')
,('dbo.mcaid_claim_header', 'billing_npi', '12', 'varchar(200)', 'mcaid_claim_header', 'billing_provider_npi', '', 'bigint')
,('dbo.mcaid_claim_header', 'drg_code', '13', 'varchar(200)', 'mcaid_claim_header', 'drvd_drg_code', '', 'varchar(255)')
,('dbo.mcaid_claim_header', 'unit_srvc_h', '14', 'numeric', 'NULL', 'NULL', 'NULL', 'NULL')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'insrnc_cvrg_code', '', 'varchar(255)')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'last_pymnt_date', '', 'date')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'bill_date', '', 'date')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'system_in_date', '', 'date')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'claim_header_id_date', '', 'date')
,('dbo.mcaid_claim_summary', 'id', '1', 'varchar(200)', 'mcaid_claim_header', 'id_mcaid', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'tcn', '2', 'bigint', 'mcaid_claim_header', 'claim_header_id', '', 'bigint')
,('dbo.mcaid_claim_summary', 'clm_type_code', '3', 'varchar(200)', 'mcaid_claim_header', 'clm_type_mcaid_id', '', 'varchar(20)')
,('dbo.mcaid_claim_summary', 'from_date', '4', 'date', 'mcaid_claim_header', 'first_service_date', '', 'date')
,('dbo.mcaid_claim_summary', 'to_date', '5', 'date', 'mcaid_claim_header', 'last_service_date', '', 'date')
,('dbo.mcaid_claim_summary', 'patient_status', '6', 'varchar(200)', 'mcaid_claim_header', 'patient_status', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'adm_source', '7', 'varchar(200)', 'mcaid_claim_header', 'admsn_source', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'pos_code', '8', 'varchar(200)', 'mcaid_claim_header', 'place_of_service_code', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'clm_cat_code', '9', 'varchar(200)', 'NULL', 'NULL', 'NULL', 'NULL')
,('dbo.mcaid_claim_summary', 'bill_type_code', '10', 'varchar(200)', 'mcaid_claim_header', 'type_of_bill_code', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'clm_status_code', '11', 'varchar(200)', 'mcaid_claim_header', 'clm_status_code', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'billing_npi', '12', 'varchar(200)', 'mcaid_claim_header', 'billing_provider_npi', '', 'bigint')
,('dbo.mcaid_claim_summary', 'drg_code', '13', 'varchar(200)', 'mcaid_claim_header', 'drvd_drg_code', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'unit_srvc_h', '14', 'numeric', 'NULL', 'NULL', 'NULL', 'NULL')
,('dbo.mcaid_claim_summary', 'dx_norm', '15', 'varchar(200)', 'mcaid_claim_header', 'primary_diagnosis', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'dx_ver', '16', 'tinyint', 'mcaid_claim_header', 'icdcm_version', '', 'tinyint')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'primary_diagnosis_poa', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'mental_dx1', '17', 'tinyint', 'mcaid_claim_header', 'mental_dx1', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'mental_dxany', '18', 'tinyint', 'mcaid_claim_header', 'mental_dxany', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'mental_dx_rda_any', '19', 'tinyint', 'mcaid_claim_header', 'mental_dx_rda_any', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'sud_dx_rda_any', '20', 'tinyint', 'mcaid_claim_header', 'sud_dx_rda_any', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'maternal_dx1', '21', 'tinyint', 'mcaid_claim_header', 'maternal_dx1', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'maternal_broad_dx1', '22', 'tinyint', 'mcaid_claim_header', 'maternal_broad_dx1', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'newborn_dx1', '23', 'tinyint', 'mcaid_claim_header', 'newborn_dx1', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed', '24', 'tinyint', 'mcaid_claim_header', 'ed', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_nohosp', '25', 'tinyint', 'mcaid_claim_header', 'ed_nohosp', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_bh', '26', 'tinyint', 'mcaid_claim_header', 'ed_bh', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_avoid_ca', '27', 'tinyint', 'mcaid_claim_header', 'ed_avoid_ca', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_avoid_ca_nohosp', '28', 'tinyint', 'mcaid_claim_header', 'ed_avoid_ca_nohosp', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_ne_nyu', '29', 'tinyint', 'mcaid_claim_header', 'ed_ne_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_pct_nyu', '30', 'tinyint', 'mcaid_claim_header', 'ed_pct_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_pa_nyu', '31', 'tinyint', 'mcaid_claim_header', 'ed_pa_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_npa_nyu', '32', 'tinyint', 'mcaid_claim_header', 'ed_npa_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_mh_nyu', '33', 'tinyint', 'mcaid_claim_header', 'ed_mh_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_sud_nyu', '34', 'tinyint', 'mcaid_claim_header', 'ed_sud_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_alc_nyu', '35', 'tinyint', 'mcaid_claim_header', 'ed_alc_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_injury_nyu', '36', 'tinyint', 'mcaid_claim_header', 'ed_injury_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_unclass_nyu', '37', 'tinyint', 'mcaid_claim_header', 'ed_unclass_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_emergent_nyu', '38', 'tinyint', 'mcaid_claim_header', 'ed_emergent_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_nonemergent_nyu', '39', 'tinyint', 'mcaid_claim_header', 'ed_nonemergent_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_intermediate_nyu', '40', 'tinyint', 'mcaid_claim_header', 'ed_intermediate_nyu', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'inpatient', '41', 'tinyint', 'mcaid_claim_header', 'inpatient', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ipt_medsurg', '42', 'tinyint', 'mcaid_claim_header', 'ipt_medsurg', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ipt_bh', '43', 'tinyint', 'mcaid_claim_header', 'ipt_bh', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'intent', '44', 'varchar(200)', 'mcaid_claim_header', 'intent', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'mechanism', '45', 'varchar(200)', 'mcaid_claim_header', 'mechanism', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'sdoh_any', '46', 'tinyint', 'mcaid_claim_header', 'sdoh_any', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ed_sdoh', '47', 'tinyint', 'mcaid_claim_header', 'ed_sdoh', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ipt_sdoh', '48', 'tinyint', 'mcaid_claim_header', 'ipt_sdoh', '', 'tinyint')
,('dbo.mcaid_claim_summary', 'ccs', '49', 'varchar(200)', 'mcaid_claim_header', 'ccs', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'ccs_description', '50', 'varchar(500)', 'mcaid_claim_header', 'ccs_description', '', 'varchar(500)')
,('dbo.mcaid_claim_summary', 'ccs_description_plain_lang', '51', 'varchar(500)', 'mcaid_claim_header', 'ccs_description_plain_lang', '', 'varchar(500)')
,('dbo.mcaid_claim_summary', 'ccs_mult1', '52', 'varchar(200)', 'mcaid_claim_header', 'ccs_mult1', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'ccs_mult1_description', '53', 'varchar(500)', 'mcaid_claim_header', 'ccs_mult1_description', '', 'varchar(500)')
,('dbo.mcaid_claim_summary', 'ccs_mult2', '54', 'varchar(200)', 'mcaid_claim_header', 'ccs_mult2', '', 'varchar(255)')
,('dbo.mcaid_claim_summary', 'ccs_mult2_description', '55', 'varchar(500)', 'mcaid_claim_header', 'ccs_mult2_description', '', 'varchar(500)')
,('dbo.mcaid_claim_summary', 'ccs_mult2_plain_lang', '56', 'varchar(500)', 'mcaid_claim_header', 'ccs_mult2_plain_lang', '', 'varchar(500)')
,('dbo.mcaid_claim_summary', 'ccs_final_description', '57', 'varchar(500)', 'mcaid_claim_header', 'ccs_final_description', '', 'varchar(500)')
,('dbo.mcaid_claim_summary', 'ccs_final_plain_lang', '58', 'varchar(500)', 'mcaid_claim_header', 'ccs_final_plain_lang', '', 'varchar(500)')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_header', 'last_run', '', 'datetime')

,('dbo.mcaid_claim_line', 'id', '1', 'varchar(200)', 'final.mcaid_claim_line', 'id_mcaid', '1', 'varchar(200)')
,('dbo.mcaid_claim_line', 'tcn', '2', 'varchar(200)', 'final.mcaid_claim_line', 'claim_header_id', '2', 'bigint')
,('dbo.mcaid_claim_line', 'tcn_line', '3', 'varchar(200)', 'final.mcaid_claim_line', 'claim_line_id', '3', 'bigint')
,('dbo.mcaid_claim_line', 'rcode', '4', 'varchar(200)', 'final.mcaid_claim_line', 'rev_code', '4', 'varchar(200)')
,('dbo.mcaid_claim_line', 'rac_code_l', '5', 'varchar(200)', 'final.mcaid_claim_line', 'rac_code_line', '5', 'int')
,('NULL', 'NULL', 'NULL', 'NULL', 'final.mcaid_claim_line', 'last_run', '6', 'datetime')

,('dbo.mcaid_claim_pharm', 'id', '1', 'varchar(200)', 'final.mcaid_claim_pharm', 'id_mcaid', '1', 'varchar(255)')
,('dbo.mcaid_claim_pharm', 'tcn', '2', 'varchar(200)', 'final.mcaid_claim_pharm', 'claim_header_id', '2', 'bigint')
,('dbo.mcaid_claim_pharm', 'ndc_code', '3', 'varchar(200)', 'final.mcaid_claim_pharm', 'ndc', '3', 'varchar(255)')
,('dbo.mcaid_claim_pharm', 'drug_strength', '4', 'varchar(200)', 'NULL', 'NULL', 'NULL', 'NULL')
,('dbo.mcaid_claim_pharm', 'drug_supply_d', '5', 'smallint', 'final.mcaid_claim_pharm', 'rx_days_supply', '4', 'smallint')
,('dbo.mcaid_claim_pharm', 'drug_dosage', '6', 'varchar(200)', 'NULL', 'NULL', 'NULL', 'NULL')
,('dbo.mcaid_claim_pharm', 'drug_dispensed_amt', '7', 'numeric', 'final.mcaid_claim_pharm', 'rx_quantity', '5', 'numeric')
,('dbo.mcaid_claim_pharm', 'drug_fill_date', '8', 'date', 'final.mcaid_claim_pharm', 'rx_fill_date', '6', 'date')
,('NULL', 'NULL', 'NULL', 'NULL', 'final.mcaid_claim_pharm', 'prescriber_id_format', '7', 'varchar(10)')
,('dbo.mcaid_claim_pharm', 'prescriber_id', '9', 'varchar(200)', 'final.mcaid_claim_pharm', 'prescriber_id', '8', 'varchar(255)')
,('dbo.mcaid_claim_pharm', 'prescriber_id', '9', 'varchar(200)', 'final.mcaid_claim_pharm', 'pharmacy_npi', '9', 'bigint')
,('NULL', 'NULL', 'NULL', 'NULL', 'final.mcaid_claim_pharm', 'last_run', '10', 'datetime')

,('dbo.mcaid_claim_proc', 'id', '1', 'varchar(200)', 'mcaid_claim_procedure', 'id_mcaid', '1', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'tcn', '2', 'varchar(200)', 'mcaid_claim_procedure', 'claim_header_id', '2', 'bigint')
,('dbo.mcaid_claim_proc', 'pcode', '3', 'varchar(200)', 'mcaid_claim_procedure', 'procedure_code', '3', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'proc_number', '4', 'varchar(4)', 'mcaid_claim_procedure', 'procedure_code_number', '4', 'varchar(4)')
,('dbo.mcaid_claim_proc', 'pcode_mod_1', '5', 'varchar(200)', 'mcaid_claim_procedure', 'modifier_1', '5', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'pcode_mod_2', '6', 'varchar(200)', 'mcaid_claim_procedure', 'modifier_2', '6', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'pcode_mod_3', '7', 'varchar(200)', 'mcaid_claim_procedure', 'modifier_3', '7', 'varchar(200)')
,('dbo.mcaid_claim_proc', 'pcode_mod_4', '8', 'varchar(200)', 'mcaid_claim_procedure', 'modifier_4', '8', 'varchar(200)')
,('NULL', 'NULL', 'NULL', 'NULL', 'mcaid_claim_procedure', 'last_run', '9', 'datetime')

SELECT * FROM [metadata].[mcaid_format_change];

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