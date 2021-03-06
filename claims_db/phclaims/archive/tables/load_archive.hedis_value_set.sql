
USE PHClaims;
GO

INSERT INTO [archive].[hedis_measure]

SELECT 
 [version]
,[measure_id]
,[measure_name]
FROM [PHClaims].[ref].[hedis_measure]
ORDER BY [version], [measure_id];

INSERT INTO [archive].[hedis_value_set]

SELECT 
 [version]
,[measure_id]
,[value_set_name]
,[value_set_oid]
FROM [PHClaims].[ref].[hedis_value_set]
ORDER BY [version], [measure_id], [value_set_name];

INSERT INTO [archive].[hedis_medication_list]

SELECT 
 [version]
,[measure_id]
,[medication_list_name]
FROM [PHClaims].[ref].[hedis_medication_list]
ORDER BY [version], [measure_id], [medication_list_name];

INSERT INTO [archive].[hedis_code_system]

SELECT 
 [version]
,[value_set_name]
,[code_system]
,[code]
,[definition]
,[value_set_version]
,[code_system_version]
,[value_set_oid]
,[code_system_oid]
FROM [PHClaims].[ref].[hedis_code_system]
ORDER BY [version], [value_set_name], [code_system], [code];

INSERT INTO [archive].[hedis_ndc_code]

SELECT 
 [version]
,[medication_list_name]
,[ndc_code]
,[brand_name]
,[generic_product_name]
,[route]
,[description]
,[drug_id]
,[drug_name]
,[package_size]
,[unit]
,[dose]
,[form]
,[med_conversion_factor]
FROM [PHClaims].[ref].[hedis_ndc_code]
ORDER BY [version], [medication_list_name], [ndc_code];