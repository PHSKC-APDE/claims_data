
USE [PHClaims];
GO

/*
Set up a table with condition names and documentation
*/
IF OBJECT_ID('[ref].[comorb_cond_lookup]') IS NOT NULL
DROP TABLE [ref].[comorb_cond_lookup];
CREATE TABLE [ref].[comorb_cond_lookup]
([cond_id] SMALLINT NOT NULL
,[short_name] VARCHAR(50) NULL
,[long_name] VARCHAR(255) NULL
,[definition] VARCHAR(50) NULL
,[elixhauser_wgt] SMALLINT NULL
,[charlson_wgt] SMALLINT NULL
,[gagne_wgt] SMALLINT NULL
,[reference] VARCHAR(MAX) NULL
,CONSTRAINT [pk_comorb_cond_lookup] PRIMARY KEY CLUSTERED ([cond_id]));

INSERT INTO [ref].[comorb_cond_lookup]
VALUES
 (1,'CHF','Congestive heart failure','Elixhauser',1,1,2,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(2,'Arrhythmia','Cardiac arrhythmias','Elixhauser',1,NULL,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(3,'Valvular','Valvular disease','Elixhauser',1,NULL,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(4,'PulmCirc','Pulmonary circulation Disorders','Elixhauser',1,NULL,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(5,'PVD','Peripheral vascular disorders','Elixhauser',1,1,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(6,'HypertU','Hypertension, uncomplicated','Elixhauser',1,NULL,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(7,'HypertC','Hypertension, complicated','Elixhauser',1,NULL,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(8,'Paralysis','Paralysis (Hemiplegia or paraplegia)','Elixhauser',1,2,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(9,'Neuro','Other neurological disorders','Elixhauser',1,NULL,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(10,'COPD','Chronic pulmonary disease','Elixhauser',1,1,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(11,'DiabU_El','Diabetes, uncomplicated (Elixhauser definition)','Elixhauser',1,NULL,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(12,'DiabC_El','Diabetes, complicated (Elixhauser definition)','Elixhauser',1,NULL,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(13,'Thyroid','Hypothyroidism','Elixhauser',1,NULL,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(14,'RenalFail_El','Renal failure (Elixhauser definition)','Elixhauser',1,NULL,2,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(15,'Liver_El','Liver disease (Elixhauser definition)','Elixhauser',1,NULL,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(16,'PepticUlcer_El','Peptic ulcer disease excluding bleeding (Elixhauser definition)','Elixhauser',1,NULL,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(17,'HIVAIDS','AIDS/HIV','Elixhauser',1,6,-1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(18,'Lymphoma','Lymphoma','Elixhauser',1,NULL,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(19,'Metastatic','Metastatic cancer','Elixhauser',1,6,5,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(20,'Tumor_El','Solid tumor without metastasis (Elixhauser definition)','Elixhauser',1,NULL,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(21,'Rheumatic_El','Rheumatoid arthritis/collagen vascular diseases (Elixhauser definition)','Elixhauser',1,NULL,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(22,'Coagulopathy','Coagulopathy','Elixhauser',1,NULL,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(23,'Obesity','Obesity','Elixhauser',1,NULL,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(24,'WeightLoss','Weight loss','Elixhauser',1,NULL,2,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(25,'Electrolyte','Fluid and electrolyte disorders','Elixhauser',1,NULL,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(26,'AnemiaB','Blood loss anemia','Elixhauser',1,NULL,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(27,'AnemiaD','Deficiency anemia','Elixhauser',1,NULL,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(28,'Alcohol','Alcohol abuse','Elixhauser',1,NULL,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(29,'DrugAbuse','Drug abuse','Elixhauser',1,NULL,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(30,'Psychoses','Psychoses','Elixhauser',1,NULL,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(31,'Depression','Depression','Elixhauser',1,NULL,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(32,'MI','Myocardial infarction','Charlson',NULL,1,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(33,'Cerebrovascular','Cerebrovascular disease','Charlson',NULL,1,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(34,'Dementia','Dementia','Charlson',NULL,1,2,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(35,'Rheumatic_Ch','Rheumatic disease (Charlson definition)','Charlson',NULL,1,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(36,'PepticUlcer_Ch','Peptic ulcer disease (Charlson definition)','Charlson',NULL,1,0,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(37,'MildLiver_Ch','Mild liver disease (Charlson definition)','Charlson',NULL,1,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(38,'DiabU_Ch','Diabetes without chronic complication (Charlson definition)','Charlson',NULL,1,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(39,'DiabC_Ch','Diabetes with chronic complication (Charlson definition)','Charlson',NULL,2,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(40,'RenalFail_Ch','Renal disease (Charlson definition)','Charlson',NULL,2,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(41,'Tumor_Ch','Any malignancy, including lymphoma and leukemia, except malignant neoplasm of skin (Charlson definition)','Charlson',NULL,2,1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(42,'SevereLiver_Ch','Moderate or severe liver disease (Charlson definition)','Charlson',NULL,3,NULL,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')
,(43,'HypertAny','Hypertension (Uncomplicated or complicated)','Elixhauser',NULL,NULL,-1,'Quan, Hude, et al. "Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data." Medical care (2005): 1130-1139.')

/*
Create some variables to store column names from [ref].[comorb_cond_lookup]
to dynamically generate column lists for tables below.
*/
DECLARE @StuffedCond_ID AS NVARCHAR(MAX) = (
SELECT STUFF(
(
SELECT ',' + QUOTENAME([cond_id])
FROM 
(
SELECT DISTINCT [cond_id]
FROM [ref].[comorb_cond_lookup]
) AS a
ORDER BY [cond_id]
FOR XML PATH(''), TYPE).value('.[1]', 'VARCHAR(MAX)'), 1, 1, ''));

DECLARE @StuffedShortName AS NVARCHAR(MAX) = (
SELECT STUFF(
(
SELECT ',' + QUOTENAME([short_name])
FROM 
(
SELECT DISTINCT [cond_id], [short_name]
FROM [ref].[comorb_cond_lookup]
) AS a
ORDER BY [cond_id]
FOR XML PATH(''), TYPE).value('.[1]', 'VARCHAR(MAX)'), 1, 1, ''));

DECLARE @StuffedShortNameDDL AS NVARCHAR(MAX) = (
SELECT STUFF(
(
SELECT ',' + QUOTENAME([short_name]) + ' TINYINT NULL'
FROM 
(
SELECT DISTINCT [cond_id], [short_name]
FROM [ref].[comorb_cond_lookup]
) AS a
ORDER BY [cond_id]
FOR XML PATH(''), TYPE).value('.[1]', 'VARCHAR(MAX)'), 1, 1, ''));

DECLARE @ISNULLShortName AS NVARCHAR(MAX) = (
SELECT STUFF(
(
SELECT ',' + 'ISNULL(' + QUOTENAME([short_name]) + ', 0) AS ' + QUOTENAME([short_name])
FROM 
(
SELECT DISTINCT [cond_id], [short_name]
FROM [ref].[comorb_cond_lookup]
) AS a
ORDER BY [cond_id]
FOR XML PATH(''), TYPE).value('.[1]', 'VARCHAR(MAX)'), 1, 1, ''));

DECLARE @StuffedShortDesc AS NVARCHAR(MAX) = (
SELECT STUFF(
(
SELECT ',' + QUOTENAME([short_name])
FROM 
(
SELECT DISTINCT [cond_id], [short_name] + '_Desc' AS [short_name]
FROM [ref].[comorb_cond_lookup]
) AS a
ORDER BY [cond_id]
FOR XML PATH(''), TYPE).value('.[1]', 'VARCHAR(MAX)'), 1, 1, ''));

DECLARE @StuffedShortNameAndDesc AS NVARCHAR(MAX) = (
SELECT STUFF(
(
SELECT ',' + QUOTENAME([short_name])
FROM (
SELECT DISTINCT [cond_id], [short_name]
FROM [ref].[comorb_cond_lookup]
UNION ALL 
SELECT DISTINCT [cond_id], [short_name] + '_Desc' AS [short_name]
FROM [ref].[comorb_cond_lookup]
) AS a
ORDER BY [cond_id], [short_name]
FOR XML PATH(''), TYPE).value('.[1]', 'VARCHAR(MAX)'), 1, 1, ''));

DECLARE @StuffedShortNameAndDescDDL AS NVARCHAR(MAX) = (
SELECT STUFF(
(
SELECT ',' + [short_name]
FROM 
(
SELECT DISTINCT [cond_id], QUOTENAME([short_name]) + ' VARCHAR(50) NULL' AS [short_name]
FROM [ref].[comorb_cond_lookup]
UNION ALL
SELECT DISTINCT [cond_id], QUOTENAME([short_name] + '_Desc') + ' VARCHAR(255) NULL' AS [short_name]
FROM [ref].[comorb_cond_lookup]
) AS a
ORDER BY [cond_id], [short_name]
FOR XML PATH(''), TYPE).value('.[1]', 'VARCHAR(MAX)'), 1, 1, ''));
/*
PRINT @StuffedCond_ID;
PRINT @StuffedShortName;
PRINT @StuffedShortNameDDL;
PRINT @ISNULLShortName;
PRINT @StuffedShortDesc;
PRINT @StuffedShortNameAndDesc;
PRINT @StuffedShortNameAndDescDDL;
*/
/*
[1],[2],[3],...
[CHF],[Arrhythmia],[Valvular],...
[CHF] TINYINT NULL,[Arrhythmia] TINYINT NULL,[Valvular] TINYINT NULL,...
ISNULL([CHF], 0) AS [CHF],ISNULL([Arrhythmia], 0) AS [Arrhythmia],ISNULL([Valvular], 0) AS [Valvular],...
[CHF_Desc],[Arrhythmia_Desc],[Valvular_Desc],...
[CHF],[CHF_Desc],[Arrhythmia],[Arrhythmia_Desc],[Valvular],[Valvular_Desc],...
[CHF] VARCHAR(50) NULL,[CHF_Desc] VARCHAR(255) NULL,[Arrhythmia] VARCHAR(50) NULL,[Arrhythmia_Desc] VARCHAR(255) NULL,[Valvular] VARCHAR(50) NULL,[Valvular_Desc] VARCHAR(255) NULL,...
*/

/*
Now use the column lists above to create the wide
[ref].[comorb_dx_lookup] table.
*/
DECLARE @create_comorb_dx_lookup AS NVARCHAR(MAX) = '
IF OBJECT_ID(''[ref].[comorb_dx_lookup]'') IS NOT NULL
DROP TABLE [ref].[comorb_dx_lookup];
CREATE TABLE [ref].[comorb_dx_lookup]
([dx] VARCHAR(50) NOT NULL
,[dx_ver] TINYINT NOT NULL
,' + @StuffedShortNameDDL + '
,CONSTRAINT [pk_comorb_dx_lookup] PRIMARY KEY CLUSTERED([dx_ver], [dx]));';
--PRINT @create_comorb_dx_lookup;

EXEC (@create_comorb_dx_lookup);

/*
Now create the normalized value set table.
*/
IF OBJECT_ID('[ref].[comorb_value_set]') IS NOT NULL
DROP TABLE [ref].[comorb_value_set];
CREATE TABLE [ref].[comorb_value_set]
([dx] VARCHAR(50) NOT NULL
,[dx_ver] TINYINT NOT NULL
,[cond_id] SMALLINT NOT NULL
,[short_name] VARCHAR(50) NULL
,[definition] VARCHAR(50) NULL
,[elixhauser_wgt] SMALLINT NULL
,[charlson_wgt] SMALLINT NULL
,[gagne_wgt] SMALLINT NULL
,[flag] SMALLINT NOT NULL
,CONSTRAINT [pk_comorb_value_set] PRIMARY KEY CLUSTERED([cond_id], [dx_ver], [dx]));

/*
Now load the tables created above.
*/

/*
String-replace ICD9CM values
*/
IF OBJECT_ID('tempdb..#icd9', 'U') IS NOT NULL 
DROP TABLE #icd9;
SELECT DISTINCT 
 [dx]
,[dx_ver]
,SUBSTRING([dx], 1, 3) AS ICD9_3
,SUBSTRING([dx], 1, 4) AS ICD9_4
,SUBSTRING([dx], 1, 5) AS ICD9_5
INTO #icd9
FROM [ref].[dx_lookup]
WHERE [dx_ver] = 9;

CREATE UNIQUE NONCLUSTERED INDEX idx_nc_#icd9 ON #icd9(ICD9_3, ICD9_4, ICD9_5) INCLUDE([dx]);

/*
String-replace ICD10CM values
*/
IF OBJECT_ID('tempdb..#icd10', 'U') IS NOT NULL 
DROP TABLE #icd10;
SELECT DISTINCT
 [dx]
,[dx_ver]
,SUBSTRING([dx], 1, 3) AS ICD10_3
,SUBSTRING([dx], 1, 4) AS ICD10_4
,SUBSTRING([dx], 1, 5) AS ICD10_5
,SUBSTRING([dx], 1, 6) AS ICD10_6
,SUBSTRING([dx], 1, 7) AS ICD10_7
INTO #icd10
FROM [ref].[dx_lookup]
WHERE [dx_ver] = 10;

CREATE UNIQUE NONCLUSTERED INDEX idx_nc_#icd10 ON #icd10(ICD10_3, ICD10_4, ICD10_5, ICD10_6, ICD10_7) INCLUDE([dx]);

/*
Flag ICD9CM codes in #icd9 that belong to conditions in 
[ref].[comorb_cond_lookup]. The conditions are identified by [cond_id] from 
[ref].[comorb_cond_lookup] rather than their names, so new conditions can be 
added by adding a row to the [ref].[comorb_cond_lookup] table and adding CASE 
statements below.
*/
IF OBJECT_ID('tempdb..#flag_icd9_conditions', 'U') IS NOT NULL 
DROP TABLE #flag_icd9_conditions;
SELECT 
 [dx]
,[dx_ver]
,CASE WHEN 
 [ICD9_5] = '39891' OR
 [ICD9_5] = '40201' OR
 [ICD9_5] = '40211' OR
 [ICD9_5] = '40291' OR
 [ICD9_5] = '40401' OR
 [ICD9_5] = '40403' OR
 [ICD9_5] = '40411' OR
 [ICD9_5] = '40413' OR
 [ICD9_5] = '40491' OR
 [ICD9_5] = '40493' OR
 ([ICD9_4] >= '4254' AND [ICD9_4] <= '4259') OR
 [ICD9_3] = '428'
 THEN 1 ELSE NULL END AS [1]
,CASE WHEN 
 [ICD9_4] = '4260' OR
 [ICD9_5] = '42613' OR
 [ICD9_4] = '4267' OR
 [ICD9_4] = '4269' OR
 [ICD9_5] = '42610' OR
 [ICD9_5] = '42612' OR
 ([ICD9_4] >= '4270' AND [ICD9_4] <= '4274') OR
 ([ICD9_4] >= '4276' AND [ICD9_4] <= '4279') OR
 [ICD9_4] = '7850' OR
 [ICD9_5] = '99601' OR
 [ICD9_5] = '99604' OR
 [ICD9_4] = 'V450' OR
 [ICD9_4] = 'V533'
 THEN 1 ELSE NULL END AS [2]
,CASE WHEN 
 [ICD9_4] = '0932' OR
 ([ICD9_3] >= '394' AND [ICD9_3] <= '397') OR
 [ICD9_3] = '424' OR
 ([ICD9_4] >= '7463' AND [ICD9_4] <= '7466') OR
 [ICD9_4] = 'V422' OR
 [ICD9_4] = 'V433'
 THEN 1 ELSE NULL END AS [3]
,CASE WHEN 
 [ICD9_4] = '4150' OR
 [ICD9_4] = '4151' OR
 [ICD9_3] = '416' OR
 [ICD9_4] = '4170' OR
 [ICD9_4] = '4178' OR
 [ICD9_4] = '4179'
 THEN 1 ELSE NULL END AS [4]
,CASE WHEN 
 [ICD9_4] = '0930' OR
 [ICD9_4] = '4373' OR
 [ICD9_3] = '440' OR
 [ICD9_3] = '441' OR
 ([ICD9_4] >= '4431' AND [ICD9_4] <= '4439') OR
 [ICD9_4] = '4471' OR
 [ICD9_4] = '5571' OR
 [ICD9_4] = '5579' OR
 [ICD9_4] = 'V434'
 THEN 1 ELSE NULL END AS [5]
,CASE WHEN 
 [ICD9_3] = '401'
 THEN 1 ELSE NULL END AS [6]
,CASE WHEN 
 ([ICD9_3] >= '402' AND [ICD9_3] <= '405')
 THEN 1 ELSE NULL END AS [7]
,CASE WHEN 
 [ICD9_4] = '3341' OR
 [ICD9_3] = '342' OR
 [ICD9_3] = '343' OR
 ([ICD9_4] >= '3440' AND [ICD9_4] <= '3446') OR
 [ICD9_4] = '3449'
 THEN 1 ELSE NULL END AS [8]
,CASE WHEN 
 [ICD9_4] = '3319' OR
 [ICD9_4] = '3320' OR
 [ICD9_4] = '3321' OR
 [ICD9_4] = '3334' OR
 [ICD9_4] = '3335' OR
 [ICD9_5] = '33392' OR
 ([ICD9_3] >= '334' AND [ICD9_3] <= '335') OR
 [ICD9_4] = '3362' OR
 [ICD9_3] = '340' OR
 [ICD9_3] = '341' OR
 [ICD9_3] = '345' OR
 [ICD9_4] = '3481' OR
 [ICD9_4] = '3483' OR
 [ICD9_4] = '7803' OR
 [ICD9_4] = '7843'
 THEN 1 ELSE NULL END AS [9]
,CASE WHEN 
 [ICD9_4] = '4168' OR
 [ICD9_4] = '4169' OR
 ([ICD9_3] >= '490' AND [ICD9_3] <= '505') OR
 [ICD9_4] = '5064' OR
 [ICD9_4] = '5081' OR
 [ICD9_4] = '5088'
 THEN 1 ELSE NULL END AS [10]
,CASE WHEN 
 ([ICD9_4] >= '2500' AND [ICD9_4] <= '2503')
 THEN 1 ELSE NULL END AS [11]
,CASE WHEN 
 ([ICD9_4] >= '2504' AND [ICD9_4] <= '2509')
 THEN 1 ELSE NULL END AS [12]
,CASE WHEN 
 [ICD9_4] = '2409' OR
 [ICD9_3] = '243' OR
 [ICD9_3] = '244' OR
 [ICD9_4] = '2461' OR
 [ICD9_4] = '2468'
 THEN 1 ELSE NULL END AS [13]
,CASE WHEN 
 [ICD9_5] = '40301' OR
 [ICD9_5] = '40311' OR
 [ICD9_5] = '40391' OR
 [ICD9_5] = '40402' OR
 [ICD9_5] = '40403' OR
 [ICD9_5] = '40412' OR
 [ICD9_5] = '40413' OR
 [ICD9_5] = '40492' OR
 [ICD9_5] = '40493' OR
 [ICD9_3] = '585' OR
 [ICD9_3] = '586' OR
 [ICD9_5] = '5880' OR
 [ICD9_5] = 'V420' OR
 [ICD9_5] = 'V451' OR
 [ICD9_3] = 'V56'
 THEN 1 ELSE NULL END AS [14]
,CASE WHEN 
 [ICD9_5] = '07022' OR
 [ICD9_5] = '07023' OR
 [ICD9_5] = '07032' OR
 [ICD9_5] = '07033' OR
 [ICD9_5] = '07044' OR
 [ICD9_5] = '07054' OR
 [ICD9_4] = '0706' OR
 [ICD9_4] = '0709' OR
 ([ICD9_4] >= '4560' AND [ICD9_4] <= '4562') OR
 [ICD9_3] = '570' OR
 [ICD9_3] = '571' OR
 ([ICD9_4] >= '5722' AND [ICD9_4] <= '5728') OR
 [ICD9_4] = '5733' OR
 [ICD9_4] = '5734' OR
 [ICD9_4] = '5738' OR
 [ICD9_4] = '5739' OR
 [ICD9_4] = 'V427'
 THEN 1 ELSE NULL END AS [15]
,CASE WHEN 
 [ICD9_4] = '5317' OR
 [ICD9_4] = '5319' OR
 [ICD9_4] = '5327' OR
 [ICD9_4] = '5329' OR
 [ICD9_4] = '5337' OR
 [ICD9_4] = '5339' OR
 [ICD9_4] = '5347' OR
 [ICD9_4] = '5349'
 THEN 1 ELSE NULL END AS [16]
,CASE WHEN 
 ([ICD9_3] >= '042' AND [ICD9_3] <= '044')
 THEN 1 ELSE NULL END AS [17]
,CASE WHEN 
 ([ICD9_3] >= '200' AND [ICD9_3] <= '202') OR
 [ICD9_4] = '2030' OR
 [ICD9_4] = '2386'
 THEN 1 ELSE NULL END AS [18]
,CASE WHEN 
 ([ICD9_3] >= '196' AND [ICD9_3] <= '199')
 THEN 1 ELSE NULL END AS [19]
,CASE WHEN 
 ([ICD9_3] >= '140' AND [ICD9_3] <= '172') OR
 ([ICD9_3] >= '174' AND [ICD9_3] <= '195')
 THEN 1 ELSE NULL END AS [20]
,CASE WHEN 
 [ICD9_3] = '446' OR
 [ICD9_4] = '7010' OR
 ([ICD9_4] >= '7100' AND [ICD9_4] <= '7104') OR
 [ICD9_4] = '7108' OR
 [ICD9_4] = '7109' OR
 [ICD9_4] = '7112' OR
 [ICD9_3] = '714' OR
 [ICD9_4] = '7193' OR
 [ICD9_3] = '720' OR
 [ICD9_3] = '725' OR
 [ICD9_4] = '7285' OR
 [ICD9_5] = '72889' OR
 [ICD9_5] = '72930'
 THEN 1 ELSE NULL END AS [21]
,CASE WHEN 
 [ICD9_3] = '286' OR
 [ICD9_4] = '2871' OR
 ([ICD9_4] >= '2873' AND [ICD9_4] <= '2875')
 THEN 1 ELSE NULL END AS [22]
,CASE WHEN 
 [ICD9_4] = '2780'
 THEN 1 ELSE NULL END AS [23]
,CASE WHEN 
 ([ICD9_3] >= '260' AND [ICD9_3] <= '263') OR
 [ICD9_4] = '7832' OR
 [ICD9_4] = '7994'
 THEN 1 ELSE NULL END AS [24]
,CASE WHEN 
 [ICD9_4] = '2536' OR
 [ICD9_3] = '276'
 THEN 1 ELSE NULL END AS [25]
,CASE WHEN 
 [ICD9_4] = '2800'
 THEN 1 ELSE NULL END AS [26]
,CASE WHEN 
 ([ICD9_4] >= '2801' AND [ICD9_4] <= '2809') OR
 [ICD9_3] = '281'
 THEN 1 ELSE NULL END AS [27]
,CASE WHEN 
 [ICD9_4] = '2652' OR
 ([ICD9_4] >= '2911' AND [ICD9_4] <= '2913') OR
 ([ICD9_4] >= '2915' AND [ICD9_4] <= '2919') OR
 [ICD9_4] = '3030' OR
 [ICD9_4] = '3039' OR
 [ICD9_4] = '3050' OR
 [ICD9_4] = '3575' OR
 [ICD9_4] = '4255' OR
 [ICD9_4] = '5353' OR
 ([ICD9_4] >= '5710' AND [ICD9_4] <= '5713') OR
 [ICD9_3] = '980' OR
 [ICD9_4] = 'V113'
 THEN 1 ELSE NULL END AS [28]
,CASE WHEN 
 [ICD9_3] = '292' OR
 [ICD9_3] = '304' OR
 ([ICD9_4] >= '3052' AND [ICD9_4] <= '3059') OR
 [ICD9_5] = 'V6542'
 THEN 1 ELSE NULL END AS [29]
,CASE WHEN 
 [ICD9_4] = '2938' OR
 [ICD9_3] = '295' OR
 [ICD9_5] = '29604' OR
 [ICD9_5] = '29614' OR
 [ICD9_5] = '29644' OR
 [ICD9_5] = '29654' OR
 [ICD9_3] = '297' OR
 [ICD9_3] = '298'
 THEN 1 ELSE NULL END AS [30]
,CASE WHEN 
 [ICD9_4] = '2962' OR
 [ICD9_4] = '2963' OR
 [ICD9_4] = '2965' OR
 [ICD9_4] = '3004' OR
 [ICD9_3] = '309' OR
 [ICD9_3] = '311'
 THEN 1 ELSE NULL END AS [31]
,CASE WHEN 
 [ICD9_3] = '410' OR
 [ICD9_3] = '412'
 THEN 1 ELSE NULL END AS [32]
,CASE WHEN 
 [ICD9_5] = '36234' OR
 ([ICD9_3] >= '430' AND [ICD9_3] <= '438')
 THEN 1 ELSE NULL END AS [33]
,CASE WHEN 
 [ICD9_3] = '290' OR
 [ICD9_4] = '2941' OR
 [ICD9_4] = '3312'
 THEN 1 ELSE NULL END AS [34]
,CASE WHEN 
 [ICD9_4] = '4465' OR
 ([ICD9_4] >= '7100' AND [ICD9_4] <= '7104') OR
 ([ICD9_4] >= '7140' AND [ICD9_4] <= '7142') OR
 [ICD9_4] = '7148' OR
 [ICD9_3] = '725'
 THEN 1 ELSE NULL END AS [35]
,CASE WHEN 
 ([ICD9_3] >= '531' AND [ICD9_3] <= '534')
 THEN 1 ELSE NULL END AS [36]
,CASE WHEN 
 [ICD9_5] = '07022' OR
 [ICD9_5] = '07023' OR
 [ICD9_5] = '07032' OR
 [ICD9_5] = '07033' OR
 [ICD9_5] = '07044' OR
 [ICD9_5] = '07054' OR
 [ICD9_4] = '0706' OR
 [ICD9_4] = '0709' OR
 [ICD9_3] = '570' OR
 [ICD9_3] = '571' OR
 [ICD9_4] = '5733' OR
 [ICD9_4] = '5734' OR
 [ICD9_4] = '5738' OR
 [ICD9_4] = '5739' OR
 [ICD9_4] = 'V427'
 THEN 1 ELSE NULL END AS [37]
,CASE WHEN 
 ([ICD9_4] >= '2500' AND [ICD9_4] <= '2503') OR
 [ICD9_4] = '2508' OR
 [ICD9_4] = '2509'
 THEN 1 ELSE NULL END AS [38]
,CASE WHEN 
 ([ICD9_4] >= '2504' AND [ICD9_4] <= '2507')
 THEN 1 ELSE NULL END AS [39]
,CASE WHEN 
 [ICD9_5] = '40301' OR
 [ICD9_5] = '40311' OR
 [ICD9_5] = '40391' OR
 [ICD9_5] = '40402' OR
 [ICD9_5] = '40403' OR
 [ICD9_5] = '40412' OR
 [ICD9_5] = '40413' OR
 [ICD9_5] = '40492' OR
 [ICD9_5] = '40493' OR
 [ICD9_3] = '582' OR
 ([ICD9_4] >= '5830' AND [ICD9_4] <= '5837') OR
 [ICD9_3] = '585' OR
 [ICD9_3] = '586' OR
 [ICD9_4] = '5880' OR
 [ICD9_4] = 'V420' OR
 [ICD9_4] = 'V451' OR
 [ICD9_3] = 'V56'
 THEN 1 ELSE NULL END AS [40]
,CASE WHEN 
 ([ICD9_3] >= '140' AND [ICD9_3] <= '172') OR
 ([ICD9_3] >= '174' AND [ICD9_3] <= '195') OR
 ([ICD9_3] >= '200' AND [ICD9_3] <= '208') OR
 [ICD9_4] = '2386'
 THEN 1 ELSE NULL END AS [41]
,CASE WHEN 
 ([ICD9_4] >= '4560' AND [ICD9_4] <= '4562') OR
 ([ICD9_4] >= '5722' AND [ICD9_4] <= '5728')
 THEN 1 ELSE NULL END AS [42]
,CASE WHEN 
 [ICD9_3] = '401' OR
 ([ICD9_3] >= '402' AND [ICD9_3] <= '405')
 THEN 1 ELSE NULL END AS [43]
INTO #flag_icd9_conditions
FROM #icd9;

IF OBJECT_ID('tempdb..#flag_icd10_conditions', 'U') IS NOT NULL 
DROP TABLE #flag_icd10_conditions;
SELECT 
 [dx]
,[dx_ver]
,CASE WHEN 
 [ICD10_4] = 'I099' OR
 [ICD10_4] = 'I110' OR
 [ICD10_4] = 'I130' OR
 [ICD10_4] = 'I132' OR
 [ICD10_4] = 'I255' OR
 [ICD10_4] = 'I420' OR
 ([ICD10_4] >= 'I425' AND [ICD10_4] <= 'I429') OR
 [ICD10_3] = 'I43' OR
 [ICD10_3] = 'I50' OR
 [ICD10_4] = 'P290'
 THEN 1 ELSE NULL END AS [1]
,CASE WHEN 
 ([ICD10_4] >= 'I441' AND [ICD10_4] <= 'I443') OR
 [ICD10_4] = 'I456' OR
 [ICD10_4] = 'I459' OR
 ([ICD10_3] >= 'I47' AND [ICD10_3] <= 'I49') OR
 [ICD10_4] = 'ROOO' OR
 [ICD10_4] = 'ROO1' OR
 [ICD10_4] = 'ROO8' OR
 [ICD10_4] = 'T821' OR
 [ICD10_4] = 'Z450' OR
 [ICD10_4] = 'Z950'
 THEN 1 ELSE NULL END AS [2]
,CASE WHEN 
 [ICD10_4] = 'A520' OR
 ([ICD10_3] >= 'I05' AND [ICD10_3] <= 'I08') OR
 [ICD10_4] = 'I091' OR
 [ICD10_4] = 'I098' OR
 ([ICD10_3] >= 'I34' AND [ICD10_3] <= 'I39') OR
 ([ICD10_4] >= 'Q23O' AND [ICD10_4] <= 'Q233') OR
 [ICD10_4] = 'Z952' OR
 [ICD10_4] = 'Z954'
 THEN 1 ELSE NULL END AS [3]
,CASE WHEN 
 [ICD10_3] = 'I26' OR
 [ICD10_3] = 'I27' OR
 [ICD10_4] = 'I280' OR
 [ICD10_4] = 'I288' OR
 [ICD10_4] = 'I289'
 THEN 1 ELSE NULL END AS [4]
,CASE WHEN 
 [ICD10_3] = 'I70' OR
 [ICD10_3] = 'I71' OR
 [ICD10_4] = 'I731' OR
 [ICD10_4] = 'I738' OR
 [ICD10_4] = 'I739' OR
 [ICD10_4] = 'I771' OR
 [ICD10_4] = 'I790' OR
 [ICD10_4] = 'I792' OR
 [ICD10_4] = 'K551' OR
 [ICD10_4] = 'K558' OR
 [ICD10_4] = 'K559' OR
 [ICD10_4] = 'Z958' OR
 [ICD10_4] = 'Z959'
 THEN 1 ELSE NULL END AS [5]
,CASE WHEN 
 [ICD10_3] = 'I10'
 THEN 1 ELSE NULL END AS [6]
,CASE WHEN 
 ([ICD10_3] >= 'I11' AND [ICD10_3] <= 'I13') OR
 [ICD10_3] = 'I15'
 THEN 1 ELSE NULL END AS [7]
,CASE WHEN 
 [ICD10_4] = 'G041' OR
 [ICD10_4] = 'G114' OR
 [ICD10_4] = 'G801' OR
 [ICD10_4] = 'G802' OR
 [ICD10_3] = 'G81' OR
 [ICD10_3] = 'G82' OR
 ([ICD10_4] >= 'G830' AND [ICD10_4] <= 'G834') OR
 [ICD10_4] = 'G839'
 THEN 1 ELSE NULL END AS [8]
,CASE WHEN 
 ([ICD10_3] >= 'G10' AND [ICD10_3] <= 'G13') OR
 ([ICD10_3] >= 'G20' AND [ICD10_3] <= 'G22') OR
 [ICD10_4] = 'G254' OR
 [ICD10_4] = 'G255' OR
 [ICD10_4] = 'G312' OR
 [ICD10_4] = 'G318' OR
 [ICD10_4] = 'G319' OR
 [ICD10_3] = 'G32' OR
 ([ICD10_3] >= 'G35' AND [ICD10_3] <= 'G37') OR
 [ICD10_3] = 'G40' OR
 [ICD10_3] = 'G41' OR
 [ICD10_4] = 'G931' OR
 [ICD10_4] = 'G934' OR
 [ICD10_4] = 'R470' OR
 [ICD10_3] = 'R56'
 THEN 1 ELSE NULL END AS [9]
,CASE WHEN 
 [ICD10_4] = 'I278' OR
 [ICD10_4] = 'I279' OR
 ([ICD10_3] >= 'J40' AND [ICD10_3] <= 'J47') OR
 ([ICD10_3] >= 'J60' AND [ICD10_3] <= 'J67') OR
 [ICD10_4] = 'J684' OR
 [ICD10_4] = 'J701' OR
 [ICD10_4] = 'J703'
 THEN 1 ELSE NULL END AS [10]
,CASE WHEN 
 [ICD10_4] = 'E100' OR
 [ICD10_4] = 'E101' OR
 [ICD10_4] = 'E109' OR
 [ICD10_4] = 'E110' OR
 [ICD10_4] = 'E111' OR
 [ICD10_4] = 'E119' OR
 [ICD10_4] = 'E120' OR
 [ICD10_4] = 'E121' OR
 [ICD10_4] = 'E129' OR
 [ICD10_4] = 'E130' OR
 [ICD10_4] = 'E131' OR
 [ICD10_4] = 'E139' OR
 [ICD10_4] = 'E140' OR
 [ICD10_4] = 'E141' OR
 [ICD10_4] = 'E149'
 THEN 1 ELSE NULL END AS [11]
,CASE WHEN 
 ([ICD10_4] >= 'E102' AND [ICD10_4] <= 'E108') OR
 ([ICD10_4] >= 'E112' AND [ICD10_4] <= 'E118') OR
 ([ICD10_4] >= 'E122' AND [ICD10_4] <= 'E128') OR
 ([ICD10_4] >= 'E132' AND [ICD10_4] <= 'E138') OR
 ([ICD10_4] >= 'E142' AND [ICD10_4] <= 'E148')
 THEN 1 ELSE NULL END AS [12]
,CASE WHEN 
 ([ICD10_3] >= 'E00' AND [ICD10_3] <= 'E03') OR
 [ICD10_4] = 'E890'
 THEN 1 ELSE NULL END AS [13]
,CASE WHEN 
 [ICD10_4] = 'I120' OR
 [ICD10_4] = 'I131' OR
 [ICD10_3] = 'N18' OR
 [ICD10_3] = 'N19' OR
 [ICD10_4] = 'N250' OR
 ([ICD10_4] >= 'Z490' AND [ICD10_4] <= 'Z492') OR
 [ICD10_4] = 'Z940' OR
 [ICD10_4] = 'Z992'
 THEN 1 ELSE NULL END AS [14]
,CASE WHEN 
 [ICD10_3] = 'B18' OR
 [ICD10_3] = 'I85' OR
 [ICD10_4] = 'I864' OR
 [ICD10_4] = 'I982' OR
 [ICD10_3] = 'K70' OR
 [ICD10_4] = 'K711' OR
 ([ICD10_4] >= 'K713' AND [ICD10_4] <= 'K715') OR
 [ICD10_4] = 'K717' OR
 ([ICD10_3] >= 'K72' AND [ICD10_3] <= 'K74') OR
 [ICD10_4] = 'K760' OR
 ([ICD10_4] >= 'K762' AND [ICD10_4] <= 'K769') OR
 [ICD10_4] = 'Z944'
 THEN 1 ELSE NULL END AS [15]
,CASE WHEN 
 [ICD10_4] = 'K257' OR
 [ICD10_4] = 'K259' OR
 [ICD10_4] = 'K267' OR
 [ICD10_4] = 'K269' OR
 [ICD10_4] = 'K277' OR
 [ICD10_4] = 'K279' OR
 [ICD10_4] = 'K287' OR
 [ICD10_4] = 'K289'
 THEN 1 ELSE NULL END AS [16]
,CASE WHEN 
 ([ICD10_3] >= 'B20' AND [ICD10_3] <= 'B22') OR
 [ICD10_3] = 'B24'
 THEN 1 ELSE NULL END AS [17]
,CASE WHEN 
 ([ICD10_3] >= 'C81' AND [ICD10_3] <= 'C85') OR
 [ICD10_3] = 'C88' OR
 [ICD10_3] = 'C96' OR
 [ICD10_4] = 'C900' OR
 [ICD10_4] = 'C902'
 THEN 1 ELSE NULL END AS [18]
,CASE WHEN 
 ([ICD10_3] >= 'C77' AND [ICD10_3] <= 'C80')
 THEN 1 ELSE NULL END AS [19]
,CASE WHEN 
 ([ICD10_3] >= 'C00' AND [ICD10_3] <= 'C26') OR
 ([ICD10_3] >= 'C30' AND [ICD10_3] <= 'C34') OR
 ([ICD10_3] >= 'C37' AND [ICD10_3] <= 'C41') OR
 [ICD10_3] = 'C43' OR
 ([ICD10_3] >= 'C45' AND [ICD10_3] <= 'C58') OR
 ([ICD10_3] >= 'C60' AND [ICD10_3] <= 'C76') OR
 [ICD10_3] = 'C97'
 THEN 1 ELSE NULL END AS [20]
,CASE WHEN 
 [ICD10_4] = 'L940' OR
 [ICD10_4] = 'L941' OR
 [ICD10_4] = 'L943' OR
 [ICD10_3] = 'M05' OR
 [ICD10_3] = 'M06' OR
 [ICD10_3] = 'M08' OR
 [ICD10_4] = 'M120' OR
 [ICD10_4] = 'M123' OR
 [ICD10_3] = 'M30' OR
 ([ICD10_4] >= 'M310' AND [ICD10_4] <= 'M313') OR
 ([ICD10_3] >= 'M32' AND [ICD10_3] <= 'M35') OR
 [ICD10_3] = 'M45' OR
 [ICD10_4] = 'M461' OR
 [ICD10_4] = 'M468' OR
 [ICD10_4] = 'M469'
 THEN 1 ELSE NULL END AS [21]
,CASE WHEN 
 ([ICD10_3] >= 'D65' AND [ICD10_3] <= 'D68') OR
 [ICD10_4] = 'D691' OR
 ([ICD10_4] >= 'D693' AND [ICD10_4] <= 'D696')
 THEN 1 ELSE NULL END AS [22]
,CASE WHEN 
 [ICD10_3] = 'E66'
 THEN 1 ELSE NULL END AS [23]
,CASE WHEN 
 ([ICD10_3] >= 'E40' AND [ICD10_3] <= 'E46') OR
 [ICD10_4] = 'R634' OR
 [ICD10_3] = 'R64'
 THEN 1 ELSE NULL END AS [24]
,CASE WHEN 
 [ICD10_4] = 'E222' OR
 [ICD10_3] = 'E86' OR
 [ICD10_3] = 'E87'
 THEN 1 ELSE NULL END AS [25]
,CASE WHEN 
 [ICD10_4] = 'D500'
 THEN 1 ELSE NULL END AS [26]
,CASE WHEN 
 [ICD10_4] = 'D508' OR
 [ICD10_4] = 'D509' OR
 ([ICD10_3] >= 'D51' AND [ICD10_3] <= 'D53')
 THEN 1 ELSE NULL END AS [27]
,CASE WHEN 
 [ICD10_3] = 'F10' OR
 [ICD10_3] = 'E52' OR
 [ICD10_4] = 'G621' OR
 [ICD10_4] = 'I426' OR
 [ICD10_4] = 'K292' OR
 [ICD10_4] = 'K700' OR
 [ICD10_4] = 'K703' OR
 [ICD10_4] = 'K709' OR
 [ICD10_3] = 'T51' OR
 [ICD10_4] = 'Z502' OR
 [ICD10_4] = 'Z714' OR
 [ICD10_4] = 'Z721'
 THEN 1 ELSE NULL END AS [28]
,CASE WHEN 
 ([ICD10_3] >= 'F11' AND [ICD10_3] <= 'F16') OR
 [ICD10_3] = 'F18' OR
 [ICD10_3] = 'F19' OR
 [ICD10_4] = 'Z715' OR
 [ICD10_4] = 'Z722'
 THEN 1 ELSE NULL END AS [29]
,CASE WHEN 
 [ICD10_3] = 'F20' OR
 ([ICD10_3] >= 'F22' AND [ICD10_3] <= 'F25') OR
 [ICD10_3] = 'F28' OR
 [ICD10_3] = 'F29' OR
 [ICD10_4] = 'F302' OR
 [ICD10_4] = 'F312' OR
 [ICD10_4] = 'F315'
 THEN 1 ELSE NULL END AS [30]
,CASE WHEN 
 [ICD10_4] = 'F204' OR
 ([ICD10_4] >= 'F313' AND [ICD10_4] <= 'F315') OR
 [ICD10_3] = 'F32' OR
 [ICD10_3] = 'F33' OR
 [ICD10_4] = 'F341' OR
 [ICD10_4] = 'F412' OR
 [ICD10_4] = 'F432'
 THEN 1 ELSE NULL END AS [31]
,CASE WHEN 
 [ICD10_3] = 'I21' OR
 [ICD10_3] = 'I22' OR
 [ICD10_4] = 'I252'
 THEN 1 ELSE NULL END AS [32]
,CASE WHEN 
 [ICD10_3] = 'G45' OR
 [ICD10_3] = 'G46' OR
 [ICD10_4] = 'H340' OR
 ([ICD10_3] >= 'I60' AND [ICD10_3] <= 'I69')
 THEN 1 ELSE NULL END AS [33]
,CASE WHEN 
 ([ICD10_3] >= 'F00' AND [ICD10_3] <= 'F03') OR
 [ICD10_4] = 'F051' OR
 [ICD10_3] = 'G30' OR
 [ICD10_4] = 'G311'
 THEN 1 ELSE NULL END AS [34]
,CASE WHEN 
 [ICD10_3] = 'M05' OR
 [ICD10_3] = 'M06' OR
 [ICD10_4] = 'M315' OR
 ([ICD10_3] >= 'M32' AND [ICD10_3] <= 'M34') OR
 [ICD10_4] = 'M351' OR
 [ICD10_4] = 'M353' OR
 [ICD10_4] = 'M360'
 THEN 1 ELSE NULL END AS [35]
,CASE WHEN 
 ([ICD10_3] >= 'K25' AND [ICD10_3] <= 'K28')
 THEN 1 ELSE NULL END AS [36]
,CASE WHEN 
 [ICD10_3] = 'B18' OR
 ([ICD10_4] >= 'K700' AND [ICD10_4] <= 'K703') OR
 [ICD10_4] = 'K709' OR
 ([ICD10_4] >= 'K713' AND [ICD10_4] <= 'K715') OR
 [ICD10_4] = 'K717' OR
 [ICD10_3] = 'K73' OR
 [ICD10_3] = 'K74' OR
 [ICD10_4] = 'K760' OR
 ([ICD10_4] >= 'K762' AND [ICD10_4] <= 'K764') OR
 [ICD10_4] = 'K768' OR
 [ICD10_4] = 'K769' OR
 [ICD10_4] = 'Z944'
 THEN 1 ELSE NULL END AS [37]
,CASE WHEN 
 [ICD10_4] = 'E100' OR
 [ICD10_4] = 'E10l' OR
 [ICD10_4] = 'E106' OR
 [ICD10_4] = 'E108' OR
 [ICD10_4] = 'E109' OR
 [ICD10_4] = 'E110' OR
 [ICD10_4] = 'E111' OR
 [ICD10_4] = 'E116' OR
 [ICD10_4] = 'E118' OR
 [ICD10_4] = 'E119' OR
 [ICD10_4] = 'E120' OR
 [ICD10_4] = 'E121' OR
 [ICD10_4] = 'E126' OR
 [ICD10_4] = 'E128' OR
 [ICD10_4] = 'E129' OR
 [ICD10_4] = 'E130' OR
 [ICD10_4] = 'E131' OR
 [ICD10_4] = 'E136' OR
 [ICD10_4] = 'E138' OR
 [ICD10_4] = 'E139' OR
 [ICD10_4] = 'E140' OR
 [ICD10_4] = 'E141' OR
 [ICD10_4] = 'E146' OR
 [ICD10_4] = 'E148' OR
 [ICD10_4] = 'E149'
 THEN 1 ELSE NULL END AS [38]
,CASE WHEN 
 ([ICD10_4] >= 'E102' AND [ICD10_4] <= 'E105') OR
 [ICD10_4] = 'E107' OR
 ([ICD10_4] >= 'E112' AND [ICD10_4] <= 'E115') OR
 [ICD10_4] = 'E117' OR
 ([ICD10_4] >= 'E122' AND [ICD10_4] <= 'E125') OR
 [ICD10_4] = 'E127' OR
 ([ICD10_4] >= 'E132' AND [ICD10_4] <= 'E135') OR
 [ICD10_4] = 'E137' OR
 ([ICD10_4] >= 'E142' AND [ICD10_4] <= 'E145') OR
 [ICD10_4] = 'E147'
 THEN 1 ELSE NULL END AS [39]
,CASE WHEN 
 [ICD10_4] = 'I120' OR
 [ICD10_4] = 'I131' OR
 ([ICD10_4] >= 'N032' AND [ICD10_4] <= 'N037') OR
 ([ICD10_4] >= 'N052' AND [ICD10_4] <= 'N057') OR
 [ICD10_3] = 'N18' OR
 [ICD10_3] = 'N19' OR
 [ICD10_4] = 'N250' OR
 ([ICD10_4] >= 'Z490' AND [ICD10_4] <= 'Z492') OR
 [ICD10_4] = 'Z940' OR
 [ICD10_4] = 'Z992'
 THEN 1 ELSE NULL END AS [40]
,CASE WHEN 
 ([ICD10_3] >= 'C00' AND [ICD10_3] <= 'C26') OR
 ([ICD10_3] >= 'C30' AND [ICD10_3] <= 'C34') OR
 ([ICD10_3] >= 'C37' AND [ICD10_3] <= 'C41') OR
 [ICD10_3] = 'C43' OR
 ([ICD10_3] >= 'C45' AND [ICD10_3] <= 'C58') OR
 ([ICD10_3] >= 'C60' AND [ICD10_3] <= 'C76') OR
 ([ICD10_3] >= 'C81' AND [ICD10_3] <= 'C85') OR
 [ICD10_3] = 'C88' OR
 ([ICD10_3] >= 'C90' AND [ICD10_3] <= 'C97')
 THEN 1 ELSE NULL END AS [41]
,CASE WHEN 
 [ICD10_4] = 'I850' OR
 [ICD10_4] = 'I859' OR
 [ICD10_4] = 'I864' OR
 [ICD10_4] = 'I982' OR
 [ICD10_4] = 'K704' OR
 [ICD10_4] = 'K711' OR
 [ICD10_4] = 'K721' OR
 [ICD10_4] = 'K729' OR
 [ICD10_4] = 'K765' OR
 [ICD10_4] = 'K766' OR
 [ICD10_4] = 'K767'
 THEN 1 ELSE NULL END AS [42]
,CASE WHEN 
 [ICD10_3] = 'I10' OR
 ([ICD10_3] >= 'I11' AND [ICD10_3] <= 'I13') OR
 [ICD10_3] = 'I15'
 THEN 1 ELSE NULL END AS [43]
INTO #flag_icd10_conditions
FROM #icd10;

CREATE CLUSTERED INDEX idx_cl_#flag_icd9_conditions ON #flag_icd9_conditions([dx_ver], [dx]);
CREATE CLUSTERED INDEX idx_cl_#flag_icd10_conditions ON #flag_icd10_conditions([dx_ver], [dx]);

/*
Now insert from #flag_icd9_conditions and #flag_icd10_conditions into
[ref].[comorb_dx_lookup] and [ref].[comorb_value_set]
*/
DECLARE @insert_icd9_conditions AS NVARCHAR(MAX) = '
WITH Unpivot_Flag AS
(
SELECT 
 [dx]
,[dx_ver]
,[cond_id]
,[flag]
FROM #flag_icd9_conditions
UNPIVOT ([flag] FOR [cond_id] IN
(' + @StuffedCond_ID + '
)) AS U
),

Join_ShortName AS
(
SELECT
 [dx]
,[dx_ver]
,[short_name]
,[flag]
FROM Unpivot_Flag AS a
INNER JOIN [ref].[comorb_cond_lookup] AS b
ON a.[cond_id] = b.[cond_id]
)

INSERT INTO [ref].[comorb_dx_lookup]
([dx]
,[dx_ver]
,' + @StuffedShortName + ')
SELECT 
 [dx]
,[dx_ver]
,' + @ISNULLShortName + '
FROM Join_ShortName
PIVOT (MAX([flag]) FOR [short_name] IN
(' + @StuffedShortName + '
)) AS P
ORDER BY [dx_ver], [dx];';

DECLARE @insert_icd10_conditions AS NVARCHAR(MAX) = '
WITH Unpivot_Flag AS
(
SELECT 
 [dx]
,[dx_ver]
,[cond_id]
,[flag]
FROM #flag_icd10_conditions
UNPIVOT ([flag] FOR [cond_id] IN
(' + @StuffedCond_ID + '
)) AS U
),

Join_ShortName AS
(
SELECT
 [dx]
,[dx_ver]
,[short_name]
,[flag]
FROM Unpivot_Flag AS a
INNER JOIN [ref].[comorb_cond_lookup] AS b
ON a.[cond_id] = b.[cond_id]
)

INSERT INTO [ref].[comorb_dx_lookup]
([dx]
,[dx_ver]
,' + @StuffedShortName + ')
SELECT
 [dx]
,[dx_ver]
,' + @ISNULLShortName + '
FROM Join_ShortName
PIVOT (MAX([flag]) FOR [short_name] IN
(' + @StuffedShortName + '
)) AS P
ORDER BY [dx_ver], [dx];';

DECLARE @insert_comorb_value_set AS NVARCHAR(MAX) = '
WITH Unpivot_Flag AS
(
SELECT
 [dx]
,[dx_ver]
,[cond_id]
,[flag]
FROM #flag_icd9_conditions
UNPIVOT ([flag] FOR [cond_id] IN
(' + @StuffedCond_ID + '
)) AS U

UNION ALL

SELECT
 [dx]
,[dx_ver]
,[cond_id]
,[flag]
FROM #flag_icd10_conditions
UNPIVOT ([flag] FOR [cond_id] IN
(' + @StuffedCond_ID + '
)) AS U
)

INSERT INTO [ref].[comorb_value_set]
([dx]
,[dx_ver]
,[cond_id]
,[short_name]
,[definition]
,[elixhauser_wgt]
,[charlson_wgt]
,[gagne_wgt]
,[flag])

SELECT
 a.[dx]
,a.[dx_ver]
,a.[cond_id]
,b.[short_name]
,b.[definition]
,b.[elixhauser_wgt]
,b.[charlson_wgt]
,b.[gagne_wgt]
,a.[flag]
FROM Unpivot_Flag AS a
INNER JOIN [ref].[comorb_cond_lookup] AS b
ON a.[cond_id] = b.[cond_id]
ORDER BY [cond_id], [dx_ver], [dx];';
/*
PRINT @insert_icd9_conditions;
PRINT @insert_icd10_conditions;
PRINT @insert_comorb_value_set;
*/

EXEC(@insert_icd9_conditions);
EXEC(@insert_icd10_conditions);
EXEC(@insert_comorb_value_set);