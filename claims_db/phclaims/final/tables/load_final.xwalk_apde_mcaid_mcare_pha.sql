/****** COPY FROM STAGE to FINAL  ******/
	IF OBJECT_ID('[PHClaims].[final].[xwalk_apde_mcaid_mcare_pha]', 'U') IS NOT NULL 
		DROP TABLE [PHClaims].[final].[xwalk_apde_mcaid_mcare_pha]

	SELECT *
		INTO [PHClaims].[final].[xwalk_apde_mcaid_mcare_pha]	
		FROM [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha]


/****** Ensure case sensitivity for Mcare id ******/
ALTER TABLE [PHClaims].[final].[xwalk_apde_mcaid_mcare_pha]
ALTER COLUMN id_mcare varchar(255) collate SQL_Latin1_General_CP1_CS_AS NULL;


/****** ADD COLUMSTORE CLUSTERED INDEX ******/
	CREATE CLUSTERED COLUMNSTORE INDEX idx_final_xwalk_apde_mcaid_mcare_pha
	ON [PHClaims].[final].[xwalk_apde_mcaid_mcare_pha]
	WITH (DROP_EXISTING = OFF)


/****** BASIC ERROR CHECKING COMPARING STAGE & FINAL ******/
	SELECT COUNT(*) FROM [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha]
	SELECT COUNT(*) FROM [PHClaims].[final].[xwalk_apde_mcaid_mcare_pha]

	SELECT SUM(CAST(id_apde AS BIGINT)) FROM [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha]
	SELECT SUM(CAST(id_apde AS BIGINT)) FROM [PHClaims].[final].[xwalk_apde_mcaid_mcare_pha]

/****** DROP STAGE TABLE ******/
	DROP TABLE [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha]