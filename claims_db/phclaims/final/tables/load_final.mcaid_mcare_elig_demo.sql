/****** COPY FROM STAGE to FINAL  ******/
	IF OBJECT_ID('[PHClaims].[final].[mcaid_mcare_elig_demo]', 'U') IS NOT NULL 
		DROP TABLE [PHClaims].[final].[mcaid_mcare_elig_demo]

	SELECT *
		INTO [PHClaims].[final].[mcaid_mcare_elig_demo]	
		FROM [PHClaims].[stage].[mcaid_mcare_elig_demo]


/****** ADD COLUMSTORE CLUSTERED INDEX ******/
	CREATE CLUSTERED COLUMNSTORE INDEX idx_final_mcaid_mcare_elig_demo
	ON [PHClaims].[final].[mcaid_mcare_elig_demo]
	WITH (DROP_EXISTING = OFF)


/****** BASIC ERROR CHECKING COMPARING STAGE & FINAL ******/
	SELECT COUNT(*) FROM [PHClaims].[stage].[mcaid_mcare_elig_demo]
	SELECT COUNT(*) FROM [PHClaims].[final].[mcaid_mcare_elig_demo]

	SELECT geo_kc_ever, 
	count(*) FROM [PHClaims].[stage].[mcaid_mcare_elig_demo]
	  GROUP BY geo_kc_ever
	  ORDER BY -count(*)

	 SELECT geo_kc_ever, 
	count(*) FROM [PHClaims].[final].[mcaid_mcare_elig_demo]
	  GROUP BY geo_kc_ever
	  ORDER BY -count(*)