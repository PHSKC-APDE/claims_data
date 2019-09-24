/****** COPY FROM STAGE to FINAL  ******/
	IF OBJECT_ID('[PHClaims].[final].[mcare_elig_timevar]', 'U') IS NOT NULL 
		DROP TABLE [PHClaims].[final].[mcare_elig_timevar]

	SELECT *
		INTO [PHClaims].[final].[mcare_elig_timevar]	
		FROM [PHClaims].[stage].[mcare_elig_timevar]


/****** ADD COLUMSTORE CLUSTERED INDEX ******/
	CREATE CLUSTERED COLUMNSTORE INDEX idx_final_mcare_elig_timevar
	ON [PHClaims].[final].[mcare_elig_timevar]
	WITH (DROP_EXISTING = OFF)


/****** BASIC ERROR CHECKING COMPARING STAGE & FINAL ******/
	SELECT COUNT(*) FROM [PHClaims].[stage].[mcare_elig_timevar]
	SELECT COUNT(*) FROM [PHClaims].[final].[mcare_elig_timevar]

	SELECT contiguous, 
	count(*) FROM [PHClaims].[stage].[mcare_elig_timevar]
	  GROUP BY contiguous
	  ORDER BY -count(*)

	 SELECT contiguous, 
	count(*) FROM [PHClaims].[final].[mcare_elig_timevar]
	  GROUP BY contiguous
	  ORDER BY -count(*)