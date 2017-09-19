/****** ALL ED visits, 2016  ******/
select count(*) 

	from (select distinct tcn, from_srvc_date
	--from (select distinct MEDICAID_RECIPIENT_ID
	--select distinct TCN, MEDICAID_RECIPIENT_ID,FROM_SRVC_DATE,REVENUE_CODE, PRCDR_CODE_1, PRCDR_CODE_2, PRCDR_CODE_3, PRCDR_CODE_4, PRCDR_CODE_5, PLACE_OF_SERVICE, PRIMARY_DIAGNOSIS_CODE
		from dbo.NewClaims
		where (FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31') 
  
		AND

		/*select ED visits using HEDIS ED value set or ED place of stay*/
		((REVENUE_CODE LIKE '045[01269]' OR REVENUE_CODE LIKE '0981' OR PRCDR_CODE_1 LIKE '9928[1-5]'
		OR PRCDR_CODE_2 LIKE '9928[1-5]' OR PRCDR_CODE_3 LIKE '9928[1-5]' OR PRCDR_CODE_4 LIKE '9928[1-5]' OR PRCDR_CODE_5 LIKE '9928[1-5]')
		OR (PLACE_OF_SERVICE LIKE '%23%')) 

	) as internalQuery

/****** ED visits due to SDOH, primary diagnosis field only  ******/
--select count(distinct tcn)
--Count distinct combinations of TCN and FROM_SRVC_DATE (same result as just TCN, but this is more careful)
select count(*) 

	from (select distinct tcn, from_srvc_date
	--from (select distinct MEDICAID_RECIPIENT_ID
	--select distinct TCN, MEDICAID_RECIPIENT_ID,FROM_SRVC_DATE,REVENUE_CODE, PRCDR_CODE_1, PRCDR_CODE_2, PRCDR_CODE_3, PRCDR_CODE_4, PRCDR_CODE_5, PLACE_OF_SERVICE, PRIMARY_DIAGNOSIS_CODE
		from dbo.NewClaims
		where (FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31') 
  
		AND

		/*select ED visits using HEDIS ED value set or ED place of stay*/
		((REVENUE_CODE LIKE '045[01269]' OR REVENUE_CODE LIKE '0981' OR PRCDR_CODE_1 LIKE '9928[1-5]'
		OR PRCDR_CODE_2 LIKE '9928[1-5]' OR PRCDR_CODE_3 LIKE '9928[1-5]' OR PRCDR_CODE_4 LIKE '9928[1-5]' OR PRCDR_CODE_5 LIKE '9928[1-5]')
		OR (PLACE_OF_SERVICE LIKE '%23%')) 

		AND

		/*select SDOH-related visits, using 3M article*/
		(PRIMARY_DIAGNOSIS_CODE like 'Z5[5679]%' OR PRIMARY_DIAGNOSIS_CODE like 'Z6[02345]%')
		
	) as internalQuery
	 
	--order by FROM_SRVC_DATE


/****** ED visits due to SDOH, ALL diagnosis fields ******/
--select count(distinct tcn)
--Count distinct combinations of TCN and FROM_SRVC_DATE (same result as just TCN, but this is more careful)
select count(*) 

	--from (select distinct tcn, from_srvc_date
	from (select distinct MEDICAID_RECIPIENT_ID
	--select distinct TCN, MEDICAID_RECIPIENT_ID,FROM_SRVC_DATE,REVENUE_CODE, PRCDR_CODE_1, PRCDR_CODE_2, PRCDR_CODE_3, PRCDR_CODE_4, PRCDR_CODE_5, PLACE_OF_SERVICE, PRIMARY_DIAGNOSIS_CODE
		from dbo.NewClaims
		where (FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31') 
  
		AND

		/*select ED visits using HEDIS ED value set or ED place of stay*/
		((REVENUE_CODE LIKE '045[01269]' OR REVENUE_CODE LIKE '0981' OR PRCDR_CODE_1 LIKE '9928[1-5]'
		OR PRCDR_CODE_2 LIKE '9928[1-5]' OR PRCDR_CODE_3 LIKE '9928[1-5]' OR PRCDR_CODE_4 LIKE '9928[1-5]' OR PRCDR_CODE_5 LIKE '9928[1-5]')
		OR (PLACE_OF_SERVICE LIKE '%23%')) 

		AND

		(
		/*select SDOH-related visits, using 3M article*/
		(PRIMARY_DIAGNOSIS_CODE like 'Z5[5679]%' OR PRIMARY_DIAGNOSIS_CODE like 'Z6[02345]%')

		OR

		/*select SDOH-related visits, using 3M article*/
		(DIAGNOSIS_CODE_2 like 'Z5[5679]%' OR DIAGNOSIS_CODE_2 like 'Z6[02345]%')
		
		OR

		/*select SDOH-related visits, using 3M article*/
		(DIAGNOSIS_CODE_3 like 'Z5[5679]%' OR DIAGNOSIS_CODE_3 like 'Z6[02345]%')
		
		OR

		/*select SDOH-related visits, using 3M article*/
		(DIAGNOSIS_CODE_4 like 'Z5[5679]%' OR DIAGNOSIS_CODE_4 like 'Z6[02345]%')
		
		OR

		/*select SDOH-related visits, using 3M article*/
		(DIAGNOSIS_CODE_5 like 'Z5[5679]%' OR DIAGNOSIS_CODE_5 like 'Z6[02345]%')
		)
	) as internalQuery
	 
	--order by FROM_SRVC_DATE
