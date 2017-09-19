--############
--members with inpatient, SNF, or home health asthma claims
SELECT distinct id, tcn
FROM
(SELECT top 10000 MEDICAID_RECIPIENT_ID AS id, tcn, PRIMARY_DIAGNOSIS_CODE AS dx1,
	DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
	DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
	DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
FROM PHClaims.dbo.NewClaims

WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016, 1-year ref period per CCW
	AND CLM_TYPE_CID in (31, 12, 23) --inpatient, snf, hha claims
)as a
--CCW ICD9 and ICD10 codes for asthma
unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')

--############
--members with inpatient, SNF, home health, or crossover inpatient asthma claims
SELECT distinct id, tcn
FROM
(SELECT top 10000 MEDICAID_RECIPIENT_ID AS id, tcn, PRIMARY_DIAGNOSIS_CODE AS dx1,
	DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
	DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
	DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
FROM PHClaims.dbo.NewClaims

WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016, 1-year ref period per CCW
	AND CLM_TYPE_CID in (31, 12, 23, 33) --inpatient, snf, hha claims, part A XO inpatient
)as a
--CCW ICD9 and ICD10 codes for asthma
unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')

--############
--members with outpatient or professional asthma claims
SELECT distinct id
FROM
	(SELECT DISTINCT id, COUNT(DISTINCT fr_sdt) AS clm_cnt
	FROM
		(SELECT top 100000 MEDICAID_RECIPIENT_ID AS id, tcn, FROM_SRVC_DATE AS fr_sdt, PRIMARY_DIAGNOSIS_CODE AS dx1,
		DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
		DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
		DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
		FROM PHClaims.dbo.NewClaims
		WHERE (FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016
			AND (CLM_TYPE_CID in (3, 26, 1))) --outpatient & professional claims
		)as a
	--CCW ICD9 and ICD10 codes for asthma
	unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
	where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')
	GROUP BY id) b
WHERE clm_cnt>1

--outpatient or professional asthma claims (to intersect with IDs with >1 claims in R)
SELECT distinct id, tcn
FROM
(SELECT top 10000 MEDICAID_RECIPIENT_ID AS id, tcn, PRIMARY_DIAGNOSIS_CODE AS dx1,
	DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
	DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
	DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
FROM PHClaims.dbo.NewClaims

WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016, 1-year ref period per CCW
	AND CLM_TYPE_CID in (3, 26, 1) --outpatient & professional claims
)as a
--CCW ICD9 and ICD10 codes for asthma
unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')

--############
--members with outpatient, professional, part A XO outpatient, part B XO, epsdt, kidney center, or ambulatory surgery asthma claims
SELECT distinct id
FROM
	(SELECT DISTINCT id, COUNT(DISTINCT fr_sdt) AS clm_cnt
	FROM
		(SELECT top 100000 MEDICAID_RECIPIENT_ID AS id, tcn, FROM_SRVC_DATE AS fr_sdt, PRIMARY_DIAGNOSIS_CODE AS dx1,
		DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
		DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
		DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
		FROM PHClaims.dbo.NewClaims
		WHERE (FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016
			AND (CLM_TYPE_CID in (3, 26, 1, 34, 28, 27, 25, 19))) --claim type
		)as a
	--CCW ICD9 and ICD10 codes for asthma
	unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
	where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')
	GROUP BY id) b
WHERE clm_cnt>1

--outpatient, professional, part A XO outpatient, part B XO, epsdt, kidney center, or ambulatory surgery asthma claims (to intersect with IDs with >1 claims in R)
SELECT distinct id, tcn
FROM
(SELECT top 10000 MEDICAID_RECIPIENT_ID AS id, tcn, PRIMARY_DIAGNOSIS_CODE AS dx1,
	DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
	DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
	DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
FROM PHClaims.dbo.NewClaims

WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016, 1-year ref period per CCW
	AND CLM_TYPE_CID in (3, 26, 1, 34, 28, 27, 25, 19) --claim type
)as a
--CCW ICD9 and ICD10 codes for asthma
unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')