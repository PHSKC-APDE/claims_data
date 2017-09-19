-- Code to process the Medicaid eligibility table from HCA
-- Public Health — Seattle & King County
-- Eli Kern
-- 2017-09-18

/*
This code aims to assign one DOB to each individual (defined by distinct combo of Medicaid ID and SSN)
*/

--Select distinct ID-SSN-RAC rows for one calendar month
select distinct y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.RAC_NAME, y.RAC_CODE, y.CLNDR_YEAR_MNTH
	FROM (
		SELECT z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.RAC_NAME, z.RAC_CODE, z.CLNDR_YEAR_MNTH
		FROM [PHClaims].[dbo].[NewEligibility] as z
	) as y
where CLNDR_YEAR_MNTH = '201702'
order by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.RAC_NAME, y.RAC_CODE

--Select and count distinct ID-SSN-RAC rows by calendar month
select x.CLNDR_YEAR_MNTH, x.RAC_NAME, x.RAC_CODE, count(x.MEDICAID_RECIPIENT_ID) as idssn_cnt
	from (
	select distinct y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.RAC_NAME, y.RAC_CODE, y.CLNDR_YEAR_MNTH
		from (
			SELECT z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.RAC_NAME, z.RAC_CODE, z.CLNDR_YEAR_MNTH
			FROM [PHClaims].[dbo].[NewEligibility] as z
		) as y
	) as x
group by x.CLNDR_YEAR_MNTH, x.RAC_NAME, x.RAC_CODE

--Select distinct ID-SSN-RAC rows by calendar month and reportable RAC code (Code for R studio)
SELECT distinct MEDICAID_RECIPIENT_ID as id, SOCIAL_SECURITY_NMBR as ssn, RAC_NAME as rac_name, RAC_CODE as rac_code, CLNDR_YEAR_MNTH as elig_month
FROM [PHClaims].[dbo].[NewEligibility]

--How to count families
select distinct y.CLNDR_YEAR_MNTH, y. RAC_CODE, y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.HOH_ID
	FROM (
		SELECT z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.CLNDR_YEAR_MNTH, z.HOH_ID, z.RAC_CODE
		FROM [PHClaims].[dbo].[NewEligibility] as z
	) as y
where y.RAC_CODE in (1206, 1207, 1212, 1213)
order by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR

--
select distinct y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.HOH_ID, count(*) as hoh_cnt
	FROM (
		SELECT z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.HOH_ID
		FROM [PHClaims].[dbo].[NewEligibility] as z
	) as y
where y.HOH_ID is not null
group by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.HOH_ID
order by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, hoh_cnt desc, y.HOH_ID
