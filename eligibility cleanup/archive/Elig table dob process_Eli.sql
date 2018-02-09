-- Code to process the Medicaid eligibility table from HCA
-- Public Health — Seattle & King County
-- Eli Kern
-- 2017-05

/*
This code aims to assign one DOB to each individual (defined by distinct combo of Medicaid ID and SSN)
*/

--Count number of rows per distinct ID-SSN-DOB
select distinct y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.BIRTH_DATE, count(*) as dob_cnt
	FROM (
		SELECT top 50000 z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.BIRTH_DATE
		FROM [PHClaims].[dbo].[NewEligibility] as z
	) as y
group by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.BIRTH_DATE
order by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, dob_cnt desc, y.BIRTH_DATE