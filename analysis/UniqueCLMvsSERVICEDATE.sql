--############
--distinct members with claims in 2016
SELECT count (distinct id)
FROM
(SELECT top 100000 MEDICAID_RECIPIENT_ID AS id
FROM PHClaims.dbo.NewClaims

WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016, 1-year ref period per CCW
)as a

--############
--distinct members with claims in 2016
select count(*) as count
from
	(SELECT distinct id, tcn
	FROM
	(SELECT top 100000 MEDICAID_RECIPIENT_ID AS id, TCN
		FROM PHClaims.dbo.NewClaims
		WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31') as a
	) as b

--############
--distinct members with claims in 2016
select count(*) as count
from
	(SELECT distinct id, FROM_SRVC_DATE
	FROM
	(SELECT top 100000 MEDICAID_RECIPIENT_ID AS id, FROM_SRVC_DATE
		FROM PHClaims.dbo.NewClaims
		WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31') as a
	) as b