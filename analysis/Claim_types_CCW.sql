--Understand claim types needed for using CCW algorithm

select CLM_TYPE_NAME, count (distinct TCN) as clmtype_cnt
from PHClaims.dbo.NewClaims
where FROM_SRVC_DATE between '2016-01-01' AND '2016-12-31'
group by CLM_TYPE_NAME
