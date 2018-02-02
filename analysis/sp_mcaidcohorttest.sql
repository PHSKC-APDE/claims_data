--Eli Kern
--1/25/18
--APDE
--Code to return a demographic subset of the Medicaid pop for a specific time period

--select database
use PH_APDEStore
go

--drop stored procedure before creating new
drop procedure sp_mcaidcohort
go

--create stored procedure
create proc sp_mcaidcohort
	(
	@begin as date,
	@end as date,
	@agemin as int,
	@agemax as int
	)
as
begin

--column specs for final joined select query
select cov.id, cov.covd, age.age

--1st table - coverage
from (
	select y.id, sum(y.covd) as 'covd'
	from (

		select distinct x.MEDICAID_RECIPIENT_ID as 'id', x.startdate, x.enddate,

			/**if coverage period fully contains date range then person time is just date range */
			iif(startdate <= @begin and enddate >= @end, datediff(day, @begin, @end) + 1, 
	
			/**if coverage period begins before date range start and ends within date range */
			iif(startdate <= @begin and enddate < @end, datediff(day, @begin, enddate) + 1,

			/**if coverage period begins after date range start and ends after date range end */
			iif(startdate > @begin and enddate >= @end, datediff(day, startdate, @end) + 1,

			/**if coverage period begins after date range start and ends before date range end */
			iif(startdate > @begin and enddate < @end, datediff(day, startdate, enddate) + 1,

			null)))) as 'covd'
			from PHClaims.dbo.mcaid_elig_overall as x
			where startdate < @end AND enddate > @begin
		) as y
	group by y.id
) as cov

--2nd table - age
inner join (
	select distinct id, dobnew,
	floor((datediff(day, dobnew, @end) + 1) / 365.25) as 'age'
	from PHClaims.dbo.mcaid_elig_dob
) as age

--join on ID
on cov.id = age.id

--subset age if specified
where age.age >= @agemin and age.age <= @agemax

end
