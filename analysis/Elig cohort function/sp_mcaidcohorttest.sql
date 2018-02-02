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
	@duration as float,
	@covmin as float,
	@agemin as int,
	@agemax as int,
	@female as varchar(max),
	@male as varchar(max),
	@aian as varchar(max),
	@asian as varchar(max),
	@black as varchar(max),
	@nhpi as varchar(max),
	@white as varchar(max),
	@latino as varchar (max),
	@zip as varchar(max),
	@region as varchar(max)
	)
as
begin

--column specs for final joined select query
select cov.id, cov.covd, cov.covper, age.age, sex.male, sex.female, sex.male_t, sex.female_t, race.aian, race.asian, race.black,
	race.nhpi, race.white, race.latino, race.aian_t, race.asian_t, race.black_t, race.nhpi_t, race.white_t, race.latino_t, geo.zip_new,
	geo.kcreg_zip, geo.homeless_e

--1st table - coverage
from (
	select z.id, z.covd, z.covper
	from (
		select y.id, sum(y.covd) as 'covd', round(sum(y.covd) / @duration * 100, 1) as 'covper'
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
			where startdate < @end and enddate > @begin
		) as y
		group by y.id
	) as z
	where z.covper >= @covmin
)as cov

--2nd table - age
inner join (
	select agex.id, agex.dobnew, agex.age
	from( 	
		select distinct id, dobnew,
		floor((datediff(day, dobnew, @end) + 1) / 365.25) as 'age'
		from PHClaims.dbo.mcaid_elig_dob
		) as agex
	where agex.age >= @agemin and agex.age <= @agemax
) as age
--join on ID
on cov.id = age.id

--3rd table - gender
inner join (
	select id, male, female, male_t, female_t
	from PHClaims.dbo.mcaid_elig_gender
	where (@male is null or male = @male) and (@female is null or female = @female)
) as sex
--join on ID
on cov.id = sex.id

--4th table - race
inner join (
	select id, aian, asian, black, nhpi, white, latino, aian_t, asian_t, black_t, nhpi_t, white_t, latino_t
	from PHClaims.dbo.mcaid_elig_race
	where (@aian is null or aian = @aian) and (@asian is null or asian = @asian) and
		(@black is null or black = @black) and (@nhpi is null or nhpi = @nhpi) and
		(@white is null or white = @white) and (@latino is null or latino = @latino)
) as race
--join on ID
on cov.id = race.id 

--5th table - sub-county areas
inner join (
	select distinct x.id,

				/**if one zip per client for time period, keep zip value */
				iif(zip.zip_cnt = 1, str(x.zip_new, 5, 0), 
	
				/**if > 1 zip per client for time period, return "multiple" */
				iif(zip.zip_cnt > 1, 'multiple',

				null)) as 'zip_new',

				/**if one zip-based region per client for time period, keep zip-based region value */
				iif(reg.reg_cnt = 1, x.kcreg_zip, 
	
				/**if > 1 zip-based region per client for time period, return "multiple" */
				iif(reg.reg_cnt > 1, 'multiple',

				null)) as 'kcreg_zip',

				homeless.homeless_e

	--client level table
	from (
		select id, from_add, to_add, zip_new, kcreg_zip, homeless
		from PHClaims.dbo.mcaid_elig_address
		where from_add < @end and to_add > @begin
	) as x

	--count distinct ZIPs per client (count nulls as a unique value)
	left join (
		select id, (count(distinct zip_new) + count(distinct case when zip_new is null then 1 end)) as zip_cnt
		from PHClaims.dbo.mcaid_elig_address
		where from_add < @end and to_add > @begin
		group by id
	) as zip
	on x.id = zip.id

	--count distinct regions per client (count nulls as a unique value)
	left join (
		select id, (count(distinct kcreg_zip) + count(distinct case when kcreg_zip is null then 1 end)) as reg_cnt
		from PHClaims.dbo.mcaid_elig_address
		where from_add < @end and to_add > @begin
		group by id
	) as reg
	on x.id = reg.id

	--take max of homeless value (ever homeless)
	left join (
		select id, max(homeless) as homeless_e
		from PHClaims.dbo.mcaid_elig_address
		group by id
	) as homeless
	on x.id = homeless.id

	--pass in zip and/or region specifications if provided
	where ((@zip is null) or zip_new in (select * from PH_APDEStore.dbo.Split(@zip, ',')))
	and (@region is null or kcreg_zip in (select * from PH_APDEStore.dbo.Split(@region, ',')))

) as geo

--join on ID
on cov.id = geo.id

--order statement for final joined table
order by cov.id

end
