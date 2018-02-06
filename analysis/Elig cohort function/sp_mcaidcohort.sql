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
	@duration as int,
	@covmin as decimal(4,1),
	@dualmax as decimal(4,1),
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
	@region as varchar(max),
	@english as varchar(max), 
	@spanish as varchar(max),
	@vietnamese as varchar(max),
	@chinese as varchar(max),
	@somali as varchar(max),
	@russian as varchar(max),
	@arabic as varchar(max),
	@korean as varchar(max),
	@ukrainian as varchar(max),
	@amharic as varchar(max),
	@maxlang as varchar(max)
	)
as
begin

--column specs for final joined select query
select cov.id, cov.covd, cov.covper, dual.duald, dual.dualper, demo.age, demo.male, demo.female, demo.male_t, demo.female_t, demo.aian, demo.asian, demo.black,
	demo.nhpi, demo.white, demo.latino, demo.aian_t, demo.asian_t, demo.black_t, demo.nhpi_t, demo.white_t, demo.latino_t, geo.zip_new,
	geo.kcreg_zip, geo.homeless_e, demo.maxlang, demo.english, demo.spanish, demo.vietnamese, demo.chinese, demo.somali, demo.russian, demo.arabic,
	demo.korean, demo.ukrainian, demo.amharic, demo.english_t, demo.spanish_t, demo.vietnamese_t, demo.chinese_t, demo.somali_t, demo.russian_t,
	demo.arabic_t, demo.korean_t, demo.ukrainian_t, demo.amharic_t 

--1st table - coverage
from (
	select z.id, z.covd, z.covper
	from (
		select y.id, sum(y.covd) as 'covd', cast(sum((y.covd * 1.0)) / (@duration * 1.0) * 100.0 as decimal(4,1)) as 'covper'

		from (
			select distinct x.MEDICAID_RECIPIENT_ID as 'id', x.startdate, x.enddate,

			/**if coverage period fully contains date range then person time is just date range */
			iif(x.startdate <= @begin and x.enddate >= @end, datediff(day, @begin, @end) + 1, 
	
			/**if coverage period begins before date range start and ends within date range */
			iif(x.startdate <= @begin and x.enddate < @end, datediff(day, @begin, x.enddate) + 1,

			/**if coverage period begins after date range start and ends after date range end */
			iif(x.startdate > @begin and x.enddate >= @end, datediff(day, x.startdate, @end) + 1,

			/**if coverage period begins after date range start and ends before date range end */
			iif(x.startdate > @begin and x.enddate < @end, datediff(day, x.startdate, x.enddate) + 1,

			null)))) as 'covd'
			from PHClaims.dbo.mcaid_elig_overall as x
			where x.startdate < @end and x.enddate > @begin
		) as y
		group by y.id
	) as z
	where z.covper >= @covmin
)as cov

--2nd table - dual eligibility duration
inner join (
select z.id, z.duald, z.dualper
from (
	select y.id, sum(y.duald) as 'duald', 
	cast(sum((y.duald * 1.0)) / (@duration * 1.0) * 100.0 as decimal(4,1)) as 'dualper'

		from (
			select distinct x.id, x.dual, x.calstart, x.calend,

			/**if coverage period fully contains date range then person time is just date range */
			iif(x.calstart <= @begin and x.calend >= @end and x.dual = 'Y', datediff(day, @begin, @end) + 1, 
	
			/**if coverage period begins before date range start and ends within date range */
			iif(x.calstart <= @begin and x.calend < @end and x.dual = 'Y', datediff(day, @begin, x.calend) + 1,

			/**if coverage period begins after date range start and ends after date range end */
			iif(x.calstart > @begin and x.calend >= @end and x.dual = 'Y', datediff(day, x.calstart, @end) + 1,

			/**if coverage period begins after date range start and ends before date range end */
			iif(x.calstart > @begin and x.calend < @end and x.dual = 'Y', datediff(day, x.calstart, x.calend) + 1,

			0)))) as 'duald'
			from PHClaims.dbo.mcaid_elig_dual as x
			where x.calstart < @end and x.calend > @begin
		) as y
		group by y.id
	) as z
	where z.dualper <= @dualmax
) as dual
on cov.id = dual.id

--3rd table - sub-county areas
inner join (
	select distinct x.id, zipdur.zip_new, zregdur.kcreg_zip, homeless.homeless_e

	--client level table
	from (
		select distinct id, zip_new, kcreg_zip
		from PHClaims.dbo.mcaid_elig_address
		where from_add < @end AND to_add > @begin
	) as x

	--take max of homeless value (ever homeless)
	left join (
		select id, max(homeless) as homeless_e
		from PHClaims.dbo.mcaid_elig_address
		group by id
	) as homeless
	on x.id = homeless.id

	--select ZIP code with greatest duration during time range (no ties allowed given row_number() is used instead of rank())
	left join (
		select y.id, y.zip_new
		from (
			select x.id, x.zip_new, x.zip_dur, row_number() over (partition by x.id order by x.zip_dur desc) as 'zipr'
			from (
				select id, zip_new, sum(datediff(day, from_add, to_add) + 1) as 'zip_dur'
				from PHClaims.dbo.mcaid_elig_address
				where from_add < @end AND to_add > @begin
				group by id, zip_new
			) as x
		) as y
		where y.zipr = 1
	) as zipdur
	on x.id = zipdur.id

	--select ZIP-based region with greatest duration during time range (no ties allowed given row_number() is used instead of rank())
	left join (
		select y.id, y.kcreg_zip
		from (
			select x.id, x.kcreg_zip, x.zreg_dur, row_number() over (partition by x.id order by x.zreg_dur desc) as 'zregr'
			from (
				select id, kcreg_zip, sum(datediff(day, from_add, to_add) + 1) as 'zreg_dur'
				from PHClaims.dbo.mcaid_elig_address
				where from_add < @end AND to_add > @begin
				group by id, kcreg_zip
			) as x
		) as y
		where y.zregr = 1
	) as zregdur
	on x.id = zregdur.id

	--pass in zip and/or region specifications if provided
	where ((@zip is null) or zipdur.zip_new in (select * from PH_APDEStore.dbo.Split(@zip, ',')))
	and (@region is null or zregdur.kcreg_zip in (select * from PH_APDEStore.dbo.Split(@region, ',')))

) as geo
--join on ID
on cov.id = geo.id

--4th table - age, gender, race, and language
inner join (
	select x.id, x.dobnew, x.age, x.male, x.female, x.male_t, x.female_t, x.aian, x.asian,
		x.black, x.nhpi, x.white, x.latino, x.aian_t, x.asian_t, x.black_t, x.nhpi_t, x.white_t,
		x.latino_t, x.maxlang, x.english, x.spanish, x.vietnamese, x.chinese, x.somali, x.russian,
		x.arabic, x.korean, x.ukrainian, x.amharic, x. english_t, x.spanish_t, x.vietnamese_t,
		x.chinese_t, x.somali_t, x.russian_t, x.arabic_t, x.korean_t, x.ukrainian_t, x.amharic_t
	from( 	
		select distinct id, 
		--age vars
		dobnew, floor((datediff(day, dobnew, @end) + 1) / 365.25) as 'age',
		--gender vars
		male, female, male_t, female_t,
		--race vars
		aian, asian, black, nhpi, white, latino, aian_t, asian_t, black_t, nhpi_t, white_t, latino_t,
		--language vars
		maxlang, english, spanish, vietnamese, chinese, somali, russian, arabic, korean, ukrainian, amharic,
		english_t, spanish_t, vietnamese_t, chinese_t, somali_t, russian_t, arabic_t, korean_t, ukrainian_t,
		amharic_t
		from PHClaims.dbo.mcaid_elig_demoever
		) as x
	--age subsets
	where x.age >= @agemin and x.age <= @agemax
	--gender subsets
	and (@male is null or x.male = @male) and (@female is null or x.female = @female)
	--race subsets
	and (@aian is null or aian = @aian) and (@asian is null or asian = @asian) and
		(@black is null or black = @black) and (@nhpi is null or nhpi = @nhpi) and
		(@white is null or white = @white) and (@latino is null or latino = @latino)
	--language subsets
	and (@english is null or english = @english) and (@spanish is null or spanish = @spanish) and
		(@vietnamese is null or vietnamese = @vietnamese) and (@chinese is null or chinese = @chinese) and
		(@somali is null or somali = @somali) and (@russian is null or russian = @russian) and
		(@arabic is null or arabic = @arabic) and (@korean is null or korean = @korean) and
		(@ukrainian is null or ukrainian = @ukrainian) and (@amharic is null or amharic = @amharic) and
		((@maxlang is null) or maxlang in (select * from PH_APDEStore.dbo.Split(@maxlang, ',')))
) as demo
--join on ID
on cov.id = demo.id

--order statement for final joined table
order by cov.id

end
