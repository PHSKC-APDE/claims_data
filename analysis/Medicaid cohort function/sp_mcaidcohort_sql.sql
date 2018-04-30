--Eli Kern
--Assessment, Policy Development & Evaluation, Public Health - Seattle & King County
--2/27/18
--Code to return a demographic subset of the King County Medicaid member population for a specific time period
--This script creates a stored procedure for use within SQL (only difference is that this creates a global temp table)

--Refer to README file on GitHub to understand parameters below
--https://github.com/PHSKC-APDE/Medicaid/tree/master/analysis/Medicaid%20cohort%20function

--select database
use PHClaims
go

--drop stored procedure before creating new
drop procedure dbo.sp_mcaidcohort_sql
go

--create stored procedure
create proc dbo.sp_mcaidcohort_sql
	(
	@from_date as date,
	@to_date as date,
	@duration as int,
	@covmin as decimal(4,1),
	@ccov_min int,
	@covgap_max int,
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
	@maxlang as varchar(max),
	@id as varchar(max)
	)
as
begin

--Drop temp table output if it exists
if object_id('tempdb..##mcaidcohort') IS NOT NULL 
  drop table ##mcaidcohort

--column specs for final joined select query
select cov.id, cov.covd, cov.covper, cov.ccovd_max, cov.covgap_max, dual.duald, dual.dualper, demo.dobnew, demo.age, demo.gender_mx, demo.male, demo.female, demo.male_t, demo.female_t, demo.race_mx,
	demo.aian, demo.asian, demo.black, demo.nhpi, demo.white, demo.latino, demo.aian_t, demo.asian_t, demo.black_t, demo.nhpi_t, demo.white_t, demo.latino_t, geo.zip_new,
	geo.kcreg_zip, geo.homeless_e, demo.maxlang, demo.english, demo.spanish, demo.vietnamese, demo.chinese, demo.somali, demo.russian, demo.arabic,
	demo.korean, demo.ukrainian, demo.amharic, demo.english_t, demo.spanish_t, demo.vietnamese_t, demo.chinese_t, demo.somali_t, demo.russian_t,
	demo.arabic_t, demo.korean_t, demo.ukrainian_t, demo.amharic_t

--create temp table for SQL analysis
into ##mcaidcohort

--1st table - coverage
from (
	select a.id, a.covd, a.covper, a.ccovd_max, a.covgap_max
	from (
		select z.id, z.covd, z.covper, z.ccovd_max,
			case
				when z.pregap_max >= z.postgap_max then z.pregap_max
				else z.postgap_max
			end as 'covgap_max'

		from (
			select y.id, sum(y.covd) as 'covd', cast(sum((y.covd * 1.0)) / (@duration * 1.0) * 100.0 as decimal(4,1)) as 'covper',
				max(y.covd) as 'ccovd_max', max(y.pregap) as 'pregap_max', max(y.postgap) as 'postgap_max'

			from (
			select distinct x.id, x.from_date, x.to_date,

			--calculate coverage days during specified time period
			/**if coverage period fully contains date range then person time is just date range */
			iif(x.from_date <= @from_date and x.to_date >= @to_date, datediff(day, @from_date, @to_date) + 1, 
	
			/**if coverage period begins before date range start and ends within date range */
			iif(x.from_date <= @from_date and x.to_date < @to_date, datediff(day, @from_date, x.to_date) + 1,

			/**if coverage period begins after date range start and ends after date range end */
			iif(x.from_date > @from_date and x.to_date >= @to_date, datediff(day, x.from_date, @to_date) + 1,

			/**if coverage period begins after date range start and ends before date range end */
			iif(x.from_date > @from_date and x.to_date < @to_date, datediff(day, x.from_date, x.to_date) + 1,

			null)))) as 'covd',

			--calculate coverage gaps during specified time period
			case
				when x.from_date <= @from_date then 0
				when lag(x.to_date,1) over (partition by x.id order by x.to_date) is null then datediff(day, @from_date, x.from_date) - 1
			end as 'pregap',

			case
				when x.to_date >= @to_date then 0
				when lead(x.to_date,1) over (partition by x.id order by x.to_date) is null then datediff(day, to_date, @to_date) - 1
				else datediff(day, x.to_date, lead(x.from_date,1) over (partition by x.id order by x.from_date)) - 1
			end as 'postgap'

			from PHClaims.dbo.mcaid_elig_overall as x
			where x.from_date < @to_date and x.to_date > @from_date
			) as y
			group by y.id
		) as z
	) as a
	where a.covper >= @covmin and a.ccovd_max >= @ccov_min and (@covgap_max is null or a.covgap_max <= @covgap_max)
	and (@id is null or a.id in (select * from PHClaims.dbo.Split(@id, ',')))
)as cov

--2nd table - dual eligibility duration
inner join (
select z.id, z.duald, z.dualper
from (
	select y.id, sum(y.duald) as 'duald', 
	cast(sum((y.duald * 1.0)) / (@duration * 1.0) * 100.0 as decimal(4,1)) as 'dualper'

		from (
			select distinct x.id, x.dual, x.from_date, x.to_date,

			/**if coverage period fully contains date range then person time is just date range */
			iif(x.from_date <= @from_date and x.to_date >= @to_date and x.dual = 'Y', datediff(day, @from_date, @to_date) + 1, 
	
			/**if coverage period begins before date range start and ends within date range */
			iif(x.from_date <= @from_date and x.to_date < @to_date and x.dual = 'Y', datediff(day, @from_date, x.to_date) + 1,

			/**if coverage period begins after date range start and ends after date range end */
			iif(x.from_date > @from_date and x.to_date >= @to_date and x.dual = 'Y', datediff(day, x.from_date, @to_date) + 1,

			/**if coverage period begins after date range start and ends before date range end */
			iif(x.from_date > @from_date and x.to_date < @to_date and x.dual = 'Y', datediff(day, x.from_date, x.to_date) + 1,

			0)))) as 'duald'
			from PHClaims.dbo.mcaid_elig_dual as x
			where x.from_date < @to_date and x.to_date > @from_date
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
		where @from_date < @to_date AND @to_date > @from_date
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
				select a.id, a.zip_new, sum(a.covd) + 1 as 'zip_dur'
				from (
					select id, zip_new,

						/**if coverage period fully contains date range then person time is just date range */
						iif(@from_date <= @from_date and @to_date >= @to_date, datediff(day, @from_date, @to_date) + 1, 
	
						/**if coverage period begins before date range start and ends within date range */
						iif(@from_date <= @from_date and @to_date < @to_date, datediff(day, @from_date, to_date) + 1,

						/**if coverage period begins after date range start and ends after date range end */
						iif(@from_date > @from_date and @to_date >= @to_date, datediff(day, from_date, @to_date) + 1,

						/**if coverage period begins after date range start and ends before date range end */
						iif(@from_date > @from_date and @to_date < @to_date, datediff(day, from_date, to_date) + 1,

						null)))) as 'covd'

					from PHClaims.dbo.mcaid_elig_address
					where @from_date < @to_date and @to_date > @from_date
				) as a
				group by a.id, a.zip_new
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
				select a.id, a.kcreg_zip, sum(a.covd) + 1 as 'zreg_dur'
				from (
					select id, kcreg_zip,

						/**if coverage period fully contains date range then person time is just date range */
						iif(@from_date <= @from_date and @to_date >= @to_date, datediff(day, @from_date, @to_date) + 1, 
	
						/**if coverage period begins before date range start and ends within date range */
						iif(@from_date <= @from_date and @to_date < @to_date, datediff(day, @from_date, to_date) + 1,

						/**if coverage period begins after date range start and ends after date range end */
						iif(@from_date > @from_date and @to_date >= @to_date, datediff(day, from_date, @to_date) + 1,

						/**if coverage period begins after date range start and ends before date range end */
						iif(@from_date > @from_date and @to_date < @to_date, datediff(day, from_date, to_date) + 1,

						null)))) as 'covd'

					from PHClaims.dbo.mcaid_elig_address
					where @from_date < @to_date and @to_date > @from_date
				) as a
				group by a.id, a.kcreg_zip
			) as x
		) as y
		where y.zregr = 1
	) as zregdur
	on x.id = zregdur.id

	--pass in zip and/or region specifications if provided
	where ((@zip is null) or zipdur.zip_new in (select * from PHClaims.dbo.Split(@zip, ',')))
	and (@region is null or zregdur.kcreg_zip in (select * from PHClaims.dbo.Split(@region, ',')))

) as geo
--join on ID
on cov.id = geo.id

--4th table - age, gender, race, and language
inner join (
	select x.id, x.dobnew, x.age, x.male, x.female, x.male_t, x.female_t, x.aian, x.asian,
		x.black, x.nhpi, x.white, x.latino, x.aian_t, x.asian_t, x.black_t, x.nhpi_t, x.white_t,
		x.latino_t, x.maxlang, x.english, x.spanish, x.vietnamese, x.chinese, x.somali, x.russian,
		x.arabic, x.korean, x.ukrainian, x.amharic, x. english_t, x.spanish_t, x.vietnamese_t,
		x.chinese_t, x.somali_t, x.russian_t, x.arabic_t, x.korean_t, x.ukrainian_t, x.amharic_t,

		--mutually exclusive gender
		case
			when female_t > 0 and male_t > 0 then 'Multiple'
			when female = 1 then 'Female'
			when male = 1 then 'Male'
			else null
		end as 'gender_mx',

		--mutually exclusive race/ethnicity
		case
			when (aian + asian + black + nhpi + white + latino) > 1 then 'Multiple'
			when aian = 1 then 'AI/AN'
			when asian = 1 then 'Asian'
			when black = 1 then 'Black'
			when nhpi = 1 then 'NH/PI'
			when white = 1 then 'White'
			when latino = 1 then 'Latino'
			else null
		end as 'race_mx'

	from( 	
		select distinct id, 
		--age vars
		dobnew, floor((datediff(day, dobnew, @to_date) + 1) / 365.25) as 'age',
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
		((@maxlang is null) or maxlang in (select * from PHClaims.dbo.Split(@maxlang, ',')))
) as demo
--join on ID
on cov.id = demo.id

--order statement for final joined table
order by cov.id

end