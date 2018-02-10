--Eli Kern
--1/25/18
--APDE
--Code to return a demographic subset of the Medicaid pop for a specific time period

--Notes on specifying gender and race
--If you provide a value of 1 for say example, female, the cohort will only include members that were marked female alone or in combo ever
--If you provide a value of 1 for say example, black, and 0 for white, the cohort will only include members that were marked black alone or in combo ever AND never white
--Leaving any of these demographic parameters null will allow members to have any value for that particular field (e.g. male = 1, female = null will return cohort of members
	--who were marked male alone or in combo ever, with the female variable allowed to have any value)

--Begin code
declare @from_date date, @to_date date, @duration int, @covmin decimal(4,1), @dualmax decimal(4,1), @agemin int, @agemax int, @female varchar(max), @male varchar(max),
	@aian varchar(max), @asian varchar(max), @black varchar(max), @nhpi varchar(max), @white varchar(max), @latino varchar (max), 
	@zip varchar(max), @region varchar(max), @english varchar(max), @spanish varchar(max), @vietnamese varchar(max), @chinese varchar(max),
	@somali varchar(max), @russian varchar(max), @arabic varchar(max), @korean varchar(max), @ukrainian varchar(max), @amharic varchar(max),
	@maxlang varchar(max), @id varchar(max)

set @from_date = '2017-01-01'
set @to_date = '2017-06-30'
set @duration = datediff(day, @from_date, @to_date) + 1
set @covmin = 0
set @dualmax = 100
set @agemin = 0
set @agemax = 200
set @female = null
set @male = null
set @aian = null
set @asian = null
set @black = null
set @nhpi = null
set @white = null
set @latino = null
set @zip = null
set @region = null
set @english = null
set @spanish = null
set @vietnamese = null
set @chinese = null
set @somali = null
set @russian = null
set @arabic = null
set @korean = null
set @ukrainian = null
set @amharic = null
set @maxlang = null
set @id = null

--column specs for final joined select query
select cov.id, cov.covd, cov.covper, dual.duald, dual.dualper, age, demo.male, demo.female, demo.male_t, demo.female_t, demo.aian, demo.asian, demo.black,
	demo.nhpi, demo.white, demo.latino, demo.aian_t, demo.asian_t, demo.black_t, demo.nhpi_t, demo.white_t, demo.latino_t, geo.zip_new,
	geo.kcreg_zip, geo.homeless_e, demo.maxlang, demo.english, demo.spanish, demo.vietnamese, demo.chinese, demo.somali, demo.russian, demo.arabic,
	demo.korean, demo.ukrainian, demo.amharic, demo.english_t, demo.spanish_t, demo.vietnamese_t, demo.chinese_t, demo.somali_t, demo.russian_t,
	demo.arabic_t, demo.korean_t, demo.ukrainian_t, demo.amharic_t,
	
	round(rac.nadultd / cov.covd * 100.0, 1) as 'nadultper',
	round(rac.disabledd / cov.covd * 100.0, 1) as 'disabledper',
	round(rac.nondisabledd / cov.covd * 100.0, 1) as 'nondisabledper'

--1st table - coverage
from (
	select z.id, z.covd, z.covper
	from (
		select y.id, sum(y.covd) as 'covd', cast(sum((y.covd * 1.0)) / (@duration * 1.0) * 100.0 as decimal(4,1)) as 'covper'

		from (
			select distinct x.id, x.from_date, x.to_date,

			/**if coverage period fully contains date range then person time is just date range */
			iif(x.from_date <= @from_date and x.to_date >= @to_date, datediff(day, @from_date, @to_date) + 1, 
	
			/**if coverage period begins before date range start and ends within date range */
			iif(x.from_date <= @from_date and x.to_date < @to_date, datediff(day, @from_date, x.to_date) + 1,

			/**if coverage period begins after date range start and ends after date range end */
			iif(x.from_date > @from_date and x.to_date >= @to_date, datediff(day, x.from_date, @to_date) + 1,

			/**if coverage period begins after date range start and ends before date range end */
			iif(x.from_date > @from_date and x.to_date < @to_date, datediff(day, x.from_date, x.to_date) + 1,

			null)))) as 'covd'
			from PHClaims.dbo.mcaid_elig_overall as x
			where x.from_date < @to_date and x.to_date > @from_date
		) as y
		group by y.id
	) as z
	where z.covper >= @covmin
	and (@id is null or z.id in (select * from PH_APDEStore.dbo.Split(@id, ',')))
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

--3rd table - Coverage groups
inner join (
--create coverage group vars per zeyno's code
	select y.id, sum(y.nadultd) as 'nadultd', sum(y.disabled) as 'disabledd', sum(y.nondisabled) as 'nondisabledd'
	from (
		select distinct x.id, x.rac, x.from_date, x.to_date, x.nadult, x.disabled, x.nondisabled,

		--new adults
		/**if coverage period fully contains date range then person time is just date range */
		iif(x.from_date <= @from_date and x.to_date >= @to_date and x.nadult = 1, datediff(day, @from_date, @to_date) + 1, 
	
		/**if coverage period begins before date range start and ends within date range */
		iif(x.from_date <= @from_date and x.to_date < @to_date and x.nadult = 1, datediff(day, @from_date, x.to_date) + 1,

		/**if coverage period begins after date range start and ends after date range end */
		iif(x.from_date > @from_date and x.to_date >= @to_date and x.nadult = 1, datediff(day, x.from_date, @to_date) + 1,

		/**if coverage period begins after date range start and ends before date range end */
		iif(x.from_date > @from_date and x.to_date < @to_date and x.nadult = 1, datediff(day, x.from_date, x.to_date) + 1,

		0)))) as 'nadultd',

		--disabled
		/**if coverage period fully contains date range then person time is just date range */
		iif(x.from_date <= @from_date and x.to_date >= @to_date and x.disabled = 1, datediff(day, @from_date, @to_date) + 1, 
	
		/**if coverage period begins before date range start and ends within date range */
		iif(x.from_date <= @from_date and x.to_date < @to_date and x.disabled = 1, datediff(day, @from_date, x.to_date) + 1,

		/**if coverage period begins after date range start and ends after date range end */
		iif(x.from_date > @from_date and x.to_date >= @to_date and x.disabled = 1, datediff(day, x.from_date, @to_date) + 1,

		/**if coverage period begins after date range start and ends before date range end */
		iif(x.from_date > @from_date and x.to_date < @to_date and x.disabled = 1, datediff(day, x.from_date, x.to_date) + 1,

		0)))) as 'disabledd',

		--non-disabled
		/**if coverage period fully contains date range then person time is just date range */
		iif(x.from_date <= @from_date and x.to_date >= @to_date and x.nondisabled = 1, datediff(day, @from_date, @to_date) + 1, 
	
		/**if coverage period begins before date range start and ends within date range */
		iif(x.from_date <= @from_date and x.to_date < @to_date and x.nondisabled = 1, datediff(day, @from_date, x.to_date) + 1,

		/**if coverage period begins after date range start and ends after date range end */
		iif(x.from_date > @from_date and x.to_date >= @to_date and x.nondisabled = 1, datediff(day, x.from_date, @to_date) + 1,

		/**if coverage period begins after date range start and ends before date range end */
		iif(x.from_date > @from_date and x.to_date < @to_date and x.nondisabled = 1, datediff(day, x.from_date, x.to_date) + 1,

		0)))) as 'nondisabledd'

		from PHClaims.dbo.mcaid_elig_covgrp as x
		where x.from_date < @to_date and x.to_date > @from_date
	) as y
	group by y.id
) as rac
on cov.id = rac.id

--4th table - sub-county areas
inner join (
	select distinct x.id, zipdur.zip_new, zregdur.kcreg_zip, homeless.homeless_e

	--client level table
	from (
		select distinct id, zip_new, kcreg_zip
		from PHClaims.dbo.mcaid_elig_address
		where from_date < @to_date and to_date > @from_date
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
						iif(from_date_date <= @from_date and to_date >= @to_date, datediff(day, @from_date, @to_date) + 1, 
	
						/**if coverage period begins before date range start and ends within date range */
						iif(from_date_date <= @from_date and to_date < @to_date, datediff(day, @from_date, to_date) + 1,

						/**if coverage period begins after date range start and ends after date range end */
						iif(from_date > @from_date and to_date >= @to_date, datediff(day, from_date, @to_date) + 1,

						/**if coverage period begins after date range start and ends before date range end */
						iif(from_date > @from_date and to_date < @to_date, datediff(day, from_date, to_date) + 1,

						null)))) as 'covd'

					from PHClaims.dbo.mcaid_elig_address
					where from_date < @to_date and to_date > @from_date
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
						iif(from_date_date <= @from_date and to_date >= @to_date, datediff(day, @from_date, @to_date) + 1, 
	
						/**if coverage period begins before date range start and ends within date range */
						iif(from_date_date <= @from_date and to_date < @to_date, datediff(day, @from_date, to_date) + 1,

						/**if coverage period begins after date range start and ends after date range end */
						iif(from_date > @from_date and to_date >= @to_date, datediff(day, from_date, @to_date) + 1,

						/**if coverage period begins after date range start and ends before date range end */
						iif(from_date > @from_date and to_date < @to_date, datediff(day, from_date, to_date) + 1,

						null)))) as 'covd'

					from PHClaims.dbo.mcaid_elig_address
					where from_date < @to_date and to_date > @from_date
				) as a
				group by a.id, a.kcreg_zip
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

--5th table - age, gender, race, and language
inner join (
	select x.id, x.dobnew, x.age, x.male, x.female, x.male_t, x.female_t, x.aian, x.asian,
		x.black, x.nhpi, x.white, x.latino, x.aian_t, x.asian_t, x.black_t, x.nhpi_t, x.white_t,
		x.latino_t, x.maxlang, x.english, x.spanish, x.vietnamese, x.chinese, x.somali, x.russian,
		x.arabic, x.korean, x.ukrainian, x.amharic, x. english_t, x.spanish_t, x.vietnamese_t,
		x.chinese_t, x.somali_t, x.russian_t, x.arabic_t, x.korean_t, x.ukrainian_t, x.amharic_t
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
		((@maxlang is null) or maxlang in (select * from PH_APDEStore.dbo.Split(@maxlang, ',')))
) as demo
--join on ID
on cov.id = demo.id

--order statement for final joined table
order by cov.id
