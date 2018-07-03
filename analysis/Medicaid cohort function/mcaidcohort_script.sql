--Eli Kern
--Assessment, Policy Development & Evaluation, Public Health - Seattle & King County
--2/27/18
--Code to return a demographic subset of the King County Medicaid member population for a specific time period

--Refer to README file on GitHub to understand parameters below
--https://github.com/PHSKC-APDE/Medicaid/tree/master/analysis/Medicaid%20cohort%20function

--Begin code
declare @from_date date, @to_date date, @duration int, @covmin decimal(4,1), @ccov_min int, @covgap_max int, @dualmax decimal(4,1), @agemin int, @agemax int, 
	@female varchar(max), @male varchar(max), @aian varchar(max), @asian varchar(max), @black varchar(max), @nhpi varchar(max), 
	@white varchar(max), @latino varchar (max), @zip varchar(max), @region varchar(max), @english varchar(max), @spanish varchar(max), 
	@vietnamese varchar(max), @chinese varchar(max), @somali varchar(max), @russian varchar(max), @arabic varchar(max), 
	@korean varchar(max), @ukrainian varchar(max), @amharic varchar(max), @maxlang varchar(max), @id varchar(max)

set @from_date = '2017-01-01'
set @to_date = '2017-06-30'
set @duration = datediff(day, @from_date, @to_date) + 1
set @covmin = 0
set @ccov_min = 1
set @covgap_max = null
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
select cov.id, 
	case
		when cov.covgap_max <= 30 and dual.dual_flag = 0 then 'small gap, nondual'
		when cov.covgap_max > 30 and dual.dual_flag = 0 then 'large gap, nondual'
		when cov.covgap_max <= 30 and dual.dual_flag = 1 then 'small gap, dual'
		when cov.covgap_max > 30 and dual.dual_flag = 1 then 'large gap, dual'
	end as 'cov_cohort',

	cov.covd, cov.covper, cov.ccovd_max, cov.covgap_max, dual.duald, dual.dualper, dual.dual_flag, demo.dobnew, demo.age, demo.age_grp7, demo.gender_mx, demo.male, demo.female, 
	demo.male_t, demo.female_t, demo.gender_unk, demo.race_eth_mx, demo.race_mx, demo.aian, demo.asian, demo.black, demo.nhpi, demo.white, demo.latino, demo.aian_t, demo.asian_t, demo.black_t, 
	demo.nhpi_t, demo.white_t, demo.latino_t, demo.race_unk, geo.zip_new, geo.kcreg_zip, geo.homeless_e, demo.maxlang, demo.english, demo.spanish, demo.vietnamese, demo.chinese, demo.somali, 
	demo.russian, demo.arabic, demo.korean, demo.ukrainian, demo.amharic, demo.english_t, demo.spanish_t, demo.vietnamese_t, demo.chinese_t, demo.somali_t, demo.russian_t,
	demo.arabic_t, demo.korean_t, demo.ukrainian_t, demo.amharic_t, demo.lang_unk

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
			iif(x.from_date <= @from_date and x.to_date < @to_date and x.to_date >= @from_date, datediff(day, @from_date, x.to_date) + 1,

			/**if coverage period begins within date range and ends after date range end */
			iif(x.from_date > @from_date and x.to_date >= @to_date and x.from_date <= @to_date, datediff(day, x.from_date, @to_date) + 1,

			/**if coverage period begins after date range start and ends before date range end */
			iif(x.from_date > @from_date and x.to_date < @to_date, datediff(day, x.from_date, x.to_date) + 1,

			null)))) as 'covd',

			--calculate coverage gaps during specified time period
			case
				when x.from_date <= @from_date then 0
				when lag(x.to_date,1) over (partition by x.id order by x.to_date) is null then datediff(day, @from_date, x.from_date) - 1
				else datediff(day, lag(x.to_date,1) over (partition by x.id order by x.to_date), x.from_date) - 1
			end as 'pregap',

			case
				when x.to_date >= @to_date then 0
				when lead(x.to_date,1) over (partition by x.id order by x.to_date) is null then datediff(day, x.to_date, @to_date) - 1
				else datediff(day, x.to_date, lead(x.from_date,1) over (partition by x.id order by x.from_date)) - 1
			end as 'postgap'

			from PHClaims.dbo.mcaid_elig_overall as x
			where x.from_date <= @to_date and x.to_date >= @from_date
			) as y
			group by y.id
		) as z
	) as a
	where a.covper >= @covmin and a.ccovd_max >= @ccov_min and (@covgap_max is null or a.covgap_max <= @covgap_max)
	and (@id is null or a.id in (select * from PHClaims.dbo.Split(@id, ',')))
)as cov

--2nd table - dual eligibility duration
inner join (
select z.id, z.duald, z.dualper, case when z.duald >= 1 then 1 else 0 end as 'dual_flag'
from (
	select y.id, sum(y.duald) as 'duald', 
	cast(sum((y.duald * 1.0)) / (@duration * 1.0) * 100.0 as decimal(4,1)) as 'dualper'

		from (
			select distinct x.id, x.dual, x.from_date, x.to_date,

			/**if coverage period fully contains date range then person time is just date range */
			iif(x.from_date <= @from_date and x.to_date >= @to_date and x.dual = 'Y', datediff(day, @from_date, @to_date) + 1, 
	
			/**if coverage period begins before date range start and ends within date range */
			iif(x.from_date <= @from_date and x.to_date < @to_date and x.to_date >= @from_date and x.dual = 'Y', datediff(day, @from_date, x.to_date) + 1,

			/**if coverage period begins within date range and ends after date range end */
			iif(x.from_date > @from_date and x.to_date >= @to_date and x.from_date <= @to_date and x.dual = 'Y', datediff(day, x.from_date, @to_date) + 1,

			/**if coverage period begins after date range start and ends before date range end */
			iif(x.from_date > @from_date and x.to_date < @to_date and x.dual = 'Y', datediff(day, x.from_date, x.to_date) + 1,

			0)))) as 'duald'
			from PHClaims.dbo.mcaid_elig_covgrp as x
			where x.from_date <= @to_date and x.to_date >= @from_date
		) as y
		group by y.id
	) as z
	where z.dualper <= @dualmax
) as dual
on cov.id = dual.id

--3rd table - sub-county areas
inner join (
	select distinct x.id, zipdur.zip_new, zipdur.kcreg_zip, homeless.homeless_e

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
	--to get consistent counts by ZIP, ZIP codes are grouped by ID and ZIP, and then sorted by zip duration (desc) and ZIP (asc)
	left join (
		select zip.id, zip.zip_new, reg.kcreg_zip
		from (
			select y.id, y.zip_new
			from (
				select x.id, x.zip_new, x.zip_dur, row_number() over (partition by x.id order by x.zip_dur desc, x.zip_new) as 'zipr'
				from (
					select a.id, a.zip_new, sum(a.covd) + 1 as 'zip_dur'
					from (
						select id, zip_new,

							/**if coverage period fully contains date range then person time is just date range */
							iif(from_date <= @from_date and to_date >= @to_date, datediff(day, @from_date, @to_date) + 1, 
	
							/**if coverage period begins before date range start and ends within date range */
							iif(from_date <= @from_date and to_date < @to_date and to_date >= @from_date, datediff(day, @from_date, to_date) + 1,

							/**if coverage period begins within date range and ends after date range end */
							iif(from_date > @from_date and to_date >= @to_date and from_date <= @to_date, datediff(day, from_date, @to_date) + 1,

							/**if coverage period begins after date range start and ends before date range end */
							iif(from_date > @from_date and to_date < @to_date, datediff(day, from_date, to_date) + 1,

							null)))) as 'covd'

						from PHClaims.dbo.mcaid_elig_address
						where @from_date <= @to_date and @to_date >= @from_date
					) as a
					group by a.id, a.zip_new
				) as x
			) as y
			where y.zipr = 1
		) as zip

		--select ZIP-based region based on selected ZIP code
		left join (
			select zip, kcreg_zip
			from PHClaims.dbo.ref_region_zip_1017
		) as reg
		on zip.zip_new = reg.zip

	) as zipdur
	on x.id = zipdur.id

	--pass in zip and/or region specifications if provided
	where ((@zip is null) or zipdur.zip_new in (select * from PHClaims.dbo.Split(@zip, ',')))
	and (@region is null or zipdur.kcreg_zip in (select * from PHClaims.dbo.Split(@region, ',')))

) as geo
--join on ID
on cov.id = geo.id

--4th table - age, gender, race, and language
inner join (
	select x.id, x.dobnew, x.age, 
	
		case
			when x.age >= 0 and x.age < 5 then '0-4'
			when x.age >= 5 and x.age < 12 then '5-11'
			when x.age >= 12 and x.age < 18 then '12-17'
			when x.age >= 18 and x.age < 25 then '18-24'
			when x.age >= 25 and x.age < 45 then '25-44'
			when x.age >= 45 and x.age < 65 then '45-64'
			when x.age >= 65 then '65 and over'
		end as 'age_grp7',
	
		x.gender_mx, x.male, x.female, x.male_t, x.female_t, x.gender_unk, x.race_eth_mx, x.race_mx, x.aian, x.asian,
		x.black, x.nhpi, x.white, x.latino, x.aian_t, x.asian_t, x.black_t, x.nhpi_t, x.white_t,
		x.latino_t, x.race_unk, x.maxlang, x.english, x.spanish, x.vietnamese, x.chinese, x.somali, x.russian,
		x.arabic, x.korean, x.ukrainian, x.amharic, x. english_t, x.spanish_t, x.vietnamese_t,
		x.chinese_t, x.somali_t, x.russian_t, x.arabic_t, x.korean_t, x.ukrainian_t, x.amharic_t, x.lang_unk

	from( 	
		select distinct id, 
		--age vars
		dobnew, 		
		case
			when floor((datediff(day, dobnew, @to_date) + 1) / 365.25) >=0 then floor((datediff(day, dobnew, @to_date) + 1) / 365.25)
			when floor((datediff(day, dobnew, @to_date) + 1) / 365.25) = -1 then 0
		end as 'age',
		--gender vars
		gender_mx, male, female, male_t, female_t, gender_unk,
		--race vars
		race_eth_mx, race_mx, aian, asian, black, nhpi, white, latino, aian_t, asian_t, black_t, nhpi_t, white_t, latino_t, race_unk,
		--language vars
		maxlang, english, spanish, vietnamese, chinese, somali, russian, arabic, korean, ukrainian, amharic,
		english_t, spanish_t, vietnamese_t, chinese_t, somali_t, russian_t, arabic_t, korean_t, ukrainian_t,
		amharic_t, lang_unk
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
