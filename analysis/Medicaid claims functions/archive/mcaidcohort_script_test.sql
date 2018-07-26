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
set @to_date = '2017-12-31'
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

				select x.id, x.zip_new, x.zip_dur, row_number() over (partition by x.id order by x.zip_new, x.zip_dur desc) as 'zipr'
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
							and id = '100035146WA'
					) as a
					group by a.id, a.zip_new
				) as x




















--3rd table - sub-county areas
	select distinct x.id, zipdur.zip_new, zipdur.kcreg_zip, homeless.homeless_e

	--client level table
	from (
		select distinct id, zip_new, kcreg_zip
		from PHClaims.dbo.mcaid_elig_address
		where @from_date < @to_date AND @to_date > @from_date
			and id = '200551014WA'
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
		select zip.id, zip.zip_new, reg.kcreg_zip
		from (
			select y.id, y.zip_new
			from (
				select x.id, x.zip_new, x.zip_dur, row_number() over (partition by x.id order by x.zip_dur desc) as 'zipr'
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
