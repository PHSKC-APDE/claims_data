--Testing code for address table

declare @begin date, @end date
set @begin = '2017-01-01'
set @end = '2017-06-30'

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

--duration in each ZIP-based region
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