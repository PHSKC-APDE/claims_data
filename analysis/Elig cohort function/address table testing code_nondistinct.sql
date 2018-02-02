--Testing code for address table

declare @begin date, @end date
set @begin = '2017-01-01'
set @end = '2017-06-30'

select x.id, x.from_add, x.to_add, zipdur.zip_new, zregdur.kcreg_zip, id.id_cnt, zip.zip_cnt, reg.reg_cnt, rankt.id_rank, homeless.homeless_e

--client level table
from (
	select id, from_add, to_add, zip_new, kcreg_zip, homeless
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
) as x

--count rows per client
left join (
	select id, count(id) as id_cnt
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
	group by id
) as id
on x.id = id.id

--count distinct ZIPs per client
left join (
	select id, count(distinct zip_new) as zip_cnt
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
	group by id
) as zip
on x.id = zip.id

--count distinct regions per client
left join (
	select id, count(distinct kcreg_zip) as reg_cnt
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
	group by id
) as reg
on x.id = reg.id

--rank rows per client ID for keeping only one
left join (
	select id, from_add, kcreg_zip, rank() over (partition by id order by from_add) as 'id_rank'
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
) as rankt
on x.id = rankt.id and x.from_add = rankt.from_add

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

order by id.id_cnt desc, x.id, x.from_add