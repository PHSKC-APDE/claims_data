--Eli Kern
--Assessment, Policy Development & Evaluation, Public Health - Seattle & King County
--5/10/18
--Pull out asthma ever persons with sum of asthma-related inpatient stays and asthma-related ED visits between defined time period
--Pull this into R for demographic and coverage group join to further analyze
--Eventually I should compare this analysis with only using the primary diagnosis field for the inpatient/ED select
--This script creates a stored procedure for use within R (only difference is that this does not create a temp table)

--select database
use PHClaims
go

--drop stored procedure before creating new
drop procedure dbo.sp_mcaid_asthma_r
go

--create stored procedure
create proc dbo.sp_mcaid_asthma_r
	(
	@from_date as date,
	@to_date as date
	)
as
begin

select ast_person.id, ast_claims.ast_ipt_cnt, ast_claims.ast_ed_cnt

--select distinct IDs for people with asthma ever
from (
	select distinct id from PHClaims.dbo.mcaid_claim_asthma_person
) as ast_person

--join to IDs with asthma-related inpatient or ed visit sum information (any diagnosis field)
left join (

	select header.id, 

		sum(inpatient) as 'ast_ipt_cnt', sum(ed) as 'ast_ed_cnt'

	--pull out claim type and service dates
	from (
		select id, tcn, inpatient, ed, from_date, to_date
		from PHClaims.dbo.mcaid_claim_summary
	) header

	--inner join to claims containing a diagnosis in the CCW asthma definition
	inner join (
		select diag.id, diag.tcn, ref.asthma_ccw

		--pull out claim and diagnosis fields
		from (
			select id, tcn, dx_norm, dx_ver
			from PHClaims.dbo.mcaid_claim_diag
		) diag

		--join to diagnosis reference table, subset to those with asthma CCW
		inner join (
		select icdcode, ver, asthma_ccw
		from PHClaims.dbo.ref_diag_lookup
		where asthma_ccw = 1
		) ref

		on (diag.dx_norm = ref.icdcode) and (diag.dx_ver = ref.ver)
	) as diag

	on header.tcn = diag.tcn
	where header.from_date between @from_date and @to_date
	group by header.id

) as ast_claims

on ast_person.id = ast_claims.id

end