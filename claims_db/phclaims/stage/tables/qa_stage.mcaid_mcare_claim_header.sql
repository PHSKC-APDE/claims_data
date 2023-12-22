--code to find duals with multiple visits/events on same date
select top 100 y.*
from (
	select x.id_apde, x.first_service_date, count(*) as row_count
	from (
	--mcaid
	select b.id_apde, a.first_service_date, 'mcaid' as source_desc
	from PHClaims.final.mcaid_claim_header as a
	left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
	on a.id_mcaid = b.id_mcaid
	where pc_visit_id is not null
	union
	--mcare
	select b.id_apde, a.first_service_date, 'mcare' as source_desc
	from PHClaims.final.mcare_claim_header as a
	left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
	on a.id_mcare = b.id_mcare
	where pc_visit_id is not null
	) as x
	group by x.id_apde, x.first_service_date
) as y
where y.row_count > 1;

--ed_perform_id test
--id_apde: 1341229
--first_service_date: 2017-06-04
select id_apde, source_desc, first_service_date, ed_perform_id, ed_pophealth_id, inpatient_id, pc_visit_id
from PHClaims.stage.mcaid_mcare_claim_header
where id_apde = 1341229 and first_service_date = '2017-06-04';

--ed_pophealth_id test
--id_apde: 1736911
--first_service_date: 2016-05-15
select id_apde, source_desc, first_service_date, ed_perform_id, ed_pophealth_id, inpatient_id, pc_visit_id
from PHClaims.stage.mcaid_mcare_claim_header
where id_apde = 1736911 and first_service_date = '2016-05-15';

--inpatient_id test
--id_apde: 883188
--first_service_date: 2014-01-21
select id_apde, source_desc, first_service_date, ed_perform_id, ed_pophealth_id, inpatient_id, pc_visit_id
from PHClaims.stage.mcaid_mcare_claim_header
where id_apde = 883188 and first_service_date = '2014-01-21';

--pc_visit_id test
--id_apde: 2108964
--first_service_date: 2016-12-06
select id_apde, source_desc, first_service_date, ed_perform_id, ed_pophealth_id, inpatient_id, pc_visit_id
from PHClaims.stage.mcaid_mcare_claim_header
where id_apde = 2108964 and first_service_date = '2016-12-06';