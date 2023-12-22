--Line-level QA of stage.apcd_claim_header table
--Modified for extract 10001
--2022-03
--Eli Kern

--3/2022 updates:
--Modified so that script can be run in batch and QA results will be returned
--Modified by Susan Hernandez 3/16/22 to identify corner cases


---Check header dates 
    if object_id('tempdb..#claim_header1') is not null drop table #claim_header1;
    select count(*) count_header 
     from [PHClaims].[final].[apcd_claim_header]
	where  discharge_date <='2000-01-01';
	--expect 0

select min(discharge_date) min_date, max(discharge_date)max_date from #claim_header1;





---------------------------------------------------------
--QA 1: Inpatient stays
--1.1: Multiple inpatient discharge dates, expect max
--1.2: Verify inpatient definition logic
--~8 min run time
---------------------------------------------------------
SELECT * FROM [PHClaims].[ref].[kc_claim_type_crosswalk] where source_desc = 'apcd';


--Select relevant claim header for QA and generate expected values
if object_id('tempdb..#qa1_prep') is not null drop table #qa1_prep;
select top 1 a.claim_header_id, a.qa_expect
into #qa1_prep
from (
	select claim_header_id, count(distinct discharge_date) as discharge_date_dcount, max(discharge_date) as qa_expect
	from PHClaims.final.apcd_claim_line
	group by claim_header_id
) as a
left join (select claim_header_id, inpatient_id from PHClaims.stage.apcd_claim_header where inpatient_id is not null) as b
on a.claim_header_id = b.claim_header_id
where a.discharge_date_dcount > 1 and b.inpatient_id is not null;

--Compare selected discharge date to expected result
if object_id('tempdb..#qa1_result') is not null drop table #qa1_result;
select
	'1' as qa_item,
	'1' as qa_sub_item,
	'Maximum discharge date selected for inpatient stays with >1 discharge dates' as qa_description,
	case when a.discharge_date = (select discharge_date from #qa1_prep) then 'pass' else 'fail' end as qa_result
into #qa1_result
from (
	select id_apcd, claim_header_id, inpatient_id, discharge_date
	from PHClaims.stage.apcd_claim_header
	where claim_header_id = (select claim_header_id from #qa1_prep)
) as a

union

--Compare claim characterstics to expected values
select
	'1' as qa_item,
	'2' as qa_sub_item,
	'Verify inpatient stay logic' as qa_description,
	case when claim_type_id = 1 and type_of_setting_id = 1 and place_of_setting_id = 1
		and	header_status in ('-1', '-2', '01', '19', '02', '20') -- note these are text as opposed to numeric values
		then 'pass' else 'fail'
		end as qa_result
from PHClaims.stage.apcd_medical_claim_header
where medical_claim_header_id = (select claim_header_id from #qa1_prep);

--select * from #qa1_result;


--CONTINUE BUILDING QA CODE BY MOVING TO NEXT QA ITEM BELOW, ADAPTING ABOVE APPROACH
--UNION ALL QA RESULT TABLES AT END


--More than 1 claim header per discharge date, expect same inpatient_id value: PASS
--union 
--Compare selected discharge date to expected result
--Corner case of a person with a discharge that has more than one header ;
if object_id('tempdb..#qa2_prep') is not null drop table #qa2_prep;
select top 1 a.id_apcd, a.discharge_date, a.qa_expect
into #qa2_prep
from (
	select id_apcd, discharge_date, count(distinct claim_header_id) as qa_expect
	from PHClaims.stage.apcd_claim_header
	where discharge_date is not null and inpatient_id is not null
	group by id_apcd, discharge_date
) as a
where a.qa_expect > 1;

--select * from #qa2_prep;
--id_apcd	discharge_date	qa_expect
--11521888709	2021-07-19	3

--Compare selected discharge date to expected result
if object_id('tempdb..#qa2_result') is not null drop table #qa2_result;
--QA will pass when id_apcd has one value, claim_header_id had more than one value, discharge_date has one value, and inpatient_id has one value
select
	id_apcd,
	claim_header_id,
	discharge_date,
	inpatient_id
into #qa2_result
from (
	select id_apcd, claim_header_id, discharge_date, inpatient_id
	from PHClaims.stage.apcd_claim_header
	where discharge_date is not null AND inpatient_id IS NOT NULL
	AND id_apcd = (select id_apcd from #qa2_prep)
) as a;

select * from #qa2_result order by inpatient_id, claim_header_id;


select id_apcd, claim_header_id, inpatient_id, discharge_date
from PHClaims.stage.apcd_claim_header
where claim_header_id in (23421700001989, 221718000458448, 221815000362571)
order by first_service_date;
---------------------------------
-----------------------------------
--More than 1 PC visit on a day, expect same pc_visit_id value: PASS

if object_id('tempdb..#pc_prep') is not null drop table #pc_prep;
--6 minutes
select top 1 a.id_apcd, a.first_service_date, a.pc_expect
into #pc_prep
from (
	select id_apcd, first_service_date, count(distinct claim_header_id) as pc_expect
	from PHClaims.stage.apcd_claim_header
	where discharge_date is null and first_service_date is not null AND  pc_visit_id is not null 
	group by id_apcd, first_service_date
) as a
where a.pc_expect > 2;

select * from #pc_prep;
--id_apcd	first_service_date	pc_expect
---11277884419	2016-07-11		3
--11057903231	2014-01-02	3


--Verify pc visit logic: PASS
--Compare selected first service date to expected result

if object_id('tempdb..#pc_result') is not null drop table #pc_result;
--QA will pass when id_apcd has one value, claim_header_id had more than one value, discharge_date has one value, and inpatient_id has one value
select 
	a.id_apcd,
	a.claim_header_id,
	a.first_service_date,
	a.pc_visit_id
into #pc_result
from (
	select id_apcd, claim_header_id, first_service_date, pc_visit_id
	from PHClaims.stage.apcd_claim_header
	where first_service_date = (select first_service_date from #pc_prep) AND id_apcd = (select id_apcd from #pc_prep) AND pc_visit_id is not null 
) as a


select *from #pc_result;

--should return a procedure code OR ICDCM 10 code: PASS
	select a.procedure_code
	from PHClaims.final.apcd_claim_procedure as a
	inner join (select * from PHClaims.ref.pc_visit_oregon where code_system in ('cpt', 'hcpcs')) as b
	on a.procedure_code = b.code
	inner join (select claim_header_id from #pc_result)as c
	on a.claim_header_id=c.claim_header_id;

	select a.icdcm_norm
	from PHClaims.final.apcd_claim_icdcm_header as a
	inner join (select * from PHClaims.ref.pc_visit_oregon where code_system = 'icd10cm') as b
	on a.icdcm_norm = b.code
		inner join (select claim_header_id from #pc_result)as c
	on a.claim_header_id=c.claim_header_id;

	--should return at least one taxonomy code: PASS
	select c.primary_taxonomy, c.secondary_taxonomy
	from (
		select e.claim_header_id, provider_id_apcd,  provider_type from PHClaims.final.apcd_claim_provider as e
		inner join (select claim_header_id from #pc_result)as f
		on e.claim_header_id=f.claim_header_id
		where provider_type in ('rendering', 'attending')
		) as a
	inner join PHClaims.ref.apcd_provider_npi as b
	on a.provider_id_apcd = b.provider_id_apcd
	inner join PHClaims.ref.kc_provider_master as c
	on b.npi = c.npi
	inner join (select * from PHClaims.ref.pc_visit_oregon where code_system = 'provider_taxonomy') as d
	on (c.primary_taxonomy = d.code) or (c.secondary_taxonomy = d.code);


----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
--ed_pophealth_id, ask about lines 289 and 326
----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
--More than 1 ED visit within Yale match window, expect same ed_pophealth_id value: PASS
--1 day (yesterday, today, tomorrow)
---group by ed_pohealth_id and look for more than one claim header on different dates, but within the match window
--max, min first_service_date is 1

--counting distinct dates by ed_pophealth_id
if object_id('tempdb..#qa_ed') is not null drop table #qa_ed;
select 
ed_pophealth_id, 
count (distinct first_service_date) as distinct_date
into #qa_ed
from PHClaims.stage.apcd_claim_header
group by ed_pophealth_id;
--(16007007 rows affected)

--subsetting result to ED visits with more than 1 distinct first_service_date
if object_id('tempdb..#qa_ed2') is not null drop table #qa_ed2;
select *
into #qa_ed2
from #qa_ed
where distinct_date > 2 and ed_pophealth_id IS NOT NULL;
--(48202 rows affected)

--reviewing claim_header table for selcted QA case to confirm that ed_pophealth_id should indeed be distinct
-- consecutive ED visits, rolling 72 hours have the same id
select id_apcd, claim_header_id, claim_type_id, first_service_date, ed_pophealth_id, ed_perform_id
from PHClaims.stage.apcd_claim_header
where ed_pophealth_id = (select top 1 ed_pophealth_id from #qa_ed2)
order by first_service_date;
--id_apcd 11540379294	

--Verify ed_perform_id logic for one claim (using RDA measure/P4P measure)
--Pass
select distinct (a.claim_header_id), a. ed_perform_id, a.claim_type_id, b.revenue_code, b.place_of_service_code,
	c.procedure_code
from PHClaims.stage.apcd_claim_header as a
left join PHClaims.final.apcd_claim_line as b
on a.claim_header_id = b.claim_header_id
left join PHClaims.final.apcd_claim_procedure as c
on a.claim_header_id = c.claim_header_id
where a. ed_perform_id is not null
	and a.claim_type_id = 4
	and (b.revenue_code like '045[01269]' or c.procedure_code like '9928[123458]' or b.place_of_service_code = '23');

--Verify ed_pophealth_id logic for one visit: PASS
--can be claim type 5 AND (revenue code OR (POS & procedure code))
--can be claim type (4 or 1) AND (revenue code OR POS OR procedure code)

--~15 min
select distinct a.claim_header_id, a.ed_pophealth_id, a.claim_type_id, b.revenue_code, b.place_of_service_code,
	c.procedure_code
from PHClaims.stage.apcd_claim_header as a
left join PHClaims.final.apcd_claim_line as b
on a.claim_header_id = b.claim_header_id
left join PHClaims.final.apcd_claim_procedure as c
on a.claim_header_id = c.claim_header_id
where a.ed_pophealth_id IS NOT NULL
	and a.claim_type_id in (1,4,5)
	and (b.revenue_code like '045[01269]' or b.revenue_code = '0981' or c.procedure_code like '9928[12345]' or c.procedure_code = '99291' or b.place_of_service_code = '23');

--Verify ed_pophealth_id assignment: 1 carrier claim (professional claim ) and 1 opt claim the next day, expect same ID: PASS
--claim header IDs  first service dates 2016-04-04 and 2016-04-05
--id_apcd still works 3/28/22
select id_apcd, claim_header_id, first_service_date, claim_type_id, ed_pophealth_id, ed_perform_id
from PHClaims.stage.apcd_claim_header
where id_apcd = 11268493312
order by first_service_date;

--This code also works
select id_apcd, claim_header_id, first_service_date, claim_type_id, ed_pophealth_id, ed_perform_id
from PHClaims.stage.apcd_claim_header
where  claim_type_id  in (4,5) AND ed_pophealth_id = (select top 1 ed_pophealth_id from #qa_ed2)
order by first_service_date;

--Verify ed_pophealth_id assignment: 1 ipt and opt claim on same day, no carrier duplicate (professional), expect same ID: PASS
--subsetting result to ED visits with more than 1 distinct first_service_date
if object_id('tempdb..#qa_ed3a') is not null drop table #qa_ed3a;
select *
into #qa_ed3a
from #qa_ed
where distinct_date =1 and ed_pophealth_id IS NOT NULL;

--~30 min
if object_id('tempdb..#qa_ed3b') is not null drop table #qa_ed3b;
select top 1 x.*
into #qa_ed3b
from (
select b.ed_pophealth_id,
max(case when b.claim_type_id = 1 then 1 else 0 end) as claim_type_ipt_flag,
max(case when b.claim_type_id = 5 then 1 else 0 end) as claim_type_pro_opt_flag
from #qa_ed3a as a
inner join phclaims.stage.apcd_claim_header as b
on a.ed_pophealth_id = b.ed_pophealth_id
group by b.ed_pophealth_id
) as x
where x.claim_type_ipt_flag = 1 and x.claim_type_pro_opt_flag = 1;

select * from #qa_ed3b; 
5543743
--Use ed_pophealth_id from this query to check it meets QA requirements
select id_apcd, claim_header_id, first_service_date, claim_type_id, ed_pophealth_id, ed_perform_id
from phclaims.stage.apcd_claim_header
where ed_pophealth_id=2920101;

select id_apcd, claim_header_id, first_service_date, claim_type_id, ed_pophealth_id, ed_perform_id
from phclaims.stage.apcd_claim_header
where ed_pophealth_id=7810934;
-----------------------------------
-----------------------------------
--########################################################################################
--## create flexible QA table from which to pull multiple QA examples ##
--Claim type is professional (1, 28) AND (((procedure code 99281-99285 OR 99291) AND (place of service code 23)) OR (revenue code 0450-0459 OR 0981)).
--Claim type is hospital outpatient (3, 26, 34) AND ((revenue code 0450-0459 OR 0981) OR (procedure code 99281-99285 OR 99291) OR (place of service code 23)).
--Claim type is inpatient (31, 33) AND ((revenue code 0450-0459 OR 0981) OR (procedure code 99281-99285 OR 99291) OR (place of service code 23)).
--Yale definition allows for repeat ED visitation within 72 hours, which has been shown to be common.
--Bottom line: All ED visit claims within 72-hour window considered to be the same ED visi
--In Medicaid claims data analysis for 1/1/17 – 12/31/17 and King County residents,  inpatient claims that included an ED visit made up 6.2% of the total ED visit count.
--########################################################################################

--run time: ~6 min
drop table #temp1, #temp2, #temp3;
select ed_pophealth_id, id_apcd,
       count(distinct claim_type_id) as claim_type_dcount,
       count(distinct first_service_date) as service_date_dcount,
       max(case when claim_type_id = 1 then 1 else 0 end) as claim_type_ipt_flag,
       max(case when claim_type_id = 4 then 1 else 0 end) as claim_type_opt_flag,
       max(case when claim_type_id = 5 then 1 else 0 end) as claim_type_pro_flag
into #temp1
from PHClaims.stage.apcd_claim_header
where ed_pophealth_id is not null
group by ed_pophealth_id, id_apcd;

select top 1000 * from #temp1;

--## verify for Yale ED logic for visit with only outpatient claim type ##

--select 1 example for QA: PASS
select top 1 *
into #temp2
from #temp1
where claim_type_ipt_flag = 0 and claim_type_opt_flag = 1 and claim_type_pro_flag = 0;
select * from #temp2;

--review all ED visits for selected person to verify that selected outpatient ED visit does not have another
       --ED claim type within 72-hr match window
	   --PASS
select id_apcd, claim_header_id, first_service_date, claim_type_id, ed_pophealth_id, ed_perform_id,
case when ed_pophealth_id = (select top 1 ed_pophealth_id from #temp2) then 1 else 0 end as ed_visit_for_qa
from PHClaims.stage.apcd_claim_header
where ed_pophealth_id is not null and id_apcd = (select top 1 id_apcd from #temp2)
order by first_service_date;

--## verify for Yale ED logic for visit with only inpatient claim type ## PASS
--select 1 example for QA 
select top 1 *
into #temp3
from #temp1
where claim_type_ipt_flag = 1 and claim_type_opt_flag = 0 and claim_type_pro_flag = 0;

select * from #temp3;

--review all ED visits for selected person to verify that selected inpatient ED visit does not have another
       --ED claim type within 72-hr match window
	   --PASS
select id_apcd, claim_header_id, first_service_date, claim_type_id, ed_pophealth_id, ed_perform_id,
case when ed_pophealth_id = (select top 1 ed_pophealth_id from #temp3) then 1 else 0 end as ed_visit_for_qa
from PHClaims.stage.apcd_claim_header
where ed_pophealth_id is not null and id_apcd = (select top 1 id_apcd from #temp3)
order by first_service_date;


-----------------------------------
-----------------------------------

--Verify that ED yale logic worked for 1st and last ED visit in ordered table: PASS
--update max id below
select max(ed_pophealth_id)
from PHClaims.stage.apcd_claim_header;

select id_apcd, claim_header_id, first_service_date, claim_type_id, ed_pophealth_id, ed_perform_id
from PHClaims.stage.apcd_claim_header
where ed_pophealth_id = 1 or ed_pophealth_id = 16007006;

--get the ids from above query
select id_apcd, claim_header_id, first_service_date, claim_type_id, ed_pophealth_id, ed_perform_id
from PHClaims.stage.apcd_claim_header
where id_apcd = 11050747025
order by first_service_date;

select id_apcd, claim_header_id, first_service_date, claim_type_id, ed_pophealth_id, ed_perform_id
from PHClaims.stage.apcd_claim_header
where id_apcd = 25308200000971101
order by first_service_date;

--sort order for QAing ED pop health ID assignment in creation of #ed_yale_final table:
--select * from #ed_yale_final
--order by id_apcd, first_service_date, ed_yale_dup, ed_type;

