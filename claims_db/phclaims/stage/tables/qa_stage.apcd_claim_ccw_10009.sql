--Line-level QA of stage.apcd_claim_ccw table
--Updated for Extract 10009 and to align with 2022-02 CCW revisions
--Eli Kern
--2019-11
--Notes:
--11/1/2023 Susan updated script with the correct reference table

---Use these reference tables to QA the ccw_table
-----CCW Reference table [PHClaims].[ref].[ccw_lookup]
-----Claim Type Ref [PHClaims].[ref].[kc_claim_type_crosswalk]
-----ICDCM codes needed for each CCW condition ref.icdcm_codes

--condition_types refers to the claim_type_ columns
--claim type 1 (the type of claim requirement needed to fulfill the ccw definition for the condition), only  needs 1 claim with a claim type listed in the claim_type_1 column
--claim type 2 (two claims types on two separate days are needed to fulfill the definition)


----------------------
------------------

--QA goal: line-level QA of 1 condition for each of the following phenotypes
  --look back period (years)= 1 ; condition_type = 1 (look at reference table): ccw_mi
  --years = 1 : condition_type = 2 : ccw_pneumonia
  --years = 2 : condition_type = 1 : ccw_cataract
  --years = 2 : condition_type = 2 : ccw_diabetes
  --years = 3 : ccw_alzheimer_related
------------------
----------------------

--------------------
--Generic code to find people with more than row per condition
--------------------
select top 1 a.*
from (
select id_apcd, count(from_date) as time_cnt
from PHClaims.stage.apcd_claim_ccw
where ccw_code = 2-- change this to select different condition
group by id_apcd
) as a
where a.time_cnt > 1;

--------------------
--ccw_mi
--------------------
declare @id bigint, @ccw_code tinyint;
set @id = 11050751380;
set @ccw_code = 2;

select *
from PHClaims.stage.apcd_claim_ccw
where ccw_code = @ccw_code and id_apcd = @id
order by from_date;

select a.id_apcd, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_mi
from PHClaims.final.apcd_claim_header as a
left join PHClaims.final.apcd_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join ref.icdcm_codes  as c
on b.icdcm_norm = c.icdcm and b.icdcm_version = c.icdcm_version
where a.claim_type_id in (1) -- !! update this as needed for each condition !! --
and a.id_apcd = @id
and c.ccw_mi = 1
order by a.first_service_date;


--------------------
--ccw_pneumonia
--------------------
declare @id bigint, @ccw_code tinyint;
set @id = 11050749553;
set @ccw_code = 31;

select *
from PHClaims.stage.apcd_claim_ccw
where ccw_code = @ccw_code and id_apcd = @id
order by from_date;

select a.id_apcd, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_mi
from PHClaims.final.apcd_claim_header as a
left join PHClaims.final.apcd_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join ref.icdcm_codes   as c
on b.icdcm_norm = c.icdcm and b.icdcm_version = c.icdcm_version
where a.claim_type_id in (1,2,3,4,5) -- !! update this as needed for each condition !! --
and a.id_apcd = @id
and c.ccw_pneumonia = 1
order by a.first_service_date;


--------------------
--ccw_cataract
--------------------
declare @id bigint, @ccw_code tinyint;
set @id = 11050747079;
set @ccw_code = 9;

select *
from PHClaims.stage.apcd_claim_ccw
where ccw_code = @ccw_code and id_apcd = @id
order by from_date;

select a.id_apcd, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_cataract
from PHClaims.final.apcd_claim_header as a
left join PHClaims.final.apcd_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join ref.icdcm_codes as c
on b.icdcm_norm = c.icdcm   and b.icdcm_version = c.icdcm_version
where a.claim_type_id in (4,5) -- !! update this as needed for each condition !! --
and a.id_apcd = @id
and c.ccw_cataract = 1
order by a.first_service_date;


--------------------
--ccw_diabetes
--------------------
declare @id bigint, @ccw_code tinyint;
set @id = 11057459856;
set @ccw_code = 14;

select *
from PHClaims.stage.apcd_claim_ccw
where ccw_code = @ccw_code and id_apcd = @id
order by from_date;

select a.id_apcd, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_diabetes
from PHClaims.final.apcd_claim_header as a
left join PHClaims.final.apcd_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join ref.icdcm_codes as c
on b.icdcm_norm = c.icdcm   and b.icdcm_version = c.icdcm_version
where a.claim_type_id in (1,2,3,4,5) -- !! update this as needed for each condition !! --
and a.id_apcd = @id
and c.ccw_diabetes = 1
order by a.first_service_date;


--------------------
--ccw_alzheimer_related
--------------------
declare @id bigint, @ccw_code tinyint;
set @id = 11050747748;
set @ccw_code = 4;

select *
from PHClaims.stage.apcd_claim_ccw
where ccw_code = @ccw_code and id_apcd = @id
order by from_date;

select a.id_apcd, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_alzheimer_related
from PHClaims.final.apcd_claim_header as a
left join PHClaims.final.apcd_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join ref.icdcm_codes as c
on b.icdcm_norm = c.icdcm   and b.icdcm_version = c.icdcm_version
where a.claim_type_id in (1,2,3,4,5) -- !! update this as needed for each condition !! --
and a.id_apcd = @id
and c.ccw_alzheimer_related = 1
order by a.first_service_date;