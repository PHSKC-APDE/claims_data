#### Code to create a list of members with zero coverage time in WA state OR claims data but no enrollment data
# Eli Kern, PHSKC (APDE)
#
# 2026-09-04

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_ref_member_exclude_f <- function() {
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
    --flag out of state member months
    with temp1 as (
      select a.id_apcd, a.year_month, case when b.state = 'WA' then 0 else 1 end as out_of_state
      from (
        select internal_member_id as id_apcd, year_month, zip_code
        from stg_claims.apcd_member_month_detail
      ) as a
      left join stg_claims.ref_apcd_zip as b
      on a.zip_code = b.zip_code
    ),
    --flag members that have no in-state WA member months
    temp2 as (
      select id_apcd, min(out_of_state) as out_of_state_all
      from temp1
      group by id_apcd
    ),
    --subset to out-of-state members
    nonwa as (
      select distinct id_apcd,
      1 as non_wa_resident
      from temp2
      where out_of_state_all = 1;
    ),
    --select distinct member IDs from member_month_detail table
    temp3 as (
      select distinct internal_member_id
      from stg_claims.apcd_member_month_detail
    ),
    --select distinct member IDs in medical, pharmacy, and dental claim tables
    temp4 as (
      select distinct internal_member_id
      from stg_claims.apcd_medical_claim
      union
      select distinct internal_member_id
      from stg_claims.apcd_pharmacy_claim
      union
      select distinct internal_member_id
      from stg_claims.apcd_dental_claim
    ),
    --select IDs in claim table but not in member_month table
    no_elig_data as (
      select internal_member_id as id_apcd,
      1 as no_elig_data
      from temp4
      except
      select internal_member_id as id_apcd from temp3
    )
    --join 2 ref tables and insert into table shell
    select
    coalesce(a.id_apcd, b.id_apcd) as id_apcd,
    case when a.non_wa_resident is null then 0 else a.non_wa_resident end as non_wa_resident,
    case when b.no_elig_data is null then 0 else b.no_elig_data end as no_elig_data,
    getdate() as last_run
    from nonwa as a
    full join no_elig_data as b
    on a.id_apcd = b.id_apcd;",
    .con = dw_inthealth))
}