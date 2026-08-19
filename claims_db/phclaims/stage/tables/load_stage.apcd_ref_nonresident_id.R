#### Code to create a list of members with zero coverage time in WA state
# Eli Kern, PHSKC (APDE)
#
# 2026-08-19

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_ref_nonresident_id_f <- function() {
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
    )
    --write member IDs for out-of-state members to persistent table
    insert into stg_claims.apcd_ref_nonresident_id
    select distinct id_apcd, getdate() as last_run
    from temp2
    where out_of_state_all = 1;",
    .con = dw_inthealth))
}