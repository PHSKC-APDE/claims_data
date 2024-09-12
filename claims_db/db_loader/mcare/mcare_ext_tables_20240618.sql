''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_bh"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [from_date] DATE NULL, 
  [to_date] DATE NULL, 
  [bh_cond] VARCHAR(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_run] DATE NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_bh');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_ccw"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [from_date] DATE NULL, 
  [to_date] DATE NULL, 
  [ccw_code] TINYINT NULL, 
  [ccw_desc] VARCHAR(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_ccw');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_header"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [claim_header_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [first_service_date] DATE NULL, 
  [last_service_date] DATE NULL, 
  [primary_diagnosis] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [icdcm_version] TINYINT NULL, 
  [claim_type_mcare_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [claim_type_id] TINYINT NULL, 
  [filetype_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [facility_type_code] TINYINT NULL, 
  [service_type_code] TINYINT NULL, 
  [patient_status] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [patient_status_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [ed_perform] TINYINT NULL, 
  [ed_perform_id] BIGINT NULL, 
  [ed_pophealth] TINYINT NULL, 
  [ed_pophealth_id] BIGINT NULL, 
  [inpatient] TINYINT NULL, 
  [inpatient_id] BIGINT NULL, 
  [admission_date] DATE NULL, 
  [discharge_date] DATE NULL, 
  [ipt_admission_type] TINYINT NULL, 
  [ipt_admission_source] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [drg_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [hospice_from_date] DATE NULL, 
  [pc_visit] TINYINT NULL, 
  [pc_visit_id] BIGINT NULL, 
  [submitted_charges] NUMERIC(38,2) NULL, 
  [total_paid_mcare] NUMERIC(38,2) NULL, 
  [total_paid_insurance] NUMERIC(38,2) NULL, 
  [total_paid_bene] NUMERIC(38,2) NULL, 
  [total_cost_of_care] NUMERIC(38,2) NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_header');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_icdcm_header"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [claim_header_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [first_service_date] DATE NULL, 
  [last_service_date] DATE NULL, 
  [icdcm_raw] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [icdcm_norm] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [icdcm_version] TINYINT NULL, 
  [icdcm_number] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [filetype_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_icdcm_header');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_line"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [claim_header_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [claim_line_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [first_service_date] DATE NULL, 
  [last_service_date] DATE NULL, 
  [revenue_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [place_of_service_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [type_of_service] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [filetype_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_line');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_moud"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [last_service_date] DATE NULL, 
  [service_year] INT NULL, 
  [service_quarter] INT NULL, 
  [service_month] INT NULL, 
  [meth_proc_flag] TINYINT NULL, 
  [bup_proc_flag] TINYINT NULL, 
  [nal_proc_flag] TINYINT NULL, 
  [unspec_proc_flag] TINYINT NULL, 
  [bup_rx_flag] TINYINT NULL, 
  [nal_rx_flag] TINYINT NULL, 
  [admin_method] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [moud_flag_count] INT NULL, 
  [moud_days_supply] NUMERIC(38,1) NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_moud');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_naloxone"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [claim_header_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [description] VARCHAR(900) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [date] DATE NULL, 
  [quantity] NUMERIC(19,3) NULL, 
  [form] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [dosage_per_ml] NUMERIC(16,6) NULL, 
  [location] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_naloxone');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_pharm"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [claim_header_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [claim_line_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_service_date] DATE NULL, 
  [prscrbr_npi] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [ndc] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [facility_drug_quantity] NUMERIC(19,3) NULL, 
  [facility_drug_quantity_unit] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [cmpnd_cd] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [qty_dspnsd_num] NUMERIC(19,3) NULL, 
  [days_suply_num] SMALLINT NULL, 
  [fill_num] SMALLINT NULL, 
  [ptnt_pay_amt] NUMERIC(38,3) NULL, 
  [othr_troop_amt] NUMERIC(38,3) NULL, 
  [lics_amt] NUMERIC(38,3) NULL, 
  [plro_amt] NUMERIC(38,3) NULL, 
  [cvrd_d_plan_pd_amt] NUMERIC(38,3) NULL, 
  [ncvrd_plan_pd_amt] NUMERIC(38,3) NULL, 
  [tot_rx_cst_amt] NUMERIC(38,3) NULL, 
  [dosage_form_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [dosage_form_code_desc] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [strength] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [pharmacy_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [brand_generic_flag] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [pharmacy_type] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [filetype_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_pharm');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_pharm_char"
  ([pharmacy_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [physical_location_state_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [physical_location_open_date] DATE NULL, 
  [physical_location_close_date] DATE NULL, 
  [dispenser_class] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [primary_dispenser_type] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [primary_taxonomy_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [secondary_dispenser_type] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [secondary_taxonomy_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [tertiary_dispenser_type] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [tertiary_taxonomy_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [relationship_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [relationship_from_dt] DATE NULL, 
  [relationship_thru_dt] DATE NULL, 
  [relationship_type] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [prnt_org_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [eprscrb_srvc_ind] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [eprscrb_srvc_cd] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [dme_srvc_ind] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [dme_srvc_cd] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [walkin_clinic_ind] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [walkin_clinic_cd] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [immunizations_ind] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [immunizations_cd] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [status_340b_ind] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [status_340b_cd] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_pharm_char');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_procedure"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [claim_header_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [first_service_date] DATE NULL, 
  [last_service_date] DATE NULL, 
  [procedure_code] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [procedure_code_number] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [modifier_1] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [modifier_2] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [modifier_3] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [modifier_4] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [filetype_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_procedure');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_claim_provider"
  ([id_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [claim_header_id] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [first_service_date] DATE NULL, 
  [last_service_date] DATE NULL, 
  [provider_npi] BIGINT NULL, 
  [provider_type] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [provider_type_nch] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [provider_tin] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [provider_zip] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [provider_specialty] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [filetype_mcare] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_claim_provider');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_elig_demo"
  ([id_mcare] CHAR(15) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [dob] DATE NULL, 
  [death_dt] DATE NULL, 
  [geo_kc_ever] TINYINT NULL, 
  [gender_me] VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [gender_recent] VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [gender_female] TINYINT NULL, 
  [gender_male] TINYINT NULL, 
  [race_me] VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [race_eth_me] VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [race_recent] VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [race_eth_recent] VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [race_aian] TINYINT NULL, 
  [race_asian_pi] TINYINT NULL, 
  [race_black] TINYINT NULL, 
  [race_latino] TINYINT NULL, 
  [race_white] TINYINT NULL, 
  [race_unk] TINYINT NULL, 
  [race_eth_unk] TINYINT NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_elig_demo');

''
CREATE EXTERNAL TABLE "claims"."stage_mcare_elig_timevar"
  ([id_mcare] CHAR(15) COLLATE SQL_Latin1_General_CP1_CS_AS NULL, 
  [from_date] DATE NULL, 
  [to_date] DATE NULL, 
  [contiguous] TINYINT NULL, 
  [part_a] TINYINT NULL, 
  [part_b] TINYINT NULL, 
  [part_c] TINYINT NULL, 
  [part_d] TINYINT NULL, 
  [full_dual] TINYINT NULL, 
  [partial_dual] TINYINT NULL, 
  [state_buyin] TINYINT NULL, 
  [geo_zip] CHAR(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
  [geo_kc] TINYINT NULL, 
  [cov_time_day] SMALLINT NULL, 
  [last_run] DATETIME NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'final_mcare_elig_timevar');
