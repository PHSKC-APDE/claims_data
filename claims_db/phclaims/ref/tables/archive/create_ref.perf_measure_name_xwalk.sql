
USE [PHClaims]
GO

IF OBJECT_ID('[ref].[perf_measure_name_xwalk]') IS NOT NULL
DROP TABLE [ref].[perf_measure_name_xwalk];
CREATE TABLE [ref].[perf_measure_name_xwalk]
([measure_name] VARCHAR(200) NOT NULL
,[age_group_desc] VARCHAR(200) NOT NULL
,[hca_measure_name] VARCHAR(200) NOT NULL
,CONSTRAINT [PK_perf_measure_name_xwalk] PRIMARY KEY CLUSTERED ([measure_name], [age_group_desc], [hca_measure_name])
);

INSERT INTO [ref].[perf_measure_name_xwalk]([measure_name], [age_group_desc], [hca_measure_name])
VALUES
 ('Acute Hospital Utilization', 'Age 18+', 'MED_AHU_ROLLING')
,('Acute Hospital Utilization', 'Overall', 'MED_AHU_ROLLING')
,('All-Cause ED Visits', 'Age 0-17', 'MED_EDU_BROAD_0_17_ROLLING')
,('All-Cause ED Visits', 'Age 18-64', 'MED_EDU_BROAD_18to64_ROLLING')
,('All-Cause ED Visits', 'Age 65+', 'MED_EDU_BROAD_65Yplus_ROLLIN')
,('All-Cause ED Visits', 'Age 65+', 'MED_EDU_BROAD_65Yplus_ROLLING')
,('All-Cause ED Visits', 'Overall', 'MED_EDU_BROAD_ROLLING')
,('Child and Adolescent Access to Primary Care', 'Age 12-19', 'MED_CAP12to19Y_ROLLING')
,('Child and Adolescent Access to Primary Care', 'Age 12-24 Months', 'MED_CAP12to24MO_ROLLING')
,('Child and Adolescent Access to Primary Care', 'Age 25 Months-6', 'MED_CAP25MOto6Y_ROLLING')
,('Child and Adolescent Access to Primary Care', 'Age 7-11', 'MED_CAP7to11Y_ROLLING')
,('Child and Adolescent Access to Primary Care', 'Overall', 'MED_CAP_ROLLING')
,('Follow-up ED visit for Alcohol/Drug Abuse: 30 days', 'Age 13+', 'MED_AOD30D_ROLLING')
,('Follow-up ED visit for Alcohol/Drug Abuse: 30 days', 'Overall', 'MED_AOD30D_ROLLING')
,('Follow-up ED visit for Alcohol/Drug Abuse: 7 days', 'Age 13+', 'MED_AOD7D_ROLLING')
,('Follow-up ED visit for Alcohol/Drug Abuse: 7 days', 'Overall', 'MED_AOD7D_ROLLING')
,('Follow-up ED visit for Mental Illness: 30 days', 'Age 6+', 'MED_FUM30D_ROLLING')
,('Follow-up ED visit for Mental Illness: 30 days', 'Overall', 'MED_FUM30D_ROLLING')
,('Follow-up ED visit for Mental Illness: 7 days', 'Age 6+', 'MED_FUM7D_ROLLING')
,('Follow-up ED visit for Mental Illness: 7 days', 'Overall', 'MED_FUM7D_ROLLING')
,('Follow-up Hospitalization for Mental Illness: 30 days', 'Age 6+', 'MED_FUH30D_ROLLING')
,('Follow-up Hospitalization for Mental Illness: 30 days', 'Overall', 'MED_FUH30D_ROLLING')
,('Follow-up Hospitalization for Mental Illness: 7 days', 'Age 6+', 'MED_FUH7D_ROLLING')
,('Follow-up Hospitalization for Mental Illness: 7 days', 'Overall', 'MED_FUH7D_ROLLING')
,('Mental Health Treatment Penetration', 'Age 18-64', 'MED_MHTP18to64Y_ROLLING')
,('Mental Health Treatment Penetration', 'Age 6-17', 'MED_MHTP6to17Y_ROLLING')
,('Mental Health Treatment Penetration', 'Age 65+', 'MED_MHTP65Yplus_ROLLING')
,('Mental Health Treatment Penetration', 'Overall', 'MED_MHTP_ROLLING')
,('Plan All-Cause Readmissions (30 days)', 'Age 18-64', 'MED_PCR_ROLLING')
,('Plan All-Cause Readmissions (30 days)', 'Overall', 'MED_PCR_ROLLING')
,('SUD Treatment Penetration', 'Age 12-17', 'MED_SUD12to17Y_ROLLING')
,('SUD Treatment Penetration', 'Age 18-64', 'MED_SUD18to64Y_ROLLING')
,('SUD Treatment Penetration', 'Age 65+', 'MED_SUD65YPLUS_ROLLING')
,('SUD Treatment Penetration', 'Overall', 'MED_SUD_ROLLING')
,('SUD Treatment Penetration (Opioid)', 'Age 18-64', 'MED_SUD_OP18to64Y_ROLLING')
,('SUD Treatment Penetration (Opioid)', 'Age 65+', 'MED_SUD_OP65YPLUS_ROLLING')
,('SUD Treatment Penetration (Opioid)', 'Overall', 'MED_SUD_OP_ROLLING');
GO