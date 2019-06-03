
USE [PHClaims];
--USE [DCHS_Analytics];
GO

IF OBJECT_ID('[ref].[perf_measure]') IS NOT NULL
DROP TABLE [ref].[perf_measure];
CREATE TABLE [ref].[perf_measure]
([measure_id] INT NOT NULL
,[measure_short_name] VARCHAR(200)
,[measure_name] VARCHAR(200)
,[age_group] VARCHAR(200)
,[age_group_desc] VARCHAR(200)
,[mcaid_enrolled] VARCHAR(200)
,[expressed_as] VARCHAR(200)
,CONSTRAINT [PK_ref_perf_measure] PRIMARY KEY CLUSTERED ([measure_short_name])
);
GO
INSERT INTO [ref].[perf_measure]([measure_id], [measure_short_name], [measure_name], [age_group], [age_group_desc], [mcaid_enrolled], [expressed_as])
VALUES
 (1, 'ED', 'All-Cause ED Visits', 'age_grp_2', 'Age 0-17, Age 18-64, Age 65+', '7+ months Medicaid enrolled in measurement period', 'Per 1,000 member months')
,(2, 'AH', 'Acute Hospital Utilization', 'age_grp_1', 'Age 18+', '11+ months Medicaid enrolled in measurement period', 'Per 1,000 members')
,(3, 'FUA_7', 'Follow-up ED visit for Alcohol/Drug Abuse: 7 days', 'age_grp_3', 'Age 13+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(4, 'FUA_30', 'Follow-up ED visit for Alcohol/Drug Abuse: 30 days', 'age_grp_3', 'Age 13+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(5, 'FUM_7', 'Follow-up ED visit for Mental Illness: 7 days', 'age_grp_4', 'Age 6+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(6, 'FUM_30', 'Follow-up ED visit for Mental Illness: 30 days', 'age_grp_4', 'Age 6+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(7, 'FUH_7', 'Follow-up Hospitalization for Mental Illness: 7 days', 'age_grp_4', 'Age 6+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(8, 'FUH_30', 'Follow-up Hospitalization for Mental Illness: 30 days', 'age_grp_4', 'Age 6+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(9, 'TPM', 'Mental Health Treatment Penetration', 'age_grp_5', 'Age 6-17, Age 18-64, Age 65+', '11+ months Medicaid enrolled in measurement period', 'Proportion of members')
,(10, 'TPS', 'SUD Treatment Penetration', 'age_grp_6', 'Age 12-17, Age 18-64, Age 65+', '11+ months Medicaid enrolled in measurement period', 'Proportion of members')
,(11, 'TPO', 'SUD Treatment Penetration (Opioid)', 'age_grp_7', 'Age 18-64, Age 65+', '11+ months Medicaid enrolled in measurement period', 'Proportion of members')
,(12, 'PCR', 'Plan All-Cause Readmissions (30 days)', 'age_grp_8', 'Age 18-64', '11+ months Medicaid enrolled prior to discharge, 30+ days following discharge', 'Proportion of index events')
,(13, 'CAP', 'Child and Adolescent Access to Primary Care', 'age_grp_9_months', 'Age 12-24 Months, Age 25 Months-6, Age 7-11, Age 12-19', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
,(14, 'DC_EYE', 'Diabetes Care: Eye Exam', '', '', '', '')
,(15, 'DC_HbA1c', 'Diabetes Care: A1c Testing', '', '', '', '')
,(16, 'DC_KIDNEY', 'Diabetes Care: Kidney Screening', '', '', '', '')
,(17, 'MMA_50', 'Medication Management for Asthma: Compliance 50%', 'age_grp_10', 'Age 5-11, Age 12-18, Age 19-50, Age 51-64', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
,(18, 'MMA_75', 'Medication Management for Asthma: Compliance 75%', 'age_grp_10', 'Age 5-11, Age 12-18, Age 19-50, Age 51-64', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
,(19, 'AMR', 'Asthma Medication Ratio', 'age_grp_10', 'Age 5-11, Age 12-18, Age 19-50, Age 51-64', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
,(20, 'AMR_1', 'Asthma Medication Ratio (1-year requirement)', 'age_grp_10', 'Age 5-11, Age 12-18, Age 19-50, Age 51-64', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
<<<<<<< HEAD
,(21, '', 'Percent Homeless', '', '', '', '')
,(22, '', 'Antidepressant Medication Management', '', '', '', '')
,(23, '', 'High-dose Chronic Opioid Therapy', '', '', '', '')
,(24, '', 'Concurrent Opioids and Sedatives Prescriptions', '', '', '', '')
,(25, '', 'Statin Therapy for Heart Disease', '', '', '', '')
=======
,(21, 'HOMELESS', 'Percent Homeless', '', '', '', '')
,(22, 'AMM', 'Antidepressant Medication Management', '', '', '', '')
,(23, 'HDO', 'High-dose Chronic Opioid Therapy', '', '', '', '')
,(24, 'COS', 'Concurrent Opioids and Sedatives Prescriptions', '', '', '', '')
,(25, 'SPC', 'Statin Therapy for Heart Disease', '', '', '', '')
>>>>>>> a67667a9e83524d26debe2644dc55caef4b5de2c
GO
CREATE NONCLUSTERED INDEX idx_nc_ref_perf_measure_measure_name ON [ref].[perf_measure]([measure_name]) INCLUDE([measure_id]);
GO