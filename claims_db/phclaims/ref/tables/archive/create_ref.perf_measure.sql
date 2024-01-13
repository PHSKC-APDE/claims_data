
USE [PHClaims];
GO

IF OBJECT_ID('[ref].[perf_measure]') IS NOT NULL
DROP TABLE [ref].[perf_measure];
CREATE TABLE [ref].[perf_measure]
([measure_id] INT NOT NULL
,[measure_short_name] VARCHAR(200)
,[measure_etl_name] VARCHAR(200)
,[measure_name] VARCHAR(200)
,[age_group] VARCHAR(200)
,[age_group_desc] VARCHAR(200)
,[mcaid_enrolled] VARCHAR(200)
,[expressed_as] VARCHAR(200)
,CONSTRAINT [pk_perf_measure_measure_short_name] PRIMARY KEY CLUSTERED ([measure_short_name])
);
GO

INSERT INTO [ref].[perf_measure]([measure_id], [measure_short_name], [measure_etl_name], [measure_name], [age_group], [age_group_desc], [mcaid_enrolled], [expressed_as])
VALUES
 (1, 'ED', 'All-Cause ED Visits', 'All-Cause ED Visits', 'age_grp_2', 'Age 0-17, Age 18-64, Age 65+', '7+ months Medicaid enrolled in measurement period', 'Per 1,000 member months')
,(2, 'AH', 'Acute Hospital Utilization', 'Acute Hospital Utilization', 'age_grp_1', 'Age 18+', '11+ months Medicaid enrolled in measurement period', 'Per 1,000 members')
,(3, 'FUA_7', 'Follow-up ED visit for Alcohol/Drug Abuse', 'Follow-up ED visit for Alcohol/Drug Abuse: 7 days', 'age_grp_3', 'Age 13+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(4, 'FUA_30', 'Follow-up ED visit for Alcohol/Drug Abuse', 'Follow-up ED visit for Alcohol/Drug Abuse: 30 days', 'age_grp_3', 'Age 13+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(5, 'FUM_7', 'Follow-up ED visit for Mental Illness', 'Follow-up ED visit for Mental Illness: 7 days', 'age_grp_4', 'Age 6+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(6, 'FUM_30', 'Follow-up ED visit for Mental Illness', 'Follow-up ED visit for Mental Illness: 30 days', 'age_grp_4', 'Age 6+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(7, 'FUH_7', 'Follow-up Hospitalization for Mental Illness', 'Follow-up Hospitalization for Mental Illness: 7 days', 'age_grp_4', 'Age 6+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(8, 'FUH_30', 'Follow-up Hospitalization for Mental Illness', 'Follow-up Hospitalization for Mental Illness: 30 days', 'age_grp_4', 'Age 6+', '30+ days Medicaid enrolled following event', 'Proportion of index events')
,(9, 'TPM', 'Mental Health Treatment Penetration', 'Mental Health Treatment Penetration', 'age_grp_5', 'Age 6-17, Age 18-64, Age 65+', '11+ months Medicaid enrolled in measurement period', 'Proportion of members')
,(10, 'TPS', 'SUD Treatment Penetration', 'SUD Treatment Penetration', 'age_grp_6', 'Age 12-17, Age 18-64, Age 65+', '11+ months Medicaid enrolled in measurement period', 'Proportion of members')
,(11, 'TPO', 'SUD Treatment Penetration (Opioid)', 'SUD Treatment Penetration (Opioid)', 'age_grp_7', 'Age 18-64, Age 65+', '11+ months Medicaid enrolled in measurement period', 'Proportion of members')
,(12, 'PCR', 'Plan All-Cause Readmissions (30 days)', 'Plan All-Cause Readmissions (30 days)', 'age_grp_8', 'Age 18-64', '11+ months Medicaid enrolled prior to discharge, 30+ days following discharge', 'Proportion of index events')
,(13, 'CAP', 'Child and Adolescent Access to Primary Care', 'Child and Adolescent Access to Primary Care', 'age_grp_9_months', 'Age 12-24 Months, Age 25 Months-6, Age 7-11, Age 12-19', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
,(14, 'DC_EYE', 'Diabetes Care: Eye Exam', 'Diabetes Care: Eye Exam', '', '', '', '')
,(15, 'DC_HbA1c', 'Diabetes Care: A1c Testing', 'Diabetes Care: A1c Testing', '', '', '', '')
,(16, 'DC_KIDNEY', 'Diabetes Care: Kidney Screening', 'Diabetes Care: Kidney Screening', '', '', '', '')
,(17, 'MMA_50', 'Medication Management for Asthma: Compliance 50%', 'Medication Management for Asthma: Compliance 50%', 'age_grp_10', 'Age 5-11, Age 12-18, Age 19-50, Age 51-64', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
,(18, 'MMA_75', 'Medication Management for Asthma: Compliance 75%', 'Medication Management for Asthma: Compliance 75%', 'age_grp_10', 'Age 5-11, Age 12-18, Age 19-50, Age 51-64', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
,(19, 'AMR', 'Asthma Medication Ratio', 'Asthma Medication Ratio', 'age_grp_10', 'Age 5-11, Age 12-18, Age 19-50, Age 51-64', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
,(20, 'AMR_1', 'Asthma Medication Ratio (1-year requirement)', 'Asthma Medication Ratio (1-year requirement)', 'age_grp_10', 'Age 5-11, Age 12-18, Age 19-50, Age 51-64', '11+ months Medicaid enrolled in measurement period, if applicable: 11+ months Medicaid enrolled in year prior to measurement period', 'Proportion of members')
,(21, 'HOMELESS', 'Percent Homeless', 'Percent Homeless', '', '', '', '')
,(22, 'AMM', 'Antidepressant Medication Management', 'Antidepressant Medication Management', '', '', '', '')
,(23, 'HDO', 'High-dose Chronic Opioid Therapy', 'High-dose Chronic Opioid Therapy', '', '', '', '')
,(24, 'COS', 'Concurrent Opioids and Sedatives Prescriptions', 'Concurrent Opioids and Sedatives Prescriptions', '', '', '', '')
,(25, 'SPC', 'Statin Therapy for Heart Disease', 'Statin Therapy for Heart Disease', '', '', '', '')
,(26, 'INI', 'SUD Treatment Initiation', 'SUD Treatment Initiation', '', '', '', '')
,(27, 'ENG', 'SUD Treatment Initiation', 'SUD Treatment Engagement', '', '', '', '')
,(28, 'INI_NM', 'SUD Treatment Initiation (No Modifiers)', 'SUD Treatment Initiation (No Modifiers)', '', '', '', '')
,(29, 'ENG_NM', 'SUD Treatment Initiation (No Modifiers)', 'SUD Treatment Engagement (No Modifiers)', '', '', '', '')
,(30, 'TPM_ADHD', 'MH Treatment Penetration by Diagnosis', 'MH Treatment Penetration: ADHD', 'age_grp_5', 'Age 6-17, Age 18-64, Age 65+', '', 'Proportion of members')
,(31, 'TPM_Adjustment', 'MH Treatment Penetration by Diagnosis', 'MH Treatment Penetration: Adjustment', 'age_grp_5', 'Age 6-17, Age 18-64, Age 65+', '', 'Proportion of members')
,(32, 'TPM_Anxiety', 'MH Treatment Penetration by Diagnosis', 'MH Treatment Penetration: Anxiety', 'age_grp_5', 'Age 6-17, Age 18-64, Age 65+', '', 'Proportion of members')
,(33, 'TPM_Depression', 'MH Treatment Penetration by Diagnosis', 'MH Treatment Penetration: Depression', 'age_grp_5', 'Age 6-17, Age 18-64, Age 65+', '', 'Proportion of members')
,(34, 'TPM_Impulse', 'MH Treatment Penetration by Diagnosis', 'MH Treatment Penetration: Disrup/Impulse/Conduct', 'age_grp_5', 'Age 6-17, Age 18-64, Age 65+', '', 'Proportion of members')
,(35, 'TPM_Bipolar', 'MH Treatment Penetration by Diagnosis', 'MH Treatment Penetration: Mania/Bipolar', 'age_grp_5', 'Age 6-17, Age 18-64, Age 65+', '', 'Proportion of members')
,(36, 'TPM_Psychotic', 'MH Treatment Penetration by Diagnosis', 'MH Treatment Penetration: Psychotic', 'age_grp_5', 'Age 6-17, Age 18-64, Age 65+', '', 'Proportion of members')
GO
CREATE NONCLUSTERED INDEX idx_nc_perf_measure_measure_name ON [ref].[perf_measure]([measure_name]) INCLUDE([measure_id]);
GO