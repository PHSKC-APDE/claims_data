use PHClaims
go

------------------------------------------------
--Make table for avoidable ED visits, CA definition, ICD9-CM and ICD10-CM
------------------------------------------------
if object_id('dbo.tmp_avoidable_ed_icd_ca_norm', 'U') IS NOT NULL 
  drop table dbo.tmp_avoidable_ed_icd_ca_norm;

SELECT distinct icdcode AS icdcode_aed, version, min(description1) AS description
INTO dbo.tmp_avoidable_ed_icd_ca_norm
FROM
(SELECT icdcode as icdcode1, decription1 AS description1, version, cast(
			case
				when (version = 9 and len(icdcode) = 3) then RTRIM(icdcode) + '00'
				when (version = 9 and len(icdcode) = 4) then RTRIM(icdcode) + '0'
				else icdcode
			end
			as varchar(200)) as 'icdcode'
FROM dbo.tmp_avoidable_ed_icd_ca_raw) a
GROUP BY icdcode, version;

------------------------------------------------
--Make complete diagnosis lookup table for claims data
------------------------------------------------
if object_id('dbo.ref_diag_lookup', 'U') IS NOT NULL 
  drop table dbo.ref_diag_lookup;

SELECT DISTINCT icdcode, dx_description, ver, 
 MIN(asthma_ccw) AS asthma_ccw, 
 MIN(copd_ccw) AS copd_ccw, 
 MIN(diabetes_ccw) AS diabetes_ccw, 
 MIN(ischemic_heart_dis_ccw) AS ischemic_heart_dis_ccw,
 MIN(heart_failure_ccw) AS heart_failure_ccw, 
 MIN(chr_kidney_dis_ccw) AS chr_kidney_dis_ccw, 
 MIN(depression_ccw) AS depression_ccw, 
 MIN(ed_avoidable)	AS avoidable_ed,
 MIN(unint_injury) AS unint_injury

 INTO dbo.ref_diag_lookup

FROM
	(SELECT *,
	 CASE WHEN ccw_code=6 THEN 1 ELSE NULL END AS asthma_ccw,
	 CASE WHEN ccw_code=11 THEN 1 ELSE NULL END AS copd_ccw,
	 CASE WHEN ccw_code=13 THEN 1 ELSE NULL END AS diabetes_ccw,
	 CASE WHEN ccw_code=19 THEN 1 ELSE NULL END AS ischemic_heart_dis_ccw,
	 CASE WHEN ccw_code=15 THEN 1 ELSE NULL END AS heart_failure_ccw,
	 CASE WHEN ccw_code=18 THEN 1 ELSE NULL END AS hypertension_ccw,
	 CASE WHEN ccw_code=10 THEN 1 ELSE NULL END AS chr_kidney_dis_ccw,
	 CASE WHEN ccw_code=12 THEN 1 ELSE NULL END AS depression_ccw,
	 CASE WHEN icdcode_aed IS NOT NULL THEN 1 ELSE NULL END AS ed_avoidable,
	 CASE WHEN (ver=9 AND (icdcode BETWEEN 'E800%' AND 'E869%' OR icdcode BETWEEN 'E880%' AND 'E929%'))
	 OR (ver=10 AND (icdcode BETWEEN 'V01' AND 'X59%' OR icdcode LIKE 'Y85%' OR icdcode like 'Y86%'))
		THEN 1 ELSE NULL END AS unint_injury
	FROM
		(SELECT a.*, b.dx, b.ccw_code, c.icdcode_aed
		FROM (
			select 	
			cast(
				case
					when (ver = 9 and len(icdcode) = 3) then icdcode + '00'
					when (ver = 9 and len(icdcode) = 4) then icdcode + '0' 
					else icdcode
				end
			as varchar(200)) as 'icdcode',
			dx_description, ver
			from dbo.tmp_icd_cm_codes
		) a
		LEFT JOIN (
			select *
			from dbo.tmp_ccw
		) b
		ON a.icdcode = b.dx and a.ver = b.ver
		LEFT JOIN (
			select *
			from dbo.tmp_avoidable_ed_icd_ca_norm
		) c
		ON a.icdcode = c.icdcode_aed and a.ver = c.version)
	d)
d
GROUP BY icdcode, dx_description, ver;



