--QA of stage.apcd_claim_ccw table
--5/13/19
--Eli Kern

--person who should not have cataract CCW because of DX fields condition
id_apcd = 11050947020
claim_header_id = 629257980861086

--claim that should not be included in BPH CCW definition because of DX exclusions
id_apcd = 11278002499
claim_header_id = 629250025757699

--a qualifying claim for BPH
id_apcd = 11051002955
claim_header_id = 629257853156008

--a claim that should be excluded from stroke definition by second criteria
id_apcd = 11058143040
claim_header_id = 629246622926380

--a person who should have stroke
id_apcd = 11050947017