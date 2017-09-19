select distinct [DUAL_ELIG]
from dbo.NewEligibility

select dual_elig, count (distinct MEDICAID_RECIPIENT_ID)
from dbo.NewEligibility
where RAC_CODE in ('1002','1003','1005','1014','1015','1016','1017','1018','1019','1020','1021','1022','1023','1024','1044','1045','1047'
    ,'1049','1067','1070','1075','1076','1086','1091','1105','1107','1110','1111','1121','1126','1134','1147','1150','1151','1153','1162','1163'
    ,'1164','1165','1168','1169','1175','1197','1219','1221','1224','1225','1237','1239','1242','1243','1245','1247','1252','1253','1254','1255'
    ,'1258','1261','1263','1267','1268','1269')
group by DUAL_ELIG

select rac_code, rac_name, count (distinct tcn)
from dbo.NewClaims
where PAID_AMT_H = '.00' or PAID_AMT_H is NULL
group by RAC_CODE, rac_name




                     