declare @begin date, @end date, @duration int, @covmin decimal(4,1), @dualmax decimal(4,1), @agemin int, @agemax int, @female varchar(max), @male varchar(max),
	@aian varchar(max), @asian varchar(max), @black varchar(max), @nhpi varchar(max), @white varchar(max), @latino varchar (max), 
	@zip varchar(max), @region varchar(max), @english varchar(max), @spanish varchar(max), @vietnamese varchar(max), @chinese varchar(max),
	@somali varchar(max), @russian varchar(max), @arabic varchar(max), @korean varchar(max), @ukrainian varchar(max), @amharic varchar(max),
	@maxlang varchar(max), @id varchar(max)

set @begin = '2017-01-01'
set @end = '2017-06-30'
set @duration = datediff(day, @begin, @end) + 1

--create coverage group vars per zeyno's code
select y.id, sum(y.nadultd) as 'nadultd', sum(y.disabled) as 'disabled', sum(y.nondisabled) as 'nondisabled'
from (
	select distinct x.MEDICAID_RECIPIENT_ID as 'id', x.RAC_CODE as 'rac', x.startdate, x.enddate,

	--new adults
	/**if coverage period fully contains date range then person time is just date range */
	iif(x.startdate <= @begin and x.enddate >= @end and x.RAC_CODE in ('1201','1217'), datediff(day, @begin, @end) + 1, 
	
	/**if coverage period begins before date range start and ends within date range */
	iif(x.startdate <= @begin and x.enddate < @end and x.RAC_CODE in ('1201','1217'), datediff(day, @begin, x.enddate) + 1,

	/**if coverage period begins after date range start and ends after date range end */
	iif(x.startdate > @begin and x.enddate >= @end and x.RAC_CODE in ('1201','1217'), datediff(day, x.startdate, @end) + 1,

	/**if coverage period begins after date range start and ends before date range end */
	iif(x.startdate > @begin and x.enddate < @end and x.RAC_CODE in ('1201','1217'), datediff(day, x.startdate, x.enddate) + 1,

	0)))) as 'nadultd',

	--disabled
	/**if coverage period fully contains date range then person time is just date range */
	iif(x.startdate <= @begin and x.enddate >= @end and x.RAC_CODE in ('1002','1003','1005','1014','1015','1016','1017',
	'1018','1019','1020','1021','1022','1023','1024','1044','1045','1047','1049','1067','1070','1075','1076','1086','1091',
	'1105','1107','1110','1111','1121','1126','1134','1147','1150','1151','1153','1162','1163','1164','1165','1168','1169',
	'1175','1197','1219','1221','1224','1225','1237','1239','1242','1243','1245','1247','1252','1253','1254','1255','1258',
	'1261','1263','1267','1268','1269'), datediff(day, @begin, @end) + 1, 
	
	/**if coverage period begins before date range start and ends within date range */
	iif(x.startdate <= @begin and x.enddate < @end and x.RAC_CODE in ('1002','1003','1005','1014','1015','1016','1017',
	'1018','1019','1020','1021','1022','1023','1024','1044','1045','1047','1049','1067','1070','1075','1076','1086','1091',
	'1105','1107','1110','1111','1121','1126','1134','1147','1150','1151','1153','1162','1163','1164','1165','1168','1169',
	'1175','1197','1219','1221','1224','1225','1237','1239','1242','1243','1245','1247','1252','1253','1254','1255','1258',
	'1261','1263','1267','1268','1269'), datediff(day, @begin, x.enddate) + 1,

	/**if coverage period begins after date range start and ends after date range end */
	iif(x.startdate > @begin and x.enddate >= @end and x.RAC_CODE in ('1002','1003','1005','1014','1015','1016','1017',
	'1018','1019','1020','1021','1022','1023','1024','1044','1045','1047','1049','1067','1070','1075','1076','1086','1091',
	'1105','1107','1110','1111','1121','1126','1134','1147','1150','1151','1153','1162','1163','1164','1165','1168','1169',
	'1175','1197','1219','1221','1224','1225','1237','1239','1242','1243','1245','1247','1252','1253','1254','1255','1258',
	'1261','1263','1267','1268','1269'), datediff(day, x.startdate, @end) + 1,

	/**if coverage period begins after date range start and ends before date range end */
	iif(x.startdate > @begin and x.enddate < @end and x.RAC_CODE in ('1002','1003','1005','1014','1015','1016','1017',
	'1018','1019','1020','1021','1022','1023','1024','1044','1045','1047','1049','1067','1070','1075','1076','1086','1091',
	'1105','1107','1110','1111','1121','1126','1134','1147','1150','1151','1153','1162','1163','1164','1165','1168','1169',
	'1175','1197','1219','1221','1224','1225','1237','1239','1242','1243','1245','1247','1252','1253','1254','1255','1258',
	'1261','1263','1267','1268','1269'), datediff(day, x.startdate, x.enddate) + 1,

	0)))) as 'disabled',

	--non-disabled
	/**if coverage period fully contains date range then person time is just date range */
	iif(x.startdate <= @begin and x.enddate >= @end and x.RAC_CODE in ('1000','1001','1004','1026','1027','1028','1029','1030',
	'1031','1032','1038','1039','1042','1043','1046','1048','1050','1051','1052','1053','1055','1059','1063','1065','1066','1068',
	'1069','1071','1072','1073','1074','1083','1084','1088','1095','1096','1101','1102','1103','1104','1106','1108','1109','1122',
	'1123','1124','1140','1146','1148','1149','1152','1174','1196','1198','1199','1200','1202','1203','1204','1205','1206','1207',
	'1208','1209','1210','1211','1212','1213','1218','1222','1223','1236','1238','1240','1241','1244','1246','1248','1249','1250',
	'1251','1256','1257','1260','1264','1265','1266'), datediff(day, @begin, @end) + 1, 
	
	/**if coverage period begins before date range start and ends within date range */
	iif(x.startdate <= @begin and x.enddate < @end and x.RAC_CODE in ('1000','1001','1004','1026','1027','1028','1029','1030',
	'1031','1032','1038','1039','1042','1043','1046','1048','1050','1051','1052','1053','1055','1059','1063','1065','1066','1068',
	'1069','1071','1072','1073','1074','1083','1084','1088','1095','1096','1101','1102','1103','1104','1106','1108','1109','1122',
	'1123','1124','1140','1146','1148','1149','1152','1174','1196','1198','1199','1200','1202','1203','1204','1205','1206','1207',
	'1208','1209','1210','1211','1212','1213','1218','1222','1223','1236','1238','1240','1241','1244','1246','1248','1249','1250',
	'1251','1256','1257','1260','1264','1265','1266'), datediff(day, @begin, x.enddate) + 1,

	/**if coverage period begins after date range start and ends after date range end */
	iif(x.startdate > @begin and x.enddate >= @end and x.RAC_CODE in ('1000','1001','1004','1026','1027','1028','1029','1030',
	'1031','1032','1038','1039','1042','1043','1046','1048','1050','1051','1052','1053','1055','1059','1063','1065','1066','1068',
	'1069','1071','1072','1073','1074','1083','1084','1088','1095','1096','1101','1102','1103','1104','1106','1108','1109','1122',
	'1123','1124','1140','1146','1148','1149','1152','1174','1196','1198','1199','1200','1202','1203','1204','1205','1206','1207',
	'1208','1209','1210','1211','1212','1213','1218','1222','1223','1236','1238','1240','1241','1244','1246','1248','1249','1250',
	'1251','1256','1257','1260','1264','1265','1266'), datediff(day, x.startdate, @end) + 1,

	/**if coverage period begins after date range start and ends before date range end */
	iif(x.startdate > @begin and x.enddate < @end and x.RAC_CODE in ('1000','1001','1004','1026','1027','1028','1029','1030',
	'1031','1032','1038','1039','1042','1043','1046','1048','1050','1051','1052','1053','1055','1059','1063','1065','1066','1068',
	'1069','1071','1072','1073','1074','1083','1084','1088','1095','1096','1101','1102','1103','1104','1106','1108','1109','1122',
	'1123','1124','1140','1146','1148','1149','1152','1174','1196','1198','1199','1200','1202','1203','1204','1205','1206','1207',
	'1208','1209','1210','1211','1212','1213','1218','1222','1223','1236','1238','1240','1241','1244','1246','1248','1249','1250',
	'1251','1256','1257','1260','1264','1265','1266'), datediff(day, x.startdate, x.enddate) + 1,

	0)))) as 'nondisabled'

	from PHClaims.dbo.mcaid_elig_rac as x
	where x.startdate < @end and x.enddate > @begin
) as y
group by y.id