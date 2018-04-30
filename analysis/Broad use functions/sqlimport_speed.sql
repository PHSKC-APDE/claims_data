BULK INSERT [PHClaims].[dbo].[temp]
FROM '\\dchs-shares01\dchsdata\DCHSPHClaimsData\Data\temp.txt'
WITH
(FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n')

BULK INSERT [PHClaims].[dbo].[temp]
FROM '\\PH3QGLK72\Temp\temp_fake.txt'
WITH
(FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n')
