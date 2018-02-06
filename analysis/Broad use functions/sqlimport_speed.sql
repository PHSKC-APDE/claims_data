BULK INSERT [PHClaims].[dbo].[temp]
FROM '\\dchs-shares01\dchsdata\DCHSPHClaimsData\Data\temp.txt'
WITH
(FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n')