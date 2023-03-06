--Retrieve 
--name, type, elevation_ft, iso_region, gps_code, local_code of airport 
--       and name, continent from countries 
--       and insert the result to MongoDB collection airports as separate documents.

USE exam

SELECT a.name, a.type, a.elevation_ft, a.iso_region, a.gps_code, a.local_code, c.name, c.continent
FROM exam.dbo.airports a
LEFT JOIN exam.dbo.countries c
ON a.iso_country = c.iso2_code