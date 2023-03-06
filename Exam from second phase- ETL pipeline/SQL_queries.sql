--What are the top 10 highest airports based on the elevation_ft (with ties)?
USE exam
SELECT TOP 10 WITH TIES name, elevation_ft
FROM airports
ORDER BY elevation_ft DESC

--Which airports are located in Africa?
USE exam
SELECT a.name AS 'Airport', c.name AS 'Country', c.continent AS 'Continent'
FROM airports a
LEFT JOIN countries c 
ON a.iso_country = c.iso2_code
WHERE c.continent = 'Africa'

--Which airports local code start with 02? Return only name and local_code.
SELECT name, local_code
FROM airports
WHERE local_code LIKE('02%')


--What are the highest elevation_ft by continents?
SELECT MAX(a.elevation_ft), c.continent
FROM airports a
LEFT JOIN countries c 
ON a.iso_country = c.iso2_code
GROUP BY c.continent


--What is the count of airports that belongs to small_airport airport type?
SELECT COUNT(*)
FROM airports
WHERE type = 'small_airport'