USE WideWorldImporters;

/* 
    Find the table that contains data about cities.
    Write a query that returns the 10 cities with the highest population, but use the paging method.
    Alias the city population column as [population].


    | CityID | CityName | population |
    ----------------------------------

*/

SELECT CityID, CityName, LatestRecordedPopulation AS [Population]
FROM Application.Cities
ORDER BY LatestRecordedPopulation DESC
OFFSET 0 ROWS
FETCH FIRST 10 ROWS ONLY;