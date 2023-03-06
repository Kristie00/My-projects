USE WideWorldImporters;

/*
    Find the table that contains data about cities.
    Write a query that returns the 10 cities with the highest population.
    Alias the city population column as [population].

    | CityID | CityName | population |
    ----------------------------------

*/

SELECT TOP 10 CityID, CityName, LatestRecordedPopulation AS Population
FROM Application.Cities
ORDER BY LatestRecordedPopulation DESC
