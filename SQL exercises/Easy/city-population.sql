USE WideWorldImporters;

/* 

    City populations in the Application.Cities table are sometimes UNKNOWN.
    
    Write a query that returns the city names where the populations are known.
    Write a query that returns the city names where the populations are UNKNOWN.
    Alias the LatestRecordedPopulation column as [population].

    | CityName | population |
    -------------------------

    Which operators do you need to use?

*/

SELECT CityName, LatestRecordedPopulation AS [population]
FROM Application.Cities
WHERE LatestRecordedPopulation IS NULL;