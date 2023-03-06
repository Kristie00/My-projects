USE WideWorldImporters;


--last-day-order-activity
/* 

    Modify the below query:
     - to return only the last day orders in general from the Sales.Orders table

    | CustomerID | OrderID | OrderDate |
    ------------------------------------

*/
SELECT CustomerID, OrderID, OrderDate 
FROM Sales.Orders
WHERE OrderDate IN (
    SELECT MAX(OrderDate)
    FROM Sales.Orders
);



--customer-last-day-order
/* 

    Modify the below query:
     - to return the orders that each customer placed on their last order day 
       from the Sales.Orders table

    | CustomerID | OrderID | OrderDate |
    ------------------------------------

*/
SELECT CustomerID, OrderID, OrderDate 
FROM Sales.Orders tab1
WHERE OrderDate IN(
    SELECT MAX(OrderDate)
    FROM Sales.Orders
    WHERE Sales.Orders.CustomerID = tab1.CustomerID)
ORDER BY CustomerID DESC
;



--missing-customer-order

/* 

    Modify the below query:
     - to return only those customers whose name starts with 'Anna'
     - AND did not place any orders in January 2014

    | CustomerID | CustomerName |
    -----------------------------

*/
SELECT CustomerID, CustomerName
FROM Sales.Customers tabCustomers
WHERE CustomerName LIKE ('Anna%') 
AND NOT EXISTS(
    SELECT CustomerID
    FROM Sales.Orders tabOrders
    WHERE tabCustomers.CustomerID = tabOrders.CustomerID
    AND YEAR(OrderDate) = 2014
    AND MONTH(OrderDate) = 1
);



--personid-problem
/* 
    Work with the following queries
*/
/* 
    Query1 - returns persons who are referenced in Sales.Orders 
             as PickedByPersonID 
    It returns 19 persons (rows).
*/
SELECT PersonID, FullName
FROM Application.People
WHERE PersonID IN (SELECT o.PickedByPersonID
                   FROM Sales.Orders o);

/* 
    Query2 - should return persons who are NOT referenced in Sales.Orders 
             as PickedByPersonID 
    It returns UNKNOWN. (no results)

    Why is that?
    I think I have to connect tables with values that are sharing with each other
    It is PickedByPersonID and PersonID
    PickedByPersonID is foreign key from People table
*/
SELECT PersonID, FullName
FROM Application.People
WHERE PersonID IN (SELECT o.PickedByPersonID
                   FROM Sales.Orders o);
/* 

    Query3 - Resolve the problem so that the NOT IN query does return rows.

*/
SELECT PersonID, FullName
FROM Application.People
WHERE PersonID NOT IN (SELECT o.PickedByPersonID
                       FROM Sales.Orders o
                       WHERE o.PickedByPersonID = People.PersonID);
/* 

    Query4 - How would you rewrite the NOT IN query with EXISTS?

*/
SELECT PersonID, FullName
FROM Application.People
WHERE NOT EXISTS (SELECT o.PickedByPersonID
                       FROM Sales.Orders o
                       WHERE o.PickedByPersonID = People.PersonID)
Order By PersonID;
-- now both queries have the same results 



--city-nextid
/* 
  
    Write a query that determines the next CityID 
    in the Application.Cities table.
  
    | CityID |  CityName  | nextcityid |
    ------------------------------------
    |   1    | Aaronsburg |      3     |
    |   3    | Abanda     |      4     |
    |   4    | Abbeville  |      5     |
    |   5    | Abbeville  |      6     |
    |  ...   |    ...     |     ...    |
    | 38186  | Zwolle     |     NULL   |

*/
SELECT CityID, CityName,
    LEAD(CityID, 1, NULL) OVER (ORDER BY cityID) AS nextcityid
FROM Application.Cities



--population-running-total

/* 
  
    Write a query that determines the running total (cumulative sum) of
    LatestRecordedPopulation of cities in the state of Colorado
    in the Application.Cities table.
  

    | CityID |  CityName  | population | run_total |
    ------------------------------------------------
    |  110   | Adams City |    NULL    |    NULL   |
    |  184   | Agate      |    NULL    |    NULL   |
    |  214   | Aguilar    |    538     |    538    |
    |  249   | Akron      |    1702    |    2240   |
    |  282   | Alamosa    |    8780    |    11020  |
    |  491   | Allenspark |    528     |    11548  |
    |  ...   | ...        |    ...     |    ...    |

*/
SELECT CityID, CityName, LatestRecordedPopulation AS population, 
    (SUM(LatestRecordedPopulation) OVER (ORDER BY CityID)) AS run_total
FROM Application.Cities tblCities
WHERE StateProvinceID =(
    SELECT StateProvinceID
    FROM Application.StateProvinces tblProvince
    WHERE StateProvinceName IN('Colorado') 
    AND tblCities.StateProvinceID = tblProvince.StateProvinceID
)
ORDER BY CityID



--city-population-calc
/*

    Modify the below query to return these columns 
    from the Application.Cities table:
     - cnt_all: count of all rows
     - cnt_pop: count of the LatestRecordedPopulation column values
     - max_pop: maximum value of LatestRecordedPopulation
     - min_pop: minimum value of LatestRecordedPopulation
     - sum_pop: sum aggregate of LatestRecordedPopulation
     - avg_pop: avg of LatestRecordedPopulation

   
    What does each column value show?

    | cnt_all | cnt_pop | max_pop | min_pop | sum_pop | avg_pop |
    -------------------------------------------------------------

*/
SELECT COUNT(*) AS cnt_all, COUNT(LatestRecordedPopulation) AS cnt_pop,
    MAX(LatestRecordedPopulation) AS max_pop, MIN(LatestRecordedPopulation) AS min_pop,
    SUM(LatestRecordedPopulation) AS sum_pop, AVG(LatestRecordedPopulation) AS avg_pop
FROM Application.Cities;

/*

    Apply a WHERE filter to either:
     - calculate with only known LatestRecordedPopulation values
     - OR calculate with only unknown LatestRecordedPopulation values

    How does the result change?

    | cnt_all | cnt_pop | max_pop | min_pop | sum_pop | avg_pop |
    -------------------------------------------------------------
*/
SELECT COUNT(*) AS cnt_all, COUNT(LatestRecordedPopulation) AS cnt_pop,
    MAX(LatestRecordedPopulation) AS max_pop, MIN(LatestRecordedPopulation) AS min_pop,
    SUM(LatestRecordedPopulation) AS sum_pop, AVG(LatestRecordedPopulation) AS avg_pop
FROM Application.Cities
WHERE LatestRecordedPopulation IS NOT NULL;

SELECT COUNT(*) AS cnt_all, COUNT(LatestRecordedPopulation) AS cnt_pop,
    MAX(LatestRecordedPopulation) AS max_pop, MIN(LatestRecordedPopulation) AS min_pop,
    SUM(LatestRecordedPopulation) AS sum_pop, AVG(LatestRecordedPopulation) AS avg_pop
FROM Application.Cities
WHERE LatestRecordedPopulation IS NULL;

/*
  
    Modify this new WHERE filter query and make sure that:
     - the cnt_pop column always show the very same value as cnt_all 
       no matter what the filter is

    | cnt_all | cnt_pop | max_pop | min_pop | sum_pop | avg_pop |
    -------------------------------------------------------------

*/
SELECT COUNT(*) AS cnt_all, COUNT(LatestRecordedPopulation) AS cnt_pop,
    MAX(LatestRecordedPopulation) AS max_pop, MIN(LatestRecordedPopulation) AS min_pop,
    SUM(LatestRecordedPopulation) AS sum_pop, AVG(LatestRecordedPopulation) AS avg_pop
FROM Application.Cities tblCities1
HAVING SUM(LatestRecordedPopulation) =
    (SELECT COUNT(*)
    FROM Application.Cities tblCities2)
;


/*

    Modify the original (not filtered) query by adding a new column:
     - cnt_unknown_pop: count of only the unknown LatestRecordedPopulation 
       values

 | cnt_all | cnt_pop | cnt_unknown_pop | max_pop | min_pop | sum_pop | avg_pop |
 -------------------------------------------------------------------------------

*/
