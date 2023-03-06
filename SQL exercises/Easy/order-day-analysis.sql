USE WideWorldImporters;

/* 

    Write queries for the Sales.Orders table that return orders that were placed
     - on a specific date
     - after a specific date
     - before a specific date
     - between specific dates
     - in one specific year
     - in multiple specific years
     - in a specific year and month
     - on the last day of a specific month
     - in different time intervals combined with AND/OR

    Watch out for the WHERE clause, try to avoid using functions 
    on the left hand side of your logical expressions!

    | OrderID | OrderDate |
    -----------------------

*/

SELECT OrderID, OrderDate
FROM Sales.Orders
WHERE OrderDate BETWEEN '20140606' AND '20140608' OR
        OrderDate BETWEEN '20140610' AND '20140612'