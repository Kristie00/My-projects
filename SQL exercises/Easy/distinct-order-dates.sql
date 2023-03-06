USE WideWorldImporters;

/* 
    Find the table where there are order dates.
    Write a query that returns the distinct order dates in descending order.
    Alias the returned column as [Order Date].  

    | Order Date |
    --------------

    Try using a different ordering, not on order date. What happens?

*/

SELECT OrderDate AS [Order Date]
FROM Sales.Orders
ORDER BY OrderDate DESC;