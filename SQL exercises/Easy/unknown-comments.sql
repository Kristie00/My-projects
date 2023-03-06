USE WideWorldImporters;

/* 

    Write a query that returns the first 100 order ids with comments 
    from the Sales.Orders table.
    
    If a comment is UNKNOWN, display the string 'not available' instead.

    Try different methods to solve this problem.

    | OrderID | Comments |
    ----------------------

*/

SELECT OrderID,
ISNULL(Comments, 'not available') AS [Comments] 
FROM Sales.Orders

