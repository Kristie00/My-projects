USE WideWorldImporters;

/* 

    Write a query for the Sales.OrderLines table that returns:
    - unique stock items and their descriptions
    - ordered by stock item ID in ascending order
    - skip the first 29 rows then return only the next 50 rows


    | StockItemID | Description |
    -----------------------------

*/

SELECT DISTINCT StockItemID, [Description]
FROM Sales.OrderLines
ORDER BY StockItemID ASC
OFFSET 29 ROWS
FETCH FIRST 50 ROWS ONLY;