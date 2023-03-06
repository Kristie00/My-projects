USE WideWorldImporters;

/* 
    Modify this query so that you add a third column in the SELECT list: (Sum_Qty - 10) AS Reduced_Qty
    What happens? Why?

    How would you resolve the problem and add the Reduced_Qty column so that the query works?

    
    | Order ID | Sum_Qty | Reduced_Qty |
    ------------------------------------
    
*/
SELECT OrderID AS [Order ID], SUM(Quantity) AS Sum_Qty, SUM(Quantity) - 10 AS Reduced_Qty
FROM Sales.OrderLines
WHERE UnitPrice > 10
GROUP BY OrderID
HAVING OrderID IN (46, 47, 48)
ORDER BY [Order ID] ASC;