USE WideWorldImporters;

/* 
    Modify this query so that the GROUP BY and the HAVING clauses also use the [Order ID] column alias.
    What happens? Why?
*/
SELECT OrderID AS [Order ID], SUM(Quantity) AS Sum_Qty
FROM WideWorldImporters.Sales.OrderLines
WHERE UnitPrice > 10
GROUP BY OrderID
HAVING OrderID IN (46, 47, 48)
ORDER BY [Order ID] ASC;