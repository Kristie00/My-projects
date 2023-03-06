USE WideWorldImporters;

/* 
    
    Write a CASE expression query for the Sales.Orders table that return 
    order dates for the year 2013, but display a verbose quarter description too
     - if the order date is in the first quarter of 2013 
       write 'First quarter of 2013'
     - if the order date is in the second quarter of 2013 
       write 'Second quarter of 2013'
     - if the order date is in the third quarter of 2013 
       write 'Third quarter of 2013'
     - if the order date is in the fourth quarter of 2013 
       write 'Fourth quarter of 2013'

    | OrderID | OrderDate | quarter |
    ---------------------------------

*/

SELECT OrderID, OrderDate,
  CASE WHEN OrderDate BETWEEN '20130101' AND '20130331' THEN 'First quarter of 2013'
     WHEN OrderDate BETWEEN '20130401' AND '20130630' THEN 'Second quarter of 2013'
     WHEN OrderDate BETWEEN '20130701' AND '20130930' THEN 'Third quarter of 2013'
     WHEN OrderDate BETWEEN '20131001' AND '20131231' THEN 'Fourth quarter of 2013'
     END AS 'quarter'
FROM WideWorldImporters.Sales.Orders