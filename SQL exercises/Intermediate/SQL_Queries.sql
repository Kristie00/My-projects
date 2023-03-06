USE WideWorldImporters;

/*      city-states

    Modify the below query to return cities from the states of Arizona, 
    Washington and Utah and also display the name of their states.

    Order the results by State Name in ascending order.


    | CityID | CityName | State Name |
    ----------------------------------

    Hint: the Application.Cities and the Application.StateProvinces tables 
          both have a StateProvinceID column that you should use.

*/
SELECT c.CityID, c.CityName, p.StateProvinceName as ['State Name']
FROM Application.Cities c
INNER JOIN  Application.StateProvinces p
ON c.StateProvinceID = p.StateProvinceID
WHERE p.StateProvinceName LIKE('Arizona') OR 
        p.StateProvinceName LIKE('Washington') OR 
        p.StateProvinceName LIKE('Utah');


USE WideWorldImporters;

/*      order-report

    Write a query that returns a report for specific Orders.

    Each order has detailed orderlines that show what items were ordered and 
    in which quantity.

    Use the Sales.Orders and Sales.OrderLines tables. Sales.OrderLines has an 
    OrderID column that you should use.

    Return only those orders that were placed between '2013-05-01' and 
    '2013-06-30' and belong to CustomerID = 50.
    Order the results by OrderDate in descending order.


    | OrderID | OrderLineID | CustomerID | OrderDate | StockItemID |
    | Description | Quantity |
    ----------------------------------------------------------------

*/

SELECT o.OrderID, ol.OrderLineID, o.CustomerID, o.OrderDate, ol.StockItemID, ol.[Description], ol.Quantity
FROM Sales.OrderLines ol
INNER JOIN Sales.Orders o
ON ol.OrderID = o.OrderID
WHERE o.OrderDate BETWEEN '2013-05-01' AND '2013-06-30' AND
        o.CustomerID = 50
ORDER BY o.OrderDate DESC


USE WideWorldImporters;

/*      top-orders

    Write a query that returns a report for specific Orders.

    Each order has detailed orderlines that show what items were ordered and 
    in which quantity.

    Use the Sales.Orders and Sales.OrderLines tables. Sales.OrderLines has 
    an OrderID column that you should use.

    Return the 5 highest aggregated order quantity for those orders 
    that were placed between '2013-09-01' and '2013-12-31' and 
    belong to CustomerID = 100.


    | OrderID | sum_qty |
    ---------------------

*/

SELECT o.OrderID, SUM(ol.Quantity) AS sum_qty
FROM Sales.OrderLines ol
INNER JOIN Sales.Orders o
ON ol.OrderID = o.OrderID
WHERE o.OrderDate BETWEEN '2013-09-01' AND '2013-12-31' AND
        o.CustomerID = 100
GROUP BY o.OrderID


USE WideWorldImporters;

/*      top-customers

    Write a query that returns a report for specific Customers.

    Each order has detailed orderlines that show what items were ordered and 
    in which quantity.

    This time you'll need to work with more than two tables!
    This is called a multi-join query.

    Use the Sales.Customers, Sales.Orders and Sales.OrderLines tables. 
    Sales.OrderLines has an OrderID column that you should use.
    Sales.Orders has a CustomerID column that you should use.

    Return those 5 customers who ordered the most quantities in 2015.


    | CustomerID | CustomerName | sum_qty |
    ---------------------------------------

*/
SELECT TOP 5 c.CustomerID, c.CustomerName, SUM(ol.Quantity) as sum_qty
FROM Sales.OrderLines ol
INNER JOIN Sales.Orders o
ON ol.OrderID = o.OrderID
INNER JOIN Sales.Customers c
ON o.CustomerID = c.CustomerID
WHERE YEAR(o.OrderDate) = 2015
GROUP BY c.CustomerID, c.CustomerName
ORDER BY sum_qty DESC


USE WideWorldImporters;

/*      top cities

    Write a query that returns a report for specific Cities.

    Each order has detailed orderlines that show what items were ordered and
    in which quantity.

    This time you'll need to work with more than two tables!
    This is called a multi-join query.

    Return those 5 cities where customers ordered the most aggregated quantities
    on the last days of any month in 2016.
    If the aggregated quantity is:
     - greater than 800 display 'Exceptional'
     - between 700 and 800 display 'Good'
     - below 700 display 'Average'
    Alias the sales classification as [sales].


    | CityName | sum_qty | sales |
    ------------------------------

*/

SELECT TOP 5 ci.CityName, o.OrderDate, SUM(ol.Quantity) as sum_qty,
    CASE 
        WHEN SUM(ol.Quantity) > 800 THEN 'Exceptional'
        WHEN SUM(ol.Quantity) BETWEEN 700 AND 800 THEN 'Good'
        WHEN SUM(ol.Quantity) < 700 THEN 'Average'
        END AS 'sales'
FROM Application.Cities ci
INNER JOIN Sales.Customers cu
ON ci.CityID = cu.PostalCityID
INNER JOIN Sales.Orders o
ON cu.CustomerID = o.CustomerID
INNER JOIN Sales.OrderLines ol
ON o.OrderID = ol.OrderID
WHERE 
    o.OrderDate = EOMONTH('2016-01-01', 0) OR
    o.OrderDate = EOMONTH('2016-01-01', 1) OR
    o.OrderDate = EOMONTH('2016-01-01', 2) OR
    o.OrderDate = EOMONTH('2016-01-01', 3) OR
    o.OrderDate = EOMONTH('2016-01-01', 4) OR
    o.OrderDate = EOMONTH('2016-01-01', 5) OR
    o.OrderDate = EOMONTH('2016-01-01', 6) OR
    o.OrderDate = EOMONTH('2016-01-01', 7) OR
    o.OrderDate = EOMONTH('2016-01-01', 8) OR
    o.OrderDate = EOMONTH('2016-01-01', 9) OR
    o.OrderDate = EOMONTH('2016-01-01', 10) OR
    o.OrderDate = EOMONTH('2016-01-01', 11)
GROUP BY ci.CityName, o.OrderDate
ORDER BY SUM(ol.Quantity) DESC


USE WideWorldImporters;

/*      orders-pickedby

    Write a query that returns all the orders and match them with persons 
    who picked up the order.

    Return only those orders that were placed in March 2014.

    | OrderID | FullName |
    ----------------------

    What do you see in the resultset?
        *1586 rows, I see OrderID with names of people, 
        but some orders don't have name of person who picked it up

    How do you display only those rows that don't match?
        *185 rows
        * I can add to WHERE this part: AND p.PersonID IS NULL

    How can you make the original query look like an INNER JOIN with a 
    WHERE predicate?
        *1401 rows
        * I can add to WHERE this part: AND p.PersonID IS NOT NULL

    1586 - 185 = 1401, so I think my queries are right
*/
SELECT o.OrderID, p.FullName
FROM Sales.Orders o
LEFT JOIN Application.People p
ON o.PickedByPersonID = p.PersonID
WHERE o.OrderDate LIKE('2014-03-__') AND
    p.FullName LIKE('Hudson%')
GROUP BY o.OrderID, p.FullName

/*

    Apply a filter in your original query that filters for full names that 
    start with 'Hudson'.
    What do you notice?
        *86 rows
        * there is Hudson Hollinworth, Hudson Onslow
        * I think that the result is similar to INNER JOIN because there is no NULL full name?
*/


USE WideWorldImporters;

/*      count-orders-pickedby - This might not be correct

    Write a query that returns all the customer IDs from orders and match the 
    orders with persons who picked up the order.

    Return only those orders that were placed in March 2014.
    Make customer groups and count how many persons there are in each group.

    | CustomerID | cnt_persons |
    ----------------------------

    Watch out for using the COUNT function!
    How should you use the COUNT function? Why?

*/
SELECT cu.CustomerID, ca.CustomerCategoryID, COUNT(cu.CustomerID) AS cnt_persons
FROM Application.People p
LEFT JOIN Sales.Orders o
ON p.PersonID = o.PickedByPersonID
LEFT JOIN Sales.Customers cu
ON o.CustomerID = cu.CustomerID
LEFT JOIN Sales.CustomerCategories ca
ON cu.CustomerCategoryID = ca.CustomerCategoryID
WHERE o.OrderDate LIKE('2014-03-__')
GROUP BY ca.CustomerCategoryID, cu.CustomerID
ORDER BY cnt_persons DESC


USE WideWorldImporters;

/*      cross-join-countries

    Modify the below query to do a CROSS JOIN.

    Moreover, this is a special type of join too, a self-join.
    The query joins the Application.Countries table to itself.


    | left_id | left_name | right_id | right_name |
    -----------------------------------------------

    How many rows do you get? Why?
    * 36100 rows
    * table itself has 190 rows
    * if I am combining everything with everything, it is like 190*190, which is 36100

*/
SELECT a.CountryID, a.CountryName, 
       b.CountryID, b.CountryName
FROM Application.Countries a
CROSS JOIN Application.Countries b



USE WideWorldImporters;

/*      number-generator

    You haven't learned about creating tables yet, so you'll work with 'virtual'
    tables.
    This is a special use case of VALUES which you'll also use when inserting 
    data into tables.

    There are two sets: 
     - m with a column named mynumber, and with column values from 0 to 9
     - n with a column named mynumber, and with column values from 0 to 9
    These act as two tables that you can join and select from.

    Modify the below query so that you get m * n unique numbers (10 * 10 = 100),
    ordered from 1 to 100.

*/
SELECT m.mynumber*n.mynumber AS unique_numbers
FROM 
(VALUES((0)),((1)),((2)),((3)),((4)),((5)),((6)),((7)),((8)),((9))) m(mynumber)
CROSS JOIN (VALUES((0)),((1)),((2)),((3)),((4)),((5)),((6)),((7)),((8)),((9))) n(mynumber)
ORDER BY unique_numbers


USE WideWorldImporters;

/*      subqueries-or-joins

    Rewrite the following subqueries as join queries.

*/


/*
SELECT OrderID, OrderDate
FROM Sales.Orders
WHERE CustomerID IN
  (SELECT CustomerID
   FROM Sales.Customers
   WHERE PostalCityID = 33832);
*/

SELECT OrderID, OrderDate
FROM Sales.Orders o
LEFT JOIN Sales.Customers cu
ON o.CustomerID = cu.CustomerID
WHERE cu.PostalCityID = 33832

/*
SELECT CustomerID, CustomerName
FROM Sales.Customers c
WHERE CustomerName LIKE 'Anna%'
    AND EXISTS (SELECT * FROM Sales.Orders o
                WHERE c.CustomerID = o.CustomerID
                AND o.OrderDate >= '20140101' AND o.OrderDate < '20140201');
*/


SELECT cu.CustomerID, cu.CustomerName
FROM Sales.Customers cu
LEFT JOIN Sales.Orders o
ON cu.CustomerID = o.CustomerID
WHERE cu.CustomerName LIKE('Anna%') AND
    o.OrderDate >= '20140101' AND o.OrderDate < '20140201'
GROUP BY cu.CustomerName, cu.CustomerID

/*
SELECT CustomerID, CustomerName
FROM Sales.Customers c
WHERE CustomerName LIKE 'Anna%'
    AND NOT EXISTS (SELECT * FROM Sales.Orders o
                    WHERE c.CustomerID = o.CustomerID
                    AND o.OrderDate >= '20140101' AND o.OrderDate < '20140201');
*/

-- not working 100% correctly
SELECT cu.CustomerID, cu.CustomerName
FROM Sales.Customers cu
INNER JOIN Sales.Orders o
ON cu.CustomerID = o.CustomerID
WHERE cu.CustomerName LIKE('Anna%') AND
    o.OrderDate NOT BETWEEN '20140101' AND '20140201'
GROUP BY cu.CustomerID, cu.CustomerName
