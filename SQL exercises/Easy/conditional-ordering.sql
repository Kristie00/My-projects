USE WideWorldImporters;

/* 
    
    CASE expressions can be used in other clauses too, other than SELECT.

    Write a query that uses conditional ordering.
    Order the people by their full name in the Application.People table
     - in ascending order if they are an employee
     - in descending order if they are a vendor (not an employee)

    | FullName | IsEmployee |
    -------------------------

*/

SELECT FullName, IsEmployee, IsSalesperson
FROM Application.People
ORDER BY 
    CASE WHEN IsEmployee = 1 THEN FullName END ASC, 
    CASE WHEN IsEmployee = 0 AND IsSalesperson = 1 THEN FullName END DESC