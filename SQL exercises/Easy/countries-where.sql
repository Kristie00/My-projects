USE WideWorldImporters;

/* 
    
    Write queries with a WHERE clause with different predicates/expressions 
    to query data from the Application.Countries table. 
    
    Try the operators in the operator precedence list 
    and combine them with AND / OR.
    
    Test how the operator precedence works.


    | CountryID | CountryName |
    ---------------------------

*/

SELECT CountryID, CountryName
FROM Application.Countries
WHERE Continent IN('Asia')
ORDER BY CountryName