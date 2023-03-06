USE WideWorldImporters;

/*

    Use the Warehouse.StockItems table.

    Write queries that return stockitems with the following characteristics:
    - name starts with 'DBA'
    - name ends with 't'
    - name does not end with 'k'
    - name contains the word 'joke'
    - name starts with the letters 'a' or 'b' or 'c' or 'f'
    - name that does not contain the words 'flash drive'
    - name that contains the word 'ham' and the following character is not 'm'
    - name starts with 'a', next character can be anything between 'l' and 't' 
      and ends with any character between 'l' and 'p'
    - name is exactly 'DBA joke mug - it depends (Black)'

    | StockItemID | StockItemName |
    -------------------------------

*/

SELECT StockItemID, StockItemName
FROM Warehouse.StockItems
WHERE StockItemName IN('DBA joke mug - it depends (Black)');