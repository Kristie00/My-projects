CREATE TABLE people(
    ID INT,
    Category VARCHAR(100),
    URL VARCHAR(500),
    Description VARCHAR(1000)
)

SELECT * FROM people
DROP TABLE people

CREATE TABLE people2(
    id INT IDENTITY(1,1),
    name VARCHAR(150),
    birth_date DATETIME,
    nationality VARCHAR(150),
    gender VARCHAR(20),
    monthly_salary INT,
    university VARCHAR(200)
)

SELECT * FROM people2