--CREATE DATABASE AirflowSet;

CREATE TABLE employee(id int IDENTITY(1,1),
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        birth_date DATE,
        gender VARCHAR(10),
        nationality VARCHAR(20),
        salary int);

CREATE TABLE transformed(id int,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender VARCHAR(10),
        name VARCHAR(20));

INSERT INTO employee VALUES ('Jane', 'Doe', '2022-11-17', 'female', 'CZ', 50000);
INSERT INTO employee VALUES ('John', 'Doe', '2022-10-17', 'male', 'CZ', 50000);


SELECT * FROM transformed;