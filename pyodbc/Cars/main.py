import pymysql, os, json
import pyodbc

try:
    with open('cars.json', 'r') as myFile:
        readFile = myFile.read()
        json_obj = json.loads(readFile)

    connect = pyodbc.connect(
        "DRIVER={SQL Server Native Client RDA 11.0};"
        "SERVER=LAPTOP-8RD2S3NP;"
        "DATABASE=Cars;"
        "Trusted_Connection=yes;"
    )

    cursor = connect.cursor()

    # cursor.execute(
    #     "CREATE TABLE cars(id int IDENTITY(1,1), brand VARCHAR(20), model VARCHAR(20), year int, condition VARCHAR(20), price int, [count] int)")

    # for i, item in enumerate(json_obj):
    #     brandTag = item.get("brand", None)
    #     modelTag = item.get("model", None)
    #     yearTag = item.get("year", None)
    #     conditionTag = item.get("condition", None)
    #     priceTag = item.get("price", None)
    #    countTag = item.get("count", None)
    #     cursor.execute("INSERT INTO Cars(brand, model, year, condition, price, [count]) VALUES (?,?,?,?,?,?)",(brandTag, modelTag, yearTag, conditionTag, priceTag, countTag))

    # cursor.execute("DELETE FROM Cars WHERE [count] = ?", 0 )
    # cursor.execute(("UPDATE cars SET price=price*0.8 WHERE condition=?"),('wreck'))

    cursor.execute("SELECT (2022 - AVG(year)) AS [avgAge] FROM cars")

    cursor.commit()
    cursor.close()
    connect.close()
    print("FINISHED")

except FileExistsError as e:
    print(e)
except FileNotFoundError as e:
    print(e)
except pyodbc.ProgrammingError as e:
    print(e)
except pyodbc.OperationalError as e:
    print(e)
