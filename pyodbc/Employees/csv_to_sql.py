import pyodbc
import pandas

try:
    connect = pyodbc.connect(
        "DRIVER={SQL Server Native Client RDA 11.0};"
        "SERVER=LAPTOP-8RD2S3NP;"
        "DATABASE=Employees;"
        "Trusted_Connection=yes;"
    )
    print("CONNECTED")
    cursor = connect.cursor()

    urlCSV = "https://raw.githubusercontent.com/green-fox-academy/teaching-materials/master/workshop/psycopg" \
             "/employees/employees.csv?token=GHSAT0AAAAAABYWPVFN2AH4FHVOT3HZMAPKY3BUSLA "

    fileCSV = pandas.read_csv(urlCSV)
    readCSV = pandas.DataFrame(fileCSV)
    print(readCSV)

    for row in readCSV.itertuples():
        print(row)

        a = row[2]
        b = row[3]
        c = pandas.to_datetime(row[4].strip(), format="%m/%d/%Y")
        d = row[5]
        e = row[6]
        cursor.execute(
            '''
            INSERT INTO employee(first_name, last_name, branch, position, birth_date, gender, nationality, university, monthly_salary, salary_by_year) VALUES (?,?,?,?,?,?,?,?,?,?)
            ''',
            a,
            b,
            None,
            None,
            c,
            d,
            None,
            None,
            e,
            e * 12
        )

    cursor.commit()
    cursor.close()
    connect.close()
    print("FINISHED")


except pyodbc.ProgrammingError as e:
    print(e)
except pyodbc.OperationalError as e:
    print(e)