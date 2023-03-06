import pyodbc
import pandas

try:
    connect = pyodbc.connect(
        "DRIVER={SQL Server Native Client RDA 11.0};"
        "SERVER=LAPTOP-8RD2S3NP;"
        "DATABASE=Employees;"
        "Trusted_Connection=yes;"
    )
    print('CONNECTED')
    cursor = connect.cursor()

    fileJSON = pandas.read_json('employees.json')
    readJSON = pandas.DataFrame(fileJSON, columns=['id', 'name', 'birth_date', 'nationality', 'gender', 'monthly_salary', 'university'])

    print(readJSON)

    readJSON['birth_date'] = pandas.to_datetime(readJSON['birth_date'])
    readJSON['monthly_salary'] = readJSON['monthly_salary'].fillna(0)

    for item in readJSON.itertuples():
        print(item)
        nameTag = item[2].split(' ')
        firstTag = nameTag[0]
        lastTag = str(nameTag[1:]).strip("['']")
        print(firstTag)
        print(lastTag)
        birthTag = item[3]
        natioTag = item[4]
        genderTag = item[5]
        monthTag = item[6]
        uniTag = item[7]
        cursor.execute(
            "INSERT INTO employee(first_name, last_name, branch, position, birth_date, gender, nationality, university, monthly_salary, salary_by_year) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (firstTag, lastTag, None, None, birthTag, genderTag, natioTag, uniTag, monthTag, monthTag * 12))

    cursor.commit()
    cursor.close()
    connect.close()
    print("FINISHED")


except pyodbc.ProgrammingError as e:
    print(e)
except pyodbc.OperationalError as e:
    print(e)