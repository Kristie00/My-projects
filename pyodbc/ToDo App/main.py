import pyodbc

print(pyodbc.drivers())

try:
    connect = pyodbc.connect(
        "DRIVER={SQL Server Native Client RDA 11.0};"
        "SERVER=LAPTOP-8RD2S3NP;"
        "DATABASE=ToDoApp;"
        "Trusted_Connection=yes;"
    )
    print("CONNECTED")

    cursor = connect.cursor()

    # cursor.execute("CREATE TABLE tasks(id int IDENTITY(1,1), name VARCHAR(20), isDone VARCHAR(5), description VARCHAR(50))")

    command = input('''Pass your command: 
                    "-l" - for showing all tasks
                    "-a" - for adding new task
                    "-c ID" - to check task as done
                    "-r ID" - to remove task
                    ''')

    if command[0:2] == '-l':
        cursor.execute("SELECT * FROM tasks")
        for row in cursor:
            print(row)
    elif command[0:2] == '-a':
        desc = input("Describe your task: ")
        task_name = command[4:]
        cursor.execute("INSERT INTO tasks(name, isDone, description) VALUES(?, ?, ?)",(task_name, 'false', desc))
    elif command[0:2] == '-r':
        num = command[3]
        cursor.execute("DELETE FROM tasks WHERE id=?", num)
    elif command[0:2] == '-c':
        num = command[3]
        cursor.execute("UPDATE tasks SET isDone=? WHERE id=?", ('True', num))
    else:
        print("Invalid command!")

    cursor.commit()
    cursor.close()
    connect.close()
    print("FINISHED")

except pyodbc.ProgrammingError as e:
    print(e)
    print('Something wrong with SQL statement')
except pyodbc.OperationalError as e:
    print(e)
    print('SQL connection error')
