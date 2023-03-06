import flask
from flask import Flask, jsonify, request
import pyodbc

app = Flask(__name__)

# connect to movies database that will store data about movies
# create table for movies
connect = pyodbc.connect(
    "DRIVER={SQL Server Native Client RDA 11.0};"
    "SERVER=LAPTOP-8RD2S3NP;"
    "DATABASE=movies;"
    "Trusted_Connection=yes;"
)
cursor = connect.cursor()
print('CONNECTED')

# cursor.execute(
# "CREATE TABLE movie(id int IDENTITY(1,1), title VARCHAR(200), [year] int, genre VARCHAR(50), description VARCHAR(500))")

# selecting an API KEY
response = flask.Response()
response.headers['API_KEY'] = 'thisismypassword'


@app.route('/')
def hello_here():  # put application's code here
    return 'Welcome!'


@app.route('/api/movies', methods=['GET', 'POST'])
def get_without_number():
    if request.method == "GET":
        cursor.execute("SELECT * FROM movies")
        rows = cursor.fetchall()
        values = []
        for row in rows:
            values.append({'id': row.id, 'title': row.title, 'year': row.year, 'genre': row.genre,
                           'description': row.description})
        return jsonify(values)
    elif request.method == "POST":
        values = []
        if request.headers.get('API_KEY'):
            cursor.execute("INSERT INTO movies (title, [year], genre, description) VALUES (?,?,?,?)",
                           request.form.get('title'), request.form.get('year'), request.form.get('genre'),
                           request.form.get('description'))
            cursor.execute("SELECT * FROM movies ORDER BY id DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY")
            rows = cursor.fetchone()
            for row in rows:
                values.append({'id': row.id, 'title': row.title, 'year': row.year, 'genre': row.genre,
                               'description': row.description})
            return jsonify(values), 200
        else:
            return jsonify({"ERROR": "Invalid API_KEY"}), 403


@app.route('/api/movies/<int:movie_id>', methods=['GET', 'PUT', 'DELETE'])
def get_with_number(movie_id):
    args = request.args
    number = args.get('movie_id')
    cursor.execute("SELECT * FROM movies")
    rows = cursor.fetchall()
    valuesGET = []
    valuesPUT = []
    if request.method == 'GET':
        for row in rows:
            if number == row.id:
                valuesGET.append({'id': row.id, 'title': row.title, 'year': row.year, 'genre': row.genre,
                                  'description': row.description})
                return jsonify(valuesGET)
            else:
                pass
    elif request.method == 'PUT':
        if request.headers.get('API_KEY'):
            for row in rows:
                if number == row.id:
                    valuesPUT.append(
                        {'id': row.id, 'title': request.form.get('title'), 'year': request.form.get('year'),
                         'genre': request.form.get('genre'),
                         'description': request.form.get('description')})
                    cursor.execute("UPDATE movies SET id, title , [year], genre, description WHERE id = number), VALUES (?,?,?,?)",
                                   row.id, request.form.get('title'), request.form.get('year'),
                                   request.form.get('genre'),
                                   request.form.get('description'))
                    return jsonify(valuesPUT), 200
        else:
            return jsonify({"ERROR": "No movie found with <movie_id> ID"}), 404
    elif request.method == 'DELETE':
        if request.headers.get('API_KEY'):
            for row in rows:
                if row.id == number:
                    cursor.execute("DELETE FROM movies WHERE id=number")
                    return jsonify({"SUCCESS:": "Movie deleted"})
                elif number not in row.id:
                    return jsonify({"ERROR": "No movie found with <movie_id> ID"}), 404
        else:
            return jsonify({"ERROR": "Invalid API_KEY"}), 403


cursor.commit()
cursor.close()
connect.close()
print("FINISHED")

if __name__ == '__main__':
    app.run()
