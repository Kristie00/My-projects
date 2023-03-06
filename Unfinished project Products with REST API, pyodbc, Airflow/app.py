import flask
from flask import Flask, request, redirect, jsonify
import pyodbc

app = Flask(__name__)

try:
    # connect to database that will store data
    connect = pyodbc.connect(
        "DRIVER={SQL Server Native Client RDA 11.0};"
        "SERVER=LAPTOP-8RD2S3NP;"
        "DATABASE=products;"
        "Trusted_Connection=yes;"
    )
    cursor = connect.cursor()
    print('CONNECTED')

    # creating table for storage of data
    # cursor.execute("CREATE TABLE product_info ([id] int IDENTITY(1,1), [name] VARCHAR(100), [price] int, type VARCHAR(100), [featured] BIT, [visibility] BIT, [description] VARCHAR(500))")

    # selecting an API KEY
    response = flask.Response()
    response.headers['API KEY'] = "123456"

    # WORKING
    @app.route('/')
    def welcome():
        return "Welcome!"


    # Show all products
    # WORKING
    @app.route('/api/products', methods=['GET'])
    def get_all():
        products_listed = []
        cursor.execute("SELECT * FROM dbo.product_info")
        for row in cursor.fetchall():
            products_listed.append(
                {'id': row.id, 'name': row.name, 'type': row.type, 'featured': row.featured,
                 'visibility': row.visibility,
                 'description': row.description})
        cursor.commit()
        return jsonify(products_listed)


    # Show product with desired type
    # WORKING
    @app.route('/api/products/products', methods=['GET'])
    def get_by_type():
        product_type = request.args.get('type')
        product_listed = []
        cursor.execute("SELECT * FROM dbo.product_info WHERE type = ?", product_type)
        for row in cursor.fetchall():
            product_listed.append(
                {'id': row.id, 'name': row.name, 'type': row.type, 'featured': row.featured,
                 'visibility': row.visibility,
                 'description': row.description})
        cursor.commit()
        return jsonify(product_listed)


    # Show product with desired id
    # WORKING
    @app.route('/api/products/<int:product_id>', methods=['GET'])
    def get_by_id(product_id):
        product_listed = []
        cursor.execute("SELECT * FROM dbo.product_info WHERE id = ?", product_id)
        for row in cursor.fetchall():
            product_listed.append(
                {'id': row.id, 'name': row.name, 'type': row.type, 'featured': row.featured,
                 'visibility': row.visibility,
                 'description': row.description})
        cursor.commit()
        return jsonify(product_listed)


    # Save new product to database
    # WORKING
    @app.route('/api/products', methods=['POST'])
    def save_new():
        new_product = []
        if request.headers.get('API_KEY'):
            cursor.execute(
                "INSERT INTO dbo.product_info (name, price, type, featured, visibility, description) VALUES (?,?,?,?,?,?)",
                request.form.get('name'), request.form.get('price'), request.form.get('type'),
                request.form.get('featured'),
                request.form.get('visibility'), request.form.get('description'))
            cursor.commit()
            cursor.execute("SELECT * FROM dbo.product_info ORDER BY id DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY")
            for row in cursor.fetchall():
                new_product.append(
                    {'name': row.name, 'type': row.type, 'featured': row.featured, 'visibility': row.visibility,
                     'description': row.description})
            cursor.commit()
            return jsonify(new_product), 200
        else:
            return jsonify({"ERROR": "Invalid API-KEY!"}), 403

    # Update info of product
    # WORKING
    @app.route('/api/products/<int:product_id>', methods=['PUT'])
    def replace(product_id):
        new_product = []
        if request.headers.get('API_KEY'):
            cursor.execute("SELECT * FROM dbo.product_info WHERE id = ?", product_id)
            data = cursor.fetchall()
            if not data:
                cursor.commit()
                return jsonify({"ERROR": "Row with this ID is not in database."}), 404
            else:
                cursor.execute(
                    "UPDATE dbo.product_info SET name=?, price=?, type=?, featured=?, visibility=?, description=? WHERE id=?",
                    (request.form.get('name'), request.form.get('price'), request.form.get('type'),
                    request.form.get('featured'),
                    request.form.get('visibility'), request.form.get('description'), product_id))
                cursor.commit()
                cursor.execute("SELECT * FROM dbo.product_info WHERE id=?", product_id)
                for row in cursor.fetchall():
                    new_product.append(
                        {'name': row.name, 'type': row.type, 'featured': row.featured, 'visibility': row.visibility,
                         'description': row.description})
                cursor.commit()
                return jsonify(new_product), 200
        else:
            return jsonify({"ERROR": "Invalid API-KEY!"}), 403

    # Remove product
    # WORKING
    @app.route('/api/products/<int:product_id>', methods=['DELETE'])
    def remove(product_id):
        new_product = []
        if request.headers.get('API_KEY'):
            cursor.execute("SELECT * FROM dbo.product_info WHERE id = ?", product_id)
            data = cursor.fetchall()
            if not data:
                cursor.commit()
                return jsonify({"ERROR": "Row with this ID is not in database."}), 404
            else:
                cursor.execute(
                    "DELETE FROM dbo.product_info WHERE id =?", product_id)
                cursor.commit()
                return jsonify({"SUCCESS": "Product removed."}), 200
        else:
            return jsonify({"ERROR": "Invalid API-KEY!"}), 403

    print("FINISHED")

    if __name__ == '__main__':
        app.run(debug=True)

except pyodbc.ProgrammingError as e:
    print(e)
    print('Something wrong with SQL statement')
except pyodbc.OperationalError as e:
    print(e)
    print('SQL connection error')
