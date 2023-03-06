from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def hello_world():
    return render_template('index.html', products=[("Milk", 3.59123), ("Bread", 2.96332), ("Rice", 0.64111)])


if __name__ == '__main__':
    app.run()
