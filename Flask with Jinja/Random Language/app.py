import random

from flask import Flask, render_template

app = Flask(__name__)

greetings= ['Hello', 'Hi', 'Čauky', 'Hola', 'Bonjour', 'Guten tag', 'Asalaam alaikum', 'Salut', 'Goddag', 'Goedendag', 'Yassas', 'Dzień dobry', 'Namaste', 'Merhaba', 'Shalom']
names = ['kristyna', 'dominika', 'dan', 'martin', 'martina', 'lenka', 'jaromir', 'margita', 'serafin']

@app.route('/')
def greet():
    rgr = random.randint(0, 14)
    rna = random.randint(0, 8)
    chosen_greet = greetings[rgr]
    chosen_name = names[rna].capitalize()
    return render_template('index.html', greet = chosen_greet, name= chosen_name)


if __name__ == '__main__':
    app.run()
