import random


def generate_message():
    message = ["Hi", "Hello", "I hope this will work", "I do not know what I am doing lol", "How are you?"]
    random_message = random.choice(message)
    return {
        'message' : random_message
    }

