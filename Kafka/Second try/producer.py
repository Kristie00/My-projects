import json
import time
from datetime import datetime
import random

from kafka.producer import KafkaProducer
from datagen import generate_message


def serializer(message):
    return json.dumps(message).encode('utf-8')


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

for i in range(1, 6):
    send_message = generate_message()
    print(f'It is {datetime.now()} and this is your {i}. {str(send_message)} from topic: first')
    producer.send('first', send_message)
    time_to_sleep = random.randint(1, 11)
    time.sleep(time_to_sleep)

for j in range(1, 6):
    send_message = generate_message()
    print(f'It is {datetime.now()} and this is your {j}. {str(send_message)} from topic: second')
    producer.send('second', send_message)
    time_to_sleep = random.randint(1, 3)
    time.sleep(time_to_sleep)

producer.flush()
