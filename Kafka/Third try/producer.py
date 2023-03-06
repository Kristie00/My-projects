import string
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer


def serializer(message):
    return json.dumps(message).encode('utf-8')


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

if __name__ == '__main__':

    while True:
        for i in range(1, 11):
            random_code = ''.join(random.choice(string.ascii_letters) for i in range(8))
            if i % 2 == 1:
                sending = {'int': i, 'code': random_code}
                producer.send('third', value=sending)
                time.sleep(3)
                print(f'Producing: {sending}')
                

