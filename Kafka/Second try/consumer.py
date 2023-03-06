from datetime import datetime

from kafka import KafkaConsumer
import json


# def deserializer(message):
    # return json.loads(message.decode('utf-8'))


#topics = ['first', 'second']


if __name__ == '__main__':

    consumer = KafkaConsumer(
        'first', 'second',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        # value_deserializer=deserializer,
        group_id=None
    )

# consumer.subscribe(topics=topics)
# consumer.subscription()
# records = consumer.poll()

    print('Polling...')
    # while True:
    #    for a in records:
    #        print(a)

    for msg in consumer:
        print(f'This is the message: {msg.value}, it is {datetime.now()}')
