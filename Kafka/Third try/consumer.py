import json
from kafka import KafkaConsumer

if __name__ == '__main__':
    def deserializer(message):
        return json.loads(message.decode('utf-8'))


    consumer = KafkaConsumer(
        'third',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=deserializer
    )

    for msg in consumer:
        print(json.loads(str(msg.value)))
