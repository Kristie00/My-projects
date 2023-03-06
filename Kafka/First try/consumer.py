import json
from kafka import KafkaConsumer

if __name__ == '__main__':

   # def deserializer(message):
        #return json.loads(message.decode('utf-8'))

    
    consumer = KafkaConsumer(
        'first_kafka_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )
    for message in consumer:
        print(json.loads(message.value))

    