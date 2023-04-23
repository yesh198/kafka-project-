from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient

consumer = KafkaConsumer('mongodb',bootstrap_servers = ['localhost:9092'],
                         auto_offset_reset = 'earliest',enable_auto_commit = True,
                         value_deserializer = lambda x:loads(x.decode('utf-8')))


client = MongoClient(host  = 'localhost')
collection = client.kafka.kafkat

for message in consumer:
    message = message.value
    print(message)
    collection.insert_one(message)
