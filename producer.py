import json
from kafka import KafkaProducer
from time import sleep

input_file_path = 'processed1_data.json'
bootstrap_servers = ['localhost:9092']
topic = 'apriori'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

with open(input_file_path, 'r') as file:
    data = json.load(file)
    for item in data:
        sleep(1)  # Simulate real-time data feed
        producer.send(topic, value=item)
        print('Data sent to Kafka:', item)

