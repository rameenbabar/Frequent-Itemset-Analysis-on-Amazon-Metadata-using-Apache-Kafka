import json
from kafka import KafkaProducer
from time import sleep

input_file_path = 'processed1_data.json'
bootstrap_servers = ['localhost:9092']
topics = ['apriori', 'PCY', 'FPGrowth']  # Added 'fp_growth' to the list of topics

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

with open(input_file_path, 'r') as file:
    data = json.load(file)
    for item in data:
        for topic in topics:
            producer.send(topic, value=item)  # Send data to all topics
            print(f'Data sent to Kafka topic {topic}:', item)
        sleep(1)  # Simulate real-time data feed

