import json
from kafka import KafkaConsumer
from collections import Counter
import pymongo

def pcy_algorithm(data, min_support, num_buckets):
    buckets = [0] * num_buckets
    item_count = Counter()

    for transaction in data:
        for item in transaction:
            item_count[item] += 1
        for i in range(len(transaction)):
            for j in range(i + 1, len(transaction)):
                bucket_index = hash((transaction[i], transaction[j])) % num_buckets
                buckets[bucket_index] += 1

    frequent_items = {item for item, count in item_count.items() if count >= min_support}
    frequent_buckets = {index for index, count in enumerate(buckets) if count >= min_support}
    candidate_pairs = set()

    for transaction in data:
        for i in range(len(transaction)):
            for j in range(i + 1, len(transaction)):
                if transaction[i] in frequent_items and transaction[j] in frequent_items:
                    if hash((transaction[i], transaction[j])) % num_buckets in frequent_buckets:
                        candidate_pairs.add((transaction[i], transaction[j]))

    return frequent_items, candidate_pairs

# Setup MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["pcy_data"]
collection = db["frequent_item_sets"]

# Kafka Consumer configuration
bootstrap_servers = ['localhost:9092']
topic = 'PCY'
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

min_support = 3
num_buckets = 100
window_size = 100
transactions = []

for message in consumer:
    item = message.value
    transaction = item.get('category')
    if transaction:
        transactions.append(transaction)
        if len(transactions) > window_size:
            transactions.pop(0)
        
        if len(transactions) == window_size:
            print("\n\nProcessing transactions with PCY...")
            frequent_items, candidate_pairs = pcy_algorithm(transactions, min_support, num_buckets)
            print("\n\nFrequent Items:", frequent_items)
            print("\n\nCandidate Pairs:", candidate_pairs)

            # Store results in MongoDB
            data_to_insert = {
                "frequent_items": list(frequent_items),
                "candidate_pairs": list(candidate_pairs)
            }
            collection.insert_one(data_to_insert)




