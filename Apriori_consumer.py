import json
from kafka import KafkaConsumer
from collections import defaultdict

# Define a function to run the Apriori algorithm on categories and related products
def apriori(data, min_support):
    # Item frequency count
    item_count = defaultdict(int)

    # Count frequency of each item
    for items in data:
        for item in items:
            item_count[item] += 1

    # Filter items by minimum support
    frequent_items = {item for item, count in item_count.items() if count >= min_support}

    # Print frequent items
    print("Frequent items/categories with support threshold {}: {}".format(min_support, frequent_items))
    return frequent_items

# Kafka consumer configuration
bootstrap_servers = ['localhost:9092']
topic = 'apriori'

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Min support threshold: Adjust based on your data size and frequency expectation
min_support = 3

# Collect data for Apriori
categories_data = []
related_data = []

# Listen for messages on the Kafka topic
for message in consumer:
    item = message.value
    if 'category' in item:
        categories_data.append(item['category'])
    if 'also_buy' in item:
        related_data.append(item['also_buy'])

    # Run Apriori periodically or conditionally based on some criteria, e.g., after collecting 100 records
    if len(categories_data) >= 100:
        print("Running Apriori on category data...")
        apriori(categories_data, min_support)
        categories_data = []  # Reset after processing

    if len(related_data) >= 100:
        print("Running Apriori on also_buy data...")
        apriori(related_data, min_support)
        related_data = []  # Reset after processing

