import json
from kafka import KafkaConsumer
from collections import defaultdict
import pymongo

def apriori(data, min_support, collection):
    """
    Function to run the Apriori algorithm on a list of transactions to find frequent itemsets.
    Args:
    - data: list of transactions (list of lists)
    - min_support: int, the minimum support threshold for an itemset to be considered frequent
    - collection: MongoDB collection to store results

    Returns:
    - set of frequent items that meet or exceed the min_support threshold
    """
    item_count = defaultdict(int)

    # Count frequency of each item in the transactions
    for items in data:
        for item in items:
            item_count[item] += 1

    # Determine which items meet the minimum support threshold
    frequent_items = {item for item, count in item_count.items() if count >= min_support}

    # Store results in MongoDB
    result = {
        "min_support": min_support,
        "frequent_items": list(frequent_items)
    }
    collection.insert_one(result)

    print(f"Frequent items/categories with support threshold {min_support}: {frequent_items}")
    return frequent_items

# Setup MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["apriori_data"]
collection = db["frequent_item_sets"]

# Kafka consumer setup
bootstrap_servers = ['localhost:9092']
topic = 'apriori'
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading at the earliest message
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON from Kafka
)

# Sliding window configuration
window_size = 100  # Size of the sliding window for transactions
min_support = 3    # Minimum support threshold for items to be considered frequent

categories_data = []  # Store category transactions
related_data = []     # Store also_buy transactions

# Consume messages from Kafka
for message in consumer:
    item = message.value
    if 'also_view' in item:
        categories_data.append(item['also_view'])
        if len(categories_data) > window_size:
            categories_data.pop(0)  # Remove the oldest transaction if window size is exceeded

    if 'also_buy' in item:
        related_data.append(item['also_buy'])
        if len(related_data) > window_size:
            related_data.pop(0)  # Same for also_buy transactions

    # Run Apriori algorithm when the window is full
    if len(categories_data) == window_size:
        print("\n\nRunning Apriori on also_view data...")
        apriori(categories_data, min_support, collection)

    if len(related_data) == window_size:
        print("\n\nRunning Apriori on also_buy data...")
        apriori(related_data, min_support, collection)

