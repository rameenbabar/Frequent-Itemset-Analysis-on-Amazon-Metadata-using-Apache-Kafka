import json
import pyfpgrowth
from kafka import KafkaConsumer

def process_transactions_with_fpgrowth(transactions, min_support):
    """
    Find frequent patterns and generate association rules using the FP-Growth algorithm.
    Args:
    - transactions: list of lists, where each sublist is a set of items (transaction)
    - min_support: int, the minimum support threshold for an itemset to be considered frequent

    Returns:
    - None, but prints frequent patterns and association rules
    """
    patterns = pyfpgrowth.find_frequent_patterns(transactions, min_support)
    rules = pyfpgrowth.generate_association_rules(patterns, 0.7)  # Confidence threshold of 70%
    print("Frequent Patterns:", patterns)
    print("Association Rules:", rules)

# Configuration for Kafka Consumer
bootstrap_servers = ['localhost:9092']
topic = 'FPGrowth'
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading at the earliest message
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON from Kafka
)

window_size = 100  # Define the size of the sliding window
min_support = 2    # Minimum support threshold

transactions = []  # Store transactions

# Consume messages from Kafka
for message in consumer:
    item = message.value
    transaction = item.get('category')  
    if transaction:
        transactions.append(transaction)
        if len(transactions) > window_size:
            transactions.pop(0)  # Remove the oldest transaction to maintain the window size

        # Process transactions when we have the exact number of window size
        if len(transactions) == window_size:
            print("\n\nRunning FP-Growth on transactions...")
            process_transactions_with_fpgrowth(transactions, min_support)

