# Frequent Itemset Analysis on Amazon Metadata using Apache Kafka

## Overview

This project implements a real-time frequent itemset mining system using Apache Kafka and MongoDB on Amazon product metadata. It simulates a streaming data environment where transactions are processed on the fly using Apriori, PCY, and FP-Growth algorithms.

The project demonstrates fundamental Big Data concepts including stream ingestion, sliding window processing, frequent pattern mining, and NoSQL integration — all while adapting traditional batch algorithms to real-time streaming scenarios using Kafka consumers.

## Dataset

We use the **Amazon Product Metadata** dataset — a large-scale collection of product information and relationships scraped from Amazon.

- Records: Millions of products
- Fields: Title, Category, Also_Buy, Also_View, etc.
- Size (after extraction): ~105GB
- Sampled: 15GB+ using a custom sampling script

**Download the dataset**: [Amazon Metadata Dataset](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/)

## Objective

Build a streaming frequent itemset mining pipeline that:

- Cleans and preprocesses raw Amazon metadata
- Streams JSON data in real-time to multiple Kafka topics
- Applies Apriori, PCY, and FP-Growth algorithms on streaming transactions
- Stores mined itemsets and associations into MongoDB
- Provides live console-based insights

## Technologies Used

- **Python** – Core logic and data processing
- **Apache Kafka** – Real-time data streaming
- **MongoDB** – Storage of frequent itemsets and pairs
- **Kafka-Python** – Python Kafka producer/consumer API
- **pyfpgrowth** – Efficient FP-Growth pattern mining
- **pandas / json** – Data parsing and preprocessing


## Workflow Overview

### 1. Data Preprocessing

Performed via:
- `src/preprocessing.py` – Cleans and normalizes JSON entries
- `notebooks/dataset_sampling_code.ipynb` – Samples large raw files to a manageable 15GB+

Output: `preprocessed_json_file.json` (clean, ready-to-stream dataset)


### 2. Streaming Setup

- `src/producer.py`: Simulates a real-time data stream from `preprocessed_json_file.json`
- Sends data to 3 Kafka topics: `apriori`, `PCY`, and `FPGrowth`


### 3. Consumers and Algorithms

#### `src/consumerAPRIORI.py`
- Uses a **sliding window** of 100 transactions
- Applies **Apriori** on `also_view` and `also_buy` fields
- Stores frequent items in MongoDB (`apriori_data.frequent_item_sets`)

#### `src/consumerPCY.py`
- Consumes from `category` field
- Applies **PCY** using hash-based bucket filtering
- Stores frequent items and candidate pairs in MongoDB (`pcy_data.frequent_item_sets`)

#### `src/consumerFPGROWTH.py`
- Applies **FP-Growth** using `pyfpgrowth` on `category` transactions
- Extracts both **frequent patterns** and **association rules**
- Confidence threshold: 0.7


## How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/Frequent-Itemset-Analysis-on-Amazon-Metadata-using-Apache-Kafka.git
cd Frequent-Itemset-Analysis-on-Amazon-Metadata-using-Apache-Kafka
```

### 2. Install Dependencies
```bash

pip install -r requirements.txt
```

### 3. Preprocess and Sample Data
```bash
notebooks/dataset_sampling_code.ipynb
python src/preprocessing.py
```

### 4. Start Kafka & MongoDB
Make sure your Kafka, Zookeeper, and MongoDB instances are running.

### 5. Launch the Streaming Pipeline
```bash
# Terminal 1 - Producer
python src/producer.py

# Terminal 2 - Apriori Consumer
python src/consumerAPRIORI.py

# Terminal 3 - PCY Consumer
python src/consumerPCY.py

# Terminal 4 - FP-Growth Consumer
python src/consumerFPGROWTH.py
```

## Output
**MongoDB:** Each consumer stores frequent itemsets in its own collection

**Console:** Real-time printed insights from each algorithm

**JSON:** Cleaned version of Amazon metadata

## Deployment Notes
For large-scale testing, run Kafka on Docker or a cloud VM.

MongoDB can be scaled using Atlas or sharded clusters.


