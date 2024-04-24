import pandas as pd
import json
import re

def clean_html(raw_html):
    """ Remove HTML tags and reduce whitespace. """
    clean_text = re.sub('<.*?>', '', raw_html)  # Remove HTML tags
    clean_text = re.sub(r'\s+', ' ', clean_text)  # Replace multiple whitespace with single space
    return clean_text.strip()

def preprocess_data(json_line):
    try:
        data = json.loads(json_line)
        df = pd.json_normalize(data)
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
        return None

    # Define expected keys and default values, focusing on necessary fields
    expected_keys = {
        'price': '0',
        'brand': 'Unknown',
        'description': 'No Description',
        'title': 'No Title',
        'asin': 'No ASIN',
        'category': [], 
        'also_buy': [],
        'also_view': []
    }

    # Handle missing values and initialization
    for key in expected_keys:
        if key not in df.columns:
            df[key] = pd.Series([expected_keys[key]])
        else:
            # Adjust this logic to handle potential iterable data
            if isinstance(df.at[0, key], list):
                # If it's a list and empty, fill it with default
                if not df.at[0, key]:
                    df.at[0, key] = expected_keys[key]
            else:
                # Check and fill NaN for non-list, scalar types
                if pd.isna(df.at[0, key]):
                    df.at[0, key] = expected_keys[key]

    # Apply HTML cleaning and convert text to lowercase
    text_fields = ['description', 'title']
    for field in text_fields:
        if field in df.columns:
            if isinstance(df.at[0, field], list):
                df.at[0, field] = [clean_html(item).lower() if isinstance(item, str) else item for item in df.at[0, field]]
            elif isinstance(df.at[0, field], str):
                df.at[0, field] = clean_html(df.at[0, field]).lower()

    # Construct the output dictionary
    result = {key: df.at[0, key] for key in df.columns if key in expected_keys}

    return result

def process_and_save_data(input_file_path, output_file_path):
    processed_data = []
    with open(input_file_path, 'r') as input_file:
        for line in input_file:
            if line.strip():  # Ensure the line is not empty
                processed_entry = preprocess_data(line)
                if processed_entry:
                    processed_data.append(processed_entry)
    
    with open(output_file_path, 'w') as f:
        json.dump(processed_data, f, indent=4)

input_file_path = 'Sampled_Amazon_Meta.json'
output_file_path = 'processed_data.json'
process_and_save_data(input_file_path, output_file_path)
