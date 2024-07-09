#!/usr/bin/env python3

import json
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, filename='/home/sean/airflow/logs/etl_pipeline.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')

def main():
    logging.info("Data transformation started")

    # Define the path to the JSON file
    input_file_path = '/home/sean/airflow/scripts/processed_game_details.json'
    output_file_path = '/home/sean/airflow/scripts/transformed_game_details.csv'

    # Load the processed data
    try:
        with open(input_file_path, 'r') as file:
            data = json.load(file)
        logging.info("Loaded processed data successfully")
    except Exception as e:
        logging.error(f"Error loading JSON data: {e}")
        return

    # Convert to DataFrame for easier manipulation
    try:
        df = pd.DataFrame(data)
    except Exception as e:
        logging.error(f"Error converting JSON data to DataFrame: {e}")
        return

    # Convert NaN values to None to ensure compatibility with SQL NULL
    df = df.where(pd.notnull(df), None)

    # Fill missing values with appropriate defaults or using forward fill for categorical data
    df.fillna({
        'currency': 'USD', 'initial': 0, 'final': 0, 'discount_percent': 0,
        'controller_support': 'none',
        'supported_languages': 'English',
        'reviews': 'No reviews'
    }, inplace=True)

    df.ffill(inplace=True)

    # Drop duplicates based on 'steam_appid' as it should be unique
    df.drop_duplicates(subset=['steam_appid'], inplace=True)

    # Normalize price details if they exist
    if 'initial' in df.columns and 'final' in df.columns:
        try:
            norm_scaler = MinMaxScaler()
            price_cols = ['initial', 'final']
            df[price_cols] = norm_scaler.fit_transform(df[price_cols])
        except Exception as e:
            logging.error(f"Error normalizing price details: {e}")
            return

    # Save the transformed data to a CSV file
    try:
        df.to_csv(output_file_path, index=False)
        logging.info(f"Data transformation complete. Data saved to {output_file_path}")
    except Exception as e:
        logging.error(f"Error saving transformed data: {e}")

if __name__ == "__main__":
    main()



