#!/usr/bin/env python3

import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, filename='/home/sean/airflow/logs/etl_pipeline.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')

def load_data():
    logging.info("Data loading started")

    try:
        # Load the transformed data from CSV
        df = pd.read_csv("/home/sean/airflow/scripts/transformed_game_details.csv")
        logging.info(f"Loaded dataframe with {df.shape[0]} rows and {df.shape[1]} columns.")
        df = df.replace({np.nan: None})
        df = df[df['steam_appid'].notna()]

        # Convert boolean columns to integers
        df['is_free'] = df['is_free'].map({'True': 1, 'False': 0})

        logging.info(f"Data preview:\n{df.head()}")
    except Exception as e:
        logging.error(f"Error loading CSV data: {e}")
        return

    try:
        # Establish connection to MySQL
        conn = mysql.connector.connect(
            host="localhost",
            user="yaseen02",
            password="BlackWater#!",
            database="games",
            port="3306"
        )
        cursor = conn.cursor()
        logging.info("Connected to MySQL")
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return

    try:
        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS steam_games (
                steam_appid INT PRIMARY KEY,
                name VARCHAR(255),
                required_age FLOAT,
                is_free BOOLEAN,
                controller_support VARCHAR(255),
                about_the_game TEXT,
                website VARCHAR(255),
                supported_languages TEXT,
                genres TEXT,
                reviews TEXT
            )
        """)
        logging.info("Table checked/created successfully")

        # Insert data into table
        for index, row in df.iterrows():
            try:
                if row['steam_appid'] is not None:
                    cursor.execute("""
                        INSERT INTO steam_games (
                            steam_appid, name, required_age, is_free, controller_support, 
                            about_the_game, website, supported_languages, genres, reviews
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            name=VALUES(name),
                            required_age=VALUES(required_age),
                            is_free=VALUES(is_free),
                            controller_support=VALUES(controller_support),
                            about_the_game=VALUES(about_the_game),
                            website=VALUES(website),
                            supported_languages=VALUES(supported_languages),
                            genres=VALUES(genres),
                            reviews=VALUES(reviews)
                    """, (
                        row['steam_appid'], row['name'], row['required_age'], row['is_free'], row['controller_support'],
                        row['about_the_game'], row['website'], row['supported_languages'], row['genres'], row['reviews']
                    ))
            except Error as e:
                logging.error(f"Error inserting data for appid {row['steam_appid']}: {e}")
                continue

        conn.commit()
        logging.info("Data loading complete")
    except Error as e:
        logging.error(f"Error during database operation: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logging.info("MySQL connection closed")

def main():
    load_data()

if __name__ == "__main__":
    main()


