import pandas as pd
from sqlalchemy import create_engine
import logging

def view_data():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        # Connection details
        airflow_db_connection_string = 'postgresql+psycopg2://airflow:BlackWater#!@localhost/airflow'
        airflow_table_name = 'steam_games'
        airflow_engine = create_engine(airflow_db_connection_string)
        logging.info("Connected to Airflow PostgreSQL database")

        # Data retrieval
        df = pd.read_sql_table(airflow_table_name, airflow_engine)
        logging.info(f"Retrieved {len(df)} records from the table {airflow_table_name}")

        # Logging data details
        logging.info("First few rows of the data:\n%s", df.head(10).to_string())
        logging.info("Data types of each column:\n%s", df.dtypes)
        logging.info("Summary statistics of the data:\n%s", df.describe().to_string())
        logging.info("Total number of rows: %d, columns: %d", df.shape[0], df.shape[1])
        logging.info("Missing values in each column:\n%s", df.isnull().sum())
    except Exception as e:
        logging.error("An error occurred while retrieving data from PostgreSQL: %s", e)

if __name__ == "__main__":
    view_data()

