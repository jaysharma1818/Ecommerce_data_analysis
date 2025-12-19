"""
Data Ingestion Script - MySQL Version
Loads CSV files into MySQL inventory database
"""

import pandas as pd
import os 
from sqlalchemy import create_engine
import logging
import time

# ==================== LOGGING SETUP ====================

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ==================== DATABASE CONNECTION ====================

# MySQL connection (instead of SQLite)
engine = create_engine('mysql+pymysql://root:mahima0366@localhost:3306/inventory')

# ==================== FUNCTIONS ====================

def ingest_db(df, table_name, engine):
    """This function will ingest the dataframe into database table"""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    """This function will load the CSVs as dataframe and ingest into db"""
    start = time.time()

    # Read from current directory (or change to 'data' if you have that folder)
    for file in os.listdir('.'):
        if '.csv' in file:
            df = pd.read_csv(file)  # No 'data/' prefix since files are in current folder
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)

    end = time.time()
    total_time = (end - start) / 60
    logging.info('--------------Ingestion Complete--------------')
    logging.info(f'\nTotal Time Taken: {total_time} minutes')

# ==================== MAIN EXECUTION ====================

if __name__ == '__main__':
    load_raw_data()
