import os
import argparse
import pyarrow.parquet as pq
import pandas as pd
import requests
from sqlalchemy import create_engine
from io import BytesIO

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Create the PostgreSQL engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Stream the Parquet file from the URL
    print(f"Streaming Parquet file from: {url}")
    
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        print(f"Failed to fetch data from URL. Status code: {response.status_code}")
        return

    # Use BytesIO to read the file in-memory
    file_bytes = BytesIO(response.content)

    # Read the Parquet file using pyarrow directly from memory
    df_iter = pq.ParquetFile(file_bytes)

    # Process and insert data in chunks
    for batch in df_iter.iter_batches(batch_size=100000):
        df = batch.to_pandas()

        # Convert datetime columns
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        # Insert data into PostgreSQL
        df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=100000)

    print("Finished ingesting data into the PostgreSQL database")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data directly into Postgres from a URL')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='URL of the Parquet file')

    args = parser.parse_args()

    main(args)
