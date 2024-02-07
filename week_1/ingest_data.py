import argparse
import gzip
from io import BytesIO
import os
import pandas as pd
import requests
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # Download the file from the URL
    response = requests.get(url)
    file_extension = os.path.splitext(url)[-1].lower()
    
    if file_extension == '.csv':
        # If the file is in CSV format
        with open('output.csv', 'wb') as f_out:
            f_out.write(response.content)
    elif file_extension == '.gz':
        # If the file is gzip-compressed
        with gzip.GzipFile(fileobj=BytesIO(response.content), mode='rb') as f_in:
            with open('output.csv', 'wb') as f_out:
                f_out.write(f_in.read())
    else:
        raise ValueError("Unsupported file format")
    
    # Create database connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the CSV file in chunks
    df_iter = pd.read_csv('output.csv', iterator=True, chunksize=100000)

    # Iterate over chunks and insert into PostgreSQL table
    for df_chunk in df_iter:
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    
    print("Data ingestion complete")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the gzip-compressed or CSV file')

    args = parser.parse_args()
    
    main(args)
