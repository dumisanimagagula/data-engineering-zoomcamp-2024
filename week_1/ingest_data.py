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
    response.raise_for_status()  # Check for network errors
    
    file_extension = os.path.splitext(url)[-1].lower()
    
    if file_extension == '.csv':
        # If the file is in CSV format
        output_file = 'output.csv'
    elif file_extension == '.gz':
        # If the file is gzip-compressed
        output_file = 'output.csv.gz'
    else:
        raise ValueError("Unsupported file format")
    
    with open(output_file, 'wb') as f_out:
        if file_extension == '.gz':
            with gzip.GzipFile(fileobj=BytesIO(response.content), mode='rb') as f_in:
                f_out.write(f_in.read())
        else:
            f_out.write(response.content)
    
    # Create database connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Read the CSV file in chunks and insert into PostgreSQL table
    for df_chunk in pd.read_csv(output_file, iterator=True, chunksize=100000):
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
