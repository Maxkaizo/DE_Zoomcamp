import os
import pandas as pd
import argparse
from time import time
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    # download the csv


    # Create connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # create iterator to open large file
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    # instantiate iterator one step at a time
    df = next(df_iter)

    # adjust format to datetime
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # create headers in database
    df.head(0).to_sql(name=table_name,con=engine, if_exists='replace')

    # ingest data ti db
    df.to_sql(name=table_name,con=engine, if_exists='append')

while True:
    try:
        t_start = time()
        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime  = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name='yellow_taxi_data',con=engine, if_exists='append')

        t_end = time()
        
        print('inserted another chunk..., took %.3f second(s)' %(t_end - t_start))
        
    except StopIteration:
        print("No more data to process. Exiting the loop.")
        break

if __name__=="__main__":

    parser = argparse.ArgumentParser(description='Ingest CSV Data to Postgres')

    # user, password, host, port, database name, table name

    parser.add_argument('user', help='user name for postgres')
    parser.add_argument('password', help='password for postgres')
    parser.add_argument('host', help='host for postgres')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('db', help='database name for postgres')
    parser.add_argument('table_name', help='name of the table for postgres')
    parser.add_argument('url', help='url of the csv file')

    args = parser.parse_args()
    print(args.accumulatr(args.integers))




